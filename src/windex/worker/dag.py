"""Run lifecycle: which tasks become claimable, and when a run is over.

This is deliberately the *dumbest possible* DAG engine. It knows nothing about
recipes, modules, ports or wire types — only that ``run_tasks.depends_on`` names
other nodes in the same run, and that a node becomes claimable when all of them
have finished cleanly. Everything it does is a pure function of rows the recipe
engine (Phase 6) writes, so the two halves stay independently mergeable: Phase 6
compiles a recipe into ``runs`` + ``run_tasks`` and this file makes them run.

Why the pool owns this at all: without it, nothing ever moves a task from
'pending' to 'ready' and the claim query correctly finds nothing to do forever.
The alternative — every producer of runs also implementing fan-out and rollup —
is the shape that gave windex's scheduler a crash window between ``dispatch_entry``
and ``_mark_ran``. One transaction, one owner.

Advancement is **idempotent and level-triggered**: it recomputes what should be
ready from current state rather than reacting to a transition. A missed call
costs latency (the next supervisor tick fixes it), never correctness — which is
the property that lets a slot call it inline for immediacy while the supervisor
sweeps as a backstop.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.worker.claim import log_event

log = logging.getLogger("windex.worker.dag")

LIVE_RUN_STATES = ("queued", "running", "blocked")


def submit_run(conn: psycopg.Connection, *, recipe: str, source: str,
               spec: Mapping[str, Any], tasks: Sequence[Mapping[str, Any]],
               trigger: str = "manual", trigger_by: str = "",
               params: Mapping[str, Any] | None = None, priority: int = 50,
               mode: str = "run", dedupe_key: str | None = None,
               recipe_version: int = 1, spec_hash: str = "") -> int | None:
    """Insert a run and fan its tasks out, in ONE transaction. Returns the run
    id, or None when a live run already holds the dedupe key.

    One transaction is the point (plan §C.5): today's scheduler inserts and then
    marks, and a crash between the two re-fires the job. And the None return is
    ``runs_dedupe_live_uniq`` doing its job — a human clicking Run while the
    timer fires, or a week of paused nightly runs releasing at once, becomes a
    harmless no-op instead of duplicate ingest.

    Each task mapping takes ``node`` and ``module`` plus any run_tasks column
    (``kind``, ``lane``, ``config``, ``depends_on``, ``preconditions``,
    ``priority``, ``max_attempts``, ``lease_seconds``, ``weight``).
    """
    key = dedupe_key or recipe
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO runs (recipe, recipe_version, source, spec, spec_hash, trigger,
                              trigger_by, params, mode, priority, dedupe_key, state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'queued')
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (recipe, recipe_version, source, Jsonb(dict(spec)), spec_hash, trigger,
             trigger_by, Jsonb(dict(params or {})), mode, priority, key),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return None
        run_id = row[0]
        for t in tasks:
            cur.execute(
                """
                INSERT INTO run_tasks (run_id, source, node, kind, module, lane, config,
                                       depends_on, preconditions, priority, max_attempts,
                                       lease_seconds, weight, state)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                """,
                (run_id, t.get("source", source), t["node"], t.get("kind", "transform"),
                 t["module"], t.get("lane", "io"), Jsonb(dict(t.get("config", {}))),
                 list(t.get("depends_on", [])), list(t.get("preconditions", [])),
                 int(t.get("priority", priority)), int(t.get("max_attempts", 3)),
                 int(t.get("lease_seconds", 300)), float(t.get("weight", 1.0))),
            )
        log_event(cur, run_id, None, "run.queued", f"{recipe} ({len(tasks)} tasks)",
                  data={"trigger": trigger, "source": source, "priority": priority})
    conn.commit()
    advance(conn, run_id)
    return run_id


def advance(conn: psycopg.Connection, run_id: int) -> dict:
    """Recompute claimability and terminal state for one run.

    Three level-triggered rules, in order:
      1. a pending task whose dependencies are all succeeded/skipped → ready;
      2. a pending task with a failed/cancelled dependency → skipped, because it
         will never be runnable and leaving it 'pending' makes a dead run look
         busy forever;
      3. a run with nothing pending/ready/running left → terminal.
    """
    with conn.cursor() as cur:
        # 1. Dependencies satisfied. The correlated unnest is the whole rule:
        # "no dependency of mine is missing or unfinished".
        cur.execute(
            """
            UPDATE run_tasks t SET state = 'ready'
             WHERE t.run_id = %s AND t.state = 'pending'
               AND NOT EXISTS (
                     SELECT 1 FROM unnest(t.depends_on) AS d(node)
                     LEFT JOIN run_tasks p
                            ON p.run_id = t.run_id AND p.node = d.node
                      WHERE p.state IS NULL OR p.state NOT IN ('succeeded', 'skipped'))
            RETURNING t.id, t.node
            """,
            (run_id,),
        )
        readied = cur.fetchall()

        # 2. Dependencies that can never be satisfied.
        cur.execute(
            """
            UPDATE run_tasks t SET state = 'skipped', finished_at = now(),
                   error = coalesce(t.error, 'dependency did not succeed')
             WHERE t.run_id = %s AND t.state IN ('pending', 'ready')
               AND EXISTS (
                     SELECT 1 FROM unnest(t.depends_on) AS d(node)
                     JOIN run_tasks p ON p.run_id = t.run_id AND p.node = d.node
                      WHERE p.state IN ('failed', 'cancelled'))
            RETURNING t.id, t.node
            """,
            (run_id,),
        )
        skipped = cur.fetchall()
        for tid, node in skipped:
            log_event(cur, run_id, tid, "task.skipped", f"{node}: dependency failed",
                      level="warn")
    conn.commit()
    state = finalize(conn, run_id)
    return {"readied": [n for _, n in readied], "skipped": [n for _, n in skipped],
            "run_state": state}


def finalize(conn: psycopg.Connection, run_id: int) -> str:
    """Move a run to its terminal state once no task can still make progress.

    A run fails if any task failed — no partial-success state — because "the run
    went green with a dead node" is the failure mode that let ``&&``-chained
    refresh jobs swallow a non-zero exit for months.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE state IN ('pending', 'ready', 'running')),
                   count(*) FILTER (WHERE state = 'failed'),
                   count(*) FILTER (WHERE state = 'cancelled'),
                   count(*)
              FROM run_tasks WHERE run_id = %s
            """,
            (run_id,),
        )
        live, failed, cancelled, total = cur.fetchone()
        cur.execute("SELECT state, cancel_requested FROM runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return ""
        run_state, cancel_requested = row
        if run_state not in LIVE_RUN_STATES:
            conn.rollback()
            return run_state
        if live or total == 0:
            # An empty run has nothing to do; leave it queued rather than
            # declare success, so a compiler bug that emits zero tasks is
            # visible instead of silently "succeeding" every night.
            conn.rollback()
            return run_state
        if failed:
            new = "failed"
        elif cancel_requested or cancelled:
            new = "cancelled"
        else:
            new = "succeeded"
        cur.execute(
            "UPDATE runs SET state = %s, finished_at = now(), updated_at = now(), "
            "error = CASE WHEN %s = 'failed' THEN coalesce(("
            "    SELECT error FROM run_tasks WHERE run_id = %s AND state = 'failed' "
            "     AND error IS NOT NULL ORDER BY finished_at LIMIT 1), 'a task failed') "
            "  ELSE error END "
            "WHERE id = %s",
            (new, new, run_id, run_id),
        )
        log_event(cur, run_id, None, f"run.{new}", "",
                  level="error" if new == "failed" else "info",
                  data={"tasks": total, "failed": failed, "cancelled": cancelled})
    conn.commit()
    return new


def advance_live(conn: psycopg.Connection) -> int:
    """Advance every live run. The supervisor's backstop for anything that
    created a run without calling advance (an API insert, a restored backup, a
    crash between insert and fan-out)."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM runs WHERE state IN ('queued', 'running') "
                    "ORDER BY id LIMIT 500")
        ids = [r[0] for r in cur.fetchall()]
    conn.commit()
    for run_id in ids:
        advance(conn, run_id)
    return len(ids)


def apply_cancellations(conn: psycopg.Connection) -> int:
    """Honour ``runs.cancel_requested``.

    Queued and ready tasks are cancelled outright; running ones are asked to
    yield and discover the cancellation at their next slice boundary, which is
    the difference between "stops cleanly with committed work" and "is killed".
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE run_tasks t SET state = 'cancelled', finished_at = now()
              FROM runs r
             WHERE r.id = t.run_id AND r.cancel_requested
               AND r.state IN ('queued', 'running')
               AND t.state IN ('pending', 'ready')
            RETURNING t.id, t.run_id
            """
        )
        rows = cur.fetchall()
        for tid, run_id in rows:
            log_event(cur, run_id, tid, "task.cancelled", "run cancelled")
        cur.execute(
            "UPDATE run_tasks t SET yield_requested = true FROM runs r "
            "WHERE r.id = t.run_id AND r.cancel_requested AND t.state = 'running' "
            "AND NOT t.yield_requested"
        )
        cur.execute("SELECT id FROM runs WHERE cancel_requested "
                    "AND state IN ('queued', 'running')")
        run_ids = [r[0] for r in cur.fetchall()]
    conn.commit()
    for run_id in run_ids:
        finalize(conn, run_id)
    return len(rows)


def request_cancel(conn: psycopg.Connection, run_id: int, by: str = "") -> bool:
    """Ask a run to stop. The API's entry point; the pool does the rest."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE runs SET cancel_requested = true, updated_at = now() "
            "WHERE id = %s AND state IN ('queued', 'running', 'blocked') RETURNING id",
            (run_id,),
        )
        ok = cur.fetchone() is not None
        if ok:
            log_event(cur, run_id, None, "run.cancel_requested", by, level="warn")
    conn.commit()
    if ok:
        apply_cancellations(conn)
    return ok
