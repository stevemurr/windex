"""Claiming, leasing and releasing tasks — the pool's whole concurrency story.

This is a direct generalization of the crawl driver's run lifecycle
(``crawl/run.py:342-403``), with three changes that the generalization forces:

1. **A lease, not just a heartbeat.** ``crawl_runs`` reclaims on
   ``heartbeat_at < now() - crawl_stale_minutes``, one global staleness constant
   for every kind of work. That cannot serve both a polite 1-req/3s fetch (which
   legitimately goes quiet for minutes) and an embed slice (which should be
   reclaimed within seconds of dying). ``lease_expires_at`` is written by the
   holder from its own ``lease_seconds``, so reclaim is a bare timestamp
   comparison and each recipe declares its own tolerance.

2. **Tasks are claimed; units are not.** The reasoning at ``crawl/run.py:95``
   holds verbatim one level up: a task is leased by exactly one worker, so rows
   *within* a leased task have no second claimant to lose a race against and
   need no second lock. Everything in this module is about the task row.

3. **A claim is a scheduling decision, not just a dequeue.** ``claim_run`` is
   FIFO, and FIFO is why a 20,000-page crawl blocks every other source for
   eleven hours. The predicate here also enforces pauses, preconditions, an
   anti-starvation cap of one running task per ``(source, lane)``, a fleet-wide
   lane cap, and weighted-fair ordering over ``source_sched.vtime``.

WHY THE ADVISORY LOCK. ``FOR UPDATE SKIP LOCKED`` makes two workers unable to
take the *same row*. It does nothing about two workers taking *different rows*
that violate the same aggregate constraint: under READ COMMITTED neither
transaction sees the other's uncommitted ``state='running'``, so both read
``count(*) = 0`` for lane cpu_heavy and both claim — and cpu_heavy's cap of 1,
which is a memory guard rather than a nicety, silently becomes 2. A
transaction-scoped advisory lock keyed on the *lane* serializes exactly the
claims that could violate it, and because two tasks with the same
``(source, lane)`` necessarily share a lane, the one lock covers the
anti-starvation check too. It is taken inside a CASE so that acquisition
provably precedes the counting quals — Postgres is free to reorder a WHERE
clause, and "we counted before we locked" would reintroduce the race in a form
that only shows up under production concurrency.

ATTEMPTS ARE NOT SLICES. ``attempts`` is incremented on *failure and reclaim
only*, never on claim. A long crawl yields hundreds of times and every yield is
a re-claim; counting those would burn ``max_attempts`` in the first six minutes
and mark a perfectly healthy task failed. This is the single most tempting wrong
line in the file.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.worker.protocol import LeaseLost

log = logging.getLogger("windex.worker.claim")

# Terminal task states, and the states a dependent may treat as "satisfied".
TERMINAL = ("succeeded", "failed", "skipped", "cancelled")
SATISFIED = ("succeeded", "skipped")

# Namespace for the transaction-scoped lane lock. A string prefix rather than a
# bare hash of the lane so the advisory-lock keyspace (which is global to the
# database) cannot collide with anything else that ever wants one.
_LANE_LOCK = "windex.worker.lane:"


@dataclass(frozen=True)
class ClaimedTask:
    """A leased task, plus the frozen run context a runner needs."""

    id: int
    run_id: int
    recipe: str
    source: str
    node: str
    kind: str
    module: str
    lane: str
    config: dict
    cursor: dict
    spec: dict
    params: dict
    mode: str
    priority: int
    attempts: int
    max_attempts: int
    lease_seconds: int
    # Counters as of the claim. The slot reports absolute values (base + slice)
    # so a mid-slice heartbeat and the slice-end write are the same number and
    # replaying either one cannot double-count.
    units_done: int
    units_failed: int
    worker: str = ""


@dataclass(frozen=True)
class Signals:
    """The control-plane flags a running slice reacts to."""

    yield_requested: bool = False
    cancelled: bool = False
    paused: str = ""          # the pause scope that covers this task, or ""

    @property
    def should_stop(self) -> bool:
        return self.yield_requested or self.cancelled or bool(self.paused)


# --- events -----------------------------------------------------------------

def log_event(cur: psycopg.Cursor, run_id: int | None, task_id: int | None,
              event: str, message: str = "", *, level: str = "info",
              data: Mapping[str, Any] | None = None) -> None:
    """Append to ``run_events``.

    Roughly 50-500 rows per run, never one per unit: per-item detail belongs in
    ``task_units``. This is what replaces tailing ``~/.windex/logs/<job>.log``,
    which only ever worked from inside whichever container wrote the file.

    WRAPPED IN A SAVEPOINT, and that is not defensive habit. Callers write events
    from INSIDE the claim transaction, and ``run_events`` is RANGE-partitioned with
    a fixed window of months pre-created and no DEFAULT partition (deliberately —
    a row landing in a default makes the later CREATE for that month fail). So a
    box that goes long enough without ``init-db`` rolling the window forward would
    raise "no partition of relation run_events found" on *every claim*, and a lost
    log line would become a total pool stall. The savepoint keeps the failure
    proportional to what was lost: the event is dropped, the claim commits.
    """
    cur.execute("SAVEPOINT windex_event")
    try:
        cur.execute(
            "INSERT INTO run_events (run_id, task_id, level, event, message, data) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (run_id, task_id, level, event, message, Jsonb(dict(data or {}))),
        )
    except psycopg.Error as exc:
        cur.execute("ROLLBACK TO SAVEPOINT windex_event")
        log.warning("run_events insert failed (%s); event %r dropped. If this is a "
                    "missing partition, run `windex init-db` to roll the window "
                    "forward.", exc, event)
    else:
        cur.execute("RELEASE SAVEPOINT windex_event")


# --- pauses -----------------------------------------------------------------

# One predicate, built once, so "paused" cannot mean two different things in the
# claim and in the running slice. It is emitted twice with different operands —
# against literals for a lookup, against columns inside the claim — which is why
# it is a function over SQL fragments rather than a constant string; the
# operands are module constants, never user input.
def _pause_sql(select: str, source: str, lane: str, recipe: str) -> str:
    return (f"SELECT {select} FROM pauses p "
            f"WHERE (p.expires_at IS NULL OR p.expires_at > now()) "
            f"AND p.scope IN ('global', 'source:' || {source}, 'lane:' || {lane}, "
            f"'recipe:' || {recipe})")


# An expired pause is simply not a pause: expiry is evaluated in SQL, so nothing
# has to sweep the table and a forgotten sweep cannot leave the fleet paused.
_PAUSE_LOOKUP = (_pause_sql("p.scope", "%s", "%s", "%s")
                 + " ORDER BY (p.scope = 'global') DESC LIMIT 1")
_PAUSE_IN_CLAIM = _pause_sql("1", "t.source", "t.lane", "r.recipe")


def pause_covering(conn: psycopg.Connection, source: str, lane: str,
                   recipe: str) -> str:
    """The scope of the pause blocking this work, or '' when nothing does."""
    with conn.cursor() as cur:
        cur.execute(_PAUSE_LOOKUP, (source, lane, recipe))
        row = cur.fetchone()
    return row[0] if row else ""


def set_pause(conn: psycopg.Connection, scope: str, *, reason: str = "",
              by: str = "", expires_at: Any = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pauses (scope, reason, paused_by, expires_at) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (scope) DO UPDATE "
            "SET reason = EXCLUDED.reason, paused_by = EXCLUDED.paused_by, "
            "    paused_at = now(), expires_at = EXCLUDED.expires_at",
            (scope, reason, by, expires_at),
        )
    conn.commit()


def clear_pause(conn: psycopg.Connection, scope: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM pauses WHERE scope = %s", (scope,))
    conn.commit()


# --- fair share -------------------------------------------------------------

def _vtime_floor(cur: psycopg.Cursor) -> float:
    """The virtual time a newly-backlogged source is rebased to.

    Without this, WFQ has a well-known and very reachable bug: a source idle for
    a week keeps its old (tiny) vtime, so when it returns it sorts ahead of
    everything until it has burned a week of credit — monopolizing the pool for
    hours. Rebasing to the minimum vtime among *currently in-flight* sources
    gives a returning source a fair position rather than a free ride, and costs
    one indexless scan of a table with one row per source.
    """
    cur.execute("SELECT coalesce(min(vtime), 0) FROM source_sched WHERE in_flight > 0")
    return float(cur.fetchone()[0])


def _charge(cur: psycopg.Cursor, source: str, seconds: float) -> None:
    """Bill a source for service time and drop its in-flight count.

    vtime advances by ``elapsed / weight``: a source with weight 2 accumulates
    virtual time half as fast and therefore gets served twice as often. Weight 0
    would divide by zero and is clamped rather than rejected, matching the
    clamp-don't-reject discipline the rest of windex validates with.
    """
    cur.execute(
        "INSERT INTO source_sched (source, vtime, in_flight) VALUES (%s, %s, 0) "
        "ON CONFLICT (source) DO UPDATE SET "
        "  vtime = source_sched.vtime + %s / greatest(source_sched.weight, 0.01), "
        "  in_flight = greatest(source_sched.in_flight - 1, 0), updated_at = now()",
        (source, max(seconds, 0.0), max(seconds, 0.0)),
    )


def reconcile_in_flight(conn: psycopg.Connection) -> int:
    """Recompute ``in_flight`` from the tasks actually running.

    ``in_flight`` is a counter in a table, which is the exact shape
    ``embed/budget.py`` rejects for the GPU budget ("a counter table would leak
    slots forever"). It is used here anyway because it only feeds the vtime
    rebase floor, where a stale value costs fairness rather than correctness —
    but only because this reconciliation runs every supervisor tick and makes
    the leak self-healing. Drop this and a SIGKILLed slot pins a source's
    in_flight above zero forever, permanently dragging the rebase floor.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE source_sched s SET in_flight = c.n, updated_at = now()
              FROM (SELECT s2.source,
                           (SELECT count(*) FROM run_tasks t
                             WHERE t.state = 'running' AND t.source = s2.source) AS n
                      FROM source_sched s2) c
             WHERE c.source = s.source AND s.in_flight <> c.n
            """
        )
        n = cur.rowcount or 0
    conn.commit()
    return n


# --- the claim --------------------------------------------------------------

_CLAIM_SQL = """
UPDATE run_tasks SET
    state            = 'running',
    lease_worker     = %(worker)s,
    lease_expires_at = now() + make_interval(secs => lease_seconds),
    heartbeat_at     = now(),
    started_at       = coalesce(started_at, now()),
    yield_requested  = false,
    error            = NULL
WHERE id = (
    SELECT t.id
      FROM run_tasks t
      JOIN runs r ON r.id = t.run_id
      LEFT JOIN source_sched s ON s.source = t.source
     WHERE t.state = 'ready'
       AND t.lane = ANY(%(lanes)s)
       -- A queued run is claimable and is promoted to running by the claim
       -- itself; nothing else has to move it, so there is no window where a
       -- run exists with ready tasks that no worker will look at.
       AND r.state IN ('queued', 'running')
       AND NOT r.cancel_requested
       -- Declared preconditions must all be in the set the pool has verified.
       -- Array containment, so a task with no preconditions always passes and
       -- an unknown precondition name is unsatisfiable rather than ignored.
       AND t.preconditions <@ %(satisfied)s::text[]
       AND NOT EXISTS (""" + _PAUSE_IN_CLAIM + """)
       -- Lock the lane FIRST, then count under it. See the module docstring:
       -- CASE fixes the evaluation order that a bare AND chain leaves to the
       -- planner, and the order is the entire correctness argument.
       AND CASE WHEN pg_try_advisory_xact_lock(
                        hashtextextended(%(lock_ns)s || t.lane, 0))
                THEN
                    -- Anti-starvation: at most one running task per
                    -- (source, lane). Without it a recipe with eight net nodes
                    -- fills the net lane by itself and every other source waits,
                    -- which is FIFO blocking wearing a scheduler's hat.
                    NOT EXISTS (SELECT 1 FROM run_tasks o
                                 WHERE o.state = 'running'
                                   AND o.source = t.source AND o.lane = t.lane)
                    AND (SELECT count(*) FROM run_tasks o
                          WHERE o.state = 'running' AND o.lane = t.lane)
                        < coalesce((%(caps)s::jsonb ->> t.lane)::int, %(default_cap)s)
                ELSE false END
     -- Priority beats fairness (manual 100 > event 70 > schedule 50 >
     -- backfill 10); vtime breaks ties by who has been served least; id breaks
     -- those, so the order is total and two workers never disagree about it.
     ORDER BY t.priority DESC, coalesce(s.vtime, 0), t.id
       FOR UPDATE OF t SKIP LOCKED
     LIMIT 1
)
RETURNING id, run_id, source, node, kind, module, lane, config, cursor,
          priority, attempts, max_attempts, lease_seconds, units_done, units_failed
"""


def claim_task(conn: psycopg.Connection, *, worker: str, lanes: Sequence[str],
               caps: Mapping[str, int], satisfied: Iterable[str],
               default_cap: int = 2) -> ClaimedTask | None:
    """Lease the best claimable task for this worker, or None.

    One statement decides everything (pauses, preconditions, anti-starvation,
    lane cap, fairness order) because a claim split across statements is a claim
    with a race between them. The bookkeeping that follows — promoting the run,
    rebasing vtime, the event row — happens in the same transaction, so a crash
    anywhere leaves either a fully claimed task or an unclaimed one.
    """
    sat = sorted(set(satisfied))
    with conn.cursor() as cur:
        cur.execute(_CLAIM_SQL, {
            "worker": worker,
            "lanes": list(lanes),
            "satisfied": sat,
            "lock_ns": _LANE_LOCK,
            "caps": Jsonb(dict(caps)),
            "default_cap": default_cap,
        })
        row = cur.fetchone()
        if row is None:
            conn.rollback()   # release the lane locks we may have taken
            return None
        (task_id, run_id, source, node, kind, module, lane, config, cursor_,
         priority, attempts, max_attempts, lease_seconds, done, failed) = row

        # Promote the run on first claim. Idempotent, and it means "queued" can
        # never be a state a run is stuck in while its tasks are running.
        cur.execute(
            "UPDATE runs SET state = 'running', started_at = coalesce(started_at, now()), "
            "updated_at = now() WHERE id = %s AND state = 'queued'",
            (run_id,),
        )
        cur.execute("SELECT recipe, spec, params, mode FROM runs WHERE id = %s", (run_id,))
        recipe, spec, params, mode = cur.fetchone()

        floor = _vtime_floor(cur)
        cur.execute(
            "INSERT INTO source_sched (source, vtime, in_flight) VALUES (%s, %s, 1) "
            "ON CONFLICT (source) DO UPDATE SET vtime = greatest(source_sched.vtime, %s), "
            "  in_flight = source_sched.in_flight + 1, updated_at = now()",
            (source, floor, floor),
        )
        log_event(cur, run_id, task_id, "task.leased", f"{node} → {worker}",
                  data={"lane": lane, "module": module, "attempt": attempts})
    conn.commit()
    return ClaimedTask(
        id=task_id, run_id=run_id, recipe=recipe, source=source, node=node, kind=kind,
        module=module, lane=lane, config=config or {}, cursor=cursor_ or {},
        spec=spec or {}, params=params or {}, mode=mode, priority=priority,
        attempts=attempts, max_attempts=max_attempts, lease_seconds=lease_seconds,
        units_done=done, units_failed=failed, worker=worker,
    )


# --- heartbeat / signals ----------------------------------------------------

def heartbeat(conn: psycopg.Connection, task_id: int, worker: str, *,
              units_done: int, units_failed: int,
              stats: Mapping[str, Any] | None = None) -> Signals:
    """Renew the lease and report progress. Raises LeaseLost if it is not ours.

    The ``lease_worker = %s`` guard is the whole safety property: once the reaper
    has handed this task to somebody else, our writes must not land. Silently
    updating a row another worker owns is how two slots end up interleaving
    counters on one task, and the symptom (units_done exceeding units_total) is
    hours away from the cause.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE run_tasks t SET
                heartbeat_at     = now(),
                lease_expires_at = now() + make_interval(secs => t.lease_seconds),
                units_done = %s, units_failed = %s,
                stats = coalesce(t.stats, '{}'::jsonb) || %s::jsonb
             WHERE t.id = %s AND t.lease_worker = %s
            RETURNING t.yield_requested, t.source, t.lane,
                      (SELECT r.cancel_requested FROM runs r WHERE r.id = t.run_id),
                      (SELECT r.recipe FROM runs r WHERE r.id = t.run_id)
            """,
            (units_done, units_failed, Jsonb(dict(stats or {})), task_id, worker),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            raise LeaseLost(f"task {task_id} is no longer leased by {worker}")
        yield_requested, source, lane, cancelled, recipe = row
        cur.execute(_PAUSE_LOOKUP, (source, lane, recipe))
        prow = cur.fetchone()
    conn.commit()
    return Signals(yield_requested=bool(yield_requested), cancelled=bool(cancelled),
                   paused=prow[0] if prow else "")


# --- release ----------------------------------------------------------------

@dataclass(frozen=True)
class Release:
    """The outcome the slot wants recorded for a finished slice."""

    outcome: str                     # yielded | succeeded | failed | cancelled
    units_done: int = 0
    units_failed: int = 0
    elapsed: float = 0.0
    cursor: dict | None = None
    stats: dict = field(default_factory=dict)
    reason: str = ""
    error: str | None = None
    units_total: int = -1
    permanent: bool = False          # skip the retry budget (PermanentTaskError)


def release(conn: psycopg.Connection, task: ClaimedTask, rel: Release) -> str:
    """Write the end of a slice. Returns the state the task landed in.

    Every exit path goes through here — success, yield, failure, cancellation —
    so there is exactly one place that decrements ``in_flight`` and charges
    vtime. Spreading that across four call sites is how in-flight counters drift.
    """
    if rel.outcome == "yielded":
        state, attempts_delta = "ready", 0
    elif rel.outcome == "succeeded":
        state, attempts_delta = "succeeded", 0
    elif rel.outcome == "cancelled":
        state, attempts_delta = "cancelled", 0
    elif rel.outcome == "failed":
        attempts_delta = 1
        exhausted_retries = rel.permanent or task.attempts + 1 >= task.max_attempts
        # A retryable failure goes back to 'ready', not 'failed': the unit tables
        # survived, so the retry resumes rather than restarts. Only the last
        # attempt is terminal.
        state = "failed" if exhausted_retries else "ready"
    else:  # pragma: no cover — programmer error, not a runtime condition
        raise ValueError(f"unknown outcome {rel.outcome!r}")

    terminal = state in TERMINAL
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE run_tasks SET
                state = %(state)s,
                attempts = attempts + %(attempts_delta)s,
                lease_worker = NULL, lease_expires_at = NULL, yield_requested = false,
                heartbeat_at = now(),
                units_done = %(done)s, units_failed = %(failed)s,
                units_total = CASE WHEN %(units_total)s >= 0 THEN %(units_total)s
                                   ELSE units_total END,
                cursor = coalesce(%(cursor)s::jsonb, cursor),
                stats = coalesce(stats, '{}'::jsonb) || %(stats)s::jsonb,
                finished_at = CASE WHEN %(terminal)s THEN now() ELSE NULL END,
                error = %(error)s
             WHERE id = %(id)s AND lease_worker = %(worker)s
            """,
            {
                "state": state, "attempts_delta": attempts_delta,
                "done": rel.units_done, "failed": rel.units_failed,
                "units_total": rel.units_total,
                "cursor": Jsonb(rel.cursor) if rel.cursor is not None else None,
                "stats": Jsonb(dict(rel.stats)), "terminal": terminal,
                "error": rel.error, "id": task.id, "worker": task.worker,
            },
        )
        if not cur.rowcount:
            conn.rollback()
            raise LeaseLost(f"task {task.id} was reclaimed before release")
        _charge(cur, task.source, rel.elapsed)
        level = "error" if state == "failed" else "info"
        log_event(cur, task.run_id, task.id, f"task.{rel.outcome}",
                  rel.error or rel.reason, level=level,
                  data={"units_done": rel.units_done, "units_failed": rel.units_failed,
                        "elapsed": round(rel.elapsed, 3), "state": state,
                        "reason": rel.reason})
    conn.commit()
    return state


# --- the reaper -------------------------------------------------------------

def reclaim_expired(conn: psycopg.Connection) -> list[dict]:
    """Return tasks whose lease ran out to 'ready' — or 'failed' at max_attempts.

    Safe for the same reason ``crawl.run.reclaim_stale`` is: the work list is
    persisted, so a reclaimed task resumes from its remaining pending units
    rather than restarting, and re-doing the last batch is a text_hash no-op in
    every ledger windex has.

    Unlike the crawl version this also *charges* the dead holder for the lane it
    was occupying (a full ``lease_seconds``). That is deliberate bias: a source
    whose tasks keep dying gets pushed back in the fair-share order instead of
    re-claiming instantly and crash-looping at the head of the queue.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE run_tasks t SET
                state = CASE WHEN t.attempts + 1 >= t.max_attempts THEN 'failed'
                             ELSE 'ready' END,
                attempts = t.attempts + 1,
                lease_worker = NULL, lease_expires_at = NULL, yield_requested = false,
                finished_at = CASE WHEN t.attempts + 1 >= t.max_attempts THEN now()
                                   ELSE NULL END,
                error = CASE WHEN t.attempts + 1 >= t.max_attempts
                             THEN 'lease expired (worker ' || coalesce(t.lease_worker, '?')
                                  || ' stopped reporting)'
                             ELSE t.error END
             WHERE t.state = 'running' AND t.lease_expires_at < now()
            RETURNING t.id, t.run_id, t.source, t.node, t.state, t.attempts,
                      t.max_attempts, t.lease_seconds
            """
        )
        rows = cur.fetchall()
        out = []
        for task_id, run_id, source, node, state, attempts, max_attempts, lease_s in rows:
            _charge(cur, source, float(lease_s))
            log_event(cur, run_id, task_id, "task.reclaimed",
                      f"{node}: lease expired (attempt {attempts}/{max_attempts})",
                      level="warn" if state == "ready" else "error",
                      data={"state": state, "attempts": attempts})
            out.append({"id": task_id, "run_id": run_id, "source": source,
                        "node": node, "state": state})
    conn.commit()
    return out


def release_worker(conn: psycopg.Connection, worker: str) -> list[dict]:
    """Reclaim everything held by a worker known to be dead, without waiting.

    Lease expiry alone would eventually do this, but "eventually" is up to
    ``lease_seconds`` (300 s by default, and a fetch task may legitimately set
    600). The supervisor knows the instant a slot dies, and a task sitting
    un-runnable for ten minutes because its slot was OOM-killed is exactly the
    stall this pool exists to remove. Expiring the lease immediately hands the
    task straight back to the normal reclaim path.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE run_tasks SET lease_expires_at = now() - interval '1 second' "
            "WHERE state = 'running' AND lease_worker = %s RETURNING id",
            (worker,),
        )
        n = len(cur.fetchall())
    conn.commit()
    return reclaim_expired(conn) if n else []


def request_yield(conn: psycopg.Connection, *, worker: str = "",
                  task_id: int | None = None, reason: str = "") -> int:
    """Ask a running task to end its slice at the next boundary.

    The polite half of the memory ceiling and of preemption: the holder finishes
    what it is doing, commits, and re-queues itself. Nothing is lost and nothing
    is killed.
    """
    if not worker and task_id is None:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE run_tasks SET yield_requested = true WHERE state = 'running' "
            "AND yield_requested = false "
            "AND (%s = '' OR lease_worker = %s) AND (%s::bigint IS NULL OR id = %s) "
            "RETURNING id, run_id",
            (worker, worker, task_id, task_id),
        )
        rows = cur.fetchall()
        for tid, run_id in rows:
            log_event(cur, run_id, tid, "task.yield_requested", reason, level="warn")
    conn.commit()
    return len(rows)


def request_yield_for_priority(conn: psycopg.Connection) -> int:
    """Preempt lower-priority holders when higher-priority work is waiting.

    "Run now" from the client sets priority 100; without this it would wait for
    whatever slice is in flight *and* for every queued task ahead of it. With
    it, the holder yields at its next boundary and the new run starts within one
    slice. Only same-lane holders are asked, because that is the only contention
    a yield actually relieves.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE run_tasks h SET yield_requested = true
             WHERE h.state = 'running' AND h.yield_requested = false
               AND EXISTS (SELECT 1 FROM run_tasks q JOIN runs r ON r.id = q.run_id
                            WHERE q.state = 'ready' AND q.lane = h.lane
                              AND q.priority > h.priority
                              AND r.state IN ('queued', 'running')
                              AND NOT r.cancel_requested)
            RETURNING h.id, h.run_id
            """
        )
        rows = cur.fetchall()
        for tid, run_id in rows:
            log_event(cur, run_id, tid, "task.yield_requested",
                      "preempted by higher-priority work")
    conn.commit()
    return len(rows)
