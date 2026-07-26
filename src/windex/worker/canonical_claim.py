"""Lease/fair-share protocol for contract-epoch 2 Run Tasks."""

from __future__ import annotations

import gzip
import hashlib
import json
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.config import Settings
from windex.pipeline.events import append
from windex.worker.protocol import LeaseLost

TERMINAL = ("succeeded", "failed", "skipped", "cancelled")
_LOCK_NAMESPACE = "windex.worker.lane:"
MAX_LEASE_RECOVERIES = 20
_MAX_INLINE_OUTPUT = 1024 * 1024


@dataclass(frozen=True)
class ClaimedTask:
    id: int
    run_id: int
    source_id: int | None
    source_name: str
    pipeline_name: str
    pipeline_version: int
    pipeline_hash: str
    state_namespace: str
    search_name: str
    id_prefix: str
    collection_key: str
    search_profile: str
    node: str
    kind: str
    module: str
    module_version: str
    module_digest: str
    executor: str
    lane: str
    config: dict[str, Any]
    cursor: dict[str, Any]
    spec: dict[str, Any]
    effective_config: dict[str, Any]
    inputs: dict[str, Any]
    flow_name: str
    mode: str
    priority: int
    attempts: int
    max_attempts: int
    lease_seconds: int
    units_done: int
    units_failed: int
    captures: tuple[str, ...] = ()
    worker: str = ""
    source_generation: int = 0


@dataclass(frozen=True)
class Signals:
    yield_requested: bool = False
    cancelled: bool = False
    paused: str = ""

    @property
    def should_stop(self) -> bool:
        return self.yield_requested or self.cancelled or bool(self.paused)


@dataclass(frozen=True)
class Release:
    outcome: str
    units_done: int = 0
    units_failed: int = 0
    elapsed: float = 0.0
    cursor: dict | None = None
    stats: dict = field(default_factory=dict)
    reason: str = ""
    error: str | None = None
    units_total: int = -1
    permanent: bool = False


_CLAIM = """
UPDATE run_tasks SET
    state = 'running',
    lease_worker = %(worker)s,
    lease_expires_at = now() + make_interval(secs => lease_seconds),
    heartbeat_at = now(),
    started_at = coalesce(started_at, now()),
    yield_requested = false,
    error = NULL
WHERE id = (
    SELECT t.id
      FROM run_tasks t
      JOIN runs r ON r.id = t.run_id
      LEFT JOIN source_sched fair ON fair.source_id = t.source_id
     WHERE t.state = 'ready'
       AND t.lane = ANY(%(lanes)s)
       AND r.state IN ('queued','running')
       AND NOT r.cancel_requested
       AND t.preconditions <@ %(satisfied)s::text[]
       AND (
           t.module = 'platform.reset'
           OR NOT EXISTS (
               SELECT 1 FROM source_control ctl
                WHERE ctl.source_id = t.source_id AND ctl.paused))
       AND CASE WHEN pg_try_advisory_xact_lock(
                    hashtextextended(%(lock_ns)s || t.lane, 0))
                THEN
                  (
                      t.lane = 'warc'
                      OR NOT EXISTS (
                          SELECT 1 FROM run_tasks active
                           WHERE active.state = 'running'
                             AND active.source_id IS NOT DISTINCT FROM t.source_id
                             AND active.lane = t.lane))
                  AND (
                      SELECT count(*) FROM run_tasks active
                       WHERE active.state = 'running' AND active.lane = t.lane)
                      < coalesce(
                          (%(caps)s::jsonb ->> t.lane)::int, %(default_cap)s)
                ELSE false END
     ORDER BY t.priority DESC, coalesce(fair.vtime, 0), t.id
       FOR UPDATE OF t SKIP LOCKED
     LIMIT 1
)
RETURNING id, run_id, source_id, coalesce(source_name, ''), node, kind, module,
          module_version, module_digest, executor, lane, config, cursor, priority,
          attempts, max_attempts, lease_seconds,
          units_done, units_failed, captures
"""


def _event(
    cur: psycopg.Cursor,
    task: ClaimedTask,
    event: str,
    message: str = "",
    *,
    level: str = "info",
    data: Mapping[str, Any] | None = None,
) -> None:
    append(
        cur, component="worker", event=event, message=message, level=level,
        source_name=task.source_name or None, pipeline_name=task.pipeline_name,
        pipeline_version=task.pipeline_version, run_id=task.run_id,
        task_id=task.id, node=task.node, module=task.module, data=data)


def claim_task(
    conn: psycopg.Connection,
    *,
    worker: str,
    lanes: Sequence[str],
    caps: Mapping[str, int],
    satisfied: Iterable[str],
    default_cap: int = 2,
) -> ClaimedTask | None:
    with conn.cursor() as cur:
        cur.execute(_CLAIM, {
            "worker": worker, "lanes": list(lanes),
            "caps": Jsonb(dict(caps)), "satisfied": sorted(set(satisfied)),
            "default_cap": default_cap, "lock_ns": _LOCK_NAMESPACE,
        })
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return None
        (
            task_id, run_id, source_id, source_name, node, kind, module,
            module_version, module_digest, executor, lane, config, cursor, priority,
            attempts, max_attempts, lease_seconds,
            units_done, units_failed, captures,
        ) = row
        cur.execute(
            """UPDATE runs SET state = 'running',
                      started_at = coalesce(started_at, now()), updated_at = now()
                WHERE id = %s AND state = 'queued'
                RETURNING id""",
            (run_id,),
        )
        run_started = cur.fetchone() is not None
        cur.execute(
            """SELECT pipeline_name, pipeline_version, pipeline_hash, frozen_spec,
                      effective_config, explicit_inputs, source_snapshot,
                      flow_name, mode
                 FROM runs WHERE id = %s""",
            (run_id,),
        )
        pipeline_name, pipeline_version, pipeline_hash, spec, effective, inputs, \
            snapshot, flow_name, mode = cur.fetchone()
        snapshot = snapshot or {}
        task = ClaimedTask(
            id=task_id, run_id=run_id, source_id=source_id,
            source_name=source_name, pipeline_name=pipeline_name,
            pipeline_version=pipeline_version, pipeline_hash=pipeline_hash,
            state_namespace=str(snapshot.get("state_namespace") or ""),
            search_name=str(snapshot.get("search_name") or ""),
            id_prefix=str(snapshot.get("id_prefix") or ""),
            collection_key=str(snapshot.get("collection_key") or ""),
            search_profile=str(snapshot.get("search_profile") or ""),
            node=node, kind=kind, module=module,
            module_version=module_version, module_digest=module_digest,
            executor=executor, lane=lane, config=config or {},
            cursor=cursor or {}, spec=spec or {}, effective_config=effective or {},
            inputs=inputs or {}, flow_name=flow_name, mode=mode,
            priority=priority, attempts=attempts,
            max_attempts=max_attempts, lease_seconds=lease_seconds,
            units_done=units_done, units_failed=units_failed, worker=worker,
            captures=tuple(captures or ()),
            source_generation=int(snapshot.get("generation") or 0),
        )
        if run_started:
            append(
                cur, component="run", event="run.running",
                source_name=source_name, pipeline_name=pipeline_name,
                pipeline_version=pipeline_version, run_id=run_id,
            )
        if source_id is not None:
            cur.execute(
                """INSERT INTO source_sched (source_id, vtime, in_flight)
                   VALUES (%s, (
                       SELECT coalesce(min(vtime), 0) FROM source_sched
                        WHERE in_flight > 0), 1)
                   ON CONFLICT (source_id) DO UPDATE SET
                       vtime = greatest(
                           source_sched.vtime, (
                           SELECT coalesce(min(vtime), 0) FROM source_sched
                            WHERE in_flight > 0)),
                       in_flight = source_sched.in_flight + 1,
                       updated_at = now()""",
                (source_id,),
            )
        _event(
            cur, task, "task.leased", f"{node} → {worker}",
            data={"lane": lane, "attempt": attempts})
    conn.commit()
    return task


def heartbeat(
    conn: psycopg.Connection,
    task_id: int,
    worker: str,
    *,
    units_done: int,
    units_failed: int,
    stats: Mapping[str, Any] | None = None,
) -> Signals:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks t SET
                      heartbeat_at = now(),
                      lease_expires_at =
                          now() + make_interval(secs => t.lease_seconds),
                      units_done = %s, units_failed = %s,
                      stats = t.stats || %s::jsonb
                WHERE t.id = %s AND t.lease_worker = %s
                RETURNING t.yield_requested, t.source_id, t.run_id, t.module,
                          (SELECT cancel_requested FROM runs WHERE id = t.run_id)""",
            (
                units_done, units_failed, Jsonb(dict(stats or {})),
                task_id, worker,
            ),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            raise LeaseLost(f"task {task_id} is no longer leased by {worker}")
        requested, source_id, _run_id, module, cancelled = row
        paused = ""
        if source_id is not None and module != "platform.reset":
            cur.execute(
                "SELECT pause_reason FROM source_control "
                "WHERE source_id = %s AND paused",
                (source_id,),
            )
            pause_row = cur.fetchone()
            if pause_row:
                paused = pause_row[0] or "source"
    conn.commit()
    return Signals(
        yield_requested=bool(requested), cancelled=bool(cancelled), paused=paused)


def _charge(cur: psycopg.Cursor, source_id: int | None, elapsed: float) -> None:
    if source_id is None:
        return
    cur.execute(
        """UPDATE source_sched
              SET vtime = vtime + %s / greatest(weight, 0.01),
                  in_flight = greatest(in_flight - 1, 0), updated_at = now()
            WHERE source_id = %s""",
        (max(elapsed, 0.0), source_id),
    )


def _boundary_declaration(task: ClaimedTask, boundary: str) -> dict[str, Any]:
    flow = (task.spec.get("flows") or {}).get(task.flow_name) or {}
    for declaration in flow.get("outputs") or ():
        if declaration.get("id") == boundary:
            return declaration
    return {"id": boundary, "type": "WireBatch"}


def _store_captured_output(
    cur: psycopg.Cursor,
    task: ClaimedTask,
    boundary: str,
    captured: Any,
) -> None:
    raw = json.dumps(
        captured, sort_keys=True, separators=(",", ":"), default=str).encode()
    declaration = _boundary_declaration(task, boundary)
    maximum = int(declaration.get("max_bytes") or 64 * 1024 * 1024)
    if len(raw) > maximum:
        raise ValueError(
            f"boundary {boundary!r} output is {len(raw)} bytes; max is {maximum}")
    checksum = "sha256:" + hashlib.sha256(raw).hexdigest()
    value_type = str(declaration.get("type") or "WireBatch")
    value: Any = captured
    if len(raw) > _MAX_INLINE_OUTPUT:
        artifact_id = str(uuid.uuid4())
        root = Settings().artifacts_dir
        relative = f"runs/{task.run_id}/{artifact_id}.json.gz"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wb") as handle:
            handle.write(raw)
        stored = path.read_bytes()
        stored_checksum = "sha256:" + hashlib.sha256(stored).hexdigest()
        cur.execute(
            """INSERT INTO run_artifacts
                   (id, run_id, boundary, media_type, relative_path, size_bytes,
                    checksum, expires_at)
               VALUES (%s, %s, %s, 'application/json+gzip', %s, %s, %s,
                       now() + interval '30 days')""",
            (
                artifact_id, task.run_id, boundary, relative, len(stored),
                stored_checksum,
            ),
        )
        value = {
            "inline": False,
            "artifact_id": artifact_id,
            "media_type": "application/json+gzip",
        }
    cur.execute(
        """INSERT INTO run_outputs
               (run_id, boundary, value_type, value, size_bytes, checksum)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON CONFLICT (run_id, boundary) DO UPDATE SET
               value_type = EXCLUDED.value_type,
               value = EXCLUDED.value,
               size_bytes = EXCLUDED.size_bytes,
               checksum = EXCLUDED.checksum,
               created_at = now()""",
        (
            task.run_id, boundary, value_type, Jsonb(value), len(raw), checksum,
        ),
    )


def release(
    conn: psycopg.Connection, task: ClaimedTask, result: Release,
) -> str:
    if result.outcome == "yielded":
        state, attempt_delta = "ready", 0
    elif result.outcome in ("succeeded", "cancelled"):
        state, attempt_delta = result.outcome, 0
    elif result.outcome == "failed":
        attempt_delta = 1
        state = (
            "failed" if result.permanent
            or task.attempts + 1 >= task.max_attempts else "ready")
    else:
        raise ValueError(f"unknown outcome {result.outcome!r}")
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks SET
                      state = %(state)s, attempts = attempts + %(attempt)s,
                      lease_worker = NULL, lease_expires_at = NULL,
                      yield_requested = false, heartbeat_at = now(),
                      units_done = %(done)s, units_failed = %(failed)s,
                      units_total = CASE WHEN %(total)s >= 0
                                         THEN %(total)s ELSE units_total END,
                      cursor = coalesce(%(cursor)s::jsonb, cursor),
                      stats = (stats - 'lease_recoveries') || %(stats)s::jsonb,
                      finished_at = CASE WHEN %(terminal)s THEN now() ELSE NULL END,
                      error = %(error)s
                WHERE id = %(id)s AND lease_worker = %(worker)s""",
            {
                "state": state, "attempt": attempt_delta,
                "done": result.units_done, "failed": result.units_failed,
                "total": result.units_total,
                "cursor": Jsonb(result.cursor) if result.cursor is not None else None,
                "stats": Jsonb(dict(result.stats)), "terminal": state in TERMINAL,
                "error": result.error, "id": task.id, "worker": task.worker,
            },
        )
        if not cur.rowcount:
            conn.rollback()
            raise LeaseLost(f"task {task.id} was reclaimed before release")
        _charge(cur, task.source_id, result.elapsed)
        output_error = ""
        if state == "succeeded" and task.captures:
            cur.execute("SAVEPOINT captured_output")
            try:
                cur.execute(
                    """SELECT outputs FROM task_units
                        WHERE task_id = %s AND state = 'done' ORDER BY seq""",
                    (task.id,),
                )
                from windex.modules.common import _load_outputs
                from windex.pipeline.wire import encode_many

                captured: list[dict[str, Any]] = []
                for row in cur.fetchall():
                    captured.extend(encode_many(_load_outputs(row[0])))
                for boundary in task.captures:
                    _store_captured_output(cur, task, boundary, captured)
            except Exception as exc:  # output persistence is a permanent failure
                cur.execute("ROLLBACK TO SAVEPOINT captured_output")
                output_error = (
                    f"terminal output validation/persistence failed: "
                    f"{type(exc).__name__}: {exc}")
                state = "failed"
                cur.execute(
                    """UPDATE run_tasks
                          SET state = 'failed', attempts = max_attempts,
                              error = %s, finished_at = now()
                        WHERE id = %s""",
                    (output_error, task.id),
                )
            else:
                cur.execute("RELEASE SAVEPOINT captured_output")
        _event(
            cur, task, "task.failed" if output_error else f"task.{result.outcome}",
            output_error or result.error or result.reason,
            level="error" if state == "failed" else "info",
            data={
                "state": state, "units_done": result.units_done,
                "units_failed": result.units_failed,
                "elapsed": round(result.elapsed, 3),
            })
    conn.commit()
    return state


def reclaim_expired(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Recover infrastructure lease loss without spending execution attempts.

    ``attempts`` belongs to failures reported by a runner. A worker restart or
    host interruption says nothing about whether the task is bad, so it has a
    separate, deliberately high recovery ceiling.
    """
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks SET
                      state = CASE
                                   WHEN coalesce(
                                       (stats->>'lease_recoveries')::integer, 0
                                   ) + 1 >= %s
                                   THEN 'failed' ELSE 'ready' END,
                      stats = jsonb_set(
                          stats, '{lease_recoveries}',
                          to_jsonb(coalesce(
                              (stats->>'lease_recoveries')::integer, 0
                          ) + 1),
                          true),
                      lease_worker = NULL,
                      lease_expires_at = NULL, yield_requested = false,
                      finished_at = CASE
                          WHEN coalesce(
                              (stats->>'lease_recoveries')::integer, 0
                          ) + 1 >= %s
                          THEN now() ELSE NULL END,
                      error = CASE
                          WHEN coalesce(
                              (stats->>'lease_recoveries')::integer, 0
                          ) + 1 >= %s
                          THEN 'lease recovery limit exceeded' ELSE error END
                WHERE state = 'running' AND lease_expires_at < now()
                RETURNING id, run_id, source_id, source_name, node, module,
                          state, (stats->>'lease_recoveries')::integer""",
            (
                MAX_LEASE_RECOVERIES,
                MAX_LEASE_RECOVERIES,
                MAX_LEASE_RECOVERIES,
            ),
        )
        rows = cur.fetchall()
        for (
            task_id, run_id, source_id, source_name, node, module, state,
            recoveries,
        ) in rows:
            _charge(cur, source_id, 0)
            append(
                cur, component="worker", event="task.lease_expired",
                level="error" if state == "failed" else "warn",
                source_name=source_name, run_id=run_id, task_id=task_id,
                node=node, module=module,
                data={"state": state, "lease_recoveries": recoveries},
            )
    conn.commit()
    return [{
        "id": row[0], "run_id": row[1], "source_id": row[2],
        "node": row[4], "state": row[6],
    } for row in rows]


def reconcile_blocked(
    conn: psycopg.Connection, satisfied: Iterable[str],
) -> dict[str, int]:
    """Project unsatisfied fleet preconditions into observable Task/Run states."""
    available = sorted(set(satisfied))
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks t
                  SET state = 'blocked',
                      error = 'unsatisfied preconditions: ' || array_to_string(
                          ARRAY(
                              SELECT unnest(t.preconditions)
                              EXCEPT SELECT unnest(%s::text[])
                          ), ', ')
                WHERE t.state = 'ready' AND cardinality(t.preconditions) > 0
                  AND NOT t.preconditions <@ %s::text[]
                RETURNING t.id, t.run_id, t.source_name, t.node, t.module,
                          t.error""",
            (available, available),
        )
        blocked = cur.fetchall()
        for task_id, run_id, source_name, node, module, reason in blocked:
            append(
                cur, component="worker", event="task.blocked", level="warn",
                source_name=source_name, run_id=run_id, task_id=task_id,
                node=node, module=module, message=reason,
            )
        cur.execute(
            """UPDATE run_tasks t
                  SET state = 'ready', error = NULL
                WHERE t.state = 'blocked'
                  AND t.error LIKE 'unsatisfied preconditions:%%'
                  AND t.preconditions <@ %s::text[]
                RETURNING t.id, t.run_id, t.source_name, t.node, t.module""",
            (available,),
        )
        unblocked = cur.fetchall()
        for task_id, run_id, source_name, node, module in unblocked:
            append(
                cur, component="worker", event="task.unblocked",
                source_name=source_name, run_id=run_id, task_id=task_id,
                node=node, module=module,
            )
        affected_runs = sorted({
            row[1] for row in blocked
        } | {
            row[1] for row in unblocked
        })
        run_blocked = run_unblocked = 0
        if affected_runs:
            cur.execute(
                """UPDATE runs r SET state = 'blocked', updated_at = now()
                    WHERE r.id = ANY(%s)
                      AND r.state IN ('queued','running')
                      AND EXISTS (
                          SELECT 1 FROM run_tasks t
                           WHERE t.run_id = r.id AND t.state = 'blocked')
                      AND NOT EXISTS (
                          SELECT 1 FROM run_tasks t
                           WHERE t.run_id = r.id
                             AND t.state IN ('ready','running'))
                    RETURNING r.id, r.source_name, r.pipeline_name,
                              r.pipeline_version""",
                (affected_runs,),
            )
            changed = cur.fetchall()
            run_blocked = len(changed)
            for run_id, source_name, pipeline_name, version in changed:
                append(
                    cur, component="worker", event="run.blocked", level="warn",
                    source_name=source_name, pipeline_name=pipeline_name,
                    pipeline_version=version, run_id=run_id,
                )
            cur.execute(
                """UPDATE runs r
                      SET state = CASE WHEN r.started_at IS NULL
                                       THEN 'queued' ELSE 'running' END,
                          updated_at = now()
                    WHERE r.id = ANY(%s) AND r.state = 'blocked'
                      AND EXISTS (
                          SELECT 1 FROM run_tasks t
                           WHERE t.run_id = r.id
                             AND t.state IN ('ready','running'))
                    RETURNING r.id, r.source_name, r.pipeline_name,
                              r.pipeline_version, r.state""",
                (affected_runs,),
            )
            changed = cur.fetchall()
            run_unblocked = len(changed)
            for run_id, source_name, pipeline_name, version, state in changed:
                append(
                    cur, component="worker", event="run.unblocked",
                    source_name=source_name, pipeline_name=pipeline_name,
                    pipeline_version=version, run_id=run_id,
                    data={"state": state},
                )
    conn.commit()
    return {
        "tasks_blocked": len(blocked),
        "tasks_unblocked": len(unblocked),
        "runs_blocked": run_blocked,
        "runs_unblocked": run_unblocked,
    }


def request_yield(
    conn: psycopg.Connection, worker: str, reason: str = "",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE run_tasks SET yield_requested = true "
            "WHERE lease_worker = %s AND state = 'running' "
            "AND NOT yield_requested",
            (worker,),
        )
        count = cur.rowcount or 0
    conn.commit()
    return count


def request_yield_for_priority(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks running SET yield_requested = true
                WHERE running.state = 'running' AND NOT running.yield_requested
                  AND EXISTS (
                      SELECT 1 FROM run_tasks waiting
                       WHERE waiting.state = 'ready'
                         AND waiting.lane = running.lane
                         AND waiting.priority > running.priority)""")
        count = cur.rowcount or 0
    conn.commit()
    return count


def reconcile_in_flight(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE source_sched fair SET
                      in_flight = (
                          SELECT count(*) FROM run_tasks t
                           WHERE t.source_id = fair.source_id
                             AND t.state = 'running'),
                      updated_at = now()""")
        count = cur.rowcount or 0
    conn.commit()
    return count


def release_worker(
    conn: psycopg.Connection,
    worker: str,
    *,
    penalize: bool = False,
) -> list[int]:
    """Immediately recover tasks held by an exited slot.

    Expected drains/recycles do not consume retry attempts. An unexpected slot
    crash does, preserving the bounded retry behavior for OOMs and process
    faults that are plausibly caused by the task itself.
    """
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks SET
                      state = CASE
                          WHEN %s AND attempts + 1 >= max_attempts
                          THEN 'failed' ELSE 'ready' END,
                      attempts = attempts + CASE WHEN %s THEN 1 ELSE 0 END,
                      lease_worker = NULL, lease_expires_at = NULL,
                      yield_requested = false,
                      finished_at = CASE
                          WHEN %s AND attempts + 1 >= max_attempts
                          THEN now() ELSE NULL END,
                      error = CASE
                          WHEN %s THEN 'worker exited unexpectedly'
                          ELSE error END
                WHERE lease_worker = %s AND state = 'running'
                RETURNING id, source_id""",
            (penalize, penalize, penalize, penalize, worker),
        )
        rows = cur.fetchall()
        for _task_id, source_id in rows:
            _charge(cur, source_id, 0)
        released = [row[0] for row in rows]
    conn.commit()
    return released


__all__ = [
    "TERMINAL", "ClaimedTask", "Release", "Signals", "claim_task", "heartbeat",
    "reclaim_expired", "reconcile_blocked", "reconcile_in_flight",
    "release", "release_worker",
    "request_yield", "request_yield_for_priority",
]
