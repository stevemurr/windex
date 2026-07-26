"""Canonical generic Pipeline and Source Run persistence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.config import Settings
from windex.pipeline import compile as pipeline_compile
from windex.pipeline import registry
from windex.pipeline.events import append
from windex.pipeline.store import get_revision
from windex.source.store import get_source

RUN_STATES = frozenset(
    {"queued", "running", "succeeded", "failed", "cancelled", "blocked"})


class RunConflictError(RuntimeError):
    pass


def _apply_revision_locks(
    conn: psycopg.Connection,
    compiled: dict[str, Any],
    locks: Mapping[str, Mapping[str, Any]],
) -> None:
    """Bind a new Run to the exact implementations frozen by its revision."""
    task_modules = {str(task["module"]) for task in compiled["tasks"]}
    missing = sorted(task_modules - set(locks))
    if missing:
        raise RunConflictError(
            "revision Module locks do not match its graph: missing "
            + ", ".join(missing))

    unavailable: list[str] = []
    selected = {
        name: locks[name]
        for name in sorted(task_modules)
    }
    for name, lock in selected.items():
        executor = str(lock.get("executor") or "")
        if executor == "sandbox":
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT v.source_digest, v.approval_state
                         FROM module_versions v
                         JOIN module_definitions d ON d.id = v.module_id
                        WHERE d.name = %s AND v.version = %s""",
                    (name, int(lock["version"])),
                )
                row = cur.fetchone()
            available = (
                row is not None
                and row[1] == "available"
                and row[0] == lock.get("digest")
            )
        else:
            available = (
                registry.implemented(name)
                and registry.implementation_digest(name) == lock.get("digest")
            )
        if not available:
            unavailable.append(name)
    if unavailable:
        raise RunConflictError(
            "module_revoked: frozen Module unavailable or changed: "
            + ", ".join(sorted(unavailable)))

    compiled["module_locks"] = {
        name: dict(lock) for name, lock in selected.items()}
    for task in compiled["tasks"]:
        lock = locks[str(task["module"])]
        task["module_version"] = str(lock["version"])
        task["module_digest"] = str(lock["digest"])
        task["executor"] = str(lock["executor"])


def _checksum(value: Any) -> tuple[bytes, str]:
    raw = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return raw, "sha256:" + hashlib.sha256(raw).hexdigest()


def _insert(
    conn: psycopg.Connection,
    *,
    pipeline: Mapping[str, Any],
    compiled: Mapping[str, Any],
    source: Mapping[str, Any] | None,
    trigger_type: str,
    trigger_by: str,
    mode: str,
    priority: int,
    dedupe_key: str | None,
    idempotency_key: str | None,
    commit: bool = True,
    searchable_continuation: bool = True,
) -> int | None:
    source_snapshot = ({
        key: source.get(key)
        for key in (
            "id", "name", "search_name", "id_prefix", "collection_key",
            "search_profile", "include_in_all", "state_namespace", "generation",
        )
    } if source else None)
    tasks = list(compiled["tasks"])
    terminal = [
        task["node"] for task in tasks
        if task["kind"] == "load"
    ]
    if source and searchable_continuation and terminal:
        # Searchability is part of the Run, not an out-of-band embedding loop.
        tasks.append({
            "node": "__index__",
            "kind": "platform",
            "module": "platform.index",
            "module_version": "1.0",
            "module_digest": "builtin:platform.index/1",
            "executor": "platform",
            "lane": "gpu",
            "config": {
                "collection_key": source["collection_key"],
                "search_profile": source["search_profile"],
                "search_name": source["search_name"],
            },
            "depends_on": terminal,
            "preconditions": ["gateway"],
            "captures": [],
            "weight": 1.0,
            "max_attempts": 3,
            "lease_seconds": 300,
        })
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO runs
                       (source_id, source_name, pipeline_name, pipeline_revision_id,
                        pipeline_version, pipeline_hash, flow_name, source_snapshot,
                        effective_config, explicit_inputs, frozen_spec, module_locks,
                        trigger_type, trigger_by, mode, priority, dedupe_key,
                        idempotency_key, state)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, 'queued')
                   ON CONFLICT DO NOTHING RETURNING id""",
                (
                    source["id"] if source else None,
                    source["name"] if source else None,
                    pipeline["pipeline_name"], pipeline["id"], pipeline["version"],
                    pipeline["spec_hash"], compiled["flow"],
                    Jsonb(source_snapshot) if source_snapshot else None,
                    Jsonb(dict(compiled["parameters"])),
                    Jsonb(dict(compiled.get("inputs") or {})),
                    Jsonb(dict(pipeline["spec"])),
                    Jsonb(dict(compiled["module_locks"])),
                    trigger_type, trigger_by, mode, priority, dedupe_key,
                    idempotency_key,
                ),
            )
            row = cur.fetchone()
            if row is None:
                return None
            run_id = row[0]
            for task in tasks:
                cur.execute(
                    """INSERT INTO run_tasks
                           (run_id, source_id, source_name, node, kind, module,
                            module_version, module_digest, executor, lane, config,
                            depends_on, preconditions, captures, priority,
                            max_attempts, lease_seconds, weight, state)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                               %s, %s, %s, %s, %s, %s, %s, 'pending')""",
                    (
                        run_id, source["id"] if source else None,
                        source["name"] if source else None, task["node"],
                        task["kind"], task["module"], task["module_version"],
                        task["module_digest"], task["executor"], task["lane"],
                        Jsonb(dict(task["config"])), list(task["depends_on"]),
                        list(task["preconditions"]), list(task["captures"]),
                        priority, task["max_attempts"], task["lease_seconds"],
                        task["weight"],
                    ),
                )
            for boundary, value in dict(compiled.get("inputs") or {}).items():
                raw, checksum = _checksum(value)
                boundary_type = next(
                    item["type"] for item in compiled["boundaries"]["inputs"]
                    if item["id"] == boundary)
                cur.execute(
                    """INSERT INTO run_outputs
                           (run_id, boundary, value_type, value, size_bytes, checksum)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        run_id, f"input:{boundary}", boundary_type, Jsonb(value),
                        len(raw), checksum,
                    ),
                )
            append(
                cur, component="run", event="run.queued",
                message=f"{pipeline['pipeline_name']}:{compiled['flow']}",
                source_name=source["name"] if source else None,
                pipeline_name=pipeline["pipeline_name"],
                pipeline_version=pipeline["version"], run_id=run_id,
                data={
                    "task_count": len(tasks),
                    "trigger": trigger_type,
                    "mode": mode,
                },
            )
            _advance_cur(cur, run_id)
        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise
    return run_id


def submit_pipeline(
    conn: psycopg.Connection,
    name: str,
    *,
    version: int | None,
    flow: str | None,
    inputs: Mapping[str, Any],
    parameters: Mapping[str, Any],
    settings: Settings | None = None,
    expected_head: str | None = None,
    priority: int = 50,
    dry_run: bool = False,
) -> int:
    registry.load_custom(conn)
    revision = get_revision(conn, name, version)
    if revision is None:
        raise KeyError((name, version))
    if version is None and expected_head and revision["spec_hash"] != expected_head:
        raise RunConflictError("Pipeline head precondition is stale")
    compiled = pipeline_compile.compile_pipeline(
        revision["spec"], flow=flow, inputs=inputs, values=parameters,
        settings=settings,
    )
    _apply_revision_locks(conn, compiled, revision["module_locks"])
    run_id = _insert(
        conn, pipeline=revision, compiled=compiled, source=None,
        trigger_type="manual", trigger_by="admin API",
        mode="dry_run" if dry_run else "run",
        priority=priority, dedupe_key=None, idempotency_key=None,
    )
    assert run_id is not None
    return run_id


def submit_source(
    conn: psycopg.Connection,
    name: str,
    *,
    flow: str | None = None,
    overrides: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Any] | None = None,
    settings: Settings | None = None,
    trigger_type: str = "manual",
    trigger_by: str = "admin API",
    priority: int = 50,
    idempotency_key: str | None = None,
    dedupe: bool = True,
    commit: bool = True,
) -> int | None:
    registry.load_custom(conn)
    source = get_source(conn, name, include_spec=True)
    if source is None:
        raise KeyError(name)
    if source["archived_at"] or not source["enabled"] or source["paused"]:
        raise RunConflictError("Source is archived, disabled, or paused")
    binding = {**source, "inputs": dict(inputs or {})}
    compiled = pipeline_compile.compile_source(
        source["spec"], binding, overrides, flow=flow, settings=settings)
    revision = get_revision(
        conn, source["pipeline_name"], source["pipeline_version"])
    assert revision is not None
    _apply_revision_locks(conn, compiled, revision["module_locks"])
    key = (
        f"source:{source['id']}:{compiled['flow']}" if dedupe else None)
    run_id = _insert(
        conn, pipeline=revision, compiled=compiled, source=source,
        trigger_type=trigger_type, trigger_by=trigger_by, mode="run",
        priority=priority, dedupe_key=key, idempotency_key=idempotency_key,
        commit=commit,
    )
    if run_id is None and idempotency_key is not None:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id FROM runs
                    WHERE source_id = %s AND idempotency_key = %s""",
                (source["id"], idempotency_key),
            )
            row = cur.fetchone()
        if row is not None:
            if commit:
                conn.commit()
            return row[0]
    return run_id


def submit_reset(
    conn: psycopg.Connection,
    source: Mapping[str, Any],
    *,
    was_paused: bool,
    pause_reason: str,
    priority: int = 100,
    commit: bool = True,
) -> int:
    revision = get_revision(
        conn, str(source["pipeline_name"]), int(source["pipeline_version"]))
    if revision is None:
        raise KeyError((source["pipeline_name"], source["pipeline_version"]))
    compiled = {
        "flow": "__reset__",
        "parameters": dict(source.get("values") or {}),
        "inputs": {},
        "boundaries": {"inputs": [], "outputs": []},
        "module_locks": {
            "platform.reset": {
                "module": "platform.reset",
                "version": "1.0",
                "digest": "builtin:platform.reset/1",
                "executor": "platform",
            },
        },
        "tasks": [{
            "node": "__reset__",
            "kind": "platform",
            "module": "platform.reset",
            "module_version": "1.0",
            "module_digest": "builtin:platform.reset/1",
            "executor": "platform",
            "lane": "maint",
            "config": {
                "was_paused": was_paused,
                "pause_reason": pause_reason,
            },
            "depends_on": [],
            "preconditions": [],
            "captures": [],
            "weight": 1.0,
            "max_attempts": 3,
            "lease_seconds": 600,
        }],
    }
    run_id = _insert(
        conn, pipeline=revision, compiled=compiled, source=source,
        trigger_type="reset", trigger_by="admin API", mode="reset",
        priority=priority, dedupe_key=f"reset:{source['id']}",
        idempotency_key=None, commit=commit, searchable_continuation=False,
    )
    if run_id is None:
        raise RunConflictError("a corpus reset is already active for this Source")
    return run_id


def _advance_cur(cur: psycopg.Cursor, run_id: int) -> None:
    cur.execute(
        """SELECT source_name, pipeline_name, pipeline_version
             FROM runs WHERE id = %s""",
        (run_id,),
    )
    context = cur.fetchone()
    if context is None:
        raise KeyError(run_id)
    cur.execute(
        """UPDATE run_tasks t SET state = 'ready'
            WHERE t.run_id = %s AND t.state = 'pending'
              AND NOT EXISTS (
                    SELECT 1 FROM unnest(t.depends_on) d(node)
                   LEFT JOIN run_tasks p
                      ON p.run_id = t.run_id AND p.node = d.node
                   WHERE p.state IS NULL
                      OR p.state NOT IN ('succeeded','skipped'))
            RETURNING t.id, t.node, t.module""",
        (run_id,),
    )
    for task_id, node, module in cur.fetchall():
        append(
            cur, component="run", event="task.ready", run_id=run_id,
            task_id=task_id, node=node, module=module,
            source_name=context[0], pipeline_name=context[1],
            pipeline_version=context[2],
        )


def advance(conn: psycopg.Connection, run_id: int) -> str:
    with conn.cursor() as cur:
        _advance_cur(cur, run_id)
        cur.execute(
            """UPDATE run_tasks t
                  SET state = 'skipped', finished_at = now(),
                      error = 'dependency did not succeed'
                WHERE t.run_id = %s AND t.state IN ('pending','ready')
                  AND EXISTS (
                      SELECT 1 FROM unnest(t.depends_on) d(node)
                      JOIN run_tasks p
                        ON p.run_id = t.run_id AND p.node = d.node
                       WHERE p.state IN ('failed','cancelled'))
                RETURNING t.id, t.node, t.module""",
            (run_id,),
        )
        skipped = cur.fetchall()
        cur.execute(
            """SELECT source_name, pipeline_name, pipeline_version
                 FROM runs WHERE id = %s""",
            (run_id,),
        )
        event_context = cur.fetchone()
        for task_id, node, module in skipped:
            append(
                cur, component="run", event="task.skipped", run_id=run_id,
                task_id=task_id, node=node, module=module,
                source_name=event_context[0], pipeline_name=event_context[1],
                pipeline_version=event_context[2],
                message="dependency did not succeed", level="warn",
            )
        cur.execute(
            """SELECT count(*) FILTER (
                        WHERE state IN ('pending','ready','running','blocked')),
                      count(*) FILTER (WHERE state = 'failed'),
                      count(*) FILTER (WHERE state = 'cancelled'), count(*)
                 FROM run_tasks WHERE run_id = %s""",
            (run_id,),
        )
        live, failed, cancelled, total = cur.fetchone()
        cur.execute(
            "SELECT state, cancel_requested, source_name, pipeline_name, "
            "pipeline_version FROM runs WHERE id = %s FOR UPDATE",
            (run_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(run_id)
        state, cancel_requested, source_name, pipeline_name, version = row
        if not live and total and state in ("queued", "running", "blocked"):
            final = (
                "failed" if failed else
                "cancelled" if cancelled or cancel_requested else "succeeded")
            cur.execute(
                """UPDATE runs
                      SET state = %s, finished_at = now(), updated_at = now(),
                          progress = %s
                    WHERE id = %s""",
                (final, Jsonb({"completed": total, "total": total}), run_id),
            )
            append(
                cur, component="run", event=f"run.{final}", run_id=run_id,
                source_name=source_name, pipeline_name=pipeline_name,
                pipeline_version=version,
                level="error" if final == "failed" else "info",
                data={"tasks": total, "failed": failed},
            )
            state = final
    conn.commit()
    return state


_RUN_COLS = """
r.id, r.source_id, r.source_name, r.pipeline_name, r.pipeline_revision_id,
r.pipeline_version, r.pipeline_hash, r.flow_name, r.source_snapshot,
r.effective_config, r.explicit_inputs, r.module_locks, r.trigger_type,
r.trigger_by, r.mode, r.priority, r.dedupe_key, r.idempotency_key, r.state,
r.cancel_requested, r.queued_at, r.started_at, r.finished_at, r.updated_at,
r.progress, r.stats, r.error
"""


def _run(row: Sequence[Any]) -> dict[str, Any]:
    keys = (
        "id", "source_id", "source_name", "pipeline_name",
        "pipeline_revision_id", "pipeline_version", "pipeline_hash", "flow_name",
        "source_snapshot", "effective_config", "explicit_inputs", "module_locks",
        "trigger_type", "trigger_by", "mode", "priority", "dedupe_key",
        "idempotency_key", "state", "cancel_requested", "queued_at",
        "started_at", "finished_at", "updated_at", "progress", "stats", "error",
    )
    out = dict(zip(keys, row))
    for key in ("queued_at", "started_at", "finished_at", "updated_at"):
        if out[key] is not None:
            out[key] = out[key].isoformat()
    return out


def list_runs(
    conn: psycopg.Connection,
    *,
    source: str | None = None,
    pipeline: str | None = None,
    state: str | None = None,
    before_id: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses, args = [], []
    for column, value in (
        ("r.source_name", source), ("r.pipeline_name", pipeline), ("r.state", state)):
        if value is not None:
            clauses.append(f"{column} = %s")
            args.append(value)
    if before_id is not None:
        clauses.append("r.id < %s")
        args.append(before_id)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    args.append(min(max(limit, 1), 200))
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_RUN_COLS} FROM runs r{where} ORDER BY r.id DESC LIMIT %s",
            args,
        )
        return [_run(row) for row in cur.fetchall()]


def get_run(
    conn: psycopg.Connection, run_id: int, *, include_spec: bool = False,
) -> dict[str, Any] | None:
    extra = ", r.frozen_spec" if include_spec else ""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_RUN_COLS}{extra} FROM runs r WHERE r.id = %s", (run_id,))
        row = cur.fetchone()
        if row is None:
            return None
        out = _run(row[:27])
        if include_spec:
            out["frozen_spec"] = row[27]
        cur.execute(
            """SELECT id, node, kind, module, module_version, module_digest,
                      executor, lane, config, depends_on, preconditions, captures,
                      state, priority, attempts, max_attempts, lease_worker,
                      lease_seconds, lease_expires_at, cursor, units_total, units_done,
                      units_failed, weight, stats, started_at, finished_at, error
                 FROM run_tasks WHERE run_id = %s ORDER BY id""",
            (run_id,),
        )
        task_keys = (
            "id", "node", "kind", "module", "module_version", "module_digest",
            "executor", "lane", "config", "depends_on", "preconditions",
            "captures", "state", "priority", "attempts", "max_attempts",
            "lease_worker", "lease_seconds", "lease_expires_at", "cursor", "units_total",
            "units_done", "units_failed", "weight", "stats", "started_at",
            "finished_at", "error",
        )
        out["tasks"] = []
        for task_row in cur.fetchall():
            task = dict(zip(task_keys, task_row))
            for key in ("lease_expires_at", "started_at", "finished_at"):
                if task[key]:
                    task[key] = task[key].isoformat()
            out["tasks"].append(task)
    return out


def cancel(conn: psycopg.Connection, run_id: int, *, by: str = "") -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE runs SET cancel_requested = true, updated_at = now()
                WHERE id = %s AND state IN ('queued','running','blocked')
                RETURNING source_name, pipeline_name, pipeline_version""",
            (run_id,),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """UPDATE run_tasks
                      SET state = 'cancelled', finished_at = now()
                    WHERE run_id = %s AND state IN ('pending','ready','blocked')
                    RETURNING id, node, module""",
                (run_id,),
            )
            for task_id, node, module in cur.fetchall():
                append(
                    cur, component="run", event="task.cancelled",
                    run_id=run_id, task_id=task_id, node=node, module=module,
                    source_name=row[0], pipeline_name=row[1],
                    pipeline_version=row[2], message=by, level="warn",
                )
            cur.execute(
                "UPDATE run_tasks SET yield_requested = true "
                "WHERE run_id = %s AND state = 'running'",
                (run_id,),
            )
            append(
                cur, component="run", event="run.cancel_requested", run_id=run_id,
                source_name=row[0], pipeline_name=row[1], pipeline_version=row[2],
                message=by, level="warn",
            )
    conn.commit()
    if row:
        advance(conn, run_id)
    return bool(row)


def rerun(conn: psycopg.Connection, run_id: int, *, priority: int | None = None) -> int:
    registry.load_custom(conn)
    historic = get_run(conn, run_id, include_spec=True)
    if historic is None:
        raise KeyError(run_id)
    if historic["mode"] == "reset":
        raise RunConflictError(
            "corpus reset operations cannot be re-run; request a new reset preview")
    locks = historic["module_locks"]
    unavailable: list[str] = []
    for name, lock in locks.items():
        if lock.get("executor") == "sandbox":
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT v.source_digest, v.approval_state
                         FROM module_versions v
                         JOIN module_definitions d ON d.id = v.module_id
                        WHERE d.name = %s AND v.version = %s""",
                    (name, int(lock["version"])),
                )
                row = cur.fetchone()
            if row is None or row[1] != "available" or row[0] != lock["digest"]:
                unavailable.append(name)
        elif (
            registry.get(name) is None
            or registry.implementation_digest(name) != lock["digest"]
        ):
            unavailable.append(name)
    if unavailable:
        raise RunConflictError(
            "module_revoked: historic Module unavailable or revoked: "
            + ", ".join(unavailable))
    flow_spec = historic["frozen_spec"]["flows"][historic["flow_name"]]
    compiled = {
        "flow": historic["flow_name"],
        "parameters": historic["effective_config"],
        "inputs": historic["explicit_inputs"],
        "boundaries": {
            "inputs": flow_spec.get("inputs") or [],
            "outputs": flow_spec.get("outputs") or [],
        },
        "module_locks": locks,
        "tasks": [{
            key: task[key]
            for key in (
                "node", "kind", "module", "module_version", "module_digest",
                "executor", "lane", "config", "depends_on", "preconditions",
                "captures", "max_attempts", "lease_seconds", "weight",
            )
        } for task in historic["tasks"] if task["executor"] != "platform"],
    }
    _apply_revision_locks(conn, compiled, locks)
    pipeline = {
        "id": historic["pipeline_revision_id"],
        "pipeline_name": historic["pipeline_name"],
        "version": historic["pipeline_version"],
        "spec_hash": historic["pipeline_hash"],
        "spec": historic["frozen_spec"],
    }
    source = historic["source_snapshot"]
    new_id = _insert(
        conn, pipeline=pipeline, compiled=compiled, source=source,
        trigger_type="rerun", trigger_by=f"run:{run_id}", mode=historic["mode"],
        priority=priority or historic["priority"], dedupe_key=None,
        idempotency_key=None,
    )
    assert new_id is not None
    return new_id


def outputs(conn: psycopg.Connection, run_id: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT boundary, value_type, value, size_bytes, checksum, created_at
                 FROM run_outputs WHERE run_id = %s
                 ORDER BY boundary""",
            (run_id,),
        )
        return [{
            "boundary": row[0], "type": row[1], "value": row[2],
            "size_bytes": row[3], "checksum": row[4],
            "created_at": row[5].isoformat(),
        } for row in cur.fetchall()]


def artifact(
    conn: psycopg.Connection, run_id: int, artifact_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT id, boundary, media_type, relative_path, size_bytes,
                      checksum, expires_at, created_at
                 FROM run_artifacts
                WHERE run_id = %s AND id = %s
                  AND (expires_at IS NULL OR expires_at > now())""",
            (run_id, artifact_id),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "id": row[0], "boundary": row[1], "media_type": row[2],
        "relative_path": row[3], "size_bytes": row[4], "checksum": row[5],
        "expires_at": row[6].isoformat() if row[6] else None,
        "created_at": row[7].isoformat(),
    }


__all__ = [
    "RUN_STATES",
    "RunConflictError",
    "advance",
    "artifact",
    "cancel",
    "get_run",
    "list_runs",
    "outputs",
    "rerun",
    "submit_pipeline",
    "submit_reset",
    "submit_source",
]
