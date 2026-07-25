"""Read and write the generic recipe-run lifecycle.

The worker owns state transitions. This module only submits a frozen recipe,
projects rows into API shapes, and requests cooperative cancellation.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

import psycopg

from windex.config import Settings
from windex.recipe import compile as recipe_compile
from windex.recipe import store
from windex.worker import dag

RUN_STATES = frozenset(
    {"queued", "running", "succeeded", "failed", "cancelled", "blocked"})


class ModulesUnavailable(ValueError):
    """A valid graph cannot execute on this build."""

    def __init__(self, modules: list[str]):
        self.modules = modules
        super().__init__(
            "recipe uses modules that are declared but not implemented: "
            + ", ".join(modules))

_RUN_COLS = (
    "id, recipe, recipe_version, source, spec_hash, trigger, trigger_by, params, "
    "mode, priority, dedupe_key, state, cancel_requested, queued_at, started_at, "
    "finished_at, updated_at, progress, stats, error"
)

_TASK_COLS = (
    "id, run_id, source, node, kind, module, lane, config, depends_on, "
    "preconditions, state, priority, attempts, max_attempts, lease_worker, "
    "lease_seconds, lease_expires_at, heartbeat_at, yield_requested, cursor, "
    "units_total, units_done, units_failed, weight, stats, started_at, "
    "finished_at, error"
)


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _mapping(columns: str, row) -> dict:
    names = [name.strip() for name in columns.split(",")]
    return {name: _json_value(value) for name, value in zip(names, row)}


def submit(conn: psycopg.Connection, *, recipe: str, settings: Settings,
           flow: str | None = None, params: Mapping[str, Any] | None = None,
           mode: str = "run", priority: int = 50,
           dedupe_key: str | None = None, trigger_by: str = "admin API") -> int | None:
    """Materialize config, freeze the recipe, and submit its DAG atomically."""
    got = store.get_recipe(conn, recipe)
    if got is None:
        raise KeyError(recipe)
    if not got["enabled"]:
        raise ValueError(f"recipe {recipe!r} is disabled")

    persisted = store.get_recipe_config(conn, recipe)
    merged = {**persisted, **dict(params or {})}
    tasks = recipe_compile.compile_tasks(
        got["spec"], flow=flow, settings=settings, values=merged)
    if not tasks:
        raise ValueError(f"recipe {recipe!r} flow has no tasks")
    unavailable = recipe_compile.unavailable_modules(tasks)
    if unavailable:
        raise ModulesUnavailable(unavailable)

    run_params = dict(merged)
    if flow:
        run_params["flow"] = flow
    return dag.submit_run(
        conn,
        recipe=recipe,
        source=got["source"],
        spec=got["spec"],
        tasks=tasks,
        trigger="manual",
        trigger_by=trigger_by,
        params=run_params,
        priority=priority,
        mode=mode,
        dedupe_key=dedupe_key,
        recipe_version=got["version"],
        spec_hash=got["spec_hash"],
    )


def list_runs(conn: psycopg.Connection, *, recipe: str | None = None,
              source: str | None = None, state: str | None = None,
              before_id: int | None = None, limit: int = 50) -> list[dict]:
    if state is not None and state not in RUN_STATES:
        raise ValueError(
            f"state must be one of: {', '.join(sorted(RUN_STATES))}")
    clauses, args = [], []
    if recipe:
        clauses.append("recipe = %s")
        args.append(recipe)
    if source:
        clauses.append("source = %s")
        args.append(source)
    if state:
        clauses.append("state = %s")
        args.append(state)
    if before_id is not None:
        clauses.append("id < %s")
        args.append(before_id)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    args.append(limit)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_RUN_COLS} FROM runs{where} ORDER BY id DESC LIMIT %s",
            args,
        )
        return [_mapping(_RUN_COLS, row) for row in cur.fetchall()]


def get_run(conn: psycopg.Connection, run_id: int,
            *, include_spec: bool = False) -> dict | None:
    cols = _RUN_COLS + (", spec" if include_spec else "")
    with conn.cursor() as cur:
        cur.execute(f"SELECT {cols} FROM runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        if row is None:
            return None
        out = _mapping(cols, row)
        cur.execute(
            f"SELECT {_TASK_COLS} FROM run_tasks "
            "WHERE run_id = %s ORDER BY id",
            (run_id,),
        )
        out["tasks"] = [
            _mapping(_TASK_COLS, task) for task in cur.fetchall()]
    return out


def list_events(conn: psycopg.Connection, run_id: int, *,
                after: int = 0, limit: int = 200) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT seq, run_id, task_id, ts, level, event, message, data
                 FROM run_events
                WHERE run_id = %s AND seq > %s
                ORDER BY seq LIMIT %s""",
            (run_id, after, limit),
        )
        return [
            _mapping(
                "seq, run_id, task_id, ts, level, event, message, data", row)
            for row in cur.fetchall()
        ]
