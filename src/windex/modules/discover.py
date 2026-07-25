"""Generic discover-node implementations.

Discover runners are roots: they create durable ``WorkUnit`` values rather than
consuming another task's output. They never advance ``source_units.ingested``;
only a clean terminal load may do that.
"""

from __future__ import annotations

import csv
import io
from typing import Any

from psycopg import sql
from psycopg.types.json import Jsonb

from windex.recipe.ports import PartitionRef, WorkUnit
from windex.recipe.wire import encode_many
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext


def _payload(raw: Any) -> dict[str, str]:
    if raw in (None, ""):
        return {}
    if not isinstance(raw, str):
        raise PermanentTaskError("static.once payload must be a comma-separated string")
    try:
        parts = next(csv.reader(io.StringIO(raw), skipinitialspace=True))
    except csv.Error as exc:
        raise PermanentTaskError(f"static.once payload is invalid CSV: {exc}") from exc
    result: dict[str, str] = {}
    for part in parts:
        key, sep, value = part.partition("=")
        key = key.strip()
        if not sep or not key:
            raise PermanentTaskError(
                f"static.once payload item {part!r} must be key=value")
        result[key] = value.strip()
    return result


def static_once(ctx: TaskContext) -> SliceResult:
    """Emit one stable work unit, exactly once even after a slice replay."""
    key = str(ctx.config.get("key", "once"))
    with ctx.conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM task_units WHERE task_id = %s AND unit_key = %s LIMIT 1",
            (ctx.task_id, key),
        )
        if cur.fetchone() is not None:
            ctx.conn.commit()
            return SliceResult(exhausted=True, units_total=1)
        unit = WorkUnit(
            ref=PartitionRef(store="", key=key),
            payload=_payload(ctx.config.get("payload", "")),
            epoch=ctx.run_id,
        )
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, finished_at)
            VALUES (%s, %s, %s, 'done', %s, now())
            """,
            (ctx.run_id, ctx.task_id, key, Jsonb(encode_many([unit]))),
        )
    ctx.conn.commit()
    ctx.heartbeat(1, 0, {"last": key})
    return SliceResult(
        units_done=1,
        exhausted=True,
        units_total=1,
        stats={"emitted": 1},
    )


_ORDER = {
    "key": sql.SQL("u.unit_key"),
    "ord": sql.SQL("u.ord NULLS LAST, u.unit_key"),
    "processed_at": sql.SQL("u.processed_at NULLS FIRST, u.unit_key"),
    "stars_desc": sql.SQL(
        "coalesce((u.attrs->>'stars')::bigint, 0) DESC, u.unit_key"),
}


def _predicate(config: dict) -> tuple[sql.Composable, list[Any]]:
    name = config.get("predicate", "token_moved")
    if name == "unseen":
        return sql.SQL("u.ingested IS NULL"), []
    if name == "token_moved":
        return sql.SQL("u.upstream IS DISTINCT FROM u.ingested"), []
    if name == "stage_in":
        stages = [s.strip() for s in str(config.get("stages", "")).split(",") if s.strip()]
        if not stages:
            raise PermanentTaskError("state.pending stage_in requires at least one stage")
        return sql.SQL("u.stage = ANY(%s)"), [stages]
    if name == "rearm":
        days = int(config.get("rearm_days", 7))
        return (
            sql.SQL(
                "(u.ingested IS NULL OR u.processed_at IS NULL "
                "OR u.processed_at < now() - (%s * interval '1 day'))"),
            [days],
        )
    if name == "rotate":
        return sql.SQL("TRUE"), []
    raise PermanentTaskError(f"state.pending has unknown predicate {name!r}")


def state_pending(ctx: TaskContext) -> SliceResult:
    """Select one bounded slice of pending permanent store rows.

    The task's own emitted unit keys are the run-local snapshot. Replaying after
    a commit excludes those rows, while a later run may select them again until
    a successful load advances ``ingested``.
    """
    store = str(ctx.config.get("store", ""))
    if not store:
        raise PermanentTaskError("state.pending requires a store")
    source = ctx.recipe or ctx.source
    batch = int(ctx.config.get("batch", 50))
    order_name = str(ctx.config.get("order", "ord"))
    order = _ORDER.get(order_name)
    if order is None:
        raise PermanentTaskError(f"state.pending has unknown order {order_name!r}")
    pending, args = _predicate(ctx.config)
    claim = str(ctx.config.get("claim", "none"))
    if claim not in ("none", "lease"):
        raise PermanentTaskError(f"state.pending has unknown claim policy {claim!r}")
    stale = int(ctx.config.get("stale_minutes", 60))
    lease = (
        sql.SQL(
            "AND (u.status <> 'processing' OR u.claimed_at IS NULL "
            "OR u.claimed_at < now() - (%s * interval '1 minute'))")
        if claim == "lease" else sql.SQL("")
    )
    query = sql.SQL(
        """
        SELECT u.unit_key, u.upstream, u.attempts, u.attrs
          FROM source_units u
         WHERE u.source = %s AND u.store = %s
           AND {pending}
           {lease}
           AND NOT EXISTS (
                 SELECT 1 FROM task_units t
                  WHERE t.task_id = %s AND t.unit_key = u.unit_key)
         ORDER BY {order}
         LIMIT %s
        """).format(pending=pending, lease=lease, order=order)
    params: list[Any] = [source, store, *args]
    if claim == "lease":
        params.append(stale)
    params.extend([ctx.task_id, batch + 1])

    with ctx.conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        selected = []
        for key, upstream, attempts, attrs in rows[:batch]:
            attrs = dict(attrs or {})
            unit = WorkUnit(
                ref=PartitionRef(
                    store=store,
                    key=key,
                    id_scope=attrs.get("id_scope"),
                ),
                payload=attrs,
                upstream=dict(upstream or {}),
                attempt=int(attempts),
                epoch=ctx.run_id,
            )
            cur.execute(
                """
                INSERT INTO task_units
                       (run_id, task_id, unit_key, state, outputs, finished_at)
                VALUES (%s, %s, %s, 'done', %s, now())
                """,
                (ctx.run_id, ctx.task_id, key, Jsonb(encode_many([unit]))),
            )
            selected.append((key, upstream, attempts, attrs))
            if ctx.should_yield():
                break
        if selected:
            keys = [row[0] for row in selected]
            if claim == "lease":
                cur.execute(
                    """
                    UPDATE source_units
                       SET status = 'processing', claimed_at = now(),
                           last_run_id = %s, updated_at = now()
                     WHERE source = %s AND store = %s AND unit_key = ANY(%s)
                    """,
                    (ctx.run_id, source, store, keys),
                )
            else:
                cur.execute(
                    """
                    UPDATE source_units
                       SET last_run_id = %s, updated_at = now()
                     WHERE source = %s AND store = %s AND unit_key = ANY(%s)
                    """,
                    (ctx.run_id, source, store, keys),
                )
    ctx.conn.commit()
    done = len(selected)
    if done:
        ctx.heartbeat(done, 0, {"last": selected[-1][0], "store": store})
    return SliceResult(
        units_done=done,
        exhausted=len(selected) == len(rows),
        stats={"emitted": done, "store": store},
    )
