"""Generic recipe store sinks."""

from __future__ import annotations

from psycopg.types.json import Jsonb

from windex.modules.common import finish_input, pending_inputs, require_type
from windex.recipe.ports import PartitionRecord
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_BATCH = 500


def _increment(base: dict, delta: dict) -> dict:
    merged = dict(base)
    for key, value in delta.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise PermanentTaskError(
                f"store.upsert delta {key!r} must be numeric")
        previous = merged.get(key, 0)
        if not isinstance(previous, (int, float)) or isinstance(previous, bool):
            raise PermanentTaskError(
                f"store.upsert cannot increment non-numeric attr {key!r}")
        merged[key] = previous + value
    return merged


def _write(ctx: TaskContext, record: PartitionRecord, policy: str) -> None:
    source = ctx.recipe or ctx.source
    if policy == "skip":
        with ctx.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO source_units
                       (source, store, unit_key, upstream, stage, attrs, last_run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, store, unit_key) DO NOTHING
                """,
                (source, record.store, record.key, Jsonb(record.upstream),
                 record.stage, Jsonb(record.payload), ctx.run_id),
            )
        return

    with ctx.conn.cursor() as cur:
        if policy == "increment":
            cur.execute(
                """
                SELECT attrs FROM source_units
                 WHERE source = %s AND store = %s AND unit_key = %s
                 FOR UPDATE
                """,
                (source, record.store, record.key),
            )
            row = cur.fetchone()
            attrs = _increment(dict(row[0] or {}) if row else {}, record.delta)
            attrs.update(record.payload)
        else:
            attrs = record.payload
        cur.execute(
            """
            INSERT INTO source_units
                   (source, store, unit_key, upstream, stage, attrs, last_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, store, unit_key) DO UPDATE
               SET upstream = EXCLUDED.upstream,
                   stage = coalesce(EXCLUDED.stage, source_units.stage),
                   attrs = source_units.attrs || EXCLUDED.attrs,
                   status = 'pending',
                   last_run_id = EXCLUDED.last_run_id,
                   updated_at = now()
            """,
            (source, record.store, record.key, Jsonb(record.upstream),
             record.stage, Jsonb(attrs), ctx.run_id),
        )


def store_upsert(ctx: TaskContext) -> SliceResult:
    """Consume PartitionRecords into the recipe's permanent store namespace."""
    store = str(ctx.config.get("store", ""))
    if not store:
        raise PermanentTaskError("store.upsert requires a store")
    policy = str(ctx.config.get("on_conflict", "merge"))
    if policy not in ("merge", "increment", "skip"):
        raise PermanentTaskError(f"store.upsert has unknown conflict policy {policy!r}")

    items, more = pending_inputs(ctx, limit=_BATCH)
    processed = []
    for item in items:
        record = require_type(item, PartitionRecord, ctx.module)
        if record.store and record.store != store:
            raise PermanentTaskError(
                f"store.upsert configured for {store!r}, got record for {record.store!r}")
        if not record.store:
            record = PartitionRecord(
                store=store,
                key=record.key,
                upstream=record.upstream,
                stage=record.stage,
                payload=record.payload,
                delta=record.delta,
                absent_ok=record.absent_ok,
            )
        if ctx.mode != "dry_run":
            _write(ctx, record, policy)
        finish_input(ctx, item)
        processed.append(item)
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {"last": processed[-1].key, "store": store})
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(items),
        stats={"stored": done, "store": store, "dry_run": ctx.mode == "dry_run"},
    )
