"""Generic Pipeline state-store sinks."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg.types.json import Jsonb

from windex.config import Settings
from windex.modules.common import finish_batch, pending_batches, require_type
from windex.pipeline.ports import PartitionRecord
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_BATCH = 500


def _advance_refs(ctx: TaskContext, records: list[PartitionRecord]) -> None:
    state_namespace = ctx.state_namespace or ctx.search_name
    refs = {
        (record.ref.store, record.ref.key)
        for record in records
        if record.ref is not None and record.ref.store
    }
    if not refs:
        return
    with ctx.conn.cursor() as cur:
        for store, key in refs:
            cur.execute(
                """
                UPDATE source_units
                   SET ingested = upstream, status = 'done',
                       processed_at = now(), claimed_at = NULL,
                       last_run_id = %s, updated_at = now()
                 WHERE state_namespace = %s AND store = %s AND unit_key = %s
                """,
                (ctx.run_id, state_namespace, store, key),
            )


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
    state_namespace = ctx.state_namespace or ctx.search_name
    if policy == "skip":
        with ctx.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO source_units
                       (state_namespace, store, unit_key, upstream, stage, attrs, last_run_id)
                VALUES (%s, %s, %s, %s, coalesce(%s, 'pending'), %s, %s)
                ON CONFLICT (state_namespace, store, unit_key) DO NOTHING
                """,
                (state_namespace, record.store, record.key, Jsonb(record.upstream),
                 record.stage, Jsonb(record.payload), ctx.run_id),
            )
        return

    with ctx.conn.cursor() as cur:
        if policy == "increment":
            cur.execute(
                """
                SELECT attrs FROM source_units
                 WHERE state_namespace = %s AND store = %s AND unit_key = %s
                 FOR UPDATE
                """,
                (state_namespace, record.store, record.key),
            )
            row = cur.fetchone()
            attrs = _increment(dict(row[0] or {}) if row else {}, record.delta)
            attrs.update(record.payload)
        else:
            attrs = record.payload
        cur.execute(
            """
            INSERT INTO source_units
                   (state_namespace, store, unit_key, upstream, stage, attrs, last_run_id)
            VALUES (%s, %s, %s, %s, coalesce(%s, 'pending'), %s, %s)
            ON CONFLICT (state_namespace, store, unit_key) DO UPDATE
               SET upstream = EXCLUDED.upstream,
                   stage = coalesce(%s, source_units.stage),
                   attrs = source_units.attrs || EXCLUDED.attrs,
                   status = 'pending',
                   last_run_id = EXCLUDED.last_run_id,
                   updated_at = now()
            """,
            (state_namespace, record.store, record.key, Jsonb(record.upstream),
             record.stage, Jsonb(attrs), ctx.run_id, record.stage),
        )


def store_upsert(ctx: TaskContext) -> SliceResult:
    """Consume PartitionRecords into the Source's permanent state namespace."""
    store = str(ctx.config.get("store", ""))
    if not store:
        raise PermanentTaskError("store.upsert requires a store")
    policy = str(ctx.config.get("on_conflict", "merge"))
    if policy not in ("merge", "increment", "skip"):
        raise PermanentTaskError(f"store.upsert has unknown conflict policy {policy!r}")

    batches, more = pending_batches(ctx, limit=_BATCH)
    processed = []
    stored = 0
    for batch in batches:
        for value in batch.values:
            record = require_type(value, PartitionRecord, ctx.module)
            if record.payload.get("_coverage_only"):
                continue
            if record.store and record.store != store:
                raise PermanentTaskError(
                    f"store.upsert configured for {store!r}, "
                    f"got record for {record.store!r}")
            if not record.store:
                record = PartitionRecord(
                    store=store,
                    key=record.key,
                    ref=record.ref,
                    upstream=record.upstream,
                    stage=record.stage,
                    payload=record.payload,
                    delta=record.delta,
                    absent_ok=record.absent_ok,
                )
            if ctx.mode != "dry_run":
                _write(ctx, record, policy)
            stored += 1
        if ctx.mode != "dry_run":
            _advance_refs(ctx, records=[
                require_type(value, PartitionRecord, ctx.module)
                for value in batch.values
            ])
        finish_batch(ctx, batch)
        processed.append(batch)
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {"last": processed[-1].key, "store": store})
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(batches),
        stats={"stored": stored, "store": store, "dry_run": ctx.mode == "dry_run"},
    )


_README_SCHEMA = pa.schema([
    ("repo_id", pa.int64()),
    ("full_name", pa.string()),
    ("readme", pa.string()),
])


def _stage_readmes(ctx: TaskContext, batch_key: str,
                   records: list[PartitionRecord]) -> Path | None:
    rows = [
        (
            int(record.payload["repo_id"]),
            str(record.payload.get("full_name") or ""),
            str(record.payload["readme"]),
        )
        for record in records
        if record.stage == "hydrated" and record.payload.get("readme")
    ]
    if not rows:
        return None
    digest = hashlib.sha256(
        f"{ctx.run_id}:{ctx.task_id}:{batch_key}".encode()).hexdigest()[:24]
    directory = Settings().repos_staging_dir / "readme"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"pipeline-{ctx.run_id}-{digest}.parquet"
    temp = path.with_suffix(".parquet.tmp")
    table = pa.table({
        "repo_id": pa.array([row[0] for row in rows], pa.int64()),
        "full_name": [row[1] for row in rows],
        "readme": [row[2] for row in rows],
    }, schema=_README_SCHEMA)
    pq.write_table(table, temp)
    os.replace(temp, path)
    return path


def _write_repo(ctx: TaskContext, record: PartitionRecord) -> None:
    payload = record.payload
    try:
        repo_id = int(payload.get("repo_id", record.key))
    except (TypeError, ValueError) as exc:
        raise PermanentTaskError(
            f"store.repos received invalid repo id {record.key!r}") from exc
    full_name = str(payload.get("full_name") or "")
    if not full_name:
        raise PermanentTaskError(
            f"store.repos record {repo_id} has no full_name")
    delta = int(record.delta.get("star_events", 0))
    params = (
        ctx.source_id,
        repo_id,
        full_name,
        payload.get("stars"),
        delta,
        payload.get("description"),
        payload.get("topics") or [],
        payload.get("primary_language"),
        payload.get("default_branch"),
        payload.get("pushed_at"),
        record.stage or "candidate",
        bool(payload.get("readme")),
    )
    statement = """
        INSERT INTO repos
               (source_id, repo_id, full_name, stars, star_events, description, topics,
                primary_language, default_branch, pushed_at, status,
                readme_fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                CASE WHEN %s THEN now() ELSE NULL END)
        ON CONFLICT (source_id, repo_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            stars = coalesce(EXCLUDED.stars, repos.stars),
            star_events = coalesce(repos.star_events, 0)
                          + EXCLUDED.star_events,
            description = coalesce(EXCLUDED.description, repos.description),
            topics = CASE WHEN cardinality(EXCLUDED.topics) > 0
                          THEN EXCLUDED.topics ELSE repos.topics END,
            primary_language = coalesce(
                EXCLUDED.primary_language, repos.primary_language),
            default_branch = coalesce(
                EXCLUDED.default_branch, repos.default_branch),
            pushed_at = coalesce(EXCLUDED.pushed_at, repos.pushed_at),
            status = coalesce(EXCLUDED.status, repos.status),
            readme_fetched_at = coalesce(
                EXCLUDED.readme_fetched_at, repos.readme_fetched_at)
    """
    with ctx.conn.cursor() as cur:
        cur.execute("SAVEPOINT pipeline_repo")
        try:
            cur.execute(statement, params)
        except psycopg.errors.UniqueViolation:
            cur.execute("ROLLBACK TO SAVEPOINT pipeline_repo")
            cur.execute(
                "UPDATE repos SET full_name = full_name || '#stale:' || repo_id "
                "WHERE source_id = %s AND full_name = %s AND repo_id <> %s",
                (ctx.source_id, full_name, repo_id),
            )
            cur.execute(statement, params)
        cur.execute("RELEASE SAVEPOINT pipeline_repo")


def store_repos(ctx: TaskContext) -> SliceResult:
    """Consume repository assertions into the indexed wide table."""
    batches, more = pending_batches(ctx, limit=200)
    processed = []
    stored = 0
    readmes = 0
    for batch in batches:
        records = [
            require_type(value, PartitionRecord, ctx.module)
            for value in batch.values
        ]
        content = [
            record for record in records
            if not record.payload.get("_coverage_only")
        ]
        if ctx.mode != "dry_run":
            _stage_readmes(ctx, batch.key, content)
            readmes += sum(
                bool(record.payload.get("readme")) for record in content)
            for record in content:
                _write_repo(ctx, record)
            _advance_refs(ctx, records)
        finish_batch(ctx, batch)
        processed.append(batch)
        stored += len(content)
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {
            "last": processed[-1].key, "repos": stored, "readmes": readmes,
        })
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(batches),
        stats={"stored": stored, "readmes": readmes,
               "dry_run": ctx.mode == "dry_run"},
    )
