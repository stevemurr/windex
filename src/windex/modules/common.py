"""Shared mechanics for non-root recipe module runners.

Values crossing a DAG edge are durable, but document bodies do not belong in
Postgres. Small batches stay inline in ``task_units.outputs``; large batches are
written as checksummed gzip artifacts under the staging tier and the row stores
only a bounded reference. One task unit remains one atomic upstream batch, so a
100k-document extractor does not create 100k scheduler-detail rows.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TypeVar

from psycopg import sql
from psycopg.types.json import Jsonb

from windex.config import Settings
from windex.recipe import wire
from windex.recipe.ports import RawBlob
from windex.worker.protocol import PermanentTaskError, TaskContext

T = TypeVar("T", bound=wire.WireValue)
_INLINE_BYTES = 256_000
_ARTIFACT_TYPE = "_WireArtifact"


@dataclass(frozen=True)
class InputBatch:
    """One upstream task unit and all values it emitted."""

    key: str
    values: tuple[wire.WireValue, ...]


def _artifact_root() -> Path:
    root = Settings().staging_dir / "_recipe_runs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _artifact_path(ctx: TaskContext, key: str) -> Path:
    digest = hashlib.sha256(f"{ctx.task_id}:{key}".encode()).hexdigest()
    return _artifact_root() / str(ctx.run_id) / str(ctx.task_id) / f"{digest}.json.gz"


def _store_outputs(ctx: TaskContext, key: str,
                   outputs: list[wire.WireValue]) -> list[dict]:
    encoded = wire.encode_many(outputs)
    raw = json.dumps(encoded, separators=(",", ":"), ensure_ascii=False).encode()
    if len(raw) <= _INLINE_BYTES:
        return encoded

    path = _artifact_path(ctx, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, prefix=".wire-", delete=False) as tmp:
        temp = Path(tmp.name)
    try:
        with gzip.open(temp, "wb", compresslevel=6) as stream:
            stream.write(raw)
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)
    relative = path.relative_to(_artifact_root())
    return [{
        "type": _ARTIFACT_TYPE,
        "path": relative.as_posix(),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "count": len(encoded),
    }]


def _load_outputs(stored) -> tuple[wire.WireValue, ...]:
    if (isinstance(stored, list) and len(stored) == 1
            and isinstance(stored[0], dict)
            and stored[0].get("type") == _ARTIFACT_TYPE):
        ref = stored[0]
        root = _artifact_root().resolve()
        path = (root / str(ref.get("path", ""))).resolve()
        if not path.is_relative_to(root):
            raise PermanentTaskError("wire artifact escaped the staging root")
        try:
            with gzip.open(path, "rb") as stream:
                raw = stream.read()
        except OSError as exc:
            raise RuntimeError(f"cannot read wire artifact {path}: {exc}") from exc
        if hashlib.sha256(raw).hexdigest() != ref.get("sha256"):
            raise RuntimeError(f"wire artifact checksum mismatch: {path}")
        try:
            stored = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"invalid wire artifact {path}: {exc}") from exc
        if len(stored) != int(ref.get("count", -1)):
            raise RuntimeError(f"wire artifact count mismatch: {path}")
    return tuple(wire.decode_many(stored))


def pending_batches(ctx: TaskContext, *, limit: int) -> tuple[list[InputBatch], bool]:
    """Read unconsumed upstream task-unit batches from every dependency."""
    query = sql.SQL(
        """
        WITH upstream AS (
            SELECT p.id AS upstream_task_id,
                   u.id AS upstream_unit_id,
                   u.outputs,
                   p.id::text || ':' || u.id::text AS lineage
              FROM run_tasks current
              CROSS JOIN LATERAL unnest(current.depends_on) AS dep(node)
              JOIN run_tasks p
                ON p.run_id = current.run_id AND p.node = dep.node
              JOIN task_units u
                ON u.task_id = p.id AND u.state = 'done'
             WHERE current.id = %s
        )
        SELECT lineage, outputs
          FROM upstream i
         WHERE NOT EXISTS (
               SELECT 1 FROM task_units consumed
                WHERE consumed.task_id = %s AND consumed.unit_key = i.lineage)
         ORDER BY upstream_task_id, upstream_unit_id
         LIMIT %s
        """)
    with ctx.conn.cursor() as cur:
        cur.execute(query, (ctx.task_id, ctx.task_id, limit + 1))
        rows = cur.fetchall()
    batches = [
        InputBatch(key=key, values=_load_outputs(outputs))
        for key, outputs in rows[:limit]
    ]
    return batches, len(rows) > limit


def require_type(value: wire.WireValue, expected: type[T], module: str) -> T:
    if not isinstance(value, expected):
        raise PermanentTaskError(
            f"{module} received {type(value).__name__}, "
            f"expected {expected.__name__}")
    return value


def finish_batch(ctx: TaskContext, batch: InputBatch, *,
                 outputs: list[wire.WireValue] | None = None,
                 counts: dict | None = None) -> None:
    """Atomically record a consumed upstream batch and its durable outputs."""
    stored = _store_outputs(ctx, batch.key, outputs or [])
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, counts, finished_at)
            VALUES (%s, %s, %s, 'done', %s, %s, now())
            """,
            (ctx.run_id, ctx.task_id, batch.key, Jsonb(stored),
             Jsonb(counts or {})),
        )


def blob_bytes(blob: RawBlob) -> bytes:
    """Read a RawBlob regardless of whether its fetcher spooled or inlined it."""
    if blob.body is not None and blob.path is not None:
        raise PermanentTaskError("RawBlob has both body and path")
    if blob.body is not None:
        return blob.body
    if blob.path is not None:
        try:
            return Path(blob.path).read_bytes()
        except OSError as exc:
            raise RuntimeError(f"cannot read fetched artifact {blob.path}: {exc}") from exc
    raise PermanentTaskError("RawBlob has neither body nor path")


def downstream_store(ctx: TaskContext) -> str:
    """The one store written by the collect node immediately downstream."""
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT config->>'store'
              FROM run_tasks
             WHERE run_id = %s AND %s = ANY(depends_on)
               AND kind = 'collect' AND config ? 'store'
            """,
            (ctx.run_id, ctx.node),
        )
        stores = [row[0] for row in cur.fetchall() if row[0]]
    if len(stores) != 1:
        raise PermanentTaskError(
            f"{ctx.module} must feed exactly one configured store, found {stores}")
    return stores[0]
