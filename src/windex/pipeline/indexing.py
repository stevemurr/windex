"""Platform-owned searchable-output continuation."""

from __future__ import annotations

import pyarrow.parquet as pq
from psycopg.types.json import Jsonb
from qdrant_client import QdrantClient
from qdrant_client import models as qm

from windex.config import Settings
from windex.embed import build_embedder
from windex.embed.base import embed_isolating
from windex.embed.pipeline import point_id
from windex.index import qdrant as qidx
from windex.index.sparse import bm25_model
from windex.modules.load import _vector_metadata
from windex.sanitize import strip_smuggled
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_BATCH = 256


def _search_text(row: dict, max_chars: int) -> str:
    return strip_smuggled(
        ((str(row.get("title") or "") + "\n\n")
         if row.get("title") else "")
        + str(
            row.get("text") or row.get("abstract")
            or row.get("story_text") or "")
    )[:max_chars]


def _point_payload(ctx: TaskContext, row: dict, text: str) -> dict:
    payload = _vector_metadata(row)
    payload.update({
        "doc_id": row["id"],
        "source": ctx.search_name,
        "snippet": text[:400],
    })
    return payload


def _rows(ctx: TaskContext, ids: list[str], refs: list[str]) -> dict[str, dict]:
    wanted = set(ids)
    result: dict[str, dict] = {}
    for reference in sorted(set(refs)):
        root = Settings().staging_dir.resolve()
        path = (root / reference).resolve()
        if root not in path.parents or not path.is_file():
            raise PermanentTaskError(f"staged text is unavailable: {reference}")
        table = pq.read_table(path, filters=[("id", "in", sorted(wanted))])
        for row in table.to_pylist():
            if row.get("id") in wanted:
                result[row["id"]] = row
    return result


def _record_ownership(
    ctx: TaskContext,
    *,
    collection: str,
    alias: str,
    model: str,
) -> None:
    """Record the exact Qdrant resources owned by this Source generation."""
    resources = (
        (
            "qdrant_collection",
            collection,
            {
                "source_id": ctx.source_id,
                "source_name": ctx.source_name,
                "model": model,
            },
        ),
        (
            "qdrant_alias",
            alias,
            {
                "source_id": ctx.source_id,
                "source_name": ctx.source_name,
                "collection": collection,
            },
        ),
    )
    with ctx.conn.cursor() as cur:
        for resource_type, resource_name, metadata in resources:
            # A collection key is immutable and unique to one Source. Keep one
            # current ownership row when corpus reset advances its generation.
            cur.execute(
                """DELETE FROM storage_ownership
                    WHERE resource_type = %s AND resource_name = %s
                      AND generation <> %s""",
                (resource_type, resource_name, ctx.source_generation),
            )
            cur.execute(
                """INSERT INTO storage_ownership
                       (generation, resource_type, resource_name, metadata)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (generation, resource_type, resource_name)
                   DO UPDATE SET metadata = EXCLUDED.metadata""",
                (
                    ctx.source_generation,
                    resource_type,
                    resource_name,
                    Jsonb(metadata),
                ),
            )


def _remove_stale_vectors(
    ctx: TaskContext,
    client: QdrantClient,
    collection: str,
    stale: list[str],
) -> int:
    """Remove vector rows whose document ledger entry is no longer searchable."""
    if not stale:
        return 0
    client.delete(
        collection_name=collection,
        points_selector=qm.PointIdsList(
            points=[point_id(doc_id) for doc_id in stale],
        ),
        wait=True,
    )
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            UPDATE documents
               SET embedded_model = NULL, indexed_at = NULL,
                   updated_at = now()
             WHERE id = ANY(%s) AND status <> 'searchable'
            """,
            (stale,),
        )
    ctx.conn.commit()
    return len(stale)


def _refresh_metadata(
    ctx: TaskContext,
    client: QdrantClient,
    collection: str,
    pending: list[tuple[str, str, str]],
    settings: Settings,
) -> int:
    """Overwrite point payloads without re-embedding unchanged text.

    Each row carries the metadata hash selected with its immutable parquet
    reference. The conditional acknowledgement prevents a concurrent newer
    ledger write from being marked indexed by an older payload refresh.
    """
    if not pending:
        return 0
    ids = [row[0] for row in pending]
    rows = _rows(ctx, ids, [row[1] for row in pending])
    missing = sorted(set(ids) - set(rows))
    if missing:
        raise PermanentTaskError(
            f"{len(missing)} metadata refresh documents are missing "
            "from their artifact")
    operations = []
    for doc_id, _reference, _metadata_hash in pending:
        row = rows[doc_id]
        operations.append(qm.OverwritePayloadOperation(
            overwrite_payload=qm.SetPayload(
                payload=_point_payload(
                    ctx,
                    row,
                    _search_text(row, settings.embed_max_tokens * 4),
                ),
                points=[point_id(doc_id)],
            ),
        ))
    client.batch_update_points(
        collection_name=collection,
        update_operations=operations,
        wait=True,
    )
    with ctx.conn.cursor() as cur:
        cur.executemany(
            """
            UPDATE documents
               SET indexed_metadata_hash = %s, updated_at = now()
             WHERE id = %s
               AND metadata_hash = %s
               AND status = 'searchable'
            """,
            [
                (metadata_hash, doc_id, metadata_hash)
                for doc_id, _reference, metadata_hash in pending
            ],
        )
    ctx.conn.commit()
    return len(pending)


def platform_index(ctx: TaskContext) -> SliceResult:
    """Make this Source's staged documents queryable before the Run succeeds."""
    if (
        ctx.source_id is None
        or not ctx.collection_key
        or ctx.source_generation <= 0
    ):
        raise PermanentTaskError(
            "platform.index requires a frozen Source deployment binding")
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
              FROM documents
             WHERE source_id = %s
               AND status <> 'searchable'
               AND embedded_model IS NOT NULL
             ORDER BY id
             LIMIT %s
            """,
            (ctx.source_id, _BATCH),
        )
        stale = [row[0] for row in cur.fetchall()]
        cur.execute(
            """
            SELECT id, text_ref, metadata_hash
              FROM documents
             WHERE source_id = %s
               AND status = 'searchable'
               AND text_ref IS NOT NULL
               AND metadata_hash IS DISTINCT FROM indexed_metadata_hash
             ORDER BY updated_at, id
             LIMIT %s
             FOR UPDATE SKIP LOCKED
            """,
            (ctx.source_id, _BATCH),
        )
        metadata_pending = cur.fetchall()
        cur.execute(
            """SELECT id, text_ref
                 FROM documents
                WHERE source_id = %s AND owner_run_id = %s
                  AND status = 'staged'
                  AND text_ref IS NOT NULL
                ORDER BY created_at, id LIMIT %s
                FOR UPDATE SKIP LOCKED""",
            (ctx.source_id, ctx.run_id, _BATCH),
        )
        pending = cur.fetchall()
        if pending:
            cur.execute(
                "UPDATE documents SET status = 'embedding', updated_at = now() "
                "WHERE id = ANY(%s)",
                ([row[0] for row in pending],),
            )
    ctx.conn.commit()
    if not pending and not stale and not metadata_pending:
        return SliceResult(
            exhausted=True, units_total=ctx.units_done if hasattr(ctx, "units_done") else -1,
            stats={"searchable": 0, "metadata_refreshed": 0})

    ids = [row[0] for row in pending]
    settings = Settings()
    embedder = None
    client = None
    searchable: list[str] = []
    failed: list[str] = []
    removed = 0
    refreshed = 0
    try:
        client = QdrantClient(url=settings.qdrant_url, timeout=120)
        collection = qidx.ensure_collection(
            client, ctx.collection_key, settings.embed_model, settings.embed_dim)
        _record_ownership(
            ctx,
            collection=collection,
            alias=qidx.alias_name(ctx.collection_key),
            model=settings.embed_model,
        )
        ctx.conn.commit()
        removed = _remove_stale_vectors(ctx, client, collection, stale)
        refreshed = _refresh_metadata(
            ctx, client, collection, metadata_pending, settings)
        if not pending:
            ctx.heartbeat(0, 0, {
                "searchable": 0,
                "failed": 0,
                "vectors_removed": removed,
                "metadata_refreshed": refreshed,
            })
            return SliceResult(
                units_done=refreshed,
                exhausted=(
                    len(stale) < _BATCH
                    and len(metadata_pending) < _BATCH
                ),
                units_total=(
                    ctx.units_done if hasattr(ctx, "units_done") else -1),
                stats={
                    "searchable": 0,
                    "vectors_removed": removed,
                    "metadata_refreshed": refreshed,
                })

        rows = _rows(ctx, ids, [row[1] for row in pending])
        missing = sorted(set(ids) - set(rows))
        if missing:
            with ctx.conn.cursor() as cur:
                cur.execute(
                    "UPDATE documents SET status = 'failed', updated_at = now() "
                    "WHERE id = ANY(%s)",
                    (missing,),
                )
            ctx.conn.commit()
            raise PermanentTaskError(
                f"{len(missing)} staged documents are missing from their artifact")

        embedder = build_embedder(settings, bulk=True)
        ordered = [rows[doc_id] for doc_id in ids]
        texts = [
            _search_text(row, settings.embed_max_tokens * 4)
            for row in ordered
        ]
        dense, accepted = embed_isolating(embedder, texts)
        sparse = list(bm25_model().embed([
            text for text, okay in zip(texts, accepted) if okay]))
        points = []
        sparse_index = 0
        for position, (row, vector, okay) in enumerate(
                zip(ordered, dense, accepted)):
            doc_id = row["id"]
            if not okay:
                failed.append(doc_id)
                continue
            payload = _point_payload(ctx, row, texts[position])
            item = sparse[sparse_index]
            sparse_index += 1
            points.append(qm.PointStruct(
                id=point_id(doc_id),
                vector={
                    qidx.DENSE: vector,
                    qidx.SPARSE: qm.SparseVector(
                        indices=item.indices.tolist(), values=item.values.tolist()),
                },
                payload=payload,
            ))
            searchable.append(doc_id)
        if points:
            client.upsert(collection_name=collection, points=points, wait=True)
        with ctx.conn.cursor() as cur:
            if searchable:
                cur.execute(
                    """UPDATE documents
                          SET status = 'searchable', embedded_model = %s,
                              indexed_at = now(),
                              indexed_metadata_hash = metadata_hash,
                              updated_at = now()
                        WHERE id = ANY(%s)""",
                    (settings.embed_model, searchable),
                )
            if failed:
                cur.execute(
                    "UPDATE documents SET status = 'failed', updated_at = now() "
                    "WHERE id = ANY(%s)",
                    (failed,),
                )
        ctx.conn.commit()
    except Exception:
        ctx.conn.rollback()
        with ctx.conn.cursor() as cur:
            cur.execute(
                "UPDATE documents SET status = 'staged', updated_at = now() "
                "WHERE id = ANY(%s) AND status = 'embedding'",
                (ids,),
            )
        ctx.conn.commit()
        raise
    finally:
        if embedder is not None:
            embedder.close()
        if client is not None:
            client.close()
    ctx.heartbeat(len(searchable), len(failed), {
        "staged": len(ids), "searchable": len(searchable), "failed": len(failed),
        "vectors_removed": removed, "metadata_refreshed": refreshed})
    return SliceResult(
        units_done=len(searchable) + refreshed, units_failed=len(failed),
        exhausted=(
            len(pending) < _BATCH
            and len(stale) < _BATCH
            and len(metadata_pending) < _BATCH
        ),
        stats={
            "searchable": len(searchable),
            "failed": len(failed),
            "vectors_removed": removed,
            "metadata_refreshed": refreshed,
        })


__all__ = ["platform_index"]
