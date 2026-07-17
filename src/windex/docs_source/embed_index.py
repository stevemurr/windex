"""Embed staged programming-docs pages from clean parquet and upsert into Qdrant.

Mirrors the wiki/arxiv/smallweb embed step: dense vectors from the user's
Embedder, sparse BM25 from fastembed, stable uuid5 point ids, per-batch
Postgres commits, the dashboard pause check, and the runtime throughput
profile. Source 'docs' lands in the "docs" collection behind the docs_current
alias. The embedded text is title + page body; the payload carries the
framework (slug base), upstream version, and the docset's upstream license
attribution (plain text, truncated — the full string lives in ``docsets``).
"""

import concurrent.futures as cf

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from qdrant_client import QdrantClient
from qdrant_client import models as qm

from windex import db
from windex.ccnews.embed_index import point_id
from windex.config import Settings
from windex.embed import build_embedder, with_runtime_profile
from windex.index import qdrant as qidx

ATTRIBUTION_CHARS = 200


def _pending_refs(conn: psycopg.Connection, limit: int) -> dict[str, list[str]]:
    """text_ref → doc ids, for the oldest pending docs pages."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT text_ref, array_agg(id)
            FROM (
                SELECT text_ref, id FROM documents
                WHERE source = 'docs' AND status = 'deduped'
                ORDER BY created_at LIMIT %s
            ) t GROUP BY text_ref
            """,
            (limit,),
        )
        return dict(cur.fetchall())


def _embed_and_upsert(batch: list[dict], embedder, bm25, client, collection: str,
                      max_chars: int, throttle: float = 0.0) -> list[str]:
    """Runs in a worker thread: dense + sparse embed, then Qdrant upsert.
    Returns the doc ids. Postgres updates stay on the caller's thread."""
    import time as time_mod

    texts = [
        ((r["title"] + "\n\n") if r["title"] else "") + (r["text"] or "")[:max_chars]
        for r in batch
    ]
    dense = embedder.embed_batch(texts)
    if throttle:
        time_mod.sleep(throttle)  # leave the embedding server a gap for queries
    sparse = list(bm25.embed(texts))
    points = [
        qm.PointStruct(
            id=point_id(r["id"]),
            vector={
                qidx.DENSE: dense[i],
                qidx.SPARSE: qm.SparseVector(
                    indices=sparse[i].indices.tolist(),
                    values=sparse[i].values.tolist(),
                ),
            },
            payload={
                "doc_id": r["id"],
                "source": "docs",
                "url": r["url"],
                "title": r["title"],
                "snippet": (r["text"] or "")[:400],
                "framework": r["framework"],
                "version": r["version"],
                "attribution": (r["attribution"] or "")[:ATTRIBUTION_CHARS],
            },
        )
        for i, r in enumerate(batch)
    ]
    client.upsert(collection_name=collection, points=points)
    return [r["id"] for r in batch]


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    settings = with_runtime_profile(conn, settings)
    embedder = build_embedder(settings)
    from windex.index.sparse import bm25_model

    bm25 = bm25_model()
    client = QdrantClient(url=settings.qdrant_url, timeout=120)
    collection = qidx.ensure_collection(client, "docs", settings.embed_model, settings.embed_dim)

    max_chars = settings.embed_max_tokens * 4  # crude token→char bound
    total = 0
    refs = _pending_refs(conn, limit)
    with cf.ThreadPoolExecutor(max(settings.embed_concurrency, 1)) as pool:
        for text_ref, ids in refs.items():
            table = pq.read_table(settings.staging_dir / text_ref)
            table = table.filter(pc.is_in(table["id"], value_set=pa.array(ids)))
            rows = table.to_pylist()
            batches = [
                rows[start : start + settings.embed_batch_size]
                for start in range(0, len(rows), settings.embed_batch_size)
            ]
            futures = [
                pool.submit(_embed_and_upsert, b, embedder, bm25, client, collection,
                            max_chars, settings.embed_throttle_seconds)
                for b in batches
            ]
            # psycopg connections aren't thread-safe: commit progress here as each
            # worker finishes, so a crash loses at most the in-flight work.
            for i, fut in enumerate(cf.as_completed(futures)):
                done_ids = fut.result()
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE documents
                        SET status = 'embedded', embedded_model = %s, indexed_at = now()
                        WHERE id = ANY(%s)
                        """,
                        (settings.embed_model, done_ids),
                    )
                conn.commit()
                total += len(done_ids)
                if i % 5 == 4 and db.get_control(conn, "indexing", "running") == "paused":
                    for f in futures:
                        f.cancel()
                    return total
    return total
