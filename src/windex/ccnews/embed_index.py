"""Embed deduped news docs from clean parquet and upsert into Qdrant.

Dense vectors come from the user's Embedder; sparse BM25 from fastembed.
Point ids are uuid5 of the stable doc id (Qdrant requires uuid/int ids); the
string id lives in the payload and is the public API id.
"""

import uuid
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from qdrant_client import QdrantClient
from qdrant_client import models as qm

from windex.config import Settings
from windex.embed import build_embedder
from windex.index import qdrant as qidx

_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # uuid5 namespace


def point_id(doc_id: str) -> str:
    return str(uuid.uuid5(_NS, doc_id))


def _pending_refs(conn: psycopg.Connection, limit: int) -> dict[str, list[str]]:
    """text_ref → doc ids, for the oldest pending docs."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT text_ref, array_agg(id)
            FROM (
                SELECT text_ref, id FROM documents
                WHERE source = 'news' AND status = 'deduped'
                ORDER BY created_at LIMIT %s
            ) t GROUP BY text_ref
            """,
            (limit,),
        )
        return dict(cur.fetchall())


def _embed_and_upsert(batch: list[dict], embedder, bm25, client, collection: str,
                      max_chars: int) -> list[str]:
    """Runs in a worker thread: dense + sparse embed, then Qdrant upsert.
    Returns the doc ids. Postgres updates stay on the caller's thread."""
    texts = [
        ((r["title"] + "\n\n") if r["title"] else "") + r["text"][:max_chars]
        for r in batch
    ]
    dense = embedder.embed_batch(texts)
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
                "source": "news",
                "url": r["url"],
                "title": r["title"],
                "snippet": r["text"][:400],
                "published_at": r["published_at"],
                "lang": r["lang"],
                "outlet": (r["canonical_url"].split("/")[2] if r["canonical_url"] else None),
            },
        )
        for i, r in enumerate(batch)
    ]
    client.upsert(collection_name=collection, points=points)
    return [r["id"] for r in batch]


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 50_000) -> int:
    import concurrent.futures as cf

    embedder = build_embedder(settings)
    from fastembed import SparseTextEmbedding

    bm25 = SparseTextEmbedding("Qdrant/bm25")
    client = QdrantClient(url=settings.qdrant_url, timeout=120)
    collection = qidx.ensure_collection(client, "news", settings.embed_model, settings.embed_dim)

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
                pool.submit(_embed_and_upsert, b, embedder, bm25, client, collection, max_chars)
                for b in batches
            ]
            # psycopg connections aren't thread-safe: commit progress here as
            # each worker finishes, so a crash loses at most the in-flight work
            for fut in cf.as_completed(futures):
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
    return total
