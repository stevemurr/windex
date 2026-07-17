"""Embed staged Hacker News stories from clean parquet and upsert into Qdrant,
plus the points-refresh path for unchanged stories.

Mirrors the wiki/arxiv/smallweb/docs embed step: dense vectors from the user's
Embedder, sparse BM25 from fastembed, stable uuid5 point ids, per-batch
Postgres commits, the dashboard pause check, and the runtime throughput
profile. Source 'hn' lands in the "hn" collection behind the hn_current alias.
The embedded text is title + story_text (Ask/Show/self posts; most stories are
title-only) and the snippet is the title. points rides in the payload under an
integer index — the future ranking boost and today's min_points filter.

refresh_payloads() is the other half of the trailing re-pull design: a story
whose text is unchanged but whose points/num_comments drifted gets a
set_payload — never a re-embed, never a full upsert (which would zero the
vectors) — so score freshness costs no embedding work at all.
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


def refresh_payloads(settings: Settings, stories: list[dict]) -> int:
    """Update points/num_comments in place for already-embedded stories whose
    text is unchanged. set_payload merges the given keys into the existing
    payload; the vectors are untouched. Callers treat this as best-effort
    (stale points self-heal on the next trailing re-pull)."""
    if not stories:
        return 0
    client = QdrantClient(url=settings.qdrant_url, timeout=30)
    alias = qidx.alias_name("hn")
    for s in stories:
        client.set_payload(
            collection_name=alias,
            payload={"points": int(s["points"]), "num_comments": int(s["num_comments"])},
            points=[point_id(s["id"])],
            wait=True,  # refreshed points should be visible on return
        )
    return len(stories)


def _pending_refs(conn: psycopg.Connection, limit: int) -> dict[str, list[str]]:
    """text_ref → doc ids, for the oldest pending hn stories."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT text_ref, array_agg(id)
            FROM (
                SELECT text_ref, id FROM documents
                WHERE source = 'hn' AND status = 'deduped'
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
        ((r["title"] + "\n\n") if r["title"] else "") + (r["story_text"] or "")[:max_chars]
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
                "source": "hn",
                "url": r["url"],                    # the HN discussion page
                "target_url": r["target_url"],      # external link (None on self posts)
                "title": r["title"],
                "snippet": r["title"][:400],        # the title IS the story
                "published_at": r["created_at"],
                "points": int(r["points"] or 0),
                "num_comments": int(r["num_comments"] or 0),
                "author": r["author"],
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
    collection = qidx.ensure_collection(client, "hn", settings.embed_model, settings.embed_dim)

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
