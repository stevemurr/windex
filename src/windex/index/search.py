"""Hybrid retrieval over the current collections: dense (user model) + sparse
BM25 fused with RRF server-side. mode=lexical works without a configured
embedding model, which lets the API run before the model arrives."""

from datetime import datetime
from typing import Literal

from qdrant_client import QdrantClient
from qdrant_client import models as qm

from windex.config import Settings
from windex.index import qdrant as qidx

Mode = Literal["hybrid", "dense", "lexical"]

_bm25 = None


def _bm25_model():
    global _bm25
    if _bm25 is None:
        from fastembed import SparseTextEmbedding

        _bm25 = SparseTextEmbedding("Qdrant/bm25")
    return _bm25


def _news_filter(published_after: datetime | None, published_before: datetime | None):
    conds = []
    if published_after or published_before:
        conds.append(
            qm.FieldCondition(
                key="published_at",
                range=qm.DatetimeRange(gte=published_after, lte=published_before),
            )
        )
    return conds


def _repo_filter(min_stars: int | None, language: str | None):
    conds = []
    if min_stars:
        conds.append(qm.FieldCondition(key="stars", range=qm.Range(gte=min_stars)))
    if language:
        conds.append(qm.FieldCondition(key="language", match=qm.MatchValue(value=language)))
    return conds


def _query_collection(
    client: QdrantClient,
    collection: str,
    q: str,
    mode: Mode,
    limit: int,
    conditions: list,
    settings: Settings,
) -> list[dict]:
    prefetch = []
    flt = qm.Filter(must=conditions) if conditions else None
    if mode in ("hybrid", "dense"):
        from windex.embed import build_embedder

        dense = build_embedder(settings).embed_batch([settings.embed_query_prefix + q])[0]
        prefetch.append(
            qm.Prefetch(query=dense, using=qidx.DENSE, limit=limit * 4, filter=flt)
        )
    if mode in ("hybrid", "lexical"):
        sparse = next(iter(_bm25_model().query_embed(q)))
        prefetch.append(
            qm.Prefetch(
                query=qm.SparseVector(
                    indices=sparse.indices.tolist(), values=sparse.values.tolist()
                ),
                using=qidx.SPARSE,
                limit=limit * 4,
                filter=flt,
            )
        )
    if len(prefetch) == 1:
        res = client.query_points(
            collection, prefetch=prefetch, query=qm.FusionQuery(fusion=qm.Fusion.RRF),
            limit=limit, with_payload=True,
        )
    else:
        res = client.query_points(
            collection, prefetch=prefetch, query=qm.FusionQuery(fusion=qm.Fusion.RRF),
            limit=limit, with_payload=True,
        )
    return [{"score": p.score, **(p.payload or {})} for p in res.points]


def search(
    settings: Settings,
    q: str,
    source: str = "all",
    limit: int = 10,
    mode: Mode = "hybrid",
    published_after: datetime | None = None,
    published_before: datetime | None = None,
    min_stars: int | None = None,
    language: str | None = None,
) -> list[dict]:
    client = QdrantClient(url=settings.qdrant_url)
    existing = {c.name for c in client.get_collections().collections}
    aliases = {a.alias_name for a in client.get_aliases().aliases}
    results = []
    targets = []
    if source in ("news", "all"):
        targets.append(("news", qidx.alias_name("news"), _news_filter(published_after, published_before)))
    if source in ("github", "all"):
        targets.append(("github", qidx.alias_name("repos"), _repo_filter(min_stars, language)))
    for _, alias, conds in targets:
        if alias not in aliases and alias not in existing:
            continue  # collection not built yet — serve what exists
        results.extend(
            _query_collection(client, alias, q, mode, limit, conds, settings)
        )
    results.sort(key=lambda r: r["score"], reverse=True)
    return results[:limit]
