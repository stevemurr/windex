"""Hybrid retrieval over the current collections: dense (user model) + sparse
BM25 fused with RRF server-side. mode=lexical works without a configured
embedding model, which lets the API run before the model arrives."""

import time
from datetime import datetime
from typing import Literal

from qdrant_client import QdrantClient
from qdrant_client import models as qm

from windex.config import Settings
from windex.index import qdrant as qidx
from windex.index.embed_breaker import EmbedBreakerOpen, breaker

Mode = Literal["hybrid", "dense", "lexical"]

_client: QdrantClient | None = None


def _qdrant(settings: Settings) -> QdrantClient:
    """One process-wide client: it holds an httpx connection pool, and building
    one per request meant a fresh pool (and handshake) for every search."""
    global _client
    if _client is None:
        _client = QdrantClient(url=settings.qdrant_url)
    return _client


def _sparse_vector(q: str) -> qm.SparseVector:
    sparse = next(iter(_bm25_model().query_embed(q)))
    return qm.SparseVector(indices=sparse.indices.tolist(), values=sparse.values.tolist())

from windex.index.sparse import bm25_model as _bm25_model


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


def _wiki_filter(published_after: datetime | None, published_before: datetime | None):
    # Wikipedia payloads index published_at (revision timestamp), so the same
    # date-window filter as news applies when requested.
    conds = []
    if published_after or published_before:
        conds.append(
            qm.FieldCondition(
                key="published_at",
                range=qm.DatetimeRange(gte=published_after, lte=published_before),
            )
        )
    return conds


def _smallweb_filter(outlet: str | None, published_after: datetime | None,
                     published_before: datetime | None):
    # Small Web payloads index outlet (feed host, keyword) and published_at
    # (post date), so outlet filtering mirrors how github filters language.
    conds = []
    if outlet:
        conds.append(qm.FieldCondition(key="outlet", match=qm.MatchValue(value=outlet)))
    if published_after or published_before:
        conds.append(
            qm.FieldCondition(
                key="published_at",
                range=qm.DatetimeRange(gte=published_after, lte=published_before),
            )
        )
    return conds


def _docs_filter(framework: str | None):
    # Programming-docs payloads index framework (slug base, keyword) and
    # version (keyword), so framework filtering mirrors how github filters
    # language. Docs pages carry no published_at (reference pages aren't dated).
    conds = []
    if framework:
        conds.append(qm.FieldCondition(key="framework", match=qm.MatchValue(value=framework)))
    return conds


def _hn_filter(min_points: int | None, published_after: datetime | None,
               published_before: datetime | None):
    # HN payloads index points (integer) and published_at (story date), so
    # min_points filtering mirrors how github filters min_stars.
    conds = []
    if min_points:
        conds.append(qm.FieldCondition(key="points", range=qm.Range(gte=min_points)))
    if published_after or published_before:
        conds.append(
            qm.FieldCondition(
                key="published_at",
                range=qm.DatetimeRange(gte=published_after, lte=published_before),
            )
        )
    return conds


def _arxiv_filter(category: str | None, published_after: datetime | None,
                  published_before: datetime | None):
    # arXiv indexes primary_category (keyword) and published_at (submission date),
    # so category filtering mirrors how github filters language.
    conds = []
    if category:
        conds.append(qm.FieldCondition(key="primary_category", match=qm.MatchValue(value=category)))
    if published_after or published_before:
        conds.append(
            qm.FieldCondition(
                key="published_at",
                range=qm.DatetimeRange(gte=published_after, lte=published_before),
            )
        )
    return conds


def _query_collection(
    client: QdrantClient,
    collection: str,
    q: str,
    mode: Mode,
    limit: int,
    conditions: list,
    settings: Settings,
    query_dense: list[float] | None,
    query_sparse: qm.SparseVector | None = None,
) -> list[dict]:
    prefetch = []
    flt = qm.Filter(must=conditions) if conditions else None
    # rescore=False: with original f32 vectors on disk, the default rescoring
    # re-reads them per query from the saturated drive; int8 recall at 4096-dim
    # doesn't need it (docs/store-tuning.md). hnsw_ef explicit = the recall knob.
    dense_params = qm.SearchParams(
        hnsw_ef=96,
        quantization=qm.QuantizationSearchParams(rescore=False),
    )
    if mode in ("hybrid", "dense") and query_dense is not None:
        prefetch.append(
            qm.Prefetch(query=query_dense, using=qidx.DENSE, limit=limit * 4,
                        filter=flt, params=dense_params)
        )
    if mode in ("hybrid", "lexical"):
        prefetch.append(
            qm.Prefetch(
                query=query_sparse if query_sparse is not None else _sparse_vector(q),
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
    category: str | None = None,
    outlet: str | None = None,
    framework: str | None = None,
    min_points: int | None = None,
) -> list[dict]:
    client = _qdrant(settings)
    existing = {c.name for c in client.get_collections().collections}
    aliases = {a.alias_name for a in client.get_aliases().aliases}

    # Embed the query under a deadline: heavy indexing load on the embedding
    # server must degrade hybrid → lexical, never stall the search. The breaker
    # skips the round trip entirely once it's a known-lost cause (embed_breaker.py).
    query_dense = None
    degraded = False
    embed_ms = 0.0
    if mode in ("hybrid", "dense"):
        from windex.embed import build_embedder

        if not breaker.allow(settings):
            # Open breaker: the embed is predicted to time out, so don't spend 9s
            # (nor add load to the GPU the pipeline needs) rediscovering that.
            # embed_ms stays 0 — that IS the truth, we never called the embedder.
            if mode == "dense":
                # Explicit dense request still fails loudly rather than quietly
                # returning lexical results — same contract as a live failure,
                # just without the doomed wait.
                raise EmbedBreakerOpen(
                    "query embedder circuit breaker is open "
                    "(embedding server unavailable); mode=dense cannot be served"
                )
            degraded = True
        else:
            t_embed = time.monotonic()
            try:
                embedder = build_embedder(settings, timeout=settings.embed_query_timeout)
                query_dense = embedder.embed_batch([settings.embed_query_prefix + q])[0]
            except Exception as exc:
                breaker.record_failure(exc, settings)
                if mode == "dense":
                    raise  # explicit dense request: fail loudly, don't change semantics
                degraded = True
            else:
                breaker.record_success()
            embed_ms = (time.monotonic() - t_embed) * 1000

    results = []
    targets = []
    if source in ("news", "all"):
        targets.append(("news", qidx.alias_name("news"), _news_filter(published_after, published_before)))
    if source in ("github", "all"):
        targets.append(("github", qidx.alias_name("repos"), _repo_filter(min_stars, language)))
    if source in ("wiki", "all"):
        targets.append(("wiki", qidx.alias_name("wiki"), _wiki_filter(published_after, published_before)))
    if source in ("arxiv", "all"):
        targets.append(("arxiv", qidx.alias_name("arxiv"),
                        _arxiv_filter(category, published_after, published_before)))
    if source in ("smallweb", "all"):
        targets.append(("smallweb", qidx.alias_name("smallweb"),
                        _smallweb_filter(outlet, published_after, published_before)))
    if source in ("docs", "all"):
        targets.append(("docs", qidx.alias_name("docs"), _docs_filter(framework)))
    if source in ("hn", "all"):
        targets.append(("hn", qidx.alias_name("hn"),
                        _hn_filter(min_points, published_after, published_before)))
    # Encode the query once, not once per target collection (source=all fans out
    # to 7 collections and re-encoded the same string for each).
    query_sparse = _sparse_vector(q) if mode in ("hybrid", "lexical") else None
    t_search = time.monotonic()
    for _, alias, conds in targets:
        if alias not in aliases and alias not in existing:
            continue  # collection not built yet — serve what exists
        results.extend(
            _query_collection(client, alias, q, mode, limit, conds, settings,
                              query_dense, query_sparse)
        )
    search_ms = (time.monotonic() - t_search) * 1000
    results.sort(key=lambda r: r["score"], reverse=True)
    return {
        "results": results[:limit],
        "degraded": degraded,
        "timings": {"embed_query_ms": round(embed_ms), "search_ms": round(search_ms)},
    }
