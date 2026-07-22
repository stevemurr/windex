"""Hybrid retrieval over the current collections: dense (user model) + sparse
BM25 fused with RRF server-side. mode=lexical works without a configured
embedding model, which lets the API run before the model arrives."""

import logging
import threading
import time
from datetime import datetime
from typing import Literal

from qdrant_client import QdrantClient
from qdrant_client import models as qm

from windex.config import Settings
from windex.embed.rerank import Reranker, build_reranker
from windex.index import qdrant as qidx
from windex.index.embed_breaker import EmbedBreakerOpen, breaker
from windex.metrics import QUERY_EMBED_DURATION, QUERY_EMBED_FAILURES

log = logging.getLogger("windex.search")
Mode = Literal["hybrid", "dense", "lexical"]

_client: QdrantClient | None = None
_client_lock = threading.Lock()
_reranker: Reranker | None = None
_reranker_key: tuple | None = None
_reranker_lock = threading.Lock()


def _get_reranker(settings: Settings) -> Reranker | None:
    """Process-wide reranker (holds an httpx pool), rebuilt only if its config
    changes. Returns None when no reranker is configured (reranking skipped).

    /v1/search runs in Starlette's threadpool, so the read-check-rebuild is locked
    (else two threads race the global) and the previous reranker's httpx pool is
    closed before it is replaced (a runtime model swap would otherwise leak it)."""
    global _reranker, _reranker_key
    key = (settings.rerank_endpoint, settings.rerank_model)
    if key != _reranker_key:
        with _reranker_lock:
            if key != _reranker_key:
                old = _reranker
                _reranker = build_reranker(settings)
                _reranker_key = key
                if old is not None:
                    old.close()  # release the replaced reranker's connection pool
    return _reranker


def _qdrant(settings: Settings) -> QdrantClient:
    """One process-wide client: it holds an httpx connection pool, and building
    one per request meant a fresh pool (and handshake) for every search."""
    global _client
    # Double-checked locking: /v1/search runs in Starlette's threadpool, so
    # concurrent cold requests could each build a client and leak all but one.
    if _client is None:
        with _client_lock:
            if _client is None:
                # Via client_from_url so the query timeout is owned in ONE place —
                # this used to build its own client and silently kept the 5s default
                # while qdrant.py's was raised, 500ing cold searches (2026-07-19).
                from windex.index.qdrant import client_from_url

                _client = client_from_url(settings.qdrant_url)
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


def _hf_filter(root: str | None, kind: str | None, published_after: datetime | None,
               published_before: datetime | None):
    # HF payloads index root (transformers, agents-course, blog) and kind
    # (docs|learn|blog) as keywords, so root filtering mirrors how github filters
    # language. published_at is blog-only — a date window therefore narrows to
    # blog posts by construction, which is what asking for one means here.
    conds = []
    if root:
        conds.append(qm.FieldCondition(key="root", match=qm.MatchValue(value=root)))
    if kind:
        conds.append(qm.FieldCondition(key="kind", match=qm.MatchValue(value=kind)))
    if published_after or published_before:
        conds.append(
            qm.FieldCondition(
                key="published_at",
                range=qm.DatetimeRange(gte=published_after, lte=published_before),
            )
        )
    return conds


def _memory_filter(conversation_id: str | None, published_after: datetime | None,
                   published_before: datetime | None):
    # Chat-memory payloads index conversation_id (keyword) and published_at
    # (chunk end time), so scoping recall to one conversation mirrors how github
    # filters language, and a date window mirrors news.
    conds = []
    if conversation_id:
        conds.append(qm.FieldCondition(key="conversation_id",
                                       match=qm.MatchValue(value=conversation_id)))
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
    want_dense = mode in ("hybrid", "dense") and query_dense is not None
    want_sparse = mode in ("hybrid", "lexical")
    sparse_vec = (query_sparse if query_sparse is not None
                  else (_sparse_vector(q) if want_sparse else None))

    if want_dense and want_sparse:
        # hybrid: RRF-fuse the dense + sparse prefetch legs. The score is the
        # fused rank-reciprocal (NOT a similarity); a real cross-collection
        # relevance score comes from the reranker (Phase 2).
        res = client.query_points(
            collection,
            prefetch=[
                qm.Prefetch(query=query_dense, using=qidx.DENSE, limit=limit * 4,
                            filter=flt, params=dense_params),
                qm.Prefetch(query=sparse_vec, using=qidx.SPARSE, limit=limit * 4,
                            filter=flt),
            ],
            query=qm.FusionQuery(fusion=qm.Fusion.RRF),
            limit=limit, with_payload=True,
        )
    elif want_dense:
        # dense-only: query the vector directly so `score` is the real COSINE
        # similarity, not an RRF-over-one-leg reciprocal. (Top-level query uses
        # query_filter/search_params, unlike the Prefetch objects above.)
        res = client.query_points(
            collection, query=query_dense, using=qidx.DENSE, limit=limit,
            query_filter=flt, search_params=dense_params, with_payload=True,
        )
    else:
        # lexical-only: query the sparse vector directly → native BM25 score.
        res = client.query_points(
            collection, query=sparse_vec, using=qidx.SPARSE, limit=limit,
            query_filter=flt, with_payload=True,
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
    root: str | None = None,
    kind: str | None = None,
    conversation_id: str | None = None,
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
            embedder = None
            try:
                embedder = build_embedder(settings, timeout=settings.embed_query_timeout)
                # Chat-memory recall is framed differently from web search; use
                # its own query prefix when configured, else the global one.
                # Memory is never part of a fan-out, so this branch is only ever
                # taken for an explicit source=memory query.
                prefix = (settings.embed_query_prefix_memory
                          if source == "memory" and settings.embed_query_prefix_memory
                          else settings.embed_query_prefix)
                query_dense = embedder.embed_batch([prefix + q])[0]
            except Exception as exc:
                breaker.record_failure(exc, settings)
                QUERY_EMBED_FAILURES.inc()
                if mode == "dense":
                    raise  # explicit dense request: fail loudly, don't change semantics
                degraded = True
            else:
                breaker.record_success()
            finally:
                # Release this query's HTTP pool — build_embedder returns a fresh
                # one-off embedder per request (no cache), so leaving it open leaks
                # a pool per /v1/search. (None-guarded: build_embedder itself could
                # raise before binding.)
                if embedder is not None:
                    embedder.close()
                # Observe every real attempt (success OR failure, including the
                # mode=dense re-raise via finally); the breaker short-circuit above
                # never reaches this branch, so the histogram measures only genuine
                # round trips — which is what its deadline caveat means.
                embed_ms = (time.monotonic() - t_embed) * 1000
                QUERY_EMBED_DURATION.observe(embed_ms / 1000.0)

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
    if source in ("hf", "all"):
        targets.append(("hf", qidx.alias_name("hf"),
                        _hf_filter(root, kind, published_after, published_before)))
    # memory is DELIBERATELY not part of "all": web-search fan-outs must never
    # silently pull personal chat history into their results, and it sidesteps
    # the per-source query-prefix conflict. Recall always asks for it explicitly.
    if source == "memory":
        targets.append(("memory", qidx.alias_name("memory"),
                        _memory_filter(conversation_id, published_after, published_before)))
    # Encode the query once, not once per target collection (source=all fans out
    # to 8 collections and re-encoded the same string for each).
    query_sparse = _sparse_vector(q) if mode in ("hybrid", "lexical") else None
    # A reranker (if configured) needs a deeper candidate pool than the final
    # limit to reorder, so over-fetch per collection when one is active.
    reranker = _get_reranker(settings)
    fetch_limit = max(settings.rerank_top_k, limit) if reranker else limit
    t_search = time.monotonic()
    for src, alias, conds in targets:
        if alias not in aliases and alias not in existing:
            continue  # collection not built yet — serve what exists
        try:
            results.extend(
                _query_collection(client, alias, q, mode, fetch_limit, conds, settings,
                                  query_dense, query_sparse)
            )
        except Exception as exc:  # noqa: BLE001 — one collection must not sink the fan-out
            # A transient per-collection failure (timeout, a collection briefly
            # locked while ensure_collection builds a payload index mid-reindex)
            # must degrade gracefully, not 500 the whole source=all request — the
            # healthy collections still answer, same best-effort spirit as rerank.
            log.warning("search: collection %s (%s) failed, skipping: %r", src, alias, exc)
    search_ms = (time.monotonic() - t_search) * 1000

    # Rerank the fused pool by true (query, passage) relevance. This is the
    # meaningful, cross-collection-comparable score (RRF reciprocals are not
    # comparable across collections), so it also fixes source=all ranking. Best
    # effort: a rerank failure/timeout degrades to the fused order, never stalls.
    rerank_ms = 0.0
    if reranker and results:
        t_rr = time.monotonic()
        try:
            docs = [f"{r.get('title') or ''}\n{r.get('snippet') or ''}".strip()
                    for r in results]
            for r, sc in zip(results, reranker.scores(q, docs)):
                r["score"] = sc  # replace retrieval score with rerank relevance
        except Exception as exc:  # noqa: BLE001 — reranker is optional
            log.warning("rerank failed, using fused order: %r", exc)
        rerank_ms = (time.monotonic() - t_rr) * 1000
    results.sort(key=lambda r: r["score"], reverse=True)
    return {
        "results": results[:limit],
        "degraded": degraded,
        "timings": {"embed_query_ms": round(embed_ms), "search_ms": round(search_ms),
                    "rerank_ms": round(rerank_ms)},
    }
