"""Cross-encoder reranker client (self-hosted, config-gated).

Reorders the fused candidate pool by true (query, passage) relevance — the
principled fix for "right neighborhood, wrong order" on short/ambiguous queries
AND for cross-collection ranking under source=all (RRF reciprocals aren't
comparable across collections; a rerank score is). Mirrors the Embedder pattern:
user-supplied model behind an OpenAI/Cohere-style HTTP endpoint (the Spark's
gateway), everything config-driven, best-effort so a rerank failure degrades to
the fused order rather than failing the search.

Wire: `WINDEX_RERANK_ENDPOINT` + `WINDEX_RERANK_MODEL` enable it; empty ⇒ off."""

from __future__ import annotations

import abc
import logging
from collections.abc import Sequence

import httpx

log = logging.getLogger("windex.rerank")


class Reranker(abc.ABC):
    model_id: str

    @abc.abstractmethod
    def scores(self, query: str, documents: Sequence[str]) -> list[float]:
        """Relevance score per document, aligned to input order (higher = better)."""


class HttpReranker(Reranker):
    """Client for a self-hosted rerank endpoint. Speaks the de-facto standard
    rerank API (Cohere / Jina / TEI / vLLM / infinity / LiteLLM):

        POST {endpoint}/rerank  {"model", "query", "documents": [str], "top_n"}
        -> {"results": [{"index": i, "relevance_score": s}, ...]}

    Results may be truncated/reordered by the server, so scores are mapped back
    to the input order by `index`; any document the server omits scores 0."""

    def __init__(self, endpoint: str, model_id: str, api_key: str = "",
                 timeout: float = 10.0, path: str = "/rerank",
                 query_instruct: str = "",
                 transport: httpx.BaseTransport | None = None):
        self.endpoint = endpoint.rstrip("/")
        self.model_id = model_id
        self.path = "/" + path.lstrip("/")
        # Instruction-tuned rerankers (Qwen3-Reranker) score the query in the
        # "<Instruct>: …\n<Query>: …" format; the raw query mis-ranks real
        # passages. Empty ⇒ send the query verbatim.
        self.query_instruct = query_instruct
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = httpx.Client(timeout=timeout, headers=headers, transport=transport)

    def scores(self, query: str, documents: Sequence[str]) -> list[float]:
        docs = list(documents)
        if not docs:
            return []
        wrapped = (f"<Instruct>: {self.query_instruct}\n<Query>: {query}"
                   if self.query_instruct else query)
        resp = self._client.post(
            f"{self.endpoint}{self.path}",
            json={"model": self.model_id, "query": wrapped,
                  "documents": docs, "top_n": len(docs)},
        )
        resp.raise_for_status()
        results = resp.json()["results"]
        out = [0.0] * len(docs)
        for r in results:
            i = r["index"]
            if 0 <= i < len(docs):
                out[i] = float(r.get("relevance_score", r.get("score", 0.0)))
        return out


def build_reranker(settings) -> Reranker | None:
    """An HttpReranker when WINDEX_RERANK_ENDPOINT + _MODEL are configured, else
    None (the search path treats None as 'skip reranking')."""
    endpoint = getattr(settings, "rerank_endpoint", "")
    model = getattr(settings, "rerank_model", "")
    if not (endpoint and model):
        return None
    return HttpReranker(
        endpoint=endpoint, model_id=model,
        api_key=getattr(settings, "rerank_api_key", ""),
        timeout=getattr(settings, "rerank_timeout", 10.0),
        path=getattr(settings, "rerank_path", "/rerank"),
        query_instruct=getattr(settings, "rerank_query_instruct", ""),
    )
