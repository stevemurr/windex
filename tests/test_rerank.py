"""Reranker client + its search integration (reorder + degrade-on-failure)."""

import httpx
import pytest

from windex.embed.rerank import HttpReranker, Reranker, build_reranker
from windex.index import qdrant as qidx
from windex.index import search as S


def _rr(handler, **kw):
    return HttpReranker("http://x", "m", transport=httpx.MockTransport(handler), **kw)


def test_scores_aligned_and_omitted_zero():
    # server reorders and drops doc 1 → scores map back to input order, gaps = 0
    rr = _rr(lambda req: httpx.Response(200, json={"results": [
        {"index": 2, "relevance_score": 0.9}, {"index": 0, "relevance_score": 0.5}]}))
    assert rr.scores("q", ["a", "b", "c"]) == [0.5, 0.0, 0.9]


def test_empty_docs_makes_no_call():
    called = []
    assert _rr(lambda req: called.append(1) or httpx.Response(200, json={"results": []})
               ).scores("q", []) == []
    assert not called


def test_close_releases_the_http_client():
    rr = _rr(lambda req: httpx.Response(200, json={"results": []}))
    assert not rr._client.is_closed
    rr.close()
    assert rr._client.is_closed


def test_reranker_rebuild_closes_the_previous_client(settings, monkeypatch):
    """When the reranker config changes at runtime, _get_reranker rebuilds it; the
    previous HttpReranker's httpx pool must be closed, not leaked."""
    S._reranker = None
    S._reranker_key = None
    built = []

    def fake_build(s):
        rr = _rr(lambda req: httpx.Response(200, json={"results": []}))
        built.append(rr)
        return rr

    monkeypatch.setattr(S, "build_reranker", fake_build)
    monkeypatch.setattr(settings, "rerank_endpoint", "http://a", raising=False)
    monkeypatch.setattr(settings, "rerank_model", "m1", raising=False)
    S._get_reranker(settings)
    monkeypatch.setattr(settings, "rerank_model", "m2", raising=False)  # config change
    S._get_reranker(settings)
    assert len(built) == 2 and built[0]._client.is_closed  # old one released
    S._reranker = None
    S._reranker_key = None


def test_relevance_or_score_field():
    rr = _rr(lambda req: httpx.Response(200, json={"results": [{"index": 0, "score": 0.7}]}))
    assert rr.scores("q", ["a"]) == [0.7]


def test_http_error_propagates():
    with pytest.raises(httpx.HTTPError):
        _rr(lambda req: httpx.Response(500, json={})).scores("q", ["a"])


def test_build_reranker_gating():
    class Off:
        rerank_endpoint = ""; rerank_model = ""
    assert build_reranker(Off()) is None

    class On:
        rerank_endpoint = "http://x"; rerank_model = "m"; rerank_api_key = ""
        rerank_timeout = 10.0; rerank_path = "/rerank"
    assert isinstance(build_reranker(On()), Reranker)


class _FakeClient:
    """Minimal Qdrant stand-in: the arxiv alias exists so the fan-out runs."""
    def get_collections(self):
        return type("C", (), {"collections": [type("N", (), {"name": qidx.alias_name("arxiv")})()]})()
    def get_aliases(self):
        return type("A", (), {"aliases": [type("N", (), {"alias_name": qidx.alias_name("arxiv")})()]})()


def _wire(monkeypatch, reranker, canned):
    monkeypatch.setattr(S, "_qdrant", lambda s: _FakeClient())
    monkeypatch.setattr(S, "_query_collection",
                        lambda *a, **k: [dict(r) for r in canned])
    monkeypatch.setattr(S, "_get_reranker", lambda s: reranker)


def test_search_reranks_by_relevance(monkeypatch):
    # retrieval order a,b,c; reranker prefers c,a,b
    canned = [{"doc_id": "a", "score": 0.9, "title": "a"},
              {"doc_id": "b", "score": 0.8, "title": "b"},
              {"doc_id": "c", "score": 0.1, "title": "c"}]

    class RR:
        def scores(self, q, docs):
            return [0.2, 0.1, 0.99]  # c wins
    _wire(monkeypatch, RR(), canned)
    out = S.search(_settings(), "q", source="arxiv", limit=3, mode="lexical")
    assert [r["doc_id"] for r in out["results"]] == ["c", "a", "b"]
    assert out["results"][0]["score"] == 0.99          # score replaced by rerank relevance


def test_search_degrades_on_rerank_failure(monkeypatch):
    canned = [{"doc_id": "a", "score": 0.9, "title": "a"},
              {"doc_id": "b", "score": 0.8, "title": "b"}]

    class Boom:
        def scores(self, q, docs):
            raise httpx.ReadTimeout("rerank down")
    _wire(monkeypatch, Boom(), canned)
    out = S.search(_settings(), "q", source="arxiv", limit=2, mode="lexical")
    # falls back to the fused order, never raises
    assert [r["doc_id"] for r in out["results"]] == ["a", "b"]


def _settings():
    from windex.config import get_settings
    return get_settings()
