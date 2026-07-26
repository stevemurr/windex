"""Reranker client + its search integration (reorder + degrade-on-failure)."""

import httpx
import pytest

from windex.embed.rerank import HttpReranker, Reranker, build_reranker
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
        rerank_endpoint = ""
        rerank_model = ""
    assert build_reranker(Off()) is None

    class On:
        rerank_endpoint = "http://x"
        rerank_model = "m"
        rerank_api_key = ""
        rerank_timeout = 10.0
        rerank_path = "/rerank"
    assert isinstance(build_reranker(On()), Reranker)
