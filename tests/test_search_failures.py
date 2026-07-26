"""Search target failures are never reported as successful empty answers."""

from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from windex import db
from windex.api import app as app_module
from windex.api import service
from windex.config import Settings
from windex.index import search as searchmod


class _Cursor:
    def __init__(self, bindings):
        self.bindings = bindings

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, *_args):
        return None

    def fetchall(self):
        return self.bindings


class _Connection:
    def __init__(self, bindings):
        self.bindings = bindings

    def cursor(self):
        return _Cursor(self.bindings)


class _Qdrant:
    def __init__(self, *, collections=(), aliases=()):
        self._collections = collections
        self._aliases = aliases

    def get_collections(self):
        return SimpleNamespace(
            collections=[SimpleNamespace(name=name) for name in self._collections])

    def get_aliases(self):
        return SimpleNamespace(
            aliases=[SimpleNamespace(alias_name=name) for name in self._aliases])


def _settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        data_root=tmp_path,
        pg_dsn="postgresql://unused",
        qdrant_url="http://unused",
        embed_model="unused",
        embed_dim=8,
    )


def _wire(
    monkeypatch,
    *,
    bindings,
    collections=(),
    aliases=(),
    query,
):
    monkeypatch.setattr(
        db, "pooled", lambda _dsn: nullcontext(_Connection(bindings)))
    monkeypatch.setattr(
        searchmod, "_qdrant",
        lambda _settings: _Qdrant(collections=collections, aliases=aliases),
    )
    monkeypatch.setattr(searchmod, "_sparse_vector", lambda _query: object())
    monkeypatch.setattr(searchmod, "_get_reranker", lambda _settings: None)
    monkeypatch.setattr(searchmod, "_query_collection", query)


def _binding(name: str) -> tuple[str, str, str, bool]:
    return name, name, name, True


def test_explicit_source_with_missing_alias_is_unavailable(tmp_path, monkeypatch):
    _wire(
        monkeypatch,
        bindings=[_binding("news")],
        query=lambda *_args, **_kwargs: pytest.fail("missing alias was queried"),
    )

    with pytest.raises(
        searchmod.SearchBackendUnavailable,
        match="unavailable for source 'news'",
    ):
        searchmod.search(
            _settings(tmp_path), "query", source="news", mode="lexical")


def test_explicit_source_query_failure_is_unavailable(tmp_path, monkeypatch):
    def fail(*_args, **_kwargs):
        raise TimeoutError("Qdrant timed out")

    _wire(
        monkeypatch,
        bindings=[_binding("news")],
        aliases=["news_current"],
        query=fail,
    )

    with pytest.raises(
        searchmod.SearchBackendUnavailable,
        match="unavailable for source 'news'",
    ):
        searchmod.search(
            _settings(tmp_path), "query", source="news", mode="lexical")


def test_all_serves_partial_results_with_explicit_degradation(tmp_path, monkeypatch):
    def partial(_client, alias, *_args, **_kwargs):
        if alias == "news_current":
            raise TimeoutError("news timed out")
        return [{"doc_id": "wiki:1", "score": 0.5}]

    _wire(
        monkeypatch,
        bindings=[_binding("news"), _binding("wiki")],
        aliases=["news_current", "wiki_current"],
        query=partial,
    )

    response = searchmod.search(
        _settings(tmp_path), "query", source="all", mode="lexical")

    assert response["results"] == [{"doc_id": "wiki:1", "score": 0.5}]
    assert response["degraded"] is True
    assert response["degradation"] == {
        "embedder": False,
        "unavailable_sources": ["news"],
    }
    monkeypatch.setattr(service, "index_search", lambda *_args, **_kwargs: response)
    monkeypatch.setattr(service, "_record_search_metric", lambda *_args: None)
    public = service.run_search(
        _settings(tmp_path), "query", source="all", mode="lexical")
    assert "partial results" in public["mode"]
    assert "unavailable sources: news" in public["mode"]
    assert "embedder busy" not in public["mode"]


def test_all_fails_when_every_eligible_collection_fails(tmp_path, monkeypatch):
    def fail(*_args, **_kwargs):
        raise TimeoutError("Qdrant timed out")

    _wire(
        monkeypatch,
        bindings=[_binding("news"), _binding("wiki")],
        aliases=["news_current", "wiki_current"],
        query=fail,
    )

    with pytest.raises(
        searchmod.SearchBackendUnavailable,
        match="unavailable for all eligible sources: news, wiki",
    ):
        searchmod.search(
            _settings(tmp_path), "query", source="all", mode="lexical")


def test_successful_empty_query_is_not_degraded(tmp_path, monkeypatch):
    _wire(
        monkeypatch,
        bindings=[_binding("news")],
        aliases=["news_current"],
        query=lambda *_args, **_kwargs: [],
    )

    response = searchmod.search(
        _settings(tmp_path), "no matches", source="news", mode="lexical")

    assert response["results"] == []
    assert response["degraded"] is False
    assert response["degradation"] == {
        "embedder": False,
        "unavailable_sources": [],
    }
    assert service._mode_label("lexical", response) == "lexical"


def test_public_search_maps_backend_unavailability_to_503(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(service, "validate_source", lambda *_args: "news")
    monkeypatch.setattr(
        service,
        "run_search",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            searchmod.SearchBackendUnavailable(
                "search index is unavailable for source 'news'")),
    )

    response = TestClient(app_module.app).get(
        "/v1/search", params={"q": "query", "source": "news"})

    assert response.status_code == 503
    assert response.json() == {
        "detail": "search index is unavailable for source 'news'"}
