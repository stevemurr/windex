"""Live-Qdrant integration: collection lifecycle, alias flip, hybrid plumbing."""

import pytest
from qdrant_client import models as qm

from windex.ccnews.embed_index import point_id
from windex.index import qdrant as qidx
from windex.index import search as searchmod

MODEL = "pytest-model"


def test_ensure_collection_idempotent_with_indexes(qclient):
    name = qidx.ensure_collection(qclient, "news", MODEL, dim=8)
    assert name == "news__pytest-model"
    assert qidx.ensure_collection(qclient, "news", MODEL, dim=8) == name  # rerun safe
    info = qclient.get_collection(name)
    assert info.config.params.vectors[qidx.DENSE].size == 8
    schema = info.payload_schema
    assert "published_at" in schema and "doc_id" in schema


def test_alias_flip(qclient):
    before = {al.alias_name: al.collection_name for al in qclient.get_aliases().aliases}
    a = qidx.ensure_collection(qclient, "news", MODEL, dim=8)
    try:
        qidx.flip_alias(qclient, "news", a)
        aliases = {al.alias_name: al.collection_name for al in qclient.get_aliases().aliases}
        assert aliases["news_current"] == a
    finally:
        # never leak the flip: restore the real alias (deleting the pytest
        # collection would otherwise silently take the alias with it)
        if "news_current" in before:
            qidx.flip_alias(qclient, "news", before["news_current"])
        else:
            qclient.update_collection_aliases(
                change_aliases_operations=[
                    qm.DeleteAliasOperation(delete_alias=qm.DeleteAlias(alias_name="news_current"))
                ]
            )


@pytest.fixture()
def seeded_collection(qclient, fake_embedder):
    name = qidx.ensure_collection(qclient, "news", MODEL, dim=8)
    from fastembed import SparseTextEmbedding

    bm25 = SparseTextEmbedding("Qdrant/bm25")
    docs = [
        ("news:aaa", "qdrant vector database releases hybrid search update"),
        ("news:bbb", "city council approves transit plan with bus lanes"),
        ("news:ccc", "semiconductor earnings beat expectations on datacenter demand"),
    ]
    dense = fake_embedder.embed_batch([t for _, t in docs])
    sparse = list(bm25.embed([t for _, t in docs]))
    qclient.upsert(
        collection_name=name,
        points=[
            qm.PointStruct(
                id=point_id(did),
                vector={
                    qidx.DENSE: dense[i],
                    qidx.SPARSE: qm.SparseVector(
                        indices=sparse[i].indices.tolist(), values=sparse[i].values.tolist()
                    ),
                },
                payload={"doc_id": did, "source": "news", "url": f"https://x/{did}",
                         "title": text[:20], "snippet": text,
                         "published_at": "2026-07-13T00:00:00Z", "lang": "en"},
            )
            for i, (did, text) in enumerate(docs)
        ],
        wait=True,
    )
    return name


def test_lexical_search_ranks_term_match_first(qclient, settings, seeded_collection, monkeypatch):
    monkeypatch.setattr(searchmod.qidx, "alias_name", lambda source: seeded_collection)
    resp = searchmod.search(settings, "transit bus lanes", source="news", mode="lexical", limit=3)
    assert resp["results"] and resp["results"][0]["doc_id"] == "news:bbb"
    assert resp["degraded"] is False
    assert resp["timings"]["embed_query_ms"] == 0  # lexical never embeds


def test_hybrid_search_uses_fake_dense(qclient, settings, seeded_collection, monkeypatch, fake_embedder):
    monkeypatch.setattr(searchmod.qidx, "alias_name", lambda source: seeded_collection)
    import windex.embed as embed_mod

    monkeypatch.setattr(embed_mod, "build_embedder", lambda s, timeout=None, **kw: fake_embedder)
    resp = searchmod.search(settings, "semiconductor datacenter earnings", source="news",
                            mode="hybrid", limit=3)
    assert resp["results"] and resp["results"][0]["doc_id"] == "news:ccc"
    assert "search_ms" in resp["timings"]


def test_hybrid_degrades_to_lexical_when_embedder_unreachable(
    qclient, settings, seeded_collection, monkeypatch
):
    monkeypatch.setattr(searchmod.qidx, "alias_name", lambda source: seeded_collection)
    # settings' embed endpoint points at a dead port — hybrid must still answer
    resp = searchmod.search(settings, "transit bus lanes", source="news",
                            mode="hybrid", limit=3)
    assert resp["results"] and resp["results"][0]["doc_id"] == "news:bbb"
    assert resp["degraded"] is True


def test_search_skips_missing_collections(settings, monkeypatch):
    monkeypatch.setattr(searchmod.qidx, "alias_name", lambda source: "does_not_exist")
    resp = searchmod.search(settings, "anything", source="github", mode="lexical")
    assert resp["results"] == []
