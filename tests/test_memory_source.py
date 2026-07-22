"""Push-based chat-memory source: full-replace ingest (parquet + change-aware
ledger delta + tombstones), the write API (validation, bearer auth), embed +
search round trip, and the status rollup. Uses the shared conftest fixtures (pg,
settings, qclient, fake_embedder) and a TestClient with get_settings patched to
the tmp-data-root settings, mirroring test_api.py / test_docs_source.py."""

from datetime import datetime, timezone

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import windex.api.app as app_mod
import windex.api.service as service_mod
from conftest import QDRANT_URL
from windex.api.app import app
from windex.ccnews.dedup import text_hash
from windex.memory_source import ingest as mingest

CID = "0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d"
OTHER_CID = "11111111-2222-3333-4444-555555555555"


def _chunk(index: int, text: str, ended_at: datetime | None = None) -> dict:
    return {"index": index, "text": text, "started_at": None,
            "ended_at": ended_at, "message_range": None}


@pytest.fixture()
def client(settings, monkeypatch):
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    service_mod._pg_stats_cache.clear()
    service_mod._pg_heavy_cache.clear()
    service_mod._timeseries_cache.clear()
    return TestClient(app)


# --- 1. push stages parquet + ledger ----------------------------------------

def test_push_stages_parquet_and_ledger(pg, settings):
    ended = datetime(2026, 7, 12, 9, 21, 40, tzinfo=timezone.utc)
    chunks = [_chunk(0, "User: the sidebar stutters\n\nAssistant: I measured it", ended),
              _chunk(1, "User: what did you find\n\nAssistant: per-frame relayout", ended)]
    res = mingest.replace_conversation(pg, settings, CID, "Sidebar jank", chunks)
    assert res == {"conversation_id": CID, "chunks": 2, "staged": 2, "skipped": 0, "deleted": 0}

    text_ref = f"memory/clean/{CID}.parquet"
    table = pq.read_table(settings.staging_dir / text_ref)
    assert table.num_rows == 2
    assert set(table.column_names) == {
        "id", "url", "title", "conversation_id", "chunk_index", "published_at", "text",
    }
    row0 = table.filter(pc.equal(table["id"], f"memory:{CID}/00000")).to_pylist()[0]
    assert row0["url"] == f"llmchat://chat/{CID}?chunk=0"
    assert row0["conversation_id"] == CID and row0["chunk_index"] == 0
    assert row0["title"] == "Sidebar jank"
    assert row0["published_at"] == ended  # published_at = ended_at

    with pg.cursor() as cur:
        cur.execute(
            "SELECT id, source, url, canonical_url, title, published_at, text_hash, "
            "status, text_ref FROM documents WHERE source='memory' ORDER BY id"
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == [f"memory:{CID}/00000", f"memory:{CID}/00001"]
    for r in rows:
        assert r[1] == "memory" and r[3] == r[2]           # source; canonical_url == url
        assert r[7] == "deduped" and r[8] == text_ref
        assert r[5] == ended                                # published_at
    # text_hash guards title + chunk body (a rename re-embeds the conversation)
    assert rows[0][6] == text_hash("Sidebar jank" + "\n\n" + chunks[0]["text"])


# --- 2. re-push identical is a no-op ----------------------------------------

def test_repush_identical_is_noop(pg, settings):
    chunks = [_chunk(0, "c0 text body"), _chunk(1, "c1 text body")]
    mingest.replace_conversation(pg, settings, CID, "T", chunks)
    res = mingest.replace_conversation(pg, settings, CID, "T", chunks)
    assert res == {"conversation_id": CID, "chunks": 2, "staged": 0, "skipped": 2, "deleted": 0}


# --- 3. append-only re-push stages EXACTLY one (the app's load-bearing contract)

def test_append_only_stages_exactly_one(pg, settings):
    base = [_chunk(0, "chunk zero body"), _chunk(1, "chunk one body")]
    mingest.replace_conversation(pg, settings, CID, "T", base)
    # mark the prefix embedded: only a genuinely re-queued chunk flips back to deduped
    with pg.cursor() as cur:
        cur.execute("UPDATE documents SET status='embedded', embedded_model='m', "
                    "indexed_at=now() WHERE source='memory'")
    pg.commit()

    res = mingest.replace_conversation(pg, settings, CID, "T",
                                       base + [_chunk(2, "chunk two body")])
    assert res == {"conversation_id": CID, "chunks": 3, "staged": 1, "skipped": 2, "deleted": 0}
    with pg.cursor() as cur:
        cur.execute("SELECT id, status FROM documents WHERE source='memory' ORDER BY id")
        rows = dict(cur.fetchall())
    assert rows[f"memory:{CID}/00000"] == "embedded"   # untouched
    assert rows[f"memory:{CID}/00001"] == "embedded"   # untouched
    assert rows[f"memory:{CID}/00002"] == "deduped"    # only the trailing chunk staged
    # full-replace: the parquet holds the whole live set, not just the delta
    table = pq.read_table(settings.staging_dir / f"memory/clean/{CID}.parquet")
    assert table.num_rows == 3


# --- 4. edited chunk re-stages; removed trailing chunks tombstone -----------

def test_edit_restages_and_removed_chunks_tombstone(pg, settings, monkeypatch):
    # never resolve the live memory_current alias from a test: point tombstone
    # deletes at a collection that can't exist (delete becomes a clean skip)
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda s: "memory__pytest-void")
    base = [_chunk(0, "chunk zero"), _chunk(1, "chunk one"), _chunk(2, "chunk two")]
    mingest.replace_conversation(pg, settings, CID, "T", base)
    with pg.cursor() as cur:
        cur.execute("UPDATE documents SET status='embedded', embedded_model='m', "
                    "indexed_at=now() WHERE source='memory'")
    pg.commit()

    res = mingest.replace_conversation(pg, settings, CID, "T", [_chunk(0, "chunk zero EDITED")])
    assert res == {"conversation_id": CID, "chunks": 1, "staged": 1, "skipped": 0, "deleted": 2}
    with pg.cursor() as cur:
        cur.execute("SELECT id, status FROM documents WHERE source='memory' ORDER BY id")
        rows = dict(cur.fetchall())
    assert rows[f"memory:{CID}/00000"] == "deduped"    # edited → re-queued
    assert rows[f"memory:{CID}/00001"] == "deleted"    # vanished → tombstoned
    assert rows[f"memory:{CID}/00002"] == "deleted"
    table = pq.read_table(settings.staging_dir / f"memory/clean/{CID}.parquet")
    assert sorted(table.column("id").to_pylist()) == [f"memory:{CID}/00000"]
    assert "EDITED" in table.to_pylist()[0]["text"]


# --- 5. DELETE tombstones everything and is idempotent ----------------------

def test_delete_conversation_tombstones_and_is_idempotent(pg, settings, monkeypatch):
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda s: "memory__pytest-void")
    mingest.replace_conversation(pg, settings, CID, "T", [_chunk(0, "a"), _chunk(1, "b")])
    res = mingest.delete_conversation(pg, settings, CID)
    assert res == {"conversation_id": CID, "deleted": 2}
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE source='memory' AND status <> 'deleted'")
        assert cur.fetchone()[0] == 0
    # second delete finds nothing live
    assert mingest.delete_conversation(pg, settings, CID)["deleted"] == 0


def test_delete_conversation_removes_the_clean_parquet(pg, settings, monkeypatch):
    """Deleting a conversation must remove its clean parquet, not only tombstone
    the ledger — else the full chat text lingers on the staging volume forever
    (an unbounded leak, and a 'delete' that doesn't delete the content)."""
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda s: "memory__pytest-void")
    mingest.replace_conversation(pg, settings, CID, "T", [_chunk(0, "secret"), _chunk(1, "more")])
    clean = settings.staging_dir / f"memory/clean/{CID}.parquet"
    assert clean.exists()
    mingest.delete_conversation(pg, settings, CID)
    assert not clean.exists(), "clean parquet left on disk after delete"


def test_emptying_a_conversation_removes_the_clean_parquet(pg, settings, monkeypatch):
    """An empty replace (the documented 'conversation emptied' signal) tombstones
    the ledger but the empty-chunks branch skipped the parquet — leaving the old
    content on disk."""
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda s: "memory__pytest-void")
    mingest.replace_conversation(pg, settings, CID, "T", [_chunk(0, "a")])
    clean = settings.staging_dir / f"memory/clean/{CID}.parquet"
    assert clean.exists()
    mingest.replace_conversation(pg, settings, CID, "T", [])  # emptied
    assert not clean.exists(), "clean parquet left after emptying the conversation"


# --- 6. write-API validations → 422 -----------------------------------------

def test_push_validation_422s(client):
    assert client.post("/v1/memory/conversations/not-a-uuid",
                       json={"title": "T", "chunks": []}).status_code == 422
    too_many = {"title": "T", "chunks": [{"index": i, "text": "x"} for i in range(501)]}
    assert client.post(f"/v1/memory/conversations/{CID}", json=too_many).status_code == 422
    gappy = {"title": "T", "chunks": [{"index": 0, "text": "a"}, {"index": 2, "text": "b"}]}
    assert client.post(f"/v1/memory/conversations/{CID}", json=gappy).status_code == 422
    oversized = {"title": "T", "chunks": [{"index": 0, "text": "x" * 16_001}]}
    assert client.post(f"/v1/memory/conversations/{CID}", json=oversized).status_code == 422
    # a valid push still lands (empty chunk list is accepted — it tombstones)
    assert client.post(f"/v1/memory/conversations/{CID}",
                       json={"title": "T", "chunks": []}).status_code == 200


# --- 7. write token guards the write side; reads stay open ------------------

def test_write_token_guards_writes_not_reads(pg, settings, monkeypatch):
    tok = settings.model_copy(update={"write_token": "s3cret"})
    monkeypatch.setattr(app_mod, "get_settings", lambda: tok)
    c = TestClient(app)

    body = {"title": "T", "chunks": []}
    assert c.post(f"/v1/memory/conversations/{CID}", json=body).status_code == 401
    assert c.get("/v1/memory/status").status_code == 401
    assert c.delete(f"/v1/memory/conversations/{CID}").status_code == 401
    assert c.post(f"/v1/memory/conversations/{CID}", json=body,
                  headers={"Authorization": "Bearer wrong"}).status_code == 401
    # correct token → the write goes through
    assert c.post(f"/v1/memory/conversations/{CID}", json=body,
                  headers={"Authorization": "Bearer s3cret"}).status_code == 200
    # reads (search/docs) are NOT gated even with a token set
    monkeypatch.setattr(
        service_mod, "index_search",
        lambda *a, **k: {"results": [], "degraded": False,
                         "timings": {"embed_query_ms": 0, "search_ms": 0}},
    )
    assert c.get("/v1/search", params={"q": "x", "source": "memory"}).status_code == 200
    assert c.get(f"/v1/docs/memory:{CID}/00000").status_code in (200, 404)


# --- 8. embed + search round trip -------------------------------------------

def test_embed_and_search_round_trip(pg, settings, qclient, fake_embedder, monkeypatch):
    import windex.embed.pipeline as embed_pipeline
    from windex.api import service as svc
    from windex.memory_source import embed_index as mem_embed

    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    ended = datetime(2026, 5, 14, 10, 0, 0, tzinfo=timezone.utc)
    mingest.replace_conversation(
        pg, settings, CID, "Sidebar jank investigation",
        [_chunk(0, "User: the sidebar stutters badly\n\nAssistant: I measured the jank", ended)],
    )
    assert mem_embed.embed_pending(pg, settings, limit=10) == 1

    # payload carries the memory contract fields (conversation_id + chunk_index
    # so a result can fetch its neighbours; published_at as RFC3339 for the index)
    from qdrant_client import QdrantClient

    pts = QdrantClient(url=QDRANT_URL).scroll("memory__pytest-model", limit=10, with_payload=True)[0]
    p = {x.payload["doc_id"]: x.payload for x in pts}[f"memory:{CID}/00000"]
    assert {"doc_id", "url", "title", "snippet", "conversation_id", "chunk_index",
            "published_at", "source"} <= set(p)
    assert p["source"] == "memory" and p["conversation_id"] == CID and p["chunk_index"] == 0
    assert p["published_at"].startswith("2026-05-14T10:00:00")

    # lexical search (no query embedder needed): source=memory returns the chunk,
    # with conversation_id/chunk_index surfaced through RESULT_FIELDS
    res = svc.run_search(settings, "sidebar jank", source="memory", mode="lexical", limit=5)
    hit = next((r for r in res["results"] if r["id"] == f"memory:{CID}/00000"), None)
    assert hit is not None and hit["conversation_id"] == CID and hit["chunk_index"] == 0

    # source=all deliberately EXCLUDES memory
    res_all = svc.run_search(settings, "sidebar jank", source="all", mode="lexical", limit=5)
    assert all(r["id"] != f"memory:{CID}/00000" for r in res_all["results"])

    # conversation_id filter narrows to the named conversation
    res_other = svc.run_search(settings, "sidebar jank", source="memory", mode="lexical",
                               limit=5, conversation_id=OTHER_CID)
    assert all(r["id"] != f"memory:{CID}/00000" for r in res_other["results"])


# --- 9. GET /v1/docs/memory:<cid>/<idx> returns full text -------------------

def test_get_document_returns_full_text(client, pg, settings):
    mingest.replace_conversation(pg, settings, CID, "T",
                                 [_chunk(0, "User: hi\n\nAssistant: the full excerpt body")])
    r = client.get(f"/v1/docs/memory:{CID}/00000")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == f"memory:{CID}/00000" and body["source"] == "memory"
    assert "the full excerpt body" in body["text"]


# --- 10. GET /v1/memory/status shape ----------------------------------------

def test_memory_status_shape(client, pg, settings):
    mingest.replace_conversation(pg, settings, CID, "T", [_chunk(0, "a"), _chunk(1, "b")])
    body = client.get("/v1/memory/status").json()
    assert body["conversations"] == 1
    assert body["chunks"] == {"embedded": 0, "pending": 2, "deleted": 0}
    assert "last_indexed_at" in body  # None until an embed pass runs
