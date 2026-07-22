"""Push-based custom sources: the registry (name validation + CRUD), the write
API (create/list/get/patch/delete, bearer auth), upsert/dedup/delete ingest,
embed + search round trip, and search-source validation. Uses the shared
conftest fixtures (pg, settings, qclient, fake_embedder) and a TestClient with
get_settings patched to the tmp-data-root settings, mirroring test_memory_source.py
and test_api.py. [pure-unit] tests need no services; the rest are [live-service]
and skip cleanly when Postgres/Qdrant are down (the pg/qclient fixtures skip)."""

import pytest
from fastapi.testclient import TestClient

import windex.api.app as app_mod
import windex.api.service as service_mod
from windex.api.app import app
from windex.custom_source import registry


@pytest.fixture()
def client(settings, monkeypatch):
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    service_mod._pg_stats_cache.clear()
    service_mod._pg_heavy_cache.clear()
    service_mod._timeseries_cache.clear()
    service_mod._source_cache.clear()  # validate_source TTL cache
    return TestClient(app)


# --- W1.1 [pure-unit] name validation ---------------------------------------

def test_validate_name_accepts_legal_names():
    for good in ("email", "my_notes2", "a1", "x_y_z"):
        assert registry.validate_name(good) == good


def test_validate_name_rejects_illegal_and_reserved():
    for bad in ("Email", "2cool", "a", "no-dashes", "with space",
                "toolongtoolongtoolongtoolongtoolong", ""):
        with pytest.raises(ValueError):
            registry.validate_name(bad)
    # reserved: built-in corpus sources + all/github/gh/ccnews/custom
    for reserved in ("memory", "news", "all", "github", "gh", "ccnews",
                     "custom", "wiki", "hn", "arxiv", "docs", "smallweb", "hf", "repos"):
        with pytest.raises(ValueError):
            registry.validate_name(reserved)


# --- W1.2 [live-service: Postgres] registry CRUD round-trip ------------------

def test_registry_crud_round_trip(pg):
    info = registry.create(pg, "notes", title="My notes", description="scratch")
    assert info["name"] == "notes" and info["title"] == "My notes"
    assert info["description"] == "scratch" and info["recipe"] is None
    assert info["doc_count"] == 0 and info["pending"] == 0

    assert registry.get(pg, "notes")["title"] == "My notes"
    assert registry.get(pg, "absent") is None
    assert [s["name"] for s in registry.list_all(pg)] == ["notes"]

    # duplicate create raises DuplicateSource and does not corrupt the connection
    with pytest.raises(registry.DuplicateSource):
        registry.create(pg, "notes")
    assert registry.get(pg, "notes") is not None

    # recipe jsonb round-trips
    recipe = {"source_tool": "list_x", "map": {"id": "$.id"}, "n": 3}
    upd = registry.update(pg, "notes", recipe=recipe)
    assert upd["recipe"] == recipe
    assert registry.get(pg, "notes")["recipe"] == recipe

    # partial update leaves unspecified fields
    upd2 = registry.update(pg, "notes", title="Renamed")
    assert upd2["title"] == "Renamed" and upd2["recipe"] == recipe

    assert registry.update(pg, "absent", title="x") is None
    assert registry.delete_row(pg, "notes") is True
    assert registry.delete_row(pg, "notes") is False
    assert registry.get(pg, "notes") is None


def test_registry_doc_counts_reflect_ledger(pg):
    registry.create(pg, "notes")
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO documents (id, source, url, status) VALUES
               ('notes:a', 'notes', 'u1', 'embedded'),
               ('notes:b', 'notes', 'u2', 'deduped'),
               ('notes:c', 'notes', 'u3', 'deleted')"""
        )
    pg.commit()
    info = registry.get(pg, "notes")
    assert info["doc_count"] == 2   # embedded + deduped, deleted excluded
    assert info["pending"] == 1     # deduped awaiting a vector


# --- W1.3 [live-service: Postgres] the CRUD API -----------------------------

def test_source_create_api(client):
    r = client.post("/v1/sources", json={"name": "email", "title": "Email",
                                         "description": "inbox"})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["name"] == "email" and body["title"] == "Email"
    assert body["doc_count"] == 0 and body["pending"] == 0 and body["recipe"] is None

    # duplicate → 409
    assert client.post("/v1/sources", json={"name": "email"}).status_code == 409
    # reserved / malformed → 422
    assert client.post("/v1/sources", json={"name": "memory"}).status_code == 422
    assert client.post("/v1/sources", json={"name": "Email"}).status_code == 422
    assert client.post("/v1/sources", json={"name": "no-dash"}).status_code == 422


def test_sources_list_and_get_api(client):
    client.post("/v1/sources", json={"name": "email", "title": "Email"})
    client.post("/v1/sources", json={"name": "notes"})

    listing = client.get("/v1/sources").json()
    assert {s["name"] for s in listing["sources"]} == {"email", "notes"}

    detail = client.get("/v1/sources/email").json()
    assert detail["name"] == "email" and "recipe" in detail
    assert detail["doc_count"] == 0 and detail["pending"] == 0
    assert client.get("/v1/sources/nope").status_code == 404


def test_source_patch_api(client):
    client.post("/v1/sources", json={"name": "email", "title": "Email"})
    recipe = {"source_tool": "list_emails", "map": {"id": "$.id"}}
    r = client.patch("/v1/sources/email", json={"recipe": recipe, "description": "inbox"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["recipe"] == recipe and body["description"] == "inbox"
    assert body["title"] == "Email"  # untouched by the partial patch
    assert client.patch("/v1/sources/nope", json={"title": "x"}).status_code == 404


def test_source_delete_api(client):
    client.post("/v1/sources", json={"name": "email"})
    assert client.delete("/v1/sources/email").status_code == 200
    assert client.get("/v1/sources/email").status_code == 404
    assert client.delete("/v1/sources/email").status_code == 404  # unknown now


def test_write_token_guards_source_writes_not_reads(settings, monkeypatch):
    tok = settings.model_copy(update={"write_token": "s3cret"})
    monkeypatch.setattr(app_mod, "get_settings", lambda: tok)
    service_mod._source_cache.clear()
    c = TestClient(app)

    body = {"name": "email"}
    assert c.post("/v1/sources", json=body).status_code == 401
    assert c.patch("/v1/sources/email", json={"title": "x"}).status_code == 401
    assert c.delete("/v1/sources/email").status_code == 401
    assert c.post("/v1/sources", json=body,
                  headers={"Authorization": "Bearer wrong"}).status_code == 401
    # correct token → the create goes through
    assert c.post("/v1/sources", json=body,
                  headers={"Authorization": "Bearer s3cret"}).status_code == 201
    # reads (list/get) are NOT gated even with a token set
    assert c.get("/v1/sources").status_code == 200
    assert c.get("/v1/sources/email").status_code == 200
