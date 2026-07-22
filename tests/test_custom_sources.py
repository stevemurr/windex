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


# --- W2 [live-service] upsert / dedup / delete ingest ------------------------

from datetime import datetime, timezone  # noqa: E402

import pyarrow.parquet as pq  # noqa: E402

from windex.custom_source import ingest as cingest  # noqa: E402

NAME = "notes"


def _doc(suffix, text, title="", url=None, published_at=None, extra=None):
    return {"id": suffix, "title": title, "text": text, "url": url,
            "published_at": published_at, "extra": extra}


def test_push_stages_delta_and_dedups(pg, settings):
    ended = datetime(2026, 7, 12, 9, 21, 40, tzinfo=timezone.utc)
    docs = [_doc("a", "alpha body", title="A", published_at=ended, extra={"k": "v"}),
            _doc("b", "beta body", title="B")]
    res = cingest.upsert_docs(pg, settings, NAME, docs)
    assert res == {"source": NAME, "docs": 2, "staged": 2, "skipped": 0}

    with pg.cursor() as cur:
        cur.execute("SELECT id, source, url, canonical_url, published_at, status, text_ref "
                    "FROM documents WHERE source=%s ORDER BY id", (NAME,))
        rows = cur.fetchall()
    assert [r[0] for r in rows] == [f"{NAME}:a", f"{NAME}:b"]
    for r in rows:
        assert r[1] == NAME and r[3] == r[2] and r[5] == "deduped"
    assert rows[0][2] == f"custom://{NAME}/a"          # default url
    assert rows[0][4] == ended                          # published_at

    # a single batch parquet under custom/<name>/, carrying the extra blob
    batch_dir = settings.staging_dir / "custom" / NAME
    files = list(batch_dir.glob("*.parquet"))
    assert len(files) == 1
    table = pq.read_table(files[0])
    assert set(table.column_names) == {"id", "url", "title", "published_at", "text", "extra"}
    row_a = {r["id"]: r for r in table.to_pylist()}[f"{NAME}:a"]
    assert row_a["extra"] == '{"k":"v"}'                # orjson-serialized blob

    # identical re-push: nothing staged, no new batch written
    res2 = cingest.upsert_docs(pg, settings, NAME, docs)
    assert res2 == {"source": NAME, "docs": 2, "staged": 0, "skipped": 2}
    assert len(list(batch_dir.glob("*.parquet"))) == 1


def test_changed_doc_restages_exactly_it(pg, settings):
    cingest.upsert_docs(pg, settings, NAME, [_doc("a", "aaa"), _doc("b", "bbb")])
    with pg.cursor() as cur:
        cur.execute("SELECT id, text_ref FROM documents WHERE source=%s ORDER BY id", (NAME,))
        orig = dict(cur.fetchall())
        cur.execute("UPDATE documents SET status='embedded' WHERE source=%s", (NAME,))
    pg.commit()

    res = cingest.upsert_docs(pg, settings, NAME, [_doc("a", "aaa"), _doc("b", "bbb CHANGED")])
    assert res == {"source": NAME, "docs": 2, "staged": 1, "skipped": 1}
    with pg.cursor() as cur:
        cur.execute("SELECT id, status, text_ref FROM documents WHERE source=%s ORDER BY id", (NAME,))
        rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    assert rows[f"{NAME}:a"][0] == "embedded"                 # untouched
    assert rows[f"{NAME}:a"][1] == orig[f"{NAME}:a"]          # unchanged doc keeps its batch
    assert rows[f"{NAME}:b"][0] == "deduped"                  # re-queued
    assert rows[f"{NAME}:b"][1] != orig[f"{NAME}:b"]          # points at the new batch
    # the new batch holds exactly the changed doc; the old one still resolves
    new_ids = pq.read_table(settings.staging_dir / rows[f"{NAME}:b"][1]).column("id").to_pylist()
    assert new_ids == [f"{NAME}:b"]
    a_ids = pq.read_table(settings.staging_dir / rows[f"{NAME}:a"][1]).column("id").to_pylist()
    assert f"{NAME}:a" in a_ids


def test_delete_docs_tombstones_and_resurrects(pg, settings, monkeypatch):
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda s: f"{s}__pytest-void")
    cingest.upsert_docs(pg, settings, NAME, [_doc("a", "aaa"), _doc("b", "bbb")])
    assert cingest.delete_docs(pg, settings, NAME, ["a"]) == {"deleted": 1}
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM documents WHERE id=%s", (f"{NAME}:a",))
        assert cur.fetchone()[0] == "deleted"
    # idempotent: re-deleting an already-tombstoned id counts 0
    assert cingest.delete_docs(pg, settings, NAME, ["a"]) == {"deleted": 0}
    # byte-identical re-push resurrects it
    assert cingest.upsert_docs(pg, settings, NAME, [_doc("a", "aaa")])["staged"] == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM documents WHERE id=%s", (f"{NAME}:a",))
        assert cur.fetchone()[0] == "deduped"


def test_delete_source_full_teardown_is_idempotent(pg, settings, monkeypatch):
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda s: f"{s}__pytest-void")
    registry.create(pg, NAME)
    cingest.upsert_docs(pg, settings, NAME, [_doc("a", "aaa"), _doc("b", "bbb")])
    batch_dir = settings.staging_dir / "custom" / NAME
    assert batch_dir.exists()

    assert cingest.delete_source(pg, settings, NAME) == {"deleted": 2}
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE source=%s AND status<>'deleted'", (NAME,))
        assert cur.fetchone()[0] == 0
    assert registry.get(pg, NAME) is None
    assert not batch_dir.exists()
    # a re-delete of the now-unknown source is the None (→404) path, not an error
    assert cingest.delete_source(pg, settings, NAME) is None


def test_push_api_limit_shapes(client):
    client.post("/v1/sources", json={"name": NAME})
    too_many = {"docs": [{"id": str(i), "text": "x"} for i in range(501)]}
    assert client.post(f"/v1/sources/{NAME}/docs", json=too_many).status_code == 422
    oversized = {"docs": [{"id": "a", "text": "x" * 16_001}]}
    assert client.post(f"/v1/sources/{NAME}/docs", json=oversized).status_code == 422
    bad_suffix = {"docs": [{"id": "has space", "text": "ok"}]}
    assert client.post(f"/v1/sources/{NAME}/docs", json=bad_suffix).status_code == 422
    big_extra = {"docs": [{"id": "a", "text": "ok", "extra": {"blob": "x" * 3000}}]}
    assert client.post(f"/v1/sources/{NAME}/docs", json=big_extra).status_code == 422
    # unknown source → 404
    assert client.post("/v1/sources/absent/docs",
                       json={"docs": [{"id": "a", "text": "ok"}]}).status_code == 404
    # a valid push lands
    r = client.post(f"/v1/sources/{NAME}/docs", json={"docs": [{"id": "a", "text": "ok"}]})
    assert r.status_code == 200 and r.json()["staged"] == 1


def test_push_staging_oserror_maps_to_503(client, monkeypatch):
    client.post("/v1/sources", json={"name": NAME})

    def boom(*a, **k):
        raise OSError("read-only staging")

    monkeypatch.setattr(service_mod, "custom_push", boom)
    r = client.post(f"/v1/sources/{NAME}/docs", json={"docs": [{"id": "a", "text": "ok"}]})
    assert r.status_code == 503


def test_docs_delete_api(client):
    client.post("/v1/sources", json={"name": NAME})
    client.post(f"/v1/sources/{NAME}/docs",
                json={"docs": [{"id": "a", "text": "aaa"}, {"id": "b", "text": "bbb"}]})
    r = client.post(f"/v1/sources/{NAME}/docs/delete", json={"ids": ["a"]})
    assert r.status_code == 200 and r.json() == {"deleted": 1}
    assert client.post("/v1/sources/absent/docs/delete", json={"ids": ["a"]}).status_code == 404
