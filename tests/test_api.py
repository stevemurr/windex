import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import windex.api.app as app_mod
import windex.api.service as service_mod
from windex.api.app import app


@pytest.fixture()
def client(settings, monkeypatch):
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    service_mod._pg_stats_cache.clear()  # stats are TTL-cached; tests need fresh reads
    return TestClient(app)


def test_dashboard_served_at_root(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "windex" in r.text and "/v1/search" in r.text


def test_search_endpoint_shapes_results(client, monkeypatch):
    canned = {
        "results": [{"doc_id": "gh:o/r", "score": 0.5, "url": "https://github.com/o/r",
                     "title": "o/r", "snippet": "desc", "source": "github", "stars": 42}],
        "degraded": False,
        "timings": {"embed_query_ms": 12, "search_ms": 3},
    }
    monkeypatch.setattr(service_mod, "index_search", lambda *a, **k: canned)
    r = client.get("/v1/search", params={"q": "tool"})
    assert r.status_code == 200
    body = r.json()
    assert body["results"][0]["id"] == "gh:o/r"
    assert body["results"][0]["stars"] == 42
    assert body["timings"]["embed_query_ms"] == 12
    assert "total_ms" in body["timings"] and "took_ms" in body


def test_search_validates_params(client):
    assert client.get("/v1/search", params={"q": ""}).status_code == 422
    assert client.get("/v1/search", params={"q": "x", "limit": 999}).status_code == 422
    assert client.get("/v1/search", params={"q": "x", "source": "bogus"}).status_code == 422


def test_search_accepts_wiki_source(client, monkeypatch):
    monkeypatch.setattr(
        service_mod, "index_search",
        lambda *a, **k: {"results": [], "degraded": False,
                         "timings": {"embed_query_ms": 0, "search_ms": 0}},
    )
    assert client.get("/v1/search", params={"q": "x", "source": "wiki"}).status_code == 200


def test_docs_endpoint_handles_slash_ids_and_404(client, pg, settings):
    text_ref = "repos/clean/t.parquet"
    path = settings.staging_dir / text_ref
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"id": ["gh:owner/repo"], "full_name": ["owner/repo"], "text": ["full doc text"]}),
        path,
    )
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO documents (id, source, url, title, status, text_ref)
               VALUES ('gh:owner/repo', 'github', 'https://github.com/owner/repo',
                       'owner/repo', 'embedded', %s)""",
            (text_ref,),
        )
    pg.commit()
    r = client.get("/v1/docs/gh:owner/repo")
    assert r.status_code == 200
    assert r.json()["text"] == "full doc text"
    assert client.get("/v1/docs/news:doesnotexist").status_code == 404


def test_recent_endpoint_orders_by_indexed_at(client, pg):
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO documents (id, source, url, title, status, indexed_at) VALUES
               ('news:old', 'news', 'u1', 'Older', 'embedded', now() - interval '2 hours'),
               ('gh:o/new', 'github', 'u2', 'Newest', 'embedded', now()),
               ('news:pending', 'news', 'u3', 'Not yet indexed', 'deduped', NULL)"""
        )
    pg.commit()
    rows = client.get("/v1/recent").json()
    assert [r["id"] for r in rows] == ["gh:o/new", "news:old"]
    assert rows[0]["title"] == "Newest" and rows[0]["indexed_at"]
    assert client.get("/v1/recent", params={"limit": 0}).status_code == 422


def test_events_stream_emits_sse(client, pg):
    with client.stream("GET", "/v1/events", params={"ticks": 1}) as r:
        assert r.headers["content-type"].startswith("text/event-stream")
        body = "".join(r.iter_text())
    assert "event: stats" in body
    assert "event: recent" in body
    assert "event: timeseries" in body
    assert '"totals"' in body  # stats payload is the full contract object


def test_workers_endpoint_reads_batch_activity(client, pg, settings):
    from windex import db as wdb

    assert client.get("/v1/workers").json()["active"] is False
    wdb.set_control(pg, "news_stage", "extracting + filtering · batch 20260501-abcd1234")
    logdir = settings.news_staging_dir / "logs" / "20260501-abcd1234"
    (logdir / "logs").mkdir(parents=True)
    (logdir / "completions").mkdir()
    (logdir / "input_files.txt").write_text("a.warc.gz\nb.warc.gz\nc.warc.gz")
    (logdir / "completions" / "00000").touch()
    (logdir / "logs" / "task_00001.log").write_text(
        "2026-07-16 10:00:00.123 | INFO | reader:read:206 - Reading input file b.warc.gz, 1/1\n"
    )
    w = client.get("/v1/workers").json()
    assert w["active"] is True and w["batch"] == "20260501-abcd1234"
    assert w["tasks_done"] == 1 and w["tasks_total"] == 3
    assert w["workers"][0]["task"] == "worker 00001"
    assert "Reading input file b.warc.gz" in w["workers"][0]["line"]


def test_control_endpoint_toggles_and_shows_in_stats(client, pg):
    assert client.post("/v1/control/pause").json() == {"indexing": "paused"}
    service_mod._pg_stats_cache.clear()
    assert client.get("/v1/stats").json()["activity"]["control"] == "paused"
    assert client.post("/v1/control/start").json() == {"indexing": "running"}
    assert client.post("/v1/control/reboot").status_code == 422  # not a valid action


def test_timeseries_zero_filled_with_seeded_activity(client, pg):
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO documents (id, source, url, status, indexed_at) VALUES
               ('news:a', 'news', 'u', 'embedded', now() - interval '3 minutes'),
               ('news:b', 'news', 'u2', 'embedded', now() - interval '3 minutes'),
               ('news:c', 'news', 'u3', 'embedded', now() - interval '90 minutes')"""
        )
        cur.execute(
            "INSERT INTO warc_files (path, status, bytes, processed_at) VALUES ('w1', 'done', 500000000, now() - interval '5 minutes')"
        )
    pg.commit()
    series = client.get("/v1/timeseries", params={"minutes": 30}).json()
    assert len(series) == 30
    assert sum(p["docs"] for p in series) == 2  # 90-minute-old doc excluded
    assert sum(p["ingested"] for p in series) == 3  # created_at defaults to now()
    assert sum(p["mb"] for p in series) == 500.0
    assert all(set(p) == {"t", "docs", "ingested", "mb"} for p in series)


def test_stats_endpoint_reports_pipeline_state_and_totals(client, pg):
    with pg.cursor() as cur:
        cur.execute("INSERT INTO warc_files (path, status) VALUES ('a.warc.gz', 'done'), ('b.warc.gz', 'pending')")
        cur.execute(
            """INSERT INTO documents (id, source, url, canonical_url, status, published_at) VALUES
               ('news:1', 'news', 'u', 'https://outlet-a.com/x', 'embedded', '2026-07-01'),
               ('news:2', 'news', 'u2', 'https://outlet-b.com/y', 'embedded', '2026-07-10'),
               ('news:3', 'news', 'u3', 'https://outlet-a.com/z', 'duplicate', NULL),
               ('gh:o/r', 'github', 'u4', NULL, 'embedded', NULL)"""
        )
    pg.commit()
    body = client.get("/v1/stats").json()
    assert body["warc_files"] == {"done": 1, "pending": 1}
    assert body["freshness"]["news_warcs_pending"] == 1
    assert "vectors" in body
    t = body["totals"]
    assert t["indexed_pages"] == 3
    assert t["news_articles"] == 2 and t["github_projects"] == 1
    assert t["duplicates_collapsed"] == 1
    assert t["news_outlets"] == 2
    assert t["news_coverage"] == ["2026-07-01", "2026-07-10"]
