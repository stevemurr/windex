"""Search-performance metrics: best-effort recording from run_search (REST and
MCP share that entry point), /v1/metrics percentile aggregation, retention,
and the dashboard wiring in stats.activity."""

import hashlib
import threading

import pytest
from fastapi.testclient import TestClient

import windex.api.app as app_mod
import windex.api.service as service_mod
from windex.api.app import app

CANNED = {
    "results": [{"doc_id": "gh:o/r", "score": 0.5, "url": "https://github.com/o/r",
                 "title": "o/r", "snippet": "desc", "source": "github"}],
    "degraded": False,
    "timings": {"embed_query_ms": 7, "search_ms": 3},
}


def _drain_metric_threads(timeout: float = 5.0) -> None:
    """The metric INSERT runs on a named daemon thread; join it so assertions
    (row present, or failure swallowed) are deterministic."""
    for t in threading.enumerate():
        if t.name == "search-metric":
            t.join(timeout)


def _seed(pg, rows):
    """rows: (minutes_ago, source, mode, degraded, q_hash, embed, search, total, results)"""
    with pg.cursor() as cur:
        cur.executemany(
            """INSERT INTO search_metrics (ts, source, mode_requested, degraded,
                   q_hash, embed_ms, search_ms, total_ms, results)
               VALUES (now() - make_interval(mins => %s), %s, %s, %s, %s, %s, %s, %s, %s)""",
            rows,
        )
    pg.commit()


@pytest.fixture(autouse=True)
def _fresh_metrics_state():
    # straggler metric threads from earlier tests must not land rows after our
    # truncate; both service caches must not serve another test's numbers
    _drain_metric_threads()
    service_mod._pg_stats_cache.clear()
    service_mod._metrics_cache.clear()
    yield
    _drain_metric_threads()


@pytest.fixture()
def client(settings, monkeypatch):
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    return TestClient(app)


def test_run_search_records_metric_row(settings, pg, monkeypatch):
    monkeypatch.setattr(service_mod, "index_search", lambda *a, **k: dict(CANNED))
    out = service_mod.run_search(settings, "rust web framework", source="github", mode="hybrid")
    assert out["results"]
    _drain_metric_threads()
    with pg.cursor() as cur:
        cur.execute("""SELECT source, mode_requested, degraded, q_hash,
                              embed_ms, search_ms, total_ms, results
                       FROM search_metrics""")
        rows = cur.fetchall()
    assert len(rows) == 1
    source, mode, degraded, q_hash, embed_ms, search_ms, total_ms, results = rows[0]
    assert (source, mode, degraded) == ("github", "hybrid", False)
    assert (embed_ms, search_ms) == (7, 3)
    assert total_ms is not None and total_ms >= 0
    assert results == 1
    assert q_hash == hashlib.sha1(b"rust web framework").hexdigest()[:12]


def test_degraded_run_search_recorded_as_degraded(settings, pg, monkeypatch):
    monkeypatch.setattr(service_mod, "index_search",
                        lambda *a, **k: dict(CANNED, degraded=True))
    out = service_mod.run_search(settings, "q", mode="hybrid")
    assert "degraded" in out["mode"]
    _drain_metric_threads()
    with pg.cursor() as cur:
        cur.execute("SELECT degraded, mode_requested FROM search_metrics")
        assert cur.fetchall() == [(True, "hybrid")]


def test_metric_write_failure_never_breaks_search(settings, pg, monkeypatch):
    monkeypatch.setattr(service_mod, "index_search", lambda *a, **k: dict(CANNED))

    def boom(dsn):
        raise RuntimeError("pg down")

    monkeypatch.setattr(service_mod.db, "pooled", boom)
    out = service_mod.run_search(settings, "still works")
    _drain_metric_threads()  # the writer must swallow the failure, not raise
    assert out["results"] and out["took_ms"] >= 0
    monkeypatch.undo()
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM search_metrics")
        assert cur.fetchone()[0] == 0  # row lost — acceptable by design


def test_q_hash_present_query_text_never_stored(settings, pg, monkeypatch):
    monkeypatch.setattr(service_mod, "index_search", lambda *a, **k: dict(CANNED))
    q = "extremely private query text"
    service_mod.run_search(settings, q)
    _drain_metric_threads()
    with pg.cursor() as cur:
        cur.execute("SELECT * FROM search_metrics")
        row = cur.fetchone()
        cols = [d.name for d in cur.description]
    assert "q_hash" in cols
    assert not {"q", "query", "query_text"} & set(cols)  # no query-text column at all
    assert q not in " ".join(str(v) for v in row)
    assert row[cols.index("q_hash")] == hashlib.sha1(q.encode()).hexdigest()[:12]


def test_metrics_percentiles_and_by_source(client, pg):
    _seed(pg, [(i, "news" if i % 2 else "github", "hybrid", False, f"h{i}",
                10 * i, i, 100 * i, 5) for i in range(1, 6)])
    _seed(pg, [(120, "news", "hybrid", False, "old", 999, 999, 9999, 5)])  # outside window
    body = client.get("/v1/metrics", params={"minutes": 60}).json()
    assert body["window_minutes"] == 60
    assert body["searches"] == 5  # 120-minute-old row excluded
    # total_ms 100..500: p50=300, p95=480, p99=496; embed 10..50: p95=48; search 1..5: p95≈5
    assert body["p50_ms"] == 300
    assert body["p95_ms"] == 480
    assert body["p99_ms"] == 496
    assert body["embed_p95_ms"] == 48
    assert body["search_p95_ms"] == 5
    assert body["by_source"] == {"news": 3, "github": 2}


def test_metrics_degraded_counting(client, pg):
    _seed(pg, [(1, "all", "hybrid", True, "a", 0, 1, 2, 0),
               (2, "all", "hybrid", False, "b", 0, 1, 2, 1),
               (3, "all", "hybrid", False, "c", 0, 1, 2, 1),
               (4, "all", "lexical", False, "d", 0, 1, 2, 1)])
    body = client.get("/v1/metrics").json()
    assert body["searches"] == 4
    assert body["degraded"] == 1
    assert body["degraded_pct"] == 25.0


def test_metrics_empty_window_returns_zeros(client, pg):
    body = client.get("/v1/metrics", params={"minutes": 15}).json()
    assert body == {"window_minutes": 15, "searches": 0, "degraded": 0,
                    "degraded_pct": 0.0, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0,
                    "embed_p95_ms": 0, "search_p95_ms": 0, "by_source": {}}
    assert client.get("/v1/metrics", params={"minutes": 0}).status_code == 422


def test_retention_prunes_rows_older_than_30_days(pg):
    _seed(pg, [(60 * 24 * 31, "all", "hybrid", False, "gone", 1, 1, 1, 0),
               (60 * 24 * 29, "all", "hybrid", False, "kept", 1, 1, 1, 0),
               (5, "all", "hybrid", False, "new", 1, 1, 1, 0)])
    assert service_mod.prune_search_metrics(pg, days=30) == 1
    with pg.cursor() as cur:
        cur.execute("SELECT q_hash FROM search_metrics ORDER BY ts")
        assert [r[0] for r in cur.fetchall()] == ["kept", "new"]


def test_stats_activity_gains_degraded_recent(client, pg):
    _seed(pg, [(5, "all", "hybrid", True, "a", 1, 2, 30, 0),
               (6, "all", "hybrid", False, "b", 1, 2, 10, 4)])
    act = client.get("/v1/stats").json()["activity"]
    assert act["degraded_recent"] == 1
    assert act["searches_1h"] == 2
    assert act["search_p95_ms"] == 29  # percentile_cont over [10, 30]
