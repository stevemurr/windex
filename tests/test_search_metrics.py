"""Canonical public-search metric recording and retention."""

import hashlib
import threading

import windex.api.service as service

CANNED = {
    "results": [{
        "doc_id": "gh:o/r",
        "score": 0.5,
        "url": "https://github.com/o/r",
        "title": "o/r",
        "snippet": "desc",
        "source": "github",
    }],
    "degraded": False,
    "timings": {"embed_query_ms": 7, "search_ms": 3},
}


def _drain() -> None:
    for thread in threading.enumerate():
        if thread.name == "search-metric":
            thread.join(5)


def _seed(pg, rows) -> None:
    with pg.cursor() as cur:
        cur.executemany(
            """INSERT INTO search_metrics
                   (ts, source, mode_requested, degraded, q_hash,
                    embed_ms, search_ms, total_ms, results)
               VALUES (now() - make_interval(mins => %s),
                       %s, %s, %s, %s, %s, %s, %s, %s)""",
            rows,
        )
    pg.commit()


def test_run_search_records_only_a_query_hash(settings, pg, monkeypatch):
    monkeypatch.setattr(service, "index_search", lambda *a, **k: dict(CANNED))
    query = "private query text"
    result = service.run_search(
        settings, query, source="github", mode="hybrid")
    assert result["results"]
    _drain()
    with pg.cursor() as cur:
        cur.execute(
            """SELECT source, mode_requested, degraded, q_hash, results
                 FROM search_metrics""")
        row = cur.fetchone()
    assert row == (
        "github",
        "hybrid",
        False,
        hashlib.sha1(query.encode()).hexdigest()[:12],
        1,
    )


def test_metric_write_failure_never_breaks_search(settings, monkeypatch):
    monkeypatch.setattr(service, "index_search", lambda *a, **k: dict(CANNED))
    monkeypatch.setattr(
        service.db, "pooled",
        lambda _dsn: (_ for _ in ()).throw(RuntimeError("postgres down")),
    )
    assert service.run_search(settings, "still works")["results"]
    _drain()


def test_metric_projection_and_retention(settings, pg):
    _seed(pg, [
        (1, "all", "hybrid", True, "new", 4, 6, 10, 1),
        (60 * 24 * 31, "all", "hybrid", False, "old", 1, 1, 2, 0),
    ])
    metrics = service.get_search_metrics(settings, minutes=60)
    assert metrics["searches"] == 1
    assert metrics["degraded"] == 1
    assert metrics["p50_ms"] == 10
    assert service.prune_search_metrics(pg, days=30) == 1
