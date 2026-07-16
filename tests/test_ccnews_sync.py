from datetime import date

from windex.ccnews import sync


def test_path_date_parses_warc_path():
    p = "crawl-data/CC-NEWS/2026/07/CC-NEWS-20260713030141-00096.warc.gz"
    assert sync.path_date(p) == date(2026, 7, 13)


def test_months_in_window_spans_year_boundary():
    months = sync.months_in_window(date(2025, 11, 15), date(2026, 2, 3))
    assert months == [(2025, 11), (2025, 12), (2026, 1), (2026, 2)]


def test_sync_inserts_only_in_window_and_is_idempotent(pg, monkeypatch):
    paths = [
        "crawl-data/CC-NEWS/2026/07/CC-NEWS-20260701000000-00001.warc.gz",
        "crawl-data/CC-NEWS/2026/07/CC-NEWS-20260710000000-00002.warc.gz",
    ]
    monkeypatch.setattr(sync, "list_month", lambda client, y, m: paths)
    today = date(2026, 7, 15)
    n = sync.sync(pg, days=7, today=today)  # window starts 07-08 → only path 2
    assert n == 1
    assert sync.pending_paths(pg, 10) == [paths[1]]
    assert sync.sync(pg, days=7, today=today) == 0  # idempotent


def test_mark_updates_status_and_counts(pg, monkeypatch):
    path = "crawl-data/CC-NEWS/2026/07/CC-NEWS-20260714000000-00003.warc.gz"
    monkeypatch.setattr(sync, "list_month", lambda client, y, m: [path])
    sync.sync(pg, days=5, today=date(2026, 7, 15))
    sync.mark(pg, [path], "done", {"clean_out": 42})
    with pg.cursor() as cur:
        cur.execute("SELECT status, doc_counts->>'clean_out' FROM warc_files WHERE path=%s", (path,))
        assert cur.fetchone() == ("done", "42")
    assert sync.pending_paths(pg, 10) == []
