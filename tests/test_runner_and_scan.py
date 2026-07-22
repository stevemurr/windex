import gzip
import json
from datetime import date
from pathlib import Path

import pytest

from windex.ccnews import runner, sync
from windex.github import tail


def _seed_warcs(pg, n=4):
    # Same crawl day, unique sequence numbers: a batch spanning several days is
    # deliberately split (see test_run_batches_splits_batches_at_day_boundaries),
    # so keep these on one day to exercise multi-WARC batching.
    paths = [
        f"crawl-data/CC-NEWS/2026/07/CC-NEWS-20260714000000-{i:05d}.warc.gz"
        for i in range(1, n + 1)
    ]
    with pg.cursor() as cur:
        cur.executemany("INSERT INTO warc_files (path) VALUES (%s)", [(p,) for p in paths])
    pg.commit()
    return paths


def test_run_batches_success_marks_done_and_cleans(pg, settings, monkeypatch, tmp_path):
    paths = _seed_warcs(pg, 4)
    downloaded, processed = [], []

    def fake_download(batch, dest):
        out = []
        for p in batch:
            f = settings.ccnews_downloads_dir / Path(p).name
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"warc")
            out.append(f)
        downloaded.extend(batch)
        return out

    monkeypatch.setattr(runner.download, "download_batch", fake_download)
    monkeypatch.setattr(
        runner.pipeline, "process_batch",
        lambda **kw: processed.append(sorted(kw["local_names"])),
    )
    monkeypatch.setattr(
        runner.dd, "run_dedup",
        lambda conn, extracted_dir, clean_path, text_ref, day: {"clean_out": 5},
    )
    staged = runner.run_batches(pg, settings, batch_size=2, keep_warcs=False)
    assert staged == 10 and len(processed) == 2
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM warc_files WHERE status = 'done'")
        assert cur.fetchone()[0] == 4
    assert not any(settings.ccnews_downloads_dir.glob("*.warc.gz"))  # cleaned up


def test_run_batches_skips_failed_batch_and_continues(pg, settings, monkeypatch):
    paths = _seed_warcs(pg, 4)
    attempts = []

    def flaky_download(batch, dest):
        attempts.append(list(batch))
        if len(attempts) == 1:
            raise RuntimeError("transient net error")
        out = []
        for p in batch:
            f = settings.ccnews_downloads_dir / Path(p).name
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"warc")
            out.append(f)
        return out

    monkeypatch.setattr(runner.download, "download_batch", flaky_download)
    monkeypatch.setattr(runner.pipeline, "process_batch", lambda **kw: None)
    monkeypatch.setattr(
        runner.dd, "run_dedup",
        lambda conn, extracted_dir, clean_path, text_ref, day: {"clean_out": 1},
    )
    staged = runner.run_batches(pg, settings, batch_size=2)
    assert staged == 1  # second batch succeeded despite first failing
    with pg.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM warc_files GROUP BY status")
        assert dict(cur.fetchall()) == {"failed": 2, "done": 2}


def test_run_batches_aborts_after_consecutive_failures(pg, settings, monkeypatch):
    _seed_warcs(pg, 4)
    monkeypatch.setattr(
        runner.download, "download_batch",
        lambda batch, dest: (_ for _ in ()).throw(RuntimeError("net down")),
    )
    with pytest.raises(RuntimeError, match="net down"):
        runner.run_batches(pg, settings, batch_size=2, max_consecutive_failures=2)
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM warc_files WHERE status = 'failed'")
        assert cur.fetchone()[0] == 4


def test_run_batches_waits_while_paused(pg, settings, monkeypatch):
    from windex import db as wdb

    _seed_warcs(pg, 2)
    wdb.set_control(pg, "indexing", "paused")
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)
        wdb.set_control(pg, "indexing", "running")  # unpause after first poll

    def fake_download(batch, dest):
        out = []
        for p in batch:
            f = settings.ccnews_downloads_dir / Path(p).name
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"w")
            out.append(f)
        return out

    monkeypatch.setattr(runner.time, "sleep", fake_sleep)
    monkeypatch.setattr(runner.download, "download_batch", fake_download)
    monkeypatch.setattr(runner.pipeline, "process_batch", lambda **kw: None)
    monkeypatch.setattr(
        runner.dd, "run_dedup",
        lambda conn, extracted_dir, clean_path, text_ref, day: {"clean_out": 2},
    )
    staged = runner.run_batches(pg, settings, batch_size=2, pause_poll_seconds=0.01)
    assert sleeps, "runner must poll while paused"
    assert staged == 2  # resumed and processed after unpause


def test_run_batches_splits_batches_at_day_boundaries(pg, settings, monkeypatch):
    """run_dedup stamps every minhash band row with a single `day` (paths[0]'s).
    A batch straddling midnight would mis-date the later day's near-dup bands and
    prune them a day early, narrowing the syndication window for exactly the
    boundary-crossing articles. So a batch must never span two crawl days."""
    dayA = [f"crawl-data/CC-NEWS/2026/07/CC-NEWS-20260713000000-{i:05d}.warc.gz" for i in range(3)]
    dayB = [f"crawl-data/CC-NEWS/2026/07/CC-NEWS-20260714000000-{i:05d}.warc.gz" for i in range(2)]
    with pg.cursor() as cur:
        cur.executemany("INSERT INTO warc_files (path) VALUES (%s)", [(p,) for p in dayA + dayB])
    pg.commit()

    seen_days, seen_counts = [], []

    def fake_download(batch, dest):
        seen_counts.append(len(batch))
        out = []
        for p in batch:
            f = settings.ccnews_downloads_dir / Path(p).name
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"w")
            out.append(f)
        return out

    monkeypatch.setattr(runner.download, "download_batch", fake_download)
    monkeypatch.setattr(runner.pipeline, "process_batch", lambda **kw: None)
    monkeypatch.setattr(
        runner.dd, "run_dedup",
        lambda conn, extracted_dir, clean_path, text_ref, day: seen_days.append(day) or {"clean_out": 1},
    )

    # batch_size far exceeds the queue: without the split this is ONE batch of 5.
    runner.run_batches(pg, settings, batch_size=16)
    assert seen_days == [date(2026, 7, 13), date(2026, 7, 14)]  # one batch per day
    assert seen_counts == [3, 2]  # day A's 3 WARCs, then day B's 2


def test_run_batches_reclaims_stale_processing_before_running(pg, settings, monkeypatch):
    """A WARC stranded in 'processing' by a prior killed run must be picked back up
    (reclaimed to pending) on the next run, not silently skipped forever."""
    path = "crawl-data/CC-NEWS/2026/07/CC-NEWS-20260714000000-09999.warc.gz"
    with pg.cursor() as cur:
        cur.execute("INSERT INTO warc_files (path, status, processed_at) "
                    "VALUES (%s, 'processing', now() - interval '3 hours')", (path,))
    pg.commit()

    def fake_download(batch, dest):
        out = []
        for p in batch:
            f = settings.ccnews_downloads_dir / Path(p).name
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"w")
            out.append(f)
        return out

    monkeypatch.setattr(runner.download, "download_batch", fake_download)
    monkeypatch.setattr(runner.pipeline, "process_batch", lambda **kw: None)
    monkeypatch.setattr(
        runner.dd, "run_dedup",
        lambda conn, extracted_dir, clean_path, text_ref, day: {"clean_out": 1},
    )

    staged = runner.run_batches(pg, settings, batch_size=4)
    assert staged == 1  # the stranded WARC was reclaimed and processed
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM warc_files WHERE path=%s", (path,))
        assert cur.fetchone()[0] == "done"


def test_batch_id_stable():
    paths = ["crawl-data/CC-NEWS/2026/07/CC-NEWS-20260713000000-00001.warc.gz"]
    assert runner.batch_id_for(paths) == runner.batch_id_for(paths)
    assert runner.batch_id_for(paths).startswith("20260713-")


def test_scan_processes_pending_hours(pg, settings, monkeypatch):
    tail.sync_hours(pg, start=date(2026, 7, 14), end=date(2026, 7, 15))

    def fake_download(client, name, dest_dir):
        # pending order is lexicographic: -0, -1, -10, -11, ... so the gap
        # must be one of the first six names for max_files=6 to reach it
        if name.endswith("-11.json.gz"):
            return None  # archive gap
        path = dest_dir / name
        with gzip.open(path, "wt") as f:
            f.write(json.dumps({"type": "WatchEvent", "repo": {"id": 9, "name": "o/r"}}) + "\n")
        return path

    monkeypatch.setattr(tail, "download_hour", fake_download)
    stats = tail.scan(pg, settings.gharchive_downloads_dir, max_files=6, keep=False)
    assert stats["missing"] == 1 and stats["files"] == 5
    assert stats["watch_events"] == 5
    with pg.cursor() as cur:
        cur.execute("SELECT star_events FROM repos WHERE repo_id = 9")
        assert cur.fetchone()[0] == 5
        cur.execute("SELECT count(*) FROM gharchive_files WHERE status = 'missing'")
        assert cur.fetchone()[0] == 1
    assert not any(settings.gharchive_downloads_dir.glob("*.json.gz"))
