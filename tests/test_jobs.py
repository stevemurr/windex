"""Job manager: strict whitelisting is the security boundary (API is on LAN)."""

import pytest
from fastapi.testclient import TestClient

import windex.api.app as app_mod
import windex.api.jobs as jobs
from windex.api.app import app


def test_build_argv_typed_and_bounded():
    job = jobs.JOBS["ccnews-run"]
    argv = jobs.build_argv(job, {"batch_size": "8", "max_batches": 4})
    assert argv[1:] == ["ccnews", "run", "--no-embed", "--batch-size", "8", "--max-batches", "4"]
    with pytest.raises(ValueError, match="out of range"):
        jobs.build_argv(job, {"batch_size": 9999})
    with pytest.raises(ValueError, match="unknown params"):
        jobs.build_argv(job, {"batch_size": 8, "rm_rf": "/"})


def test_build_argv_choice_and_date_validation():
    with pytest.raises(ValueError, match="must be one of"):
        jobs.build_argv(jobs.JOBS["reindex"], {"source": "everything; rm -rf /"})
    argv = jobs.build_argv(jobs.JOBS["reindex"], {"source": "news"})
    assert argv[1:] == ["reindex", "news", "--yes"]
    with pytest.raises(ValueError):
        jobs.build_argv(jobs.JOBS["gh-discover"], {"created_from": "not-a-date"})


def test_build_argv_wiki_jobs():
    assert jobs.build_argv(jobs.JOBS["wiki-sync"], {})[1:] == ["wiki", "sync"]
    argv = jobs.build_argv(jobs.JOBS["wiki-ingest"], {"max_files": 3})
    assert argv[1:] == ["wiki", "ingest", "--max-files", "3"]
    with pytest.raises(ValueError, match="out of range"):
        jobs.build_argv(jobs.JOBS["wiki-ingest"], {"max_files": 999})
    argv = jobs.build_argv(jobs.JOBS["wiki-embed"], {"limit": 5000})
    assert argv[1:] == ["wiki", "embed", "--limit", "5000"]


def test_build_argv_arxiv_jobs():
    assert jobs.build_argv(jobs.JOBS["arxiv-harvest"], {"days": 30})[1:] == \
        ["arxiv", "harvest", "--days", "30"]
    argv = jobs.build_argv(jobs.JOBS["arxiv-backfill"], {"from_year": 2005, "to_year": 2024})
    assert argv[1:] == ["arxiv", "harvest", "--from-year", "2005", "--to-year", "2024"]
    with pytest.raises(ValueError, match="out of range"):
        jobs.build_argv(jobs.JOBS["arxiv-backfill"], {"from_year": 1990})
    assert jobs.build_argv(jobs.JOBS["arxiv-embed"], {"limit": 5000})[1:] == \
        ["arxiv", "embed", "--limit", "5000"]


def test_arxiv_harvest_and_backfill_patterns_are_unambiguous():
    # both jobs launch `windex arxiv harvest`; their pgrep patterns must not
    # cross-match, or stopping one would kill the other (LAN-exposed control).
    harvest_cmd = " ".join(jobs.build_argv(jobs.JOBS["arxiv-harvest"], {}))
    backfill_cmd = " ".join(jobs.build_argv(jobs.JOBS["arxiv-backfill"], {}))
    assert jobs.JOBS["arxiv-harvest"].pattern in harvest_cmd
    assert jobs.JOBS["arxiv-harvest"].pattern not in backfill_cmd
    assert jobs.JOBS["arxiv-backfill"].pattern in backfill_cmd
    assert jobs.JOBS["arxiv-backfill"].pattern not in harvest_cmd


def test_no_job_accepts_arbitrary_strings_into_argv():
    # every param is int, date, or enum — no free-text reaches the command line
    for job in jobs.JOBS.values():
        for spec in job.params.values():
            assert spec.kind in ("int", "date", "choice")


@pytest.fixture()
def client(settings, monkeypatch):
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    return TestClient(app)


def test_jobs_endpoints(client, monkeypatch):
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [])
    spawned = {}

    class FakeProc:
        pid = 4242

    def fake_popen(argv, **kw):
        spawned["argv"] = argv
        return FakeProc()

    monkeypatch.setattr(jobs.subprocess, "Popen", fake_popen)
    listing = client.get("/v1/jobs").json()
    assert {j["name"] for j in listing} >= {"ccnews-run", "embed-loop", "reindex"}
    assert all(j["running"] is False for j in listing)

    r = client.post("/v1/jobs/ccnews-sync/start", json={"days": 7})
    assert r.status_code == 200 and r.json()["pid"] == 4242
    assert spawned["argv"][1:] == ["ccnews", "sync", "--days", "7"]

    assert client.post("/v1/jobs/nonsense/start", json={}).status_code == 404
    assert client.post("/v1/jobs/ccnews-run/start", json={"batch_size": -1}).status_code == 422

    monkeypatch.setattr(jobs, "_pids", lambda pattern: [999])
    assert client.post("/v1/jobs/ccnews-sync/start", json={}).status_code == 409
