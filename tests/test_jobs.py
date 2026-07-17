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
    argv = jobs.build_argv(jobs.JOBS["wiki-embed"], {})
    assert argv[1:] == ["embed-loop", "wiki"]


def test_build_argv_arxiv_jobs():
    assert jobs.build_argv(jobs.JOBS["arxiv-harvest"], {"days": 30})[1:] == \
        ["arxiv", "harvest", "--days", "30"]
    argv = jobs.build_argv(jobs.JOBS["arxiv-backfill"], {"from_year": 2005, "to_year": 2024})
    assert argv[1:] == ["arxiv", "harvest", "--from-year", "2005", "--to-year", "2024"]
    with pytest.raises(ValueError, match="out of range"):
        jobs.build_argv(jobs.JOBS["arxiv-backfill"], {"from_year": 1990})
    assert jobs.build_argv(jobs.JOBS["arxiv-embed"], {})[1:] == ["embed-loop", "arxiv"]


def test_build_argv_smallweb_jobs():
    assert jobs.build_argv(jobs.JOBS["smallweb-sync"], {})[1:] == ["smallweb", "sync"]
    argv = jobs.build_argv(jobs.JOBS["smallweb-poll"], {"max_feeds": 500})
    assert argv[1:] == ["smallweb", "poll", "--max-feeds", "500"]
    with pytest.raises(ValueError, match="out of range"):
        jobs.build_argv(jobs.JOBS["smallweb-poll"], {"max_feeds": 999999})
    assert jobs.build_argv(jobs.JOBS["smallweb-embed"], {})[1:] == ["embed-loop", "smallweb"]
    # reindex now accepts smallweb as a source
    assert jobs.build_argv(jobs.JOBS["reindex"], {"source": "smallweb"})[1:] == \
        ["reindex", "smallweb", "--yes"]


def test_build_argv_docs_jobs():
    assert jobs.build_argv(jobs.JOBS["docs-sync"], {})[1:] == ["docs", "sync"]
    argv = jobs.build_argv(jobs.JOBS["docs-ingest"], {"max_docsets": 5})
    assert argv[1:] == ["docs", "ingest", "--max-docsets", "5"]
    with pytest.raises(ValueError, match="out of range"):
        jobs.build_argv(jobs.JOBS["docs-ingest"], {"max_docsets": 9999})
    assert jobs.build_argv(jobs.JOBS["docs-embed"], {})[1:] == ["embed-loop", "docs"]
    # reindex now accepts docs as a source
    assert jobs.build_argv(jobs.JOBS["reindex"], {"source": "docs"})[1:] == \
        ["reindex", "docs", "--yes"]


def test_build_argv_hn_jobs():
    assert jobs.build_argv(jobs.JOBS["hn-harvest"], {"days": 3})[1:] == \
        ["hn", "harvest", "--days", "3"]
    argv = jobs.build_argv(jobs.JOBS["hn-backfill"], {"from_year": 2006, "to_year": 2020})
    assert argv[1:] == ["hn", "backfill", "--from-year", "2006", "--to-year", "2020"]
    with pytest.raises(ValueError, match="out of range"):
        jobs.build_argv(jobs.JOBS["hn-backfill"], {"from_year": 1999})
    with pytest.raises(ValueError, match="out of range"):
        jobs.build_argv(jobs.JOBS["hn-harvest"], {"days": 9999})
    assert jobs.build_argv(jobs.JOBS["hn-embed"], {})[1:] == ["embed-loop", "hn"]
    # reindex now accepts hn as a source
    assert jobs.build_argv(jobs.JOBS["reindex"], {"source": "hn"})[1:] == \
        ["reindex", "hn", "--yes"]
    # harvest/backfill launch different subcommands; their pgrep patterns must
    # not cross-match (stopping one would kill the other)
    harvest_cmd = " ".join(jobs.build_argv(jobs.JOBS["hn-harvest"], {}))
    backfill_cmd = " ".join(jobs.build_argv(jobs.JOBS["hn-backfill"], {}))
    assert jobs.JOBS["hn-harvest"].pattern in harvest_cmd
    assert jobs.JOBS["hn-harvest"].pattern not in backfill_cmd
    assert jobs.JOBS["hn-backfill"].pattern in backfill_cmd
    assert jobs.JOBS["hn-backfill"].pattern not in harvest_cmd


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


def test_embed_jobs_use_the_supervised_loop():
    """Every embed job must run under embed-loop, not a one-shot pass: an
    unsupervised pass dies on the first embedder hiccup (2026-07-17)."""
    from windex.api.jobs import JOBS
    from windex.cli import EMBED_SOURCES

    embed_jobs = [j for j in JOBS.values() if j.name.endswith("-embed") or j.name == "embed-loop"]
    assert len(embed_jobs) == 8  # one per embeddable source
    for j in embed_jobs:
        assert j.argv[0] == "embed-loop", f"{j.name} is not supervised: {j.argv}"
        assert j.argv[1] in EMBED_SOURCES, f"{j.name} targets unknown source {j.argv[1]}"


def test_job_patterns_match_their_own_argv():
    """The console decides 'running' by pgrep on `pattern`. If it drifts from
    argv, the dashboard reports idle while the job is working — exactly the
    decoupling the user hit on 2026-07-17."""
    from windex.api.jobs import JOBS, build_argv

    for j in JOBS.values():
        argv = build_argv(j, {})
        cmdline = " ".join(["windex", *argv[1:]])
        assert j.pattern in cmdline, f"{j.name}: pattern {j.pattern!r} won't match {cmdline!r}"


def test_stop_does_not_kill_a_shared_process_group(monkeypatch):
    """Regression (2026-07-17, user-reported: "stopping one embed job stopped
    them all"). start() uses start_new_session=True, so a job we launch leads
    its own group and killpg cleanly takes its children. A job started any other
    way (shell loop, script, cron) can share its parent's group with unrelated
    siblings — killpg there stops every other loop as collateral."""
    import os
    import signal

    from windex.api import jobs

    killed_groups, killed_pids = [], []
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [4242])
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: killed_groups.append(pgid))
    monkeypatch.setattr(os, "kill", lambda pid, sig: killed_pids.append(pid))

    # Shared group (pid is NOT the leader): must kill only this pid.
    monkeypatch.setattr(os, "getpgid", lambda pid: 999)
    jobs.stop("wiki-embed")
    assert killed_groups == [], "killpg on a shared group takes out sibling jobs"
    assert killed_pids == [4242]

    # Own group (pid leads it): killpg is safe and catches children.
    killed_groups.clear(); killed_pids.clear()
    monkeypatch.setattr(os, "getpgid", lambda pid: 4242)
    jobs.stop("wiki-embed")
    assert killed_groups == [4242]
    assert killed_pids == []
