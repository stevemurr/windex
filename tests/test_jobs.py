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


def test_stop_pattern_survives_permission_error_on_retry(monkeypatch):
    """A PermissionError on the fallback os.kill (same condition that triggered
    the fallback) must be swallowed, not surface as an unhandled 500 — the inner
    retry only caught ProcessLookupError."""
    monkeypatch.setattr(jobs, "_pids", lambda pat: [4242])
    monkeypatch.setattr(jobs.os, "getpgid",
                        lambda pid: (_ for _ in ()).throw(PermissionError()))
    monkeypatch.setattr(jobs.os, "kill",
                        lambda pid, sig: (_ for _ in ()).throw(PermissionError()))
    out = jobs._stop_pattern("ccnews-run", "pat")  # must not raise
    assert out == {"stopped": "ccnews-run", "pids": [4242]}


def test_start_serializes_concurrent_launches(monkeypatch, tmp_path):
    """TOCTOU: two near-simultaneous starts both passed the _pids() check and
    double-spawned. The spawn must be serialized so exactly one wins."""
    import threading
    import time

    monkeypatch.setattr(jobs, "LOG_DIR", tmp_path)  # keep the lockfile out of ~/.windex
    spawned = []

    monkeypatch.setattr(jobs, "_pids", lambda pat: [1] if spawned else [])
    monkeypatch.setattr(jobs, "build_argv", lambda job, p: ["x"])

    def fake_spawn(name, argv):
        time.sleep(0.05)  # widen the check→spawn window so an unlocked start races
        spawned.append(name)
        return 123

    monkeypatch.setattr(jobs, "_spawn", fake_spawn)

    errors = []

    def run():
        try:
            jobs.start("ccnews-run", {})
        except RuntimeError:
            errors.append(1)  # lost the race → "already running"

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(spawned) == 1, "double-spawn: check-and-spawn was not serialized"
    assert len(errors) == 1  # the loser got 'already running'


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
    assert len(embed_jobs) == 10  # one per embeddable source (incl. push-based memory + custom)
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


def test_serve_is_managed_but_not_in_the_whitelist():
    """serve must not be a startable/stoppable /v1/jobs entry — the LAN-exposed
    API can't be allowed to kill its own host — but it must be manageable by
    up/down/status, and its pattern must not cross-match `windex serve-mcp`."""
    assert "serve" not in jobs.JOBS
    launched = " ".join([str(jobs.VENV_BIN / "windex"), "serve", "--host", "127.0.0.1", "--port", "8100"])
    assert jobs.SERVE.pattern in launched
    assert jobs.SERVE.pattern not in "/opt/venv/bin/windex serve-mcp"


def test_embed_loop_jobs_are_every_source():
    """The one place that answers 'which loops should be running' — reused by
    windex up/status, the watchdog, and windex_loop_up."""
    from windex.cli import EMBED_SOURCES

    loops = jobs.embed_loop_jobs()
    assert len(loops) == 10  # eight pull sources + push-based memory + custom
    assert {j.argv[1] for j in loops} == set(EMBED_SOURCES)
    assert all(j.argv[0] == "embed-loop" for j in loops)


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


def test_loop_control_endpoint(client, monkeypatch):
    import windex.api.service as svc

    toggled = {}
    monkeypatch.setattr(svc, "set_loop_enabled",
                        lambda s, src, en: toggled.__setitem__("v", (src, en)) or {"source": src, "enabled": en})
    r = client.post("/v1/loops/hf", json={"enabled": False})
    assert r.status_code == 200 and toggled["v"] == ("hf", False)

    def _raise(s, src, en):
        raise KeyError(src)
    monkeypatch.setattr(svc, "set_loop_enabled", _raise)
    assert client.post("/v1/loops/bogus", json={"enabled": True}).status_code == 404


def test_ingest_endpoint(client, monkeypatch):
    import windex.api.service as svc

    seen = {}
    monkeypatch.setattr(svc, "set_ingest_enabled",
                        lambda s, src, en: seen.__setitem__("v", (src, en)) or {"source": src, "ingest_enabled": en})
    r = client.post("/v1/ingest/hf", json={"enabled": False})
    assert r.status_code == 200 and seen["v"] == ("hf", False)

    def _raise(s, src, en):
        raise KeyError(src)
    monkeypatch.setattr(svc, "set_ingest_enabled", _raise)
    assert client.post("/v1/ingest/bogus", json={"enabled": True}).status_code == 404


def test_system_action_endpoints(client, monkeypatch):
    import windex.api.service as svc

    monkeypatch.setattr(svc, "set_all_loops_enabled", lambda s, en: [{"source": "hf", "enabled": en}])
    monkeypatch.setattr(svc, "system_up", lambda s: {"action": "up", "pid": 1})
    monkeypatch.setattr(svc, "restart_loops", lambda s: {"action": "restart", "pid": 2})
    monkeypatch.setattr(svc, "run_refresh", lambda s, sources: {"action": "refresh", "sources": sources})

    assert client.post("/v1/system/loops", json={"enabled": False}).json()["loops"][0]["enabled"] is False
    assert client.post("/v1/system/up").json()["action"] == "up"
    assert client.post("/v1/system/restart").json()["action"] == "restart"
    assert client.post("/v1/system/refresh", json={"sources": ["hf"]}).json()["sources"] == ["hf"]


def test_loops_state_endpoint(client, monkeypatch):
    import windex.api.service as svc

    monkeypatch.setattr(svc, "supervisor_status", lambda s: {
        "watchdog_running": True,
        "loops": [{"source": "hf", "enabled": True, "running": True, "state": "up", "pids": [1]}],
    })
    d = client.get("/v1/loops").json()
    assert d["watchdog_running"] is True and d["loops"][0]["source"] == "hf"


def test_freshness_and_schedule_endpoints(client, monkeypatch):
    import windex.api.service as svc

    monkeypatch.setattr(svc, "freshness",
                        lambda s: [{"source": "hf", "indexed": 10, "pending": 2, "last_embed_ts": 1.0}])
    monkeypatch.setattr(svc, "schedule_status",
                        lambda s: [{"name": "daily", "running": False, "last_run_ts": None}])

    def _run(s, name):
        if name == "daily":
            return {"action": "daily", "pid": 7}
        raise KeyError(name)
    monkeypatch.setattr(svc, "run_scheduled", _run)

    assert client.get("/v1/freshness").json()[0]["source"] == "hf"
    assert client.get("/v1/schedule").json()[0]["name"] == "daily"
    assert client.post("/v1/schedule/daily/run").json()["action"] == "daily"
    assert client.post("/v1/schedule/bogus/run").status_code == 404


def test_scheduler_is_managed_but_not_in_the_whitelist():
    """The scheduler mirrors serve: a managed process the LAN-exposed /v1/jobs
    whitelist must NOT be able to stop, but up/status/the watchdog manage it.
    Its pattern must not cross-match serve/serve-mcp/embed-loop."""
    assert "scheduler" not in jobs.JOBS
    launched = " ".join([str(jobs.VENV_BIN / "windex"), "scheduler"])
    assert jobs.SCHEDULER.pattern in launched
    assert jobs.SCHEDULER.pattern not in "/opt/venv/bin/windex serve --host 127.0.0.1"
    assert jobs.SCHEDULER.pattern not in "/opt/venv/bin/windex serve-mcp"
    assert jobs.SCHEDULER.pattern not in "/opt/venv/bin/windex embed-loop hf"


def test_is_due_matching():
    """The pure due-entry predicate: enabled + hour/minute match + weekday
    (or NULL) + not already run this minute."""
    from datetime import datetime, timedelta

    from windex.api import service

    now = datetime(2026, 7, 20, 3, 15)
    dow = (now.weekday() + 1) % 7  # schedule weekday convention: Sun=0
    base = {"name": "x", "kind": "ingest", "target": "hf", "hour": 3, "minute": 15,
            "weekday": None, "enabled": True, "last_run": None}

    assert service._is_due(base, now)
    assert not service._is_due({**base, "enabled": False}, now)       # disabled
    assert not service._is_due({**base, "minute": 16}, now)           # wrong minute
    assert not service._is_due({**base, "hour": 4}, now)              # wrong hour
    assert service._is_due({**base, "weekday": dow}, now)             # weekday matches
    assert not service._is_due({**base, "weekday": (dow + 1) % 7}, now)  # weekday off
    # already fired this minute → not due; a prior minute → due again
    assert not service._is_due({**base, "last_run": now}, now)
    assert service._is_due({**base, "last_run": now - timedelta(minutes=1)}, now)


def test_run_due_dispatch_and_ingest_gate(monkeypatch):
    """One scheduler tick: fires enabled+due entries, gates ingest on the
    source's ingest_enabled flag, dispatches ingest→refresh and command→windex,
    stamps last_run, and skips not-due entries. Fully stubbed — no DB, no spawn."""
    from datetime import datetime

    from windex.api import service

    now = datetime(2026, 7, 20, 3, 0)
    entries = [
        {"name": "ingest-ccnews", "kind": "ingest", "target": "ccnews", "hour": 3,
         "minute": 0, "weekday": None, "enabled": True, "last_run": None},
        {"name": "ingest-hf", "kind": "ingest", "target": "hf", "hour": 3,
         "minute": 0, "weekday": None, "enabled": True, "last_run": None},
        {"name": "daily", "kind": "command", "target": "daily", "hour": 3,
         "minute": 0, "weekday": None, "enabled": True, "last_run": None},
        {"name": "maintain", "kind": "command", "target": "maintain", "hour": 5,
         "minute": 45, "weekday": None, "enabled": True, "last_run": None},  # not due
    ]
    spawned, marked = [], []
    monkeypatch.setattr(service, "_read_schedule", lambda s: [dict(e) for e in entries])
    monkeypatch.setattr(service, "get_ingest_enabled", lambda s: {"ccnews": True, "hf": False})
    monkeypatch.setattr(service, "_spawn_windex", lambda args, log: spawned.append(args) or 1)
    monkeypatch.setattr(service, "_mark_ran", lambda s, name, when: marked.append(name))

    fired = service.run_due(object(), now)
    assert set(fired) == {"ingest-ccnews", "daily"}   # hf gated off, maintain not due
    assert set(marked) == {"ingest-ccnews", "daily"}
    assert ["refresh", "--source", "ccnews"] in spawned   # ingest → refresh sweep
    assert ["daily"] in spawned                            # command → windex daily
    assert ["maintain"] not in spawned


def test_run_due_survives_db_blip(monkeypatch):
    """A failed table read must not raise out of the tick (the loop backs off)."""
    from windex.api import service

    def _boom(s):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(service, "_read_schedule", _boom)
    assert service.run_due(object()) == []


def test_schedule_crud_endpoints(client):
    """CRUD over the seeded schedule table via the API (DB-touching)."""
    listing = client.get("/v1/schedule").json()
    names = {e["name"] for e in listing}
    assert "daily" in names and "maintain" in names          # seeded command jobs
    assert any(e["kind"] == "ingest" for e in listing)       # seeded ingest jobs
    daily = next(e for e in listing if e["name"] == "daily")
    assert {"kind", "target", "hour", "minute", "weekday", "enabled",
            "last_run", "running"} <= set(daily)

    # create a fresh ingest entry
    r = client.put("/v1/schedule/test-ingest",
                   json={"kind": "ingest", "target": "hf", "hour": 9, "minute": 30})
    assert r.status_code == 200, r.text
    got = next(e for e in client.get("/v1/schedule").json() if e["name"] == "test-ingest")
    assert got["hour"] == 9 and got["minute"] == 30 and got["enabled"] is True

    # partial edit preserves unspecified fields (hour/minute stay)
    assert client.put("/v1/schedule/test-ingest", json={"enabled": False}).status_code == 200
    got = next(e for e in client.get("/v1/schedule").json() if e["name"] == "test-ingest")
    assert got["enabled"] is False and got["hour"] == 9

    # invalid entries → 422
    assert client.put("/v1/schedule/bad", json={"kind": "nope", "target": "x"}).status_code == 422
    assert client.put("/v1/schedule/bad2", json={"kind": "ingest", "target": "nope"}).status_code == 422
    assert client.put("/v1/schedule/bad3", json={"kind": "command", "target": "daily",
                                                 "minute": 99}).status_code == 422
    assert client.put("/v1/schedule/bad4", json={"enabled": True}).status_code == 422  # no kind/target on create

    # delete (and a second delete is 404)
    assert client.delete("/v1/schedule/test-ingest").status_code == 200
    assert client.delete("/v1/schedule/test-ingest").status_code == 404


def test_dataset_stats_endpoint(client, monkeypatch):
    import windex.api.service as svc

    monkeypatch.setattr(svc, "dataset_stats", lambda s, src: {
        "source": src, "by_status": {"embedded": 5}, "total": 5,
        "content_from": None, "content_to": None})
    assert client.get("/v1/datasets/hf/stats").json()["source"] == "hf"

    def _raise(s, src):
        raise KeyError(src)
    monkeypatch.setattr(svc, "dataset_stats", _raise)
    assert client.get("/v1/datasets/bogus/stats").status_code == 404


def test_activity_endpoint(client, monkeypatch):
    import windex.api.service as svc

    monkeypatch.setattr(svc, "activity", lambda s: [
        {"name": "refresh", "label": "Refresh sweep", "group": "action",
         "running": True, "last_ts": 1, "error": False}])
    d = client.get("/v1/activity").json()
    assert d[0]["name"] == "refresh" and d[0]["group"] == "action"
