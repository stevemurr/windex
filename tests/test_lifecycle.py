"""`windex up` / `down` / `status` — the orchestration logic only. Everything
external (containers, serve, loops, health probes) is stubbed so the tests
exercise ordering, idempotent skip-if-running, flag gating, and the JSON shape,
following the CliRunner + monkeypatch style of test_outage_guards.py."""

import subprocess

import pytest
from typer.testing import CliRunner

import windex.cli as cli
from windex.api import jobs, service

runner = CliRunner()


class _Settings:
    def __init__(self, data_root, embed_dim=0):
        self.data_root = data_root
        self.embed_dim = embed_dim
        self.pg_dsn = "postgresql://x"
        self.qdrant_url = "http://127.0.0.1:6333"
        self.embed_model = "m"
        self.serve_host = "127.0.0.1"


def _is_loop_start(event) -> bool:
    return isinstance(event, str) and (event == "embed-loop" or event.endswith("-embed"))


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    """Stub the environment so up/down/status run pure orchestration. tmp_path
    exists, so the mount preflight passes; health probes report ready."""
    events = []
    settings = _Settings(tmp_path)
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "_pg_ready", lambda s: True)
    monkeypatch.setattr(cli, "_qdrant_ready", lambda s: True)
    monkeypatch.setattr(cli, "init_db", lambda: events.append("init_db"))
    monkeypatch.setattr(cli, "ensure_collections", lambda: events.append("ensure_collections"))
    monkeypatch.setattr(subprocess, "run", lambda argv, **kw: events.append(("run", argv[-1])))
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [])
    monkeypatch.setattr(jobs, "serve_running", lambda port=8100: False)
    monkeypatch.setattr(jobs, "start_serve",
                        lambda host="127.0.0.1", port=8100: events.append("serve") or {"pid": 1})
    monkeypatch.setattr(jobs, "start", lambda name, params: events.append(name) or {"pid": 2})
    # scheduler is a managed process like serve: stub its start so `up` records
    # the intent without spawning a real timer loop. scheduler_running is left
    # real so it rides on the monkeypatched _pids (like serve's running probe).
    monkeypatch.setattr(jobs, "start_scheduler", lambda: events.append("scheduler") or {"pid": 3})
    # desired-state flags default all-enabled unless a test overrides
    monkeypatch.setattr(service, "get_loops_enabled",
                        lambda s: {src: True for src in cli.EMBED_SOURCES})
    return events, settings


def test_up_orders_containers_then_serve_then_loops(wired):
    events, _ = wired
    result = runner.invoke(cli.app, ["up"])
    assert result.exit_code == 0, result.output
    assert events[0] == ("run", "up")                      # containers first
    assert events.index(("run", "up")) < events.index("serve")
    assert events.index("serve") < events.index("embed-loop")  # loops after serve
    assert sum(_is_loop_start(e) for e in events) == 9


def test_up_skips_already_running_serve_and_loops(wired, monkeypatch):
    events, _ = wired
    monkeypatch.setattr(jobs, "serve_running", lambda port=8100: True)
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [123])
    result = runner.invoke(cli.app, ["up"])
    assert result.exit_code == 0, result.output
    assert "serve" not in events
    assert not any(_is_loop_start(e) for e in events)


def test_up_no_serve_no_loops(wired):
    events, _ = wired
    result = runner.invoke(cli.app, ["up", "--no-serve", "--no-loops"])
    assert result.exit_code == 0, result.output
    assert "serve" not in events
    assert not any(_is_loop_start(e) for e in events)


def test_up_source_subset(wired):
    events, _ = wired
    result = runner.invoke(cli.app, ["up", "--no-serve", "--source", "gh", "--source", "wiki"])
    assert result.exit_code == 0, result.output
    assert {e for e in events if _is_loop_start(e)} == {"gh-embed", "wiki-embed"}


def test_up_unknown_source_aborts(wired):
    result = runner.invoke(cli.app, ["up", "--source", "bogus"])
    assert result.exit_code == 1
    assert "unknown source" in result.output


def test_up_missing_mount_aborts_before_anything(wired):
    events, settings = wired
    settings.data_root = settings.data_root / "nonexistent"
    result = runner.invoke(cli.app, ["up"])
    assert result.exit_code == 1
    assert "not mounted" in result.output
    assert events == []


def test_up_health_gate_timeout_starts_nothing(wired, monkeypatch):
    events, _ = wired
    monkeypatch.setattr(cli, "_qdrant_ready", lambda s: False)
    result = runner.invoke(cli.app, ["up", "--timeout", "0"])
    assert result.exit_code == 1
    assert "timed out" in result.output
    assert "serve" not in events
    assert not any(_is_loop_start(e) for e in events)


def test_status_json_shape(wired, monkeypatch):
    _, _ = wired
    monkeypatch.setattr(jobs, "serve_running", lambda port=8100: True)
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [1])
    result = runner.invoke(cli.app, ["status", "--json"])
    assert result.exit_code == 0, result.output
    import json

    st = json.loads(result.output)
    assert set(st) >= {"up", "containers", "serve", "loops", "down"}
    assert st["up"] is True
    assert len(st["loops"]) == 9
    assert st["down"] == []


def test_status_json_reports_down_members(wired):
    # fixture defaults: serve down, all loops down (_pids → [])
    result = runner.invoke(cli.app, ["status", "--json"])
    assert result.exit_code == 0, result.output
    import json

    st = json.loads(result.output)
    assert st["up"] is False
    assert "serve" in st["down"]
    assert "scheduler" in st["down"]
    assert len(st["down"]) == 11  # serve + scheduler + 9 loops


def test_down_stops_loops_before_serve_and_keeps_containers(wired, monkeypatch):
    events, _ = wired
    stopped = []
    monkeypatch.setattr(jobs, "stop", lambda name: stopped.append(name) or {"pids": [1]})
    monkeypatch.setattr(jobs, "stop_serve", lambda: stopped.append("serve") or {"pids": [1]})
    result = runner.invoke(cli.app, ["down"])
    assert result.exit_code == 0, result.output
    assert stopped[-1] == "serve"                       # serve after the loops
    assert sum(1 for s in stopped if s != "serve") == 9
    assert not any(e == ("run", "down") for e in events)  # containers kept


def test_down_source_subset_leaves_serve(wired, monkeypatch):
    stopped = []
    monkeypatch.setattr(jobs, "stop", lambda name: stopped.append(name) or {"pids": []})
    monkeypatch.setattr(jobs, "stop_serve", lambda: stopped.append("serve") or {"pids": []})
    result = runner.invoke(cli.app, ["down", "--source", "hn"])
    assert result.exit_code == 0, result.output
    assert stopped == ["hn-embed"]                      # only the hn loop, serve untouched


def test_refresh_script_shape():
    s = cli._refresh_script(["gh", "arxiv"], "/venv/windex", "/repo")
    assert "true WINDEX_REFRESH" in s                    # pgrep marker
    assert 'cd "/repo"' in s
    assert '"/venv/windex" arxiv harvest --days 7' in s
    assert '"/venv/windex" gh hydrate --min-star-events 0' in s  # avoids the star-events trap
    assert " ; " in s                                    # sources isolated from each other
    assert "&&" in s                                     # steps within a source chained


def test_refresh_all_sources_covered():
    # Push sources (memory) have an embed loop but no pull ingest, so they are
    # deliberately absent from REFRESH_CHAINS; every other source has a chain.
    assert set(cli.REFRESH_CHAINS) == set(cli.EMBED_SOURCES) - jobs.PUSH_SOURCES


def test_refresh_detaches_one_sweep(monkeypatch):
    spawned = {}
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [])   # no sweep running
    monkeypatch.setattr(jobs, "_spawn",
                        lambda name, argv: spawned.setdefault("call", (name, argv)) or 4321)
    result = runner.invoke(cli.app, ["refresh"])
    assert result.exit_code == 0, result.output
    name, argv = spawned["call"]
    assert name == "refresh" and argv[0] == "bash" and argv[1] == "-lc"


def test_refresh_skips_when_already_running(monkeypatch):
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [999])
    spawned = []
    monkeypatch.setattr(jobs, "_spawn", lambda name, argv: spawned.append(name) or 1)
    result = runner.invoke(cli.app, ["refresh"])
    assert result.exit_code == 0
    assert "already running" in result.output
    assert spawned == []


def test_refresh_unknown_source_aborts(monkeypatch):
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [])
    result = runner.invoke(cli.app, ["refresh", "--source", "bogus"])
    assert result.exit_code == 1
    assert "unknown source" in result.output


def test_bare_refresh_honors_ingest_disabled(monkeypatch):
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [])
    monkeypatch.setattr(service, "get_ingest_enabled",
                        lambda s: {**{x: False for x in cli.REFRESH_CHAINS}, "hf": True})
    captured = {}
    monkeypatch.setattr(jobs, "_spawn", lambda name, argv: captured.__setitem__("argv", argv) or 1)
    result = runner.invoke(cli.app, ["refresh"])
    assert result.exit_code == 0, result.output
    script = captured["argv"][-1]
    assert "=== refresh hf ===" in script          # only the ingest-enabled source runs
    assert "=== refresh ccnews ===" not in script


def test_explicit_source_overrides_ingest_flag(monkeypatch):
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [])
    # ingest disabled everywhere, but an explicit --source is a manual "check now"
    monkeypatch.setattr(service, "get_ingest_enabled", lambda s: {x: False for x in cli.REFRESH_CHAINS})
    captured = {}
    monkeypatch.setattr(jobs, "_spawn", lambda name, argv: captured.__setitem__("argv", argv) or 1)
    result = runner.invoke(cli.app, ["refresh", "--source", "ccnews"])
    assert result.exit_code == 0, result.output
    assert "=== refresh ccnews ===" in captured["argv"][-1]


def test_refresh_script_records_ingest_timestamp():
    s = cli._refresh_script(["hf"], "/venv/windex", "/repo")
    assert '"/venv/windex" _mark-ingest hf' in s   # marks last-update on success


# --- desired-state on/off ---

def test_up_skips_disabled_sources(wired, monkeypatch):
    events, _ = wired
    monkeypatch.setattr(service, "get_loops_enabled",
                        lambda s: {**{src: True for src in cli.EMBED_SOURCES}, "hf": False})
    result = runner.invoke(cli.app, ["up", "--no-serve"])
    assert result.exit_code == 0, result.output
    started = [e for e in events if _is_loop_start(e)]
    assert "hf-embed" not in started          # disabled → not started by up (nor the watchdog)
    assert len(started) == 8                   # the other 8 enabled loops


def test_status_disabled_loop_is_not_down(wired, monkeypatch):
    """The crux: a disabled+stopped loop must NOT be in `down`, or the watchdog
    would restart it and 'off' would never stick."""
    monkeypatch.setattr(jobs, "serve_running", lambda port=8100: True)
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [])   # nothing running
    monkeypatch.setattr(service, "get_loops_enabled",
                        lambda s: {**{src: True for src in cli.EMBED_SOURCES}, "hf": False})
    result = runner.invoke(cli.app, ["status", "--json"])
    assert result.exit_code == 0, result.output
    import json
    st = json.loads(result.output)
    hf = next(entry for entry in st["loops"] if entry["source"] == "hf")
    assert hf["state"] == "disabled" and hf["enabled"] is False
    assert "hf" not in st["down"]
    # enabled loops are down, plus the (stopped) scheduler; serve is up here
    assert set(st["down"]) == (set(cli.EMBED_SOURCES) - {"hf"}) | {"scheduler"}


# --- scheduler as a supervised process ---

def test_up_starts_scheduler(wired):
    events, _ = wired
    result = runner.invoke(cli.app, ["up", "--no-serve", "--no-loops"])
    assert result.exit_code == 0, result.output
    assert "scheduler" in events          # started alongside serve + loops

def test_up_no_scheduler_flag_skips_it(wired):
    events, _ = wired
    result = runner.invoke(cli.app, ["up", "--no-serve", "--no-loops", "--no-scheduler"])
    assert result.exit_code == 0, result.output
    assert "scheduler" not in events

def test_up_skips_already_running_scheduler(wired, monkeypatch):
    events, _ = wired
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [77])   # scheduler already up
    result = runner.invoke(cli.app, ["up", "--no-serve", "--no-loops"])
    assert result.exit_code == 0, result.output
    assert "scheduler" not in events

def test_status_lists_scheduler_when_down(wired):
    # fixture defaults: serve down, _pids → [] (scheduler down)
    result = runner.invoke(cli.app, ["status", "--json"])
    assert result.exit_code == 0, result.output
    import json
    st = json.loads(result.output)
    assert st["scheduler"]["running"] is False
    assert "scheduler" in st["down"]

def test_status_scheduler_up_not_down(wired, monkeypatch):
    monkeypatch.setattr(jobs, "serve_running", lambda port=8100: True)
    monkeypatch.setattr(jobs, "_pids", lambda pattern: [1])    # everything running
    result = runner.invoke(cli.app, ["status", "--json"])
    assert result.exit_code == 0, result.output
    import json
    st = json.loads(result.output)
    assert st["scheduler"]["running"] is True
    assert "scheduler" not in st["down"]


def test_loop_command_toggles(monkeypatch):
    calls = {}
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(service, "set_loop_enabled",
                        lambda s, src, en: calls.__setitem__("args", (src, en)) or {"enabled": en})
    assert runner.invoke(cli.app, ["loop", "hf", "off"]).exit_code == 0
    assert calls["args"] == ("hf", False)
    assert runner.invoke(cli.app, ["loop", "hf", "on"]).exit_code == 0
    assert calls["args"] == ("hf", True)
    assert runner.invoke(cli.app, ["loop", "bogus", "off"]).exit_code == 1
    assert runner.invoke(cli.app, ["loop", "hf", "maybe"]).exit_code == 1
