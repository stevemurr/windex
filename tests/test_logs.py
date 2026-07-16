"""Log viewer: whitelist boundary, redaction, and tail hygiene."""

import pytest
from fastapi.testclient import TestClient

import windex.api.app as app_mod
import windex.api.logs as logsmod
from windex.api.app import app
from windex.api.logs import LogSource, QuietAccess


def test_registry_covers_jobs_and_infra():
    names = set(logsmod.LOGS)
    assert {"server", "watchdog", "postgres", "qdrant", "embed-loop", "ccnews-run"} <= names
    for s in logsmod.LOGS.values():
        assert s.kind in ("file", "container")


def test_redact_masks_configured_secrets_and_shapes(settings, monkeypatch):
    monkeypatch.setattr(
        logsmod, "_secret_values", lambda: ["sk-verysecretvalue000111"]
    )
    line = ("calling http://gpu:4000 key=sk-verysecretvalue000111 "
            "token ghp_abcdefghij0123456789xyz Authorization: bearer eyJtoken123456789012345")
    red = logsmod.redact(line)
    assert "sk-verysecretvalue000111" not in red
    assert "ghp_abcdefghij0123456789xyz" not in red
    assert "eyJtoken123456789012345" not in red
    assert red.count("•••") >= 3


def test_tail_cleans_ansi_and_progress_and_greps(tmp_path, monkeypatch):
    log = tmp_path / "x.log"
    log.write_bytes(
        b"\x1b[32mINFO\x1b[0m starting up\n"
        b"progress 1%\rprogress 50%\rprogress 100%\n"
        b"ERROR something broke\n"
    )
    monkeypatch.setitem(
        logsmod.LOGS, "x", LogSource("x", "X", "test", "server", path=log)
    )
    out = logsmod.tail("x", lines=10)
    assert out["available"] is True
    assert "\x1b" not in "".join(out["lines"])
    assert "progress 50%" in out["lines"]  # \r split into lines, not giant blanks
    grepped = logsmod.tail("x", lines=10, grep="error")
    assert grepped["lines"] == ["ERROR something broke"]


def test_tail_missing_file_is_unavailable_not_error(tmp_path, monkeypatch):
    monkeypatch.setitem(
        logsmod.LOGS, "gone",
        LogSource("gone", "G", "test", "server", path=tmp_path / "nope.log"),
    )
    assert logsmod.tail("gone") == {"name": "gone", "available": False, "lines": []}
    with pytest.raises(KeyError):
        logsmod.tail("../../etc/passwd")


def test_quiet_access_filter_drops_polling_noise():
    import logging

    f = QuietAccess()
    noisy = logging.LogRecord("uvicorn.access", 20, "", 0,
                              '127.0.0.1 - "GET /v1/stats HTTP/1.1" 200', (), None)
    useful = logging.LogRecord("uvicorn.access", 20, "", 0,
                               '127.0.0.1 - "GET /v1/search?q=x HTTP/1.1" 200', (), None)
    assert f.filter(noisy) is False
    assert f.filter(useful) is True


@pytest.fixture()
def client(settings, monkeypatch):
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    return TestClient(app)


def test_logs_endpoints(client, tmp_path, monkeypatch):
    log = tmp_path / "srv.log"
    log.write_text("hello world\n")
    monkeypatch.setitem(
        logsmod.LOGS, "server",
        LogSource("server", "Server", "test", "server", path=log),
    )
    listing = client.get("/v1/logs").json()
    assert any(s["name"] == "server" and s["available"] for s in listing)
    body = client.get("/v1/logs/server").json()
    assert body["lines"] == ["hello world"]
    assert client.get("/v1/logs/nonsense").status_code == 404
    assert client.get("/v1/logs/server", params={"lines": 99999}).status_code == 422