from typer.testing import CliRunner

import windex.cli as cli_mod
from windex.cli import app

runner = CliRunner()


def _use_test_settings(monkeypatch, settings):
    monkeypatch.setattr(cli_mod, "get_settings", lambda: settings)


def test_cli_init_db_and_health(settings, pg, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    r = runner.invoke(app, ["init-db"])
    assert r.exit_code == 0 and "schema applied" in r.output
    r = runner.invoke(app, ["health"])
    assert r.exit_code == 0 and "postgres ok" in r.output and "qdrant ok" in r.output


def test_cli_status_and_retry_failed(settings, pg, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    with pg.cursor() as cur:
        cur.execute("INSERT INTO warc_files (path, status) VALUES ('x.warc.gz', 'failed')")
    pg.commit()
    r = runner.invoke(app, ["ccnews", "status"])
    assert r.exit_code == 0 and "failed" in r.output
    r = runner.invoke(app, ["ccnews", "retry-failed"])
    assert "1 files requeued" in r.output
    r = runner.invoke(app, ["gh", "status"])
    assert r.exit_code == 0


def test_cli_ensure_collections(settings, qclient, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    r = runner.invoke(app, ["ensure-collections"])
    assert r.exit_code == 0 and "news_current" in r.output


def test_mcp_tools_wrap_service(settings, monkeypatch):
    import windex.api.mcp as mcp_mod

    monkeypatch.setattr(mcp_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(
        mcp_mod.service, "run_search",
        lambda s, q, **kw: {"query": q, "results": [], "took_ms": 1},
    )
    monkeypatch.setattr(mcp_mod.service, "get_document", lambda s, i: None)
    search_fn = getattr(mcp_mod.search_index, "fn", mcp_mod.search_index)
    doc_fn = getattr(mcp_mod.get_document, "fn", mcp_mod.get_document)
    assert search_fn("query text")["query"] == "query text"
    assert "error" in doc_fn("news:missing")
