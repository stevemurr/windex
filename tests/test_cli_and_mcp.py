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
    assert r.exit_code == 0 and "news_current" in r.output and "wiki_current" in r.output
    assert "arxiv_current" in r.output and "smallweb_current" in r.output
    assert "docs_current" in r.output and "hn_current" in r.output


def test_cli_wiki_status(settings, pg, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO wiki_dumps (name, dump_date, status) "
            "VALUES ('enwiki_content-20260712-00000.json.bz2', '20260712', 'done')"
        )
    pg.commit()
    r = runner.invoke(app, ["wiki", "status"])
    assert r.exit_code == 0 and "done" in r.output


def test_cli_arxiv_status(settings, pg, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO arxiv_windows (from_date, until_date, status) "
            "VALUES ('2024-01-01', '2024-12-31', 'done')"
        )
    pg.commit()
    r = runner.invoke(app, ["arxiv", "status"])
    assert r.exit_code == 0 and "done" in r.output


def test_cli_smallweb_status(settings, pg, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO feeds (url, host, status) VALUES "
            "('https://a.example/feed', 'a.example', 'active'),"
            "('https://b.example/feed', 'b.example', 'dead')"
        )
    pg.commit()
    r = runner.invoke(app, ["smallweb", "status"])
    assert r.exit_code == 0 and "active" in r.output and "dead" in r.output


def test_cli_docs_status(settings, pg, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO docsets (slug, release, mtime, status, ingested_mtime) VALUES "
            "('flask', '3.1.1', 1739347690, 'done', 1739347690),"
            "('vue~3', '3.5.38', 1782016732, 'pending', NULL)"
        )
    pg.commit()
    r = runner.invoke(app, ["docs", "status"])
    assert r.exit_code == 0 and "done" in r.output and "pending" in r.output


def test_cli_hn_status(settings, pg, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO hn_windows (from_ts, until_ts, status) VALUES "
            "(1159660800, 1162339200, 'done'),"       # 2006-10 backfill month
            "(1784073600, 1784332800, 'pending')"     # trailing window
        )
        cur.execute(
            "INSERT INTO documents (id, source, url, status) VALUES "
            "('hn:1', 'hn', 'https://news.ycombinator.com/item?id=1', 'embedded')"
        )
    pg.commit()
    r = runner.invoke(app, ["hn", "status"])
    assert r.exit_code == 0 and "done" in r.output and "pending" in r.output
    assert "embedded" in r.output


def test_reindex_resets_statuses_and_recreates_collections(settings, pg, qclient, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO documents (id, source, url, status, embedded_model, indexed_at)
               VALUES ('news:r1', 'news', 'u', 'embedded', 'pytest-model', now())"""
        )
        cur.execute(
            "INSERT INTO repos (repo_id, full_name, stars, status) VALUES (5, 'o/r', 20, 'embedded')"
        )
    pg.commit()
    r = runner.invoke(app, ["reindex", "all", "--yes"])
    assert r.exit_code == 0, r.output
    with pg.cursor() as cur:
        cur.execute("SELECT status, embedded_model FROM documents WHERE id = 'news:r1'")
        assert cur.fetchone() == ("deduped", None)
        cur.execute("SELECT status FROM repos WHERE repo_id = 5")
        assert cur.fetchone()[0] == "hydrated"
    from windex.index.qdrant import collection_name

    assert qclient.get_collection(collection_name("news", "pytest-model")).points_count == 0


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


def test_cli_maintain(settings, pg, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    r = runner.invoke(app, ["maintain"])
    assert r.exit_code == 0
    assert "vacuum analyze minhash_bands" in r.output
    assert "vacuum analyze documents" in r.output
    assert "skipping reindex" in r.output
    # --reindex path: tables are tiny in tests, so the >50MB size gate means
    # no index qualifies — the pass must still complete cleanly.
    r = runner.invoke(app, ["maintain", "--reindex"])
    assert r.exit_code == 0 and "vacuum analyze search_metrics" in r.output
