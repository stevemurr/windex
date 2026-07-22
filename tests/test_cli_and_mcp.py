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


def test_mcp_search_forwards_conversation_id_for_memory(settings, monkeypatch):
    """MCP is the interface most agents use, and must reach REST parity: memory
    recall needs a conversation_id to scope to one conversation. It was missing,
    so an MCP agent could only ever search the whole memory corpus."""
    import windex.api.mcp as mcp_mod

    captured = {}
    monkeypatch.setattr(mcp_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(mcp_mod.service, "run_search",
                        lambda s, q, **kw: captured.update(kw) or {"results": []})
    search_fn = getattr(mcp_mod.search_index, "fn", mcp_mod.search_index)
    search_fn("recall the sidebar bug", source="memory", conversation_id="conv-123")
    assert captured["source"] == "memory" and captured["conversation_id"] == "conv-123"


def test_reindex_commits_each_source_so_a_later_failure_does_not_roll_back(
    settings, pg, qclient, monkeypatch
):
    """reindex drops+recreates each source's Qdrant collection (irreversible) but
    committed all the status flips once at the end. A later source's Qdrant failure
    then rolled back the earlier sources' flips — leaving them status='embedded'
    but pointing at an emptied collection (unsearchable). Commit per source."""
    _use_test_settings(monkeypatch, settings)
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO documents (id, source, url, status, embedded_model, indexed_at) VALUES "
            "('news:x','news','u','embedded','pytest-model',now()),"
            "('wiki:x','wiki','u','embedded','pytest-model',now()),"
            "('arxiv:x','arxiv','u','embedded','pytest-model',now())"
        )
    pg.commit()

    from windex.index import qdrant as qidx
    real_ensure = qidx.ensure_collection

    def flaky(client, base, model, dim):
        if base == "arxiv":
            raise RuntimeError("qdrant timeout recreating arxiv")
        return real_ensure(client, base, model, dim)

    monkeypatch.setattr("windex.index.qdrant.ensure_collection", flaky)

    r = runner.invoke(app, ["reindex", "all", "--yes"])
    assert r.exit_code != 0  # arxiv failed the run
    with pg.cursor() as cur:
        cur.execute("SELECT id, status FROM documents ORDER BY id")
        st = dict(cur.fetchall())
    # news + wiki were reindexed (and their collections emptied) BEFORE arxiv
    # failed — their status must be committed 'deduped' to match, not rolled back
    assert st["news:x"] == "deduped" and st["wiki:x"] == "deduped"
    assert st["arxiv:x"] == "embedded"  # the failing source rolled back cleanly


def test_ccnews_sync_days_zero_is_honored(settings, pg, monkeypatch):
    """`--days 0` ('just today') must be honored, not silently expanded to the
    config default by `days or settings.news_backfill_days` (0 is falsy)."""
    _use_test_settings(monkeypatch, settings)
    from windex.ccnews import sync as ccsync
    captured = {}
    monkeypatch.setattr(ccsync, "sync", lambda conn, days, **k: captured.update(days=days) or 0)
    r = runner.invoke(app, ["ccnews", "sync", "--days", "0"])
    assert r.exit_code == 0
    assert captured["days"] == 0  # explicit 0, not the default window


def test_gh_embed_respects_the_dashboard_pause_flag(settings, pg, monkeypatch):
    """`gh embed` was the only per-source embed command that ignored the pause
    flag, so it kept hammering a paused/saturated embedder."""
    from windex import db as wdb

    _use_test_settings(monkeypatch, settings)
    wdb.set_control(pg, "indexing", "paused")
    called = []
    monkeypatch.setattr("windex.github.embed_index.embed_pending",
                        lambda *a, **k: called.append(1) or 0)
    r = runner.invoke(app, ["gh", "embed"])
    assert r.exit_code == 0 and not called  # paused → no embedding attempted


def test_gh_status_uses_the_configured_star_threshold(settings, pg, monkeypatch):
    """`gh status` hardcoded ≥3, unrelated to repo_star_threshold — misleading
    about how many repos will actually qualify for hydration."""
    s = settings.model_copy(update={"repo_star_threshold": 10})
    _use_test_settings(monkeypatch, s)
    with pg.cursor() as cur:
        cur.execute("INSERT INTO repos (repo_id, full_name, star_events) VALUES "
                    "(1, 'o/a', 5), (2, 'o/b', 15)")
    pg.commit()
    r = runner.invoke(app, ["gh", "status"])
    assert r.exit_code == 0
    assert "≥10 star events in window: 1" in r.output  # only o/b (15) meets ≥10


def test_daily_hydrate_carries_min_star_events_zero(settings, pg, monkeypatch):
    """daily()'s hydrate call omitted min_star_events=0, so it silently skipped
    every Search-API-sweep candidate (all star_events=0) — the fix is applied in
    the refresh chain and the gh-hydrate job but was missing from daily()."""
    s = settings.model_copy(update={"github_tokens": "tok1"})
    _use_test_settings(monkeypatch, s)
    from windex.api import service as api_service
    from windex.ccnews import dedup as dd
    from windex.ccnews import runner as ccrunner
    from windex.ccnews import sync as ccsync
    from windex.github import hydrate as gh_hydrate
    from windex.github import tail
    monkeypatch.setattr(ccsync, "sync", lambda *a, **k: 0)
    monkeypatch.setattr(ccrunner, "run_batches", lambda *a, **k: 0)
    monkeypatch.setattr(dd, "prune_bands", lambda *a, **k: 0)
    monkeypatch.setattr(tail, "sync_hours", lambda *a, **k: None)
    monkeypatch.setattr(tail, "scan", lambda *a, **k: {})
    monkeypatch.setattr(api_service, "prune_search_metrics", lambda *a, **k: 0)
    captured = {}
    monkeypatch.setattr(gh_hydrate, "hydrate", lambda conn, **kw: captured.update(kw) or {})

    r = runner.invoke(app, ["daily", "--no-embed"])
    assert r.exit_code == 0, r.output
    assert captured.get("min_star_events") == 0


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


def test_embed_loop_rejects_unknown_source(settings, monkeypatch):
    _use_test_settings(monkeypatch, settings)
    r = runner.invoke(app, ["embed-loop", "nonesuch"])
    assert r.exit_code == 1 and "unknown source" in r.output


def test_embed_loop_covers_every_embeddable_source():
    """Each source must be reachable by the supervised loop — an unsupervised
    one-shot pass dies on the first embedder hiccup (2026-07-17: 5 of 6
    backfills died within minutes that way)."""
    import importlib

    from windex.cli import EMBED_SOURCES

    assert set(EMBED_SOURCES) == {"ccnews", "wiki", "hn", "arxiv", "docs", "smallweb",
                                  "gh", "hf", "memory"}
    for src, mod in EMBED_SOURCES.items():
        assert hasattr(importlib.import_module(mod), "embed_pending"), src


def test_embed_loop_probes_forever_after_max_failures(settings, pg, monkeypatch):
    """A persistent outage must NOT kill the loop: it keeps retrying on the
    backoff and announces (once) that the endpoint looks down. Exiting used to
    strand the whole backlog when a ~25-min gateway blip tripped every loop
    (2026-07-17); --max-consecutive-failures now only marks down-mode."""
    import windex.wiki.embed_index as wiki_embed

    class _Stop(Exception):
        pass

    calls = []

    def boom(conn, s, limit=100_000):
        calls.append(1)
        raise RuntimeError("embedding request failed after 3 attempts")

    def stop_after_a_few(_):
        if len(calls) >= 5:  # threshold (3) + 2: prove it kept going past it
            raise _Stop

    _use_test_settings(monkeypatch, settings)
    monkeypatch.setattr(wiki_embed, "embed_pending", boom)
    monkeypatch.setattr("time.sleep", stop_after_a_few)
    r = runner.invoke(app, ["embed-loop", "wiki", "--max-consecutive-failures", "3"])
    assert isinstance(r.exception, _Stop), "loop must not exit on its own"
    assert r.exit_code != 2, "must not circuit-break"
    assert len(calls) >= 5, f"should keep retrying past the limit, got {len(calls)}"
    assert "endpoint appears down after 3 consecutive failures" in r.output
