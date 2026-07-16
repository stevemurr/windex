import typer
from rich.console import Console

from windex import db
from windex.config import get_settings

app = typer.Typer(no_args_is_help=True, help="windex — self-hosted web index for search agents")
ccnews_app = typer.Typer(no_args_is_help=True, help="CC-News ingestion")
app.add_typer(ccnews_app, name="ccnews")
console = Console()


@ccnews_app.command("sync")
def ccnews_sync(days: int = typer.Option(None, help="Window in days (default: config)")) -> None:
    """Record unseen in-window WARC paths as pending."""
    from windex.ccnews import sync as ccsync

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        n = ccsync.sync(conn, days or settings.news_backfill_days)
    console.print(f"[green]{n} new WARC files pending[/green]")


@ccnews_app.command("run")
def ccnews_run(
    batch_size: int = 16,
    max_batches: int = typer.Option(None),
    keep_warcs: bool = False,
    workers: int = 0,
    embed: bool = typer.Option(True, help="Embed after processing (needs WINDEX_EMBED_*)"),
) -> None:
    """Process pending WARCs: download → extract/filter → dedup [→ embed]."""
    from windex.ccnews import dedup as dd
    from windex.ccnews import runner

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        staged = runner.run_batches(
            conn, settings, batch_size=batch_size, max_batches=max_batches,
            keep_warcs=keep_warcs, workers=workers,
        )
        pruned = dd.prune_bands(conn, settings.minhash_window_days)
        console.print(f"[green]staged {staged} docs[/green] (pruned {pruned} old bands)")
        if embed and settings.embed_dim > 0:
            from windex.ccnews.embed_index import embed_pending

            n = embed_pending(conn, settings)
            console.print(f"[green]embedded {n} docs[/green]")
        elif embed:
            console.print("[yellow]skipping embed: WINDEX_EMBED_* not configured[/yellow]")


@ccnews_app.command("embed")
def ccnews_embed(limit: int = 50_000) -> None:
    """Embed deduped docs into Qdrant. Respects the dashboard pause flag."""
    from windex.ccnews.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} docs[/green]")


def _processor_alive() -> bool:
    import subprocess

    return subprocess.run(
        ["pgrep", "-f", "ccnews run"], capture_output=True
    ).returncode == 0


@ccnews_app.command("embed-loop")
def ccnews_embed_loop(
    interval: int = 30,
    max_consecutive_failures: int = 10,
) -> None:
    """Long-running embed drainer: follows the processor, backs off on errors,
    and circuit-breaks (exit code 2) instead of spinning against dead services.
    Exits cleanly when the backlog is drained and no processor is running."""
    import time as time_mod

    from windex.ccnews.embed_index import embed_pending

    settings = get_settings()
    failures = 0
    while True:
        try:
            with db.connect(settings.pg_dsn) as conn:
                if db.get_control(conn, "indexing", "running") == "paused":
                    console.print("paused — waiting")
                    time_mod.sleep(interval)
                    continue
                n = embed_pending(conn, settings)
            failures = 0
            console.print(f"embedded {n} docs")
            if n == 0:
                if not _processor_alive():
                    console.print("[green]backlog drained, no processor — done[/green]")
                    return
                time_mod.sleep(interval)
        except Exception as exc:
            failures += 1
            console.print(
                f"[red]embed cycle failed ({failures}/{max_consecutive_failures}): {exc}[/red]"
            )
            if failures >= max_consecutive_failures:
                console.print("[red]circuit breaker tripped — exiting[/red]")
                raise typer.Exit(2)
            time_mod.sleep(min(interval * failures, 300))


@ccnews_app.command("status")
def ccnews_status() -> None:
    """WARC watermark + document pipeline counts."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM warc_files GROUP BY status ORDER BY status")
        console.print({r[0]: r[1] for r in cur.fetchall()}, "warc_files")
        cur.execute(
            "SELECT status, count(*) FROM documents WHERE source='news' GROUP BY status ORDER BY status"
        )
        console.print({r[0]: r[1] for r in cur.fetchall()}, "documents")


@ccnews_app.command("retry-failed")
def ccnews_retry_failed() -> None:
    """Requeue failed WARC files as pending."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("UPDATE warc_files SET status = 'pending' WHERE status = 'failed'")
        console.print(f"[green]{cur.rowcount} files requeued[/green]")
        conn.commit()


gh_app = typer.Typer(no_args_is_help=True, help="GitHub ingestion (GH Archive + API hydration)")
app.add_typer(gh_app, name="gh")


@gh_app.command("sync-hours")
def gh_sync_hours(
    days: int = typer.Option(None, help="Trailing window of hourly files"),
    start: str = typer.Option(None, help="Explicit range start (YYYY-MM-DD)"),
    end: str = typer.Option(None, help="Explicit range end, exclusive (YYYY-MM-DD)"),
) -> None:
    """Record unseen GH Archive hour files as pending.

    Star-rich bootstrap window (pre Events-API change): --start 2024-10-01 --end 2025-10-01
    """
    from datetime import date

    from windex.github import tail

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        n = tail.sync_hours(
            conn,
            days=days,
            start=date.fromisoformat(start) if start else None,
            end=date.fromisoformat(end) if end else None,
        )
    console.print(f"[green]{n} new hour files pending[/green]")


@gh_app.command("discover")
def gh_discover(
    created_from: str = typer.Option("2025-10-01", help="Sweep repos created since (YYYY-MM-DD)"),
    created_to: str = typer.Option(None, help="Sweep upper bound (default today)"),
) -> None:
    """Search-API sweep for repos ≥ star threshold (post-2025-10 star discovery)."""
    from datetime import date

    from windex.github import discover

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        stats = discover.sweep(
            conn,
            tokens=settings.github_token_list(),
            star_threshold=settings.repo_star_threshold,
            created_from=date.fromisoformat(created_from),
            created_to=date.fromisoformat(created_to) if created_to else None,
        )
    console.print(stats)


@gh_app.command("scan")
def gh_scan(
    max_files: int = typer.Option(None, help="Stop after N files (default: all pending)"),
    keep: bool = False,
) -> None:
    """Stream pending hour files, counting star events per repo."""
    from windex.github import tail

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        stats = tail.scan(conn, settings.gharchive_downloads_dir, max_files=max_files, keep=keep)
    console.print(stats)


@gh_app.command("hydrate")
def gh_hydrate(
    limit: int = 10_000,
    min_star_events: int = typer.Option(1, help="Only hydrate candidates with ≥N star events"),
) -> None:
    """Fetch metadata + README for candidate repos via GraphQL."""
    from windex.github import hydrate as gh_hydrate_mod

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        stats = gh_hydrate_mod.hydrate(
            conn,
            tokens=settings.github_token_list(),
            readme_dir=settings.repos_staging_dir / "readme",
            star_threshold=settings.repo_star_threshold,
            limit=limit,
            min_star_events=min_star_events,
        )
    console.print(stats)


@gh_app.command("embed")
def gh_embed(limit: int = 100_000) -> None:
    """Compose, embed, and index hydrated repos."""
    from windex.github.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} repos[/green]")


wiki_app = typer.Typer(no_args_is_help=True, help="Wikipedia ingestion (CirrusSearch dumps)")
app.add_typer(wiki_app, name="wiki")


@wiki_app.command("sync")
def wiki_sync() -> None:
    """Record the newest complete Wikipedia snapshot's shard files as pending."""
    from windex.wiki import sync as wsync

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        n = wsync.sync(conn, settings.wiki_dump)
    console.print(f"[green]{n} new dump shards pending[/green]")


@wiki_app.command("ingest")
def wiki_ingest(
    max_files: int = typer.Option(None, help="Stop after N shards (default: all pending)"),
    chunk_rows: int = typer.Option(None, help="Rows per parquet row group / commit"),
) -> None:
    """Stream pending shards → clean parquet + documents ledger (changed-article delta)."""
    from windex.wiki import ingest as wingest

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        stats = wingest.ingest(conn, settings, max_files=max_files, chunk_rows=chunk_rows)
    console.print(stats)


@wiki_app.command("embed")
def wiki_embed(limit: int = 100_000) -> None:
    """Embed staged Wikipedia articles into Qdrant. Respects the dashboard pause flag."""
    from windex.wiki.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} articles[/green]")


@wiki_app.command("status")
def wiki_status() -> None:
    """Dump-shard watermark + document pipeline counts."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM wiki_dumps GROUP BY status ORDER BY status")
        console.print({r[0]: r[1] for r in cur.fetchall()}, "wiki_dumps")
        cur.execute(
            "SELECT status, count(*) FROM documents WHERE source='wiki' GROUP BY status ORDER BY status"
        )
        console.print({r[0]: r[1] for r in cur.fetchall()}, "documents")


arxiv_app = typer.Typer(no_args_is_help=True, help="arXiv ingestion (OAI-PMH metadata harvest)")
app.add_typer(arxiv_app, name="arxiv")


@arxiv_app.command("harvest")
def arxiv_harvest(
    days: int = typer.Option(None, help="Incremental window: harvest the last N days (default: config)"),
    from_year: int = typer.Option(None, help="Backfill: earliest year (per-year windows)"),
    to_year: int = typer.Option(None, help="Backfill: latest year (default: current year)"),
    max_windows: int = typer.Option(None, help="Stop after N windows (default: all pending)"),
) -> None:
    """Harvest arXiv metadata over OAI-PMH → clean parquet + documents ledger.

    Incremental (default): a rolling last-N-days window. Backfill: pass --from-year
    to plan independently restartable per-year windows (the whole corpus is
    --from-year 2005). Idempotent; the text_hash ledger keeps re-harvests to the
    changed-paper delta.
    """
    from datetime import date

    from windex.arxiv import harvest as aharvest

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if from_year is not None:
            planned = aharvest.plan_backfill(conn, from_year, to_year or date.today().year)
            console.print(f"[green]{planned} new per-year windows planned[/green]")
        else:
            frm, until = aharvest.plan_incremental(conn, days or settings.arxiv_incremental_days)
            console.print(f"[green]incremental window {frm}..{until} armed[/green]")
        stats = aharvest.harvest(conn, settings, max_windows=max_windows)
    console.print(stats)


@arxiv_app.command("embed")
def arxiv_embed(limit: int = 100_000) -> None:
    """Embed staged arXiv papers into Qdrant. Respects the dashboard pause flag."""
    from windex.arxiv.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} papers[/green]")


@arxiv_app.command("status")
def arxiv_status() -> None:
    """Harvest-window watermark + document pipeline counts."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM arxiv_windows GROUP BY status ORDER BY status")
        console.print({r[0]: r[1] for r in cur.fetchall()}, "arxiv_windows")
        cur.execute(
            "SELECT status, count(*) FROM documents WHERE source='arxiv' GROUP BY status ORDER BY status"
        )
        console.print({r[0]: r[1] for r in cur.fetchall()}, "documents")


smallweb_app = typer.Typer(no_args_is_help=True, help="Small Web ingestion (Kagi RSS/Atom blog feeds)")
app.add_typer(smallweb_app, name="smallweb")


@smallweb_app.command("sync")
def smallweb_sync() -> None:
    """Reconcile the feeds table against Kagi's smallweb.txt (idempotent)."""
    from windex.smallweb import sync as swsync

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        stats = swsync.sync(conn, url=settings.smallweb_list_url)
    console.print(stats)


@smallweb_app.command("poll")
def smallweb_poll(
    max_feeds: int = typer.Option(None, help="Stop after N feeds (default: all active)"),
) -> None:
    """Conditional-GET active feeds, fetch + extract new posts → clean parquet +
    ledger. Polite: honors robots.txt, a per-host interval, and the pause flag."""
    from windex.smallweb import poll as swpoll

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping poll[/yellow]")
            raise typer.Exit(0)
        stats = swpoll.poll(conn, settings, max_feeds=max_feeds)
    console.print(stats)


@smallweb_app.command("embed")
def smallweb_embed(limit: int = 100_000) -> None:
    """Embed staged Small Web posts into Qdrant. Respects the dashboard pause flag."""
    from windex.smallweb.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} posts[/green]")


@smallweb_app.command("status")
def smallweb_status() -> None:
    """Feed registry + document pipeline counts."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM feeds GROUP BY status ORDER BY status")
        console.print({r[0]: r[1] for r in cur.fetchall()}, "feeds")
        cur.execute(
            "SELECT status, count(*) FROM documents WHERE source='smallweb' "
            "GROUP BY status ORDER BY status"
        )
        console.print({r[0]: r[1] for r in cur.fetchall()}, "documents")


docs_app = typer.Typer(no_args_is_help=True, help="Programming docs ingestion (DevDocs bundles)")
app.add_typer(docs_app, name="docs")


@docs_app.command("sync")
def docs_sync() -> None:
    """Fetch the DevDocs manifest and upsert the docsets watermark table."""
    from windex.docs_source import sync as dsync

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        stats = dsync.sync(conn, url=settings.docs_manifest_url)
        pending = dsync.pending_docsets(conn, settings.docs_slug_list())
    console.print(stats)
    console.print(f"[green]{len(pending)} seed docsets pending ingest[/green]")


@docs_app.command("ingest")
def docs_ingest(
    max_docsets: int = typer.Option(None, help="Stop after N docsets (default: all pending)"),
) -> None:
    """Fetch pending docsets → clean parquet + documents ledger (changed-page
    delta; vanished pages tombstoned). Full-replace per slug; idempotent."""
    from windex.docs_source import ingest as dingest

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        stats = dingest.ingest(conn, settings, max_docsets=max_docsets)
    console.print(stats)


@docs_app.command("embed")
def docs_embed(limit: int = 100_000) -> None:
    """Embed staged documentation pages into Qdrant. Respects the dashboard pause flag."""
    from windex.docs_source.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} pages[/green]")


@docs_app.command("status")
def docs_status() -> None:
    """Docset watermark + document pipeline counts."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM docsets GROUP BY status ORDER BY status")
        console.print({r[0]: r[1] for r in cur.fetchall()}, "docsets")
        cur.execute(
            """SELECT count(*) FROM docsets WHERE slug = ANY(%s)
               AND (ingested_mtime IS NULL OR mtime > ingested_mtime)""",
            (settings.docs_slug_list(),),
        )
        console.print(f"seed docsets pending ingest: {cur.fetchone()[0]}")
        cur.execute(
            "SELECT status, count(*) FROM documents WHERE source='docs' GROUP BY status ORDER BY status"
        )
        console.print({r[0]: r[1] for r in cur.fetchall()}, "documents")


@app.command()
def reindex(
    source: str = typer.Argument("all", help="news | repos | wiki | arxiv | smallweb | docs | all"),
    drop_collections: bool = typer.Option(True, help="Recreate Qdrant collections from scratch"),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation"),
) -> None:
    """Rebuild vectors from staged parquet (the reproducibility path: extracted
    text is the source of truth; vectors are always derivable). Resets embedded
    docs and recreates collections; the embed loop / gh embed repopulate."""
    from windex.index import qdrant as qidx

    settings = get_settings()
    if not yes:
        typer.confirm(
            f"Drop and rebuild the {source} vector index from parquet?", abort=True
        )
    client = qidx.client_from_url(settings.qdrant_url)
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        if source in ("news", "all"):
            if drop_collections:
                name = qidx.collection_name("news", settings.embed_model)
                if client.collection_exists(name):
                    client.delete_collection(name)
                qidx.ensure_collection(client, "news", settings.embed_model, settings.embed_dim)
            cur.execute(
                """UPDATE documents SET status='deduped', embedded_model=NULL, indexed_at=NULL
                   WHERE source='news' AND status='embedded'"""
            )
            console.print(f"[green]news: {cur.rowcount} docs queued for re-embed[/green]")
        if source in ("repos", "all"):
            if drop_collections:
                name = qidx.collection_name("repos", settings.embed_model)
                if client.collection_exists(name):
                    client.delete_collection(name)
                qidx.ensure_collection(client, "repos", settings.embed_model, settings.embed_dim)
            cur.execute("UPDATE repos SET status='hydrated' WHERE status='embedded'")
            console.print(f"[green]repos: {cur.rowcount} queued for re-embed[/green]")
        if source in ("wiki", "all"):
            if drop_collections:
                name = qidx.collection_name("wiki", settings.embed_model)
                if client.collection_exists(name):
                    client.delete_collection(name)
                qidx.ensure_collection(client, "wiki", settings.embed_model, settings.embed_dim)
            cur.execute(
                """UPDATE documents SET status='deduped', embedded_model=NULL, indexed_at=NULL
                   WHERE source='wiki' AND status='embedded'"""
            )
            console.print(f"[green]wiki: {cur.rowcount} docs queued for re-embed[/green]")
        if source in ("arxiv", "all"):
            if drop_collections:
                name = qidx.collection_name("arxiv", settings.embed_model)
                if client.collection_exists(name):
                    client.delete_collection(name)
                qidx.ensure_collection(client, "arxiv", settings.embed_model, settings.embed_dim)
            cur.execute(
                """UPDATE documents SET status='deduped', embedded_model=NULL, indexed_at=NULL
                   WHERE source='arxiv' AND status='embedded'"""
            )
            console.print(f"[green]arxiv: {cur.rowcount} docs queued for re-embed[/green]")
        if source in ("smallweb", "all"):
            if drop_collections:
                name = qidx.collection_name("smallweb", settings.embed_model)
                if client.collection_exists(name):
                    client.delete_collection(name)
                qidx.ensure_collection(client, "smallweb", settings.embed_model, settings.embed_dim)
            cur.execute(
                """UPDATE documents SET status='deduped', embedded_model=NULL, indexed_at=NULL
                   WHERE source='smallweb' AND status='embedded'"""
            )
            console.print(f"[green]smallweb: {cur.rowcount} docs queued for re-embed[/green]")
        if source in ("docs", "all"):
            if drop_collections:
                name = qidx.collection_name("docs", settings.embed_model)
                if client.collection_exists(name):
                    client.delete_collection(name)
                qidx.ensure_collection(client, "docs", settings.embed_model, settings.embed_dim)
            cur.execute(
                """UPDATE documents SET status='deduped', embedded_model=NULL, indexed_at=NULL
                   WHERE source='docs' AND status='embedded'"""
            )
            console.print(f"[green]docs: {cur.rowcount} docs queued for re-embed[/green]")
        conn.commit()
    console.print(
        "run `windex ccnews embed-loop`, `windex gh embed`, `windex wiki embed`, "
        "`windex arxiv embed`, `windex smallweb embed`, `windex docs embed` to repopulate"
    )


@app.command()
def daily(embed: bool = True) -> None:
    """The daily freshness job: news sync+process+embed, gh tail+hydrate refresh.

    Cron this once a day. Idempotent: reruns are no-ops.
    """
    from windex.ccnews import dedup as dd
    from windex.ccnews import runner
    from windex.ccnews import sync as ccsync
    from windex.github import tail

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        n = ccsync.sync(conn, settings.news_backfill_days)
        console.print(f"ccnews: {n} new WARCs")
        staged = runner.run_batches(conn, settings)
        dd.prune_bands(conn, settings.minhash_window_days)
        console.print(f"ccnews: {staged} docs staged")
        if embed and settings.embed_dim > 0:
            from windex.ccnews.embed_index import embed_pending

            console.print(f"ccnews: embedded {embed_pending(conn, settings)}")
        tail.sync_hours(conn, days=2)
        stats = tail.scan(conn, settings.gharchive_downloads_dir)
        console.print(f"gh tail: {stats}")

        # retention: datatrove per-batch logs accumulate one dir per batch forever
        import shutil
        import time as time_mod

        batch_logs = settings.news_staging_dir / "logs"
        if batch_logs.exists():
            cutoff = time_mod.time() - 14 * 86400
            for d in batch_logs.iterdir():
                if d.is_dir() and d.stat().st_mtime < cutoff:
                    shutil.rmtree(d, ignore_errors=True)
        if settings.github_token_list():
            from windex.github import hydrate as gh_hydrate_mod

            hstats = gh_hydrate_mod.hydrate(
                conn,
                tokens=settings.github_token_list(),
                readme_dir=settings.repos_staging_dir / "readme",
                star_threshold=settings.repo_star_threshold,
                limit=2000,
            )
            console.print(f"gh hydrate: {hstats}")
            if embed and settings.embed_dim > 0:
                from windex.github.embed_index import embed_pending as gh_embed_pending

                console.print(f"gh: embedded {gh_embed_pending(conn, settings)}")


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8100) -> None:
    """Run the REST API + dashboard. Logs rotate at ~/.windex/logs/serve.log;
    dashboard-polling access lines are filtered out."""
    from pathlib import Path

    import uvicorn

    log_dir = Path.home() / ".windex" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {"quiet": {"()": "windex.api.logs.QuietAccess"}},
        "formatters": {"std": {"format": "%(asctime)s %(levelname)s %(message)s"}},
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": str(log_dir / "serve.log"),
                "maxBytes": 10_485_760,
                "backupCount": 5,
                "formatter": "std",
            }
        },
        "loggers": {
            "uvicorn": {"handlers": ["file"], "level": "INFO"},
            "uvicorn.error": {"handlers": ["file"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["file"], "level": "INFO",
                               "filters": ["quiet"], "propagate": False},
        },
    }
    uvicorn.run("windex.api.app:app", host=host, port=port, log_config=log_config)


@app.command("serve-mcp")
def serve_mcp() -> None:
    """Run the MCP server (stdio transport)."""
    from windex.api.mcp import main

    main()


@gh_app.command("status")
def gh_status() -> None:
    """Hour-file watermark + repo pipeline counts."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM gharchive_files GROUP BY status ORDER BY status")
        console.print({r[0]: r[1] for r in cur.fetchall()}, "gharchive_files")
        cur.execute("SELECT status, count(*) FROM repos GROUP BY status ORDER BY status")
        console.print({r[0]: r[1] for r in cur.fetchall()}, "repos")
        cur.execute("SELECT count(*) FROM repos WHERE star_events >= 3")
        console.print(f"repos with ≥3 star events in window: {cur.fetchone()[0]}")


@app.command()
def init_db() -> None:
    """Apply the schema (idempotent) and create data directories."""
    settings = get_settings()
    for d in settings.all_dirs():
        d.mkdir(parents=True, exist_ok=True)
    with db.connect(settings.pg_dsn) as conn:
        db.init_db(conn)
    console.print("[green]schema applied, data dirs ready[/green]")
    for d in settings.all_dirs():
        console.print(f"  {d}")


@app.command()
def ensure_collections() -> None:
    """Create Qdrant collections + aliases for the configured embedding model."""
    from windex.index import qdrant

    settings = get_settings()
    client = qdrant.client_from_url(settings.qdrant_url)
    for source in qdrant.SOURCES:
        name = qdrant.ensure_collection(client, source, settings.embed_model, settings.embed_dim)
        console.print(f"[green]{qdrant.alias_name(source)}[/green] → {name}")


@app.command()
def health(embed: bool = typer.Option(False, help="Also ping the embedding server")) -> None:
    """Check Postgres, Qdrant, and optionally the embedding backend."""
    from windex.index import qdrant

    settings = get_settings()
    failed = False

    try:
        with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM documents")
            docs = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM repos")
            repos = cur.fetchone()[0]
        console.print(f"[green]postgres ok[/green] documents={docs} repos={repos}")
    except Exception as exc:
        console.print(f"[red]postgres FAILED[/red] {exc}")
        failed = True

    try:
        client = qdrant.client_from_url(settings.qdrant_url)
        info = qdrant.status(client)
        console.print(f"[green]qdrant ok[/green] {info}")
    except Exception as exc:
        console.print(f"[red]qdrant FAILED[/red] {exc}")
        failed = True

    if embed:
        from windex.embed import build_embedder

        embedder = build_embedder(settings)
        if embedder.ping():
            console.print(f"[green]embedder ok[/green] model={embedder.model_id} dim={embedder.dim}")
        else:
            console.print(f"[red]embedder FAILED[/red] {settings.embed_backend} @ {settings.embed_endpoint}")
            failed = True
    elif settings.embed_dim == 0:
        console.print("[yellow]embedder not configured yet (WINDEX_EMBED_* pending)[/yellow]")

    raise typer.Exit(1 if failed else 0)


if __name__ == "__main__":
    app()
