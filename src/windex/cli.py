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


@app.command()
def reindex(
    source: str = typer.Argument("all", help="news | repos | all"),
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
        conn.commit()
    console.print("run `windex ccnews embed-loop` and `windex gh embed` to repopulate")


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
    """Run the REST API (/v1/search, /v1/docs/{id}, /v1/stats)."""
    import uvicorn

    uvicorn.run("windex.api.app:app", host=host, port=port)


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
