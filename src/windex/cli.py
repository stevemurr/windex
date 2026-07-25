import typer
from rich.console import Console

from windex import config, db
from windex.config import get_settings

app = typer.Typer(no_args_is_help=True, help="windex — self-hosted web index for search agents")
ccnews_app = typer.Typer(no_args_is_help=True, help="CC-News ingestion")
app.add_typer(ccnews_app, name="ccnews")


@ccnews_app.callback()
def _ccnews_scope() -> None:
    """Bind this process to the `ccnews` settings scope (config.use_scope),
    so runtime overrides for this source apply without a redeploy."""
    config.use_scope("ccnews")

console = Console()


@ccnews_app.command("sync")
def ccnews_sync(days: int = typer.Option(None, help="Window in days (default: config)")) -> None:
    """Record unseen in-window WARC paths as pending."""
    from windex.ccnews import sync as ccsync

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        n = ccsync.sync(conn, days if days is not None else settings.news_backfill_days)
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
    max_consecutive_failures: int = typer.Option(
        10,
        help="Consecutive-failure count at which the loop logs one louder "
             "'endpoint appears down' line and enters down-mode. It NEVER exits "
             "on failures — it keeps probing on the backoff.",
    ),
) -> None:
    """Long-running embed drainer: follows the processor, backs off on errors,
    and probes a dead endpoint forever rather than exiting. Exits cleanly only
    when the backlog is drained and no processor is running.

    It used to circuit-break (exit 2) on 10 consecutive failures to spare a
    saturated embedder, but that backfired: on 2026-07-17 ~22:17 a ~25-minute
    gateway (LiteLLM) outage made every loop exit, and nothing supervises the
    loops (the watchdog guards only the postgres/qdrant containers), so a short
    blip stalled indexing ~36 hours. Waiting is now nearly free — a down gateway
    refuses connections instantly, a saturated one is bounded by the flock budget
    plus the bulk key's 6-concurrent cap — so the loop keeps probing;
    --max-consecutive-failures only marks when it announces the endpoint looks
    down. See `windex embed-loop` for the full rationale.
    """
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
                f"[red]embed cycle failed ({failures} consecutive): {exc}[/red]"
            )
            if failures == max_consecutive_failures:
                # Cross into down-mode: announce once, keep probing. Exiting here
                # (the old circuit breaker) is what turned the 25-minute gateway
                # outage into a ~36h indexing stall on 2026-07-17.
                console.print(
                    f"[bold red]embedder endpoint appears down after {failures} "
                    f"consecutive failures — continuing to probe every 300s[/bold red]"
                )
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


@gh_app.callback()
def _gh_scope() -> None:
    """Bind this process to the `gh` settings scope (config.use_scope),
    so runtime overrides for this source apply without a redeploy."""
    config.use_scope("gh")



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
    fresh: bool = typer.Option(False, help="Clear the shard ledger for this range and re-sweep"),
) -> None:
    """Search-API sweep for repos ≥ star threshold (post-2025-10 star discovery)."""
    from datetime import date

    from windex.github import discover

    settings = get_settings()
    # Reconnecting so a transient postgres drop mid-sweep is retried on a fresh
    # connection rather than crashing the whole run (2026-07-17 incident).
    with db.Reconnecting(settings.pg_dsn) as rc:
        stats = discover.sweep(
            rc,
            tokens=settings.github_token_list(),
            star_threshold=settings.repo_star_threshold,
            created_from=date.fromisoformat(created_from),
            created_to=date.fromisoformat(created_to) if created_to else None,
            fresh=fresh,
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
    """Compose, embed, and index hydrated repos. Respects the dashboard pause flag."""
    from windex.github.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} repos[/green]")


wiki_app = typer.Typer(no_args_is_help=True, help="Wikipedia ingestion (CirrusSearch dumps)")
app.add_typer(wiki_app, name="wiki")


@wiki_app.callback()
def _wiki_scope() -> None:
    """Bind this process to the `wiki` settings scope (config.use_scope),
    so runtime overrides for this source apply without a redeploy."""
    config.use_scope("wiki")



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


@arxiv_app.callback()
def _arxiv_scope() -> None:
    """Bind this process to the `arxiv` settings scope (config.use_scope),
    so runtime overrides for this source apply without a redeploy."""
    config.use_scope("arxiv")



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
            # Ask arXiv how far back it serves: a window starting before its
            # earliestDatestamp is rejected wholesale (badArgument: "start date
            # too early"), losing that year's valid tail with it.
            planned = aharvest.plan_backfill(conn, from_year, to_year or date.today().year,
                                             earliest=aharvest.earliest_datestamp(settings))
            console.print(f"[green]{planned} new per-year windows planned[/green]")
        else:
            frm, until = aharvest.plan_incremental(
                conn, days if days is not None else settings.arxiv_incremental_days)
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


@smallweb_app.callback()
def _smallweb_scope() -> None:
    """Bind this process to the `smallweb` settings scope (config.use_scope),
    so runtime overrides for this source apply without a redeploy."""
    config.use_scope("smallweb")



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


@docs_app.callback()
def _docs_scope() -> None:
    """Bind this process to the `docs` settings scope (config.use_scope),
    so runtime overrides for this source apply without a redeploy."""
    config.use_scope("docs")



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


hn_app = typer.Typer(no_args_is_help=True, help="Hacker News ingestion (Algolia API + parquet mirror)")
app.add_typer(hn_app, name="hn")


@hn_app.callback()
def _hn_scope() -> None:
    """Bind this process to the `hn` settings scope (config.use_scope),
    so runtime overrides for this source apply without a redeploy."""
    config.use_scope("hn")



@hn_app.command("harvest")
def hn_harvest(
    days: int = typer.Option(None, help="Trailing window: re-pull the last N days (default: config)"),
    max_windows: int = typer.Option(None, help="Stop after N windows (default: all pending)"),
) -> None:
    """Harvest HN stories from the Algolia API → clean parquet + documents ledger.

    Arms a rolling trailing-days window (re-armed each run: the text_hash ledger
    skips unchanged stories while their points/num_comments are refreshed in the
    payload without re-embedding), then drains ALL pending windows — including
    any backfill months still open — splitting over-cap ranges automatically.
    """
    from windex.hn import harvest as hharvest

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        frm, until = hharvest.plan_incremental(
            conn, days if days is not None else settings.hn_incremental_days)
        console.print(
            f"[green]trailing window {hharvest.window_label(frm)}..{hharvest.window_label(until)} armed[/green]"
        )
        stats = hharvest.harvest(conn, settings, max_windows=max_windows)
    console.print(stats)


@hn_app.command("backfill")
def hn_backfill(
    from_year: int = typer.Option(2006, help="Earliest year to plan (HN starts 2006-10)"),
    from_month: int = typer.Option(None, help="Earliest month (default: Oct for 2006, else Jan)"),
    to_year: int = typer.Option(None, help="Latest year (default: current)"),
    to_month: int = typer.Option(None, help="Latest month (default: current / Dec)"),
    max_windows: int = typer.Option(None, help="Stop after N months (default: all pending)"),
    keep: bool = typer.Option(False, help="Keep downloaded monthly parquet files"),
) -> None:
    """Fast-path backfill: plan per-month windows, then drain them from the
    open-index/hacker-news parquet mirror (ODC-By 1.0) — zero Algolia load.

    Same watermarks and staging flow as `hn harvest`, so the two are
    interchangeable per window; months left pending (or failed) can be drained
    by the Algolia harvester instead. Idempotent either way.
    """
    from windex.hn import backfill as hbackfill
    from windex.hn import harvest as hharvest

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        planned = hharvest.plan_backfill(
            conn, from_year, from_month, to_year, to_month
        )
        console.print(f"[green]{planned} new per-month windows planned[/green]")
        stats = hbackfill.backfill(conn, settings, max_windows=max_windows, keep=keep)
    console.print(stats)


@hn_app.command("embed")
def hn_embed(limit: int = 100_000) -> None:
    """Embed staged HN stories into Qdrant. Respects the dashboard pause flag."""
    from windex.hn.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} stories[/green]")


@hn_app.command("status")
def hn_status() -> None:
    """Harvest-window watermark + document pipeline counts."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM hn_windows GROUP BY status ORDER BY status")
        console.print({r[0]: r[1] for r in cur.fetchall()}, "hn_windows")
        cur.execute(
            "SELECT status, count(*) FROM documents WHERE source='hn' GROUP BY status ORDER BY status"
        )
        console.print({r[0]: r[1] for r in cur.fetchall()}, "documents")


@hn_app.command("tombstone-empty")
def hn_tombstone_empty() -> None:
    """One-time cleanup: mark fully-empty hn docs (blank title AND body) 'deleted'
    and drop their vectors. Run while indexing is paused; idempotent."""
    from windex.hn import cleanup

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        n = cleanup.tombstone_empty_stories(conn, settings)
    console.print(f"[green]hn: tombstoned {n} empty docs[/green]")


@hn_app.command("backfill-duplicates")
def hn_backfill_duplicates() -> None:
    """One-time cleanup: mark exact-hash duplicate hn docs 'duplicate' of the
    earliest canonical and drop embedded dups' vectors. Run AFTER tombstone-empty,
    while indexing is paused; idempotent."""
    from windex.hn import cleanup

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        out = cleanup.backfill_exact_duplicates(conn, settings)
    console.print(f"[green]hn: marked {out['marked_duplicate']} duplicates, "
                  f"dropped {out['vectors_dropped']} vectors[/green]")


hf_app = typer.Typer(no_args_is_help=True,
                     help="Hugging Face ingestion (huggingface.co docs, courses, blog)")
app.add_typer(hf_app, name="hf")


@hf_app.callback()
def _hf_scope() -> None:
    """Bind this process to the `hf` settings scope (config.use_scope),
    so runtime overrides for this source apply without a redeploy."""
    config.use_scope("hf")



@hf_app.command("sync")
def hf_sync(
    refresh: bool = typer.Option(True, help="Re-fetch + hash every root's llms.txt (~52 requests)"),
) -> None:
    """Sitemap → doc roots + blog posts, then re-hash each root's llms.txt.

    The cheap half of the cycle: ~55 requests, ~3 minutes at HF's 1 req/3s. The
    llms.txt hash is what tells `hf crawl` which roots actually changed, so a
    quiet day costs this and nothing else. Only the doc and blog sitemap shards
    are read — the models/datasets/spaces/papers shards are recency windows, not
    catalogs, and using one as a frontier would silently index a random slice of
    the Hub (docs/huggingface-source.md).
    """
    from windex.hf import sync as hfsync

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        stats = hfsync.sync(conn, settings, refresh=refresh)
        pending = hfsync.pending_roots(conn, settings.hf_root_list())
        posts = hfsync.pending_posts(conn, 10_000)
    console.print(stats)
    console.print(f"[green]{len(pending)} roots + {len(posts)} blog posts pending crawl[/green]")


@hf_app.command("crawl")
def hf_crawl(
    max_roots: int = typer.Option(None, help="Stop after N doc roots (default: all pending)"),
    max_posts: int = typer.Option(None, help="Stop after N blog posts (default: all pending)"),
) -> None:
    """Pull .md pages for changed doc roots + new blog posts → clean parquet.

    ~3.3h cold (4,014 pages at HF's published 1 req/3s), minutes warm — an
    unchanged root costs ONE request thanks to the llms.txt hash gate.
    Idempotent and resumable: a killed run leaves its unfinished roots pending.
    """
    from windex.hf import crawl as hfcrawl

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        stats = hfcrawl.crawl(conn, settings, max_roots=max_roots, max_posts=max_posts)
    console.print(stats)


@hf_app.command("embed")
def hf_embed(limit: int = 100_000) -> None:
    """Embed staged Hugging Face pages into Qdrant. Respects the dashboard pause flag."""
    from windex.hf.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} pages[/green]")


@hf_app.command("status")
def hf_status() -> None:
    """Root/blog watermarks + document pipeline counts."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM hf_roots GROUP BY status ORDER BY status")
        console.print({r[0]: r[1] for r in cur.fetchall()}, "hf_roots")
        cur.execute(
            """SELECT count(*) FROM hf_roots WHERE llms_hash IS NOT NULL
               AND (ingested_hash IS NULL OR llms_hash IS DISTINCT FROM ingested_hash)"""
        )
        console.print(f"roots pending crawl: {cur.fetchone()[0]}")
        cur.execute(
            """SELECT count(*) FROM hf_posts
               WHERE ingested_lastmod IS NULL OR lastmod > ingested_lastmod"""
        )
        console.print(f"blog posts pending crawl: {cur.fetchone()[0]}")
        cur.execute(
            "SELECT status, count(*) FROM documents WHERE source='hf' GROUP BY status ORDER BY status"
        )
        console.print({r[0]: r[1] for r in cur.fetchall()}, "documents")


memory_app = typer.Typer(no_args_is_help=True,
                         help="Chat-memory source (push-based; no pull ingest)")
app.add_typer(memory_app, name="memory")


@memory_app.command("status")
def memory_status() -> None:
    """Conversation + chunk pipeline counts for the pushed chat-memory source."""
    from windex.memory_source import ingest as mingest

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        console.print(mingest.status(conn))


@memory_app.command("embed")
def memory_embed(limit: int = 100_000) -> None:
    """One-shot embed of staged chat-memory chunks into Qdrant (the loop is the
    unattended path). Respects the dashboard pause flag."""
    from windex.memory_source.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} chunks[/green]")


custom_app = typer.Typer(no_args_is_help=True,
                         help="Custom sources (push-based; no pull ingest)")
app.add_typer(custom_app, name="custom")


# Everything a clean ingest must forget. Grouped by what it costs to lose, because
# that is the distinction that matters when deciding what `reset` may touch.
#
#   corpus      — documents + their derived vectors and parquet. Re-fetchable, but
#                 only by re-crawling, which is the expensive part.
#   watermarks  — "what have we already fetched". Clearing these is what makes the
#                 next ingest start from nothing rather than resume.
#   runs        — execution history. Pure observability; losing it costs nothing.
#
# Settings (source_config / recipe_config), the schedule, and registered custom
# sources are NOT here: they are configuration, not data, and a reset that also
# wiped how the box is tuned would be a footgun rather than a clean slate. The
# flags below opt into those explicitly.
_RESET_CORPUS = ("documents", "repos", "minhash_bands")
_RESET_WATERMARKS = ("warc_files", "gharchive_files", "gh_shards", "wiki_dumps",
                     "arxiv_windows", "feeds", "docsets", "hn_windows",
                     "hf_roots", "hf_posts", "source_units")
_RESET_RUNS = ("crawl_urls", "crawl_runs", "task_units", "run_events",
               "run_tasks", "runs", "source_sched")


@app.command()
def reset(
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation"),
    keep_staging: bool = typer.Option(
        False, help="Keep the extracted-text parquet (vectors are still dropped)"),
    drop_sources: bool = typer.Option(
        False, help="Also unregister custom sources and their recipes"),
    drop_settings: bool = typer.Option(
        False, help="Also clear runtime settings overrides and the schedule"),
) -> None:
    """Wipe the index for a clean ingest: corpus, vectors, parquet, watermarks.

    This is the reproducibility path taken to its conclusion. `reindex` rebuilds
    vectors from staged text; this drops the staged text too, so the next run
    re-derives everything from upstream. It is what makes "the index is a pure
    function of the recipes" a claim you can actually test.

    Deliberately NOT reversible and deliberately narrow: settings, the schedule and
    registered sources survive unless you ask otherwise, because a clean slate
    should not also mean reconfiguring the box.
    """
    from windex.index import qdrant as qidx

    settings = get_settings()
    tables = list(_RESET_CORPUS + _RESET_WATERMARKS + _RESET_RUNS)
    if drop_sources:
        tables += ["custom_sources", "recipes", "recipe_revisions"]
    if drop_settings:
        tables += ["source_config", "recipe_config", "schedule", "triggers"]

    client = qidx.client_from_url(settings.qdrant_url)
    # Only collections THIS windex owns, and only for THIS embedding model.
    #
    # It used to delete every collection in the cluster. That is wrong even in
    # production — Qdrant may be shared — and it made the command impossible to
    # test safely: the test fixtures point at the real Qdrant, so running `reset`
    # under pytest deleted 13 production collections. Which it duly did.
    #
    # Scoping it is the structural fix. A destructive command that can only reach
    # its own named objects cannot be aimed at anything else by accident, whereas
    # "remember to isolate the fixture" is a rule someone eventually forgets.
    suffix = f"__{qidx.slug(settings.embed_model)}"
    owned = set(qidx.SOURCES)
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT name FROM custom_sources")
        owned |= {r[0] for r in cur.fetchall()}
    collections = sorted(
        c.name for c in client.get_collections().collections
        if c.name.endswith(suffix) and c.name[: -len(suffix)] in owned)
    skipped = sorted(
        c.name for c in client.get_collections().collections
        if c.name not in collections)

    # Say what will be destroyed BEFORE asking. A confirmation prompt that does not
    # show the blast radius is theatre.
    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents")
        docs = cur.fetchone()[0]
    console.print("[bold red]This will permanently delete:[/bold red]")
    console.print(f"  {docs:,} documents and {len(tables)} tables")
    console.print(f"  {len(collections)} Qdrant collections: "
                  f"{', '.join(collections[:4])}{' …' if len(collections) > 4 else ''}")
    if skipped:
        console.print(f"  [dim]leaving {len(skipped)} collection(s) alone — not this "
                      f"windex's, or not model {settings.embed_model!r}: "
                      f"{', '.join(skipped[:3])}{' …' if len(skipped) > 3 else ''}[/dim]")
    if not keep_staging:
        console.print(f"  all parquet under {settings.staging_dir}")
    console.print(f"  all downloads under {settings.downloads_dir}")
    if not yes:
        typer.confirm("Proceed?", abort=True)

    with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        # One statement so FKs inside the set resolve without CASCADE — naming every
        # table keeps a truncate from silently reaching one nobody meant to clear.
        present = []
        for t in tables:
            cur.execute("SELECT to_regclass(%s)", (t,))
            if cur.fetchone()[0] is not None:
                present.append(t)
        cur.execute(f"TRUNCATE {', '.join(present)}")
        # Ingest bookkeeping lives in `control`; leaving stale watermark timestamps
        # would make a fresh index claim it was already up to date.
        cur.execute("DELETE FROM control WHERE key LIKE 'ingest_ts_%%' "
                    "OR key LIKE 'loop_heartbeat_%%' OR key IN ('news_stage', 'gh_stage')")
        conn.commit()
    console.print(f"[green]truncated[/green] {len(present)} tables")

    for name in collections:
        client.delete_collection(name)      # aliases go with their collection
    console.print(f"[green]dropped[/green] {len(collections)} collections")

    import shutil
    targets = [settings.downloads_dir] + ([] if keep_staging else [settings.staging_dir])
    for d in targets:
        shutil.rmtree(d, ignore_errors=True)
    for d in settings.all_dirs():
        d.mkdir(parents=True, exist_ok=True)
    console.print(f"[green]cleared[/green] {', '.join(str(t) for t in targets)}")
    console.print("\n[dim]Clean slate. Collections are recreated on the next embed "
                  "pass; run `windex ensure-collections` to do it now.[/dim]")

@custom_app.command("status")
def custom_status(name: str = typer.Argument(..., help="custom source name")) -> None:
    """Doc pipeline counts for one custom source."""
    from windex.custom_source import ingest as cingest

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        console.print(cingest.status(conn, name))


@custom_app.command("list")
def custom_list() -> None:
    """List registered custom sources with doc counts."""
    from windex.custom_source import registry

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        for info in registry.list_all(conn):
            console.print(f"{info['name']}: {info['doc_count']} docs "
                          f"({info['pending']} pending)")


@custom_app.command("embed")
def custom_embed(limit: int = 100_000) -> None:
    """One-shot embed of staged docs for ALL registered custom sources (the loop
    is the unattended path). Respects the dashboard pause flag."""
    from windex.custom_source.embed_index import embed_pending

    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        if db.get_control(conn, "indexing", "running") == "paused":
            console.print("[yellow]paused — skipping embed[/yellow]")
            raise typer.Exit(0)
        n = embed_pending(conn, settings, limit=limit)
    console.print(f"[green]embedded {n} docs[/green]")


EMBED_SOURCES = {
    "ccnews": "windex.ccnews.embed_index",
    "wiki": "windex.wiki.embed_index",
    "hn": "windex.hn.embed_index",
    "arxiv": "windex.arxiv.embed_index",
    "docs": "windex.docs_source.embed_index",
    "smallweb": "windex.smallweb.embed_index",
    "gh": "windex.github.embed_index",
    "hf": "windex.hf.embed_index",
    "memory": "windex.memory_source.embed_index",
    # Push-based custom sources: one generic loop (`embed-loop custom`) drains
    # every registered source — embed_index.embed_pending iterates the registry.
    "custom": "windex.custom_source.embed_index",
}


@app.command("embed-loop")
def embed_loop(
    source: str = typer.Argument(..., help=f"one of: {', '.join(EMBED_SOURCES)}"),
    interval: int = 30,
    max_consecutive_failures: int = typer.Option(
        10,
        help="Consecutive-failure count at which the loop logs one louder "
             "'endpoint appears down' line and enters down-mode. It NEVER exits "
             "on failures — it keeps probing on the backoff.",
    ),
) -> None:
    """Supervised embed drainer for any source — the unattended entrypoint.

    `windex <src> embed` is a one-shot pass: it raises on the first embedding
    failure and the process dies, silently stopping that source until a human
    notices. On 2026-07-17 a saturated embedder killed 5 of 6 backfills that way
    within minutes; only ccnews survived, because it alone ran under a loop.

    So the loop backs off and retries *forever* — it never exits on consecutive
    failures. It used to circuit-break (exit 2) to avoid piling retries onto a
    saturated GPU, but that rationale is now obsolete. The model sits behind a
    gateway with per-tier keys: a DOWN gateway refuses connections instantly, so
    a retry costs nothing, and a SATURATED one is bounded by the fleet-wide flock
    budget (embed/budget.py) plus the bulk key's server-side 6-concurrent cap —
    HttpEmbedder already caps its own retries at 3. Waiting is nearly free;
    exiting is not. On 2026-07-17 ~22:17 the gateway (LiteLLM) went down for
    ~25 minutes and every loop burned its 10 failures and exited by design — but
    nothing supervises the loops (the watchdog guards only the postgres/qdrant
    containers), so a 25-minute blip stalled indexing for ~36 hours with 11.7M
    docs staged. A loop that had simply kept probing every ~5 minutes would have
    self-healed the moment the gateway returned.

    --max-consecutive-failures no longer trips a breaker: crossing it just logs
    (once) that the endpoint looks down, so the log says the stall is on purpose.
    """
    import importlib
    import time as time_mod

    if source not in EMBED_SOURCES:
        console.print(f"[red]unknown source '{source}'[/red] — pick one of: "
                      f"{', '.join(EMBED_SOURCES)}")
        raise typer.Exit(1)
    embed_pending = importlib.import_module(EMBED_SOURCES[source]).embed_pending

    # Bind the process to this source's settings scope, then RE-RESOLVE each
    # cycle rather than holding one Settings for the life of the loop: that is
    # what lets an embed knob edited in the console (throttle, concurrency,
    # order) take effect on the next pass instead of the next redeploy. The
    # override map is TTL-cached, so this costs a dict merge, not a query.
    config.use_scope(source)
    settings = get_settings()
    failures = 0
    while True:
        try:
            settings = get_settings()
            with db.connect(settings.pg_dsn) as conn:
                # Liveness heartbeat: the containerized loops run in separate
                # containers, so `windex status`/the console read this instead of
                # pgrep (see service.loop_states). Written every cycle — including
                # while paused — so a paused-but-alive loop still reads as "up".
                db.set_control(conn, f"loop_heartbeat_{source}", str(int(time_mod.time())))
                if db.get_control(conn, "indexing", "running") == "paused":
                    console.print("paused — waiting")
                    time_mod.sleep(interval)
                    continue
                n = embed_pending(conn, settings)
            failures = 0
            # Escape the source tag: rich reads a bare "[wiki]" as markup and
            # silently swallows it, so the log would omit which loop spoke.
            console.print(rf"\[{source}] embedded {n} docs")
            if n == 0:
                # Nothing staged: idle rather than exit. Upstream ingest may
                # still be running, and a drained queue is not a finished one.
                time_mod.sleep(interval)
        except Exception as exc:
            failures += 1
            console.print(
                rf"[red]\[{source}] embed cycle failed "
                rf"({failures} consecutive): {exc}[/red]"
            )
            if failures == max_consecutive_failures:
                # Cross into down-mode: say so once, loudly, then keep probing on
                # the same backoff. Exiting here (the old circuit breaker) is what
                # turned the 25-minute gateway outage into a ~36h indexing stall.
                console.print(
                    rf"[bold red]\[{source}] endpoint appears down after {failures} "
                    rf"consecutive failures — continuing to probe every 300s[/bold red]"
                )
            # Backoff caps at 300s, so a dead endpoint is re-probed ~every 5 min.
            time_mod.sleep(min(interval * failures, 300))


@app.command()
def scheduler(
    interval: int = typer.Option(60, help="Seconds between due-entry checks"),
) -> None:
    """Never-exiting timer loop for the editable job scheduler.

    About every `interval` seconds it reads the `schedule` table and fires the
    entries that are enabled and DUE (hour+minute match, weekday matches or is
    NULL, and not already run this minute). Ingest entries additionally skip when
    that source's ingest_enabled flag is off. Detached under `windex up`, it logs
    to ~/.windex/logs/scheduler.log (the supervised process redirects stdout).

    Robust like the embed loops: a Postgres blip is caught and the loop simply
    waits for the next tick — a transient DB drop must never kill the scheduler
    (the same failure mode that stalled indexing for ~36h on 2026-07-17).
    """
    import time as time_mod
    from datetime import datetime

    from windex.api import service

    settings = get_settings()
    console.print("scheduler loop started")
    while True:
        try:
            fired = service.run_due(settings)
            if fired:
                stamp = datetime.now().isoformat(timespec="seconds")
                console.print(f"{stamp} fired: {', '.join(fired)}")
        except Exception as exc:  # noqa: BLE001 — a blip must not kill the loop
            console.print(f"[red]scheduler tick failed: {exc}[/red]")
        time_mod.sleep(interval)


@app.command("crawl-loop")
def crawl_loop(
    interval: float = typer.Option(0.0, help="Seconds between polls (0 = crawl_loop_idle_seconds)"),
) -> None:
    """Never-exiting worker: claim queued crawls and execute them.

    Robust like the embed loops and the scheduler: a Postgres blip or a single
    bad crawl is caught and the loop waits for the next poll. It must NEVER exit
    on failure — nothing supervises these loops, and a short blip that killed the
    worker would leave crawls queued forever (the failure mode that stalled
    indexing ~36h on 2026-07-17).

    Reclaims runs whose worker died before draining the queue: the frontier is
    persisted, so a reclaimed run resumes from its remaining pending URLs rather
    than restarting at the seed.
    """
    import time as time_mod

    from windex.crawl import recipe as crecipe
    from windex.crawl import run as crun

    settings = get_settings()
    idle = interval or settings.crawl_loop_idle_seconds
    console.print("crawl loop started")
    while True:
        try:
            # Re-resolved per poll so a crawl-default edited in the console
            # (host interval, page budget, depth) applies to the NEXT claimed
            # run without restarting this worker.
            settings = get_settings()
            with db.connect(settings.pg_dsn) as conn:
                reclaimed = crun.reclaim_stale(conn, settings)
                if reclaimed:
                    console.print(f"[yellow]reclaimed {reclaimed} stale crawl run(s)[/yellow]")
                claimed = crun.claim_run(conn)
                if claimed is None:
                    time_mod.sleep(idle)
                    continue
                run_id, source = claimed["id"], claimed["source"]
                console.print(f"[bold]crawl run {run_id}[/bold] → {source}")
                try:
                    # The FROZEN recipe from the run row, not the source's current
                    # one: a run must execute what it was queued with.
                    recipe = crecipe.parse(claimed["recipe"], settings)
                    stats = crun.execute(
                        conn, settings, run_id, source, recipe,
                        should_stop=lambda: crun.is_cancelled(conn, run_id),
                        on_progress=lambda s: crun.heartbeat(conn, run_id, s),
                    )
                    status = "cancelled" if crun.is_cancelled(conn, run_id) else "done"
                    crun.finish(conn, run_id, status, stats)
                    console.print(f"  {status}: {stats}")
                except Exception as exc:  # noqa: BLE001 — one bad crawl, not the loop
                    conn.rollback()
                    crun.finish(conn, run_id, "failed", {}, str(exc))
                    console.print(f"[red]crawl run {run_id} failed[/red] ({exc})")
        except Exception as exc:  # noqa: BLE001 — a DB blip must not kill the loop
            console.print(f"[red]crawl loop tick failed: {exc}[/red]")
            time_mod.sleep(idle)


@app.command()
def crawl(
    source: str = typer.Option(..., help="Custom-source name to index into (created if absent)"),
    seed: str = typer.Option(..., help="Seed URL — the cluster to crawl"),
    max_depth: int = typer.Option(2, help="BFS depth from the seed"),
    max_pages: int = typer.Option(500, help="Hard page budget for the run"),
    host_interval: float = typer.Option(2.0, help="Min seconds between hits to the host"),
    path_prefix: str = typer.Option("", help="Scope prefix (default: the seed's directory)"),
    quality_filters: bool = typer.Option(False, help="Apply the smallweb quality gate"),
) -> None:
    """One-shot crawl, run inline. The API + `crawl-loop` are the normal path;
    this is for local testing and for crawling without the stack up."""
    from windex.crawl import recipe as crecipe
    from windex.crawl import run as crun
    from windex.custom_source import registry

    settings = get_settings()
    body = {
        "seeds": [seed],
        "scope": {"exclude": [r"\.(js|css|woff2?|png|svg|ico|gif|jpg|jpeg|pdf)$"]},
        "limits": {"max_depth": max_depth, "max_pages": max_pages,
                   "host_interval": host_interval},
        "extract": {"quality_filters": quality_filters},
    }
    if path_prefix:
        body["scope"]["path_prefix"] = path_prefix
    recipe = crecipe.parse(body, settings)
    with db.connect(settings.pg_dsn) as conn:
        if registry.get(conn, source) is None:
            registry.create(conn, source, source, f"Crawled from {seed}", recipe.to_dict())
            console.print(f"registered source [bold]{source}[/bold]")
        run_id = crun.create_run(conn, source, recipe)
        crun.claim_run(conn)
        console.print(f"crawl run {run_id}: {seed}")
        try:
            stats = crun.execute(conn, settings, run_id, source, recipe,
                                 on_progress=lambda s: crun.heartbeat(conn, run_id, s))
            crun.finish(conn, run_id, "done", stats)
        except Exception as exc:
            crun.finish(conn, run_id, "failed", {}, str(exc))
            raise
    console.print(f"[green]{stats}[/green]")
    console.print("embed with: windex embed-loop custom   (or wait for windex-loop-custom)")


@app.command()
def maintain(
    reindex: bool = typer.Option(False, help="Also REINDEX CONCURRENTLY bloat-flagged indexes (weekly, off-peak)"),
    density_threshold: float = typer.Option(70.0, help="REINDEX when avg leaf density falls below this %"),
) -> None:
    """Store maintenance (docs/store-tuning.md): VACUUM/ANALYZE the churn tables
    so rolling deletes and status-flip UPDATEs don't bloat unbounded; with
    --reindex, rebuild btree indexes whose measured leaf density dropped below
    the threshold — gated on measurement, never blind, one index at a time."""
    settings = get_settings()
    conn = db.connect(settings.pg_dsn)
    conn.autocommit = True  # VACUUM/REINDEX CONCURRENTLY refuse transaction blocks
    # Roll the run-detail partitions BEFORE anything slow, and before the early
    # return below. The window has no DEFAULT partition on purpose, and every task
    # claim writes a run_events row inside its claim transaction — so an exhausted
    # window is not a lost log line, it is a stalled worker pool. init-db creates
    # three months ahead, but a box that is deployed rarely relies on this nightly
    # pass instead. Retention is a DROP of whole months, never a rolling DELETE:
    # this table has the same shape as minhash_bands, whose rolling deletes never
    # reached autovacuum's threshold and needed hand-tuned settings.
    for action, part in conn.execute(
            "SELECT * FROM windex_roll_partitions(%s, %s)", (3, 3)).fetchall():
        console.print(f"[green]{action}[/green] partition {part}")

    churn_tables = ("minhash_bands", "documents", "feeds", "search_metrics")
    for table in churn_tables:
        conn.execute(f"VACUUM (ANALYZE) {table}")
        console.print(f"[green]vacuum analyze {table}[/green]")
    if not reindex:
        console.print("skipping reindex (pass --reindex for the weekly pass)")
        return
    conn.execute("CREATE EXTENSION IF NOT EXISTS pgstattuple")
    rows = conn.execute(
        """SELECT i.indexrelid::regclass::text
           FROM pg_index i JOIN pg_class c ON i.indrelid = c.oid
           JOIN pg_am am ON (SELECT relam FROM pg_class WHERE oid = i.indexrelid) = am.oid
           WHERE c.relname = ANY(%s) AND am.amname = 'btree'
             AND pg_relation_size(i.indexrelid) > 50 * 1024 * 1024""",
        (list(churn_tables),),
    ).fetchall()
    for (idx,) in rows:
        try:
            density = conn.execute(
                "SELECT avg_leaf_density FROM pgstatindex(%s)", (idx,)
            ).fetchone()[0]
        except Exception as exc:
            console.print(f"[yellow]{idx}: pgstatindex failed ({exc}); skipped[/yellow]")
            continue
        if density is not None and density < density_threshold:
            console.print(f"[yellow]{idx}: leaf density {density:.0f}% < {density_threshold:.0f}% — reindexing[/yellow]")
            conn.execute(f"REINDEX INDEX CONCURRENTLY {idx}")
            console.print(f"[green]{idx}: rebuilt[/green]")
        else:
            console.print(f"{idx}: leaf density {density:.0f}% — healthy")


@app.command("eval")
def eval_cmd(
    mode: str = typer.Option("hybrid", help="hybrid | dense | lexical"),
    k: int = typer.Option(0, help="cutoff for NDCG@k / Recall@k (0 = config eval_k)"),
    per_source: int = typer.Option(0, help="known-item samples per source (0 = config)"),
    source: list[str] = typer.Option(
        [], "--source",
        help="Restrict evaluation to these sources (repeatable); default all",
    ),
    sample_seed: str = typer.Option(
        "", help="Stable sample seed for reproducible model comparisons",
    ),
    judge: bool = typer.Option(False, help="also run the LLM-as-judge leg (needs WINDEX_JUDGE_*)"),
    persist: bool = typer.Option(True, help="write the run to search_quality"),
) -> None:
    """Measure SEARCH QUALITY (relevance): NDCG@k / MRR / Recall@k over a
    known-item (title-as-query) proxy + a curated golden set (+ optional LLM
    judge). Persists a row the Grafana search-quality panel trends. Scheduled via
    `windex scheduler` so quality is measured on a cadence, not ad hoc."""
    import subprocess

    from windex.eval import run_eval
    from windex.eval.harness import SOURCES, persist_run

    settings = get_settings()
    k = k or settings.eval_k
    per_source = per_source or settings.eval_per_source
    unknown = set(source) - set(SOURCES)
    if unknown:
        console.print(
            f"[red]unknown source(s): {sorted(unknown)}[/red] — "
            f"pick from {', '.join(SOURCES)}"
        )
        raise typer.Exit(1)
    sources = source or None
    seed = sample_seed or None
    console.print(
        f"[cyan]eval[/cyan] mode={mode} k={k} per_source={per_source} "
        f"sources={sources or 'all'} seed={seed or 'random'} judge={judge}"
    )
    result = run_eval(
        settings,
        per_source=per_source,
        k=k,
        mode=mode,
        llm_judge=judge,
        sources=sources,
        sample_seed=seed,
    )
    ov = result["overall"]
    console.print(f"  known-item  NDCG@{k}={ov[f'known_item_ndcg@{k}']:.4f}  MRR={ov['known_item_mrr']:.4f}")
    if result["golden"]:
        console.print(f"  golden      NDCG@{k}={ov.get(f'golden_ndcg@{k}')}  MRR={ov.get('golden_mrr')}  (n={result['golden']['n']})")
    if result["judge"]:
        console.print(f"  llm-judge   graded NDCG@{k}={result['judge'].get(f'graded_ndcg@{k}')}  (n={result['judge']['n']})")
    for src, v in result["known_item"].items():
        console.print(f"    {src:9s} n={v['n']:<3} ndcg@{k}={v[f'ndcg@{k}']:.3f} "
                      f"mrr={v['mrr']:.3f} hit@{k}={v[f'hit@{k}']:.3f}")
    if persist and sources:
        console.print(
            "[yellow]subset eval not persisted to the global search_quality "
            "headline; the full result remains in this command's output[/yellow]"
        )
    elif persist:
        try:
            sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                                 capture_output=True, text=True).stdout.strip()
        except Exception:  # noqa: BLE001
            sha = ""
        persist_run(settings, result, sha)
        console.print("[green]persisted to search_quality[/green]")


@app.command()
def reindex(
    source: str = typer.Argument("all", help="news | repos | wiki | arxiv | smallweb | docs | hn | hf | memory | all"),
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
        def _recreate(base: str) -> None:
            if not drop_collections:
                return
            name = qidx.collection_name(base, settings.embed_model)
            if client.collection_exists(name):
                client.delete_collection(name)
            qidx.ensure_collection(client, base, settings.embed_model, settings.embed_dim)

        # Commit after EACH source: the drop/recreate is irreversible, so a later
        # source's Qdrant failure must not roll back an already-reset source's
        # status flip — that would leave it 'embedded' but pointing at an emptied
        # collection (unsearchable, and the embed loop only re-embeds 'deduped').
        if source in ("repos", "all"):
            _recreate("repos")
            cur.execute("UPDATE repos SET status='hydrated' WHERE status='embedded'")
            console.print(f"[green]repos: {cur.rowcount} queued for re-embed[/green]")
            conn.commit()

        # For these the reindex arg == collection base == documents.source. memory
        # is included (a reindex rebuilds every collection from its staged parquet,
        # the source of truth) even though search-side `all` excludes it.
        for src in ("news", "wiki", "arxiv", "smallweb", "docs", "hn", "hf", "memory"):
            if source not in (src, "all"):
                continue
            _recreate(src)
            cur.execute(
                """UPDATE documents SET status='deduped', embedded_model=NULL, indexed_at=NULL
                   WHERE source=%s AND status='embedded'""",
                (src,),
            )
            console.print(f"[green]{src}: {cur.rowcount} docs queued for re-embed[/green]")
            conn.commit()
    console.print(
        "run `windex ccnews embed-loop`, `windex gh embed`, `windex wiki embed`, "
        "`windex arxiv embed`, `windex smallweb embed`, `windex docs embed`, "
        "`windex hn embed`, `windex hf embed` to repopulate"
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

        # retention: search_metrics grows one row per query forever otherwise
        from windex.api import service as api_service

        pruned = api_service.prune_search_metrics(conn, days=30)
        console.print(f"search metrics: pruned {pruned} rows older than 30d")
        if settings.github_token_list():
            from windex.github import hydrate as gh_hydrate_mod

            hstats = gh_hydrate_mod.hydrate(
                conn,
                tokens=settings.github_token_list(),
                readme_dir=settings.repos_staging_dir / "readme",
                star_threshold=settings.repo_star_threshold,
                # 0, matching the gh-hydrate job / refresh chain: the default (1)
                # silently skips every Search-API-sweep candidate (star_events=0),
                # which is the only discovery source for repos created after
                # 2025-10-07.
                min_star_events=0,
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
        thr = settings.repo_star_threshold
        cur.execute("SELECT count(*) FROM repos WHERE star_events >= %s", (thr,))
        console.print(f"repos with ≥{thr} star events in window: {cur.fetchone()[0]}")


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
    # Registered custom sources each get their own collection (generic payload
    # indexes). Skip cleanly if Postgres is down — the static set above is what
    # `up` truly needs; custom collections are also created lazily by the embed
    # pass's ensure_collection.
    try:
        from windex.custom_source import registry

        with db.connect(settings.pg_dsn) as conn:
            custom = [i["name"] for i in registry.list_all(conn)]
    except Exception as exc:  # noqa: BLE001 — Postgres down: static collections still ensured
        console.print(f"[yellow]custom collections skipped ({exc})[/yellow]")
        custom = []
    for source in custom:
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


# ---------------------------------------------------------------------------
# System lifecycle. `windex up` is the single, idempotent, health-gated
# entrypoint the watchdog and the launchd agent call; `status --json` is the
# agent/watchdog-readable signal. The supervised set (serve + the 8 embed loops)
# is derived from the jobs.py registry — never a second hardcoded list.
# ---------------------------------------------------------------------------

def _pg_ready(settings) -> bool:
    """Cheap 'is postgres reachable' probe — a real client connect like the
    watchdog's, not a heavy query."""
    try:
        with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False


def _qdrant_ready(settings) -> bool:
    """Cheap 'is qdrant reachable' probe: a bare GET on the root, mirroring the
    watchdog. Deliberately NOT qdrant.status() — that enumerates collections and
    can take seconds during backfill."""
    import httpx

    try:
        return httpx.get(settings.qdrant_url.rstrip("/") + "/", timeout=3).status_code == 200
    except Exception:
        return False


def _collect_status(settings) -> dict:
    """Shared status core: containers, serve, and each embed loop, plus a
    top-level `up` bool and a `down` list of supervised members that are absent
    (what the watchdog reads to decide whether to call `windex up`). A loop that
    is DISABLED (desired-state off) and stopped is not `down` — so the watchdog
    leaves it off."""
    from windex.api import jobs, service

    pg = _pg_ready(settings)
    qd = _qdrant_ready(settings)
    serve_up = jobs.serve_running()
    sched_up = jobs.scheduler_running()
    loop_jobs = jobs.embed_loop_jobs()
    # Desired-state flags need the DB; if it's down, treat all as enabled so a
    # stopped loop still reports "down" rather than being silently hidden.
    enabled = service.get_loops_enabled(settings) if pg else {j.argv[1]: True for j in loop_jobs}
    # serve + the scheduler are always-on supervised members: absent ⇒ `down`,
    # which the watchdog reads to reconcile via `windex up`.
    down = []
    if not serve_up:
        down.append("serve")
    if not sched_up:
        down.append("scheduler")
    loops = []
    for job in loop_jobs:
        src = job.argv[1]
        en = enabled.get(src, True)
        pids = jobs._pids(job.pattern)
        running = bool(pids)
        loops.append({"source": src, "name": job.name, "enabled": en, "running": running,
                      "state": "up" if running else ("down" if en else "disabled"),
                      "pids": pids})
        if en and not running:
            down.append(src)  # enabled but not running → the watchdog restarts it
    # "up" = serve + scheduler + every ENABLED loop running (disabled don't count).
    loops_up = all(entry["running"] for entry in loops if entry["enabled"])
    return {
        "up": bool(pg and qd and serve_up and sched_up and loops_up),
        "containers": {"postgres": {"reachable": pg}, "qdrant": {"reachable": qd}},
        "serve": {"running": serve_up, "port": 8100},
        "scheduler": {"running": sched_up},
        "loops": loops,
        "down": down,
    }


def _print_status(settings) -> None:
    from rich.table import Table

    def mark(alive: bool, up_word: str = "ok") -> str:
        return f"[green]{up_word}[/green]" if alive else "[red]down[/red]"

    st = _collect_status(settings)
    table = Table(title="windex status")
    for col in ("component", "state", "detail"):
        table.add_column(col)
    table.add_row("postgres", mark(st["containers"]["postgres"]["reachable"]), "")
    table.add_row("qdrant", mark(st["containers"]["qdrant"]["reachable"]), "")
    table.add_row("serve", mark(st["serve"]["running"], "up"), f":{st['serve']['port']}")
    table.add_row("scheduler", mark(st["scheduler"]["running"], "up"), "")
    state_style = {"up": "[green]up[/green]", "down": "[red]down[/red]",
                   "disabled": "[dim]disabled[/dim]"}
    for entry in st["loops"]:
        detail = f"pid {entry['pids'][0]}" if entry["pids"] else ""
        table.add_row(f"loop {entry['source']}", state_style[entry["state"]], detail)
    console.print(table)
    console.print(f"overall: {'[green]UP[/green]' if st['up'] else '[yellow]DEGRADED[/yellow]'}")


@app.command()
def up(
    host: str = typer.Option(
        None, help="Interface serve binds (default: WINDEX_SERVE_HOST, else 127.0.0.1)"),
    port: int = 8100,
    no_serve: bool = typer.Option(False, "--no-serve", help="Don't start the API server"),
    no_scheduler: bool = typer.Option(False, "--no-scheduler", help="Don't start the job scheduler"),
    no_loops: bool = typer.Option(False, "--no-loops", help="Don't start the embed loops"),
    source: list[str] = typer.Option(
        [], "--source", help="Restrict the loops to these sources (repeatable); default all"),
    foreground: bool = typer.Option(
        False, "--foreground",
        help="After containers + loops, run serve in the foreground (blocks)"),
    timeout: int = typer.Option(60, help="Seconds to wait for postgres + qdrant to be reachable"),
) -> None:
    """Bring the whole stack up in order — containers → serve → the 8 embed loops.
    Idempotent: anything already running is left alone. The unattended entrypoint
    the watchdog and the launchd agent invoke."""
    import subprocess
    import time as time_mod

    from windex.api import jobs, service

    settings = get_settings()
    host = host or settings.serve_host  # env-driven so the watchdog's `up` keeps the LAN bind
    if source:
        unknown = set(source) - set(EMBED_SOURCES)
        if unknown:
            console.print(f"[red]unknown source(s): {sorted(unknown)}[/red] — "
                          f"pick from {', '.join(EMBED_SOURCES)}")
            raise typer.Exit(1)

    # 1. Preflight the external mount: dev.sh does `mkdir -p` on the services
    # dir, which on an unmounted drive silently creates it on the internal disk
    # and lets postgres init against the wrong path — a corruption footgun.
    if not settings.data_root.exists():
        console.print(f"[red]{settings.data_root} is not mounted[/red] — refusing to start")
        raise typer.Exit(1)

    # 2. Containers via the existing script (run_or_start is idempotent and
    # recreates a wedged container).
    dev_sh = jobs.PROJECT_ROOT / "scripts" / "dev.sh"
    console.print("bringing up containers (scripts/dev.sh up)…")
    subprocess.run(["bash", str(dev_sh), "up"], check=False)

    # 3. Health-gate on cheap probes only — never the cold qdrant.status() /
    # /metrics paths — polling until both answer or the timeout elapses.
    deadline = time_mod.monotonic() + timeout
    while True:
        pg, qd = _pg_ready(settings), _qdrant_ready(settings)
        if pg and qd:
            break
        if time_mod.monotonic() >= deadline:
            console.print(f"[red]timed out after {timeout}s[/red] — "
                          f"postgres={'ok' if pg else 'DOWN'} qdrant={'ok' if qd else 'DOWN'}")
            raise typer.Exit(1)
        time_mod.sleep(2)
    console.print("[green]postgres + qdrant reachable[/green]")

    # 4. Schema + collections (both idempotent create-if-missing).
    init_db()
    if settings.embed_dim > 0:
        ensure_collections()
    else:
        console.print("[yellow]embedder not configured (WINDEX_EMBED_* pending) — "
                      "skipping ensure-collections[/yellow]")

    # 5. Serve (unless suppressed, or deferred to --foreground below).
    if not no_serve and not foreground:
        if jobs.serve_running(port):
            console.print(f"serve already running on :{port}")
        else:
            info = jobs.start_serve(host, port)
            console.print(f"[green]started serve[/green] pid {info['pid']} on {host}:{port}")

    # 5b. Scheduler (unless suppressed): the always-on timer loop that fires the
    # due schedule entries. Supervised like serve — the watchdog restarts it.
    if not no_scheduler:
        if jobs.scheduler_running():
            console.print("scheduler already running")
        else:
            info = jobs.start_scheduler()
            console.print(f"[green]started scheduler[/green] pid {info['pid']}")

    # 6. Loops (unless suppressed): start each ENABLED source that's down. A
    # disabled source (desired-state off) is skipped, so `up` — including the
    # watchdog's — never resurrects a loop the operator turned off.
    if not no_loops:
        wanted = set(source) if source else None
        enabled = service.get_loops_enabled(settings)
        for job in jobs.embed_loop_jobs():
            src = job.argv[1]
            if wanted and src not in wanted:
                continue
            if not enabled.get(src, True):
                console.print(f"loop {src} [dim]disabled[/dim] — skipping")
                continue
            if jobs._pids(job.pattern):
                console.print(f"loop {src} already running")
            else:
                info = jobs.start(job.name, {})
                console.print(f"[green]started loop {src}[/green] pid {info['pid']}")

    _print_status(settings)

    # 7. Foreground serve blocks here; the loops are already detached above.
    # Honor --no-serve here too: step 5 skips serve when --no-serve is set, but
    # this branch only checked `foreground`, so `up --no-serve --foreground`
    # started serve anyway.
    if foreground and not no_serve and not jobs.serve_running(port):
        serve(host=host, port=port)


@app.command()
def down(
    source: list[str] = typer.Option(
        [], "--source", help="Restrict to these loop sources (repeatable); serve is left alone then"),
    keep_containers: bool = typer.Option(
        True, "--keep-containers/--stop-containers",
        help="Leave postgres + qdrant running (default), or stop them too"),
) -> None:
    """Stop the embed loops and serve (reverse of `up`). Containers are kept by
    default. Idempotent — stopping something already down is a no-op."""
    import subprocess

    from windex.api import jobs

    settings = get_settings()
    if source:
        unknown = set(source) - set(EMBED_SOURCES)
        if unknown:
            console.print(f"[red]unknown source(s): {sorted(unknown)}[/red]")
            raise typer.Exit(1)

    wanted = set(source) if source else None
    for job in jobs.embed_loop_jobs():
        src = job.argv[1]
        if wanted and src not in wanted:
            continue
        res = jobs.stop(job.name)
        console.print(f"stopped loop {src}: {res['pids']}" if res["pids"]
                      else f"loop {src} not running")

    # Only touch serve + scheduler on a full down (no --source subset): both are
    # managed processes `up` starts, so a full down stops them symmetrically.
    if not source:
        res = jobs.stop_serve()
        console.print(f"stopped serve: {res['pids']}" if res["pids"] else "serve not running")
        res = jobs.stop_scheduler()
        console.print(f"stopped scheduler: {res['pids']}" if res["pids"] else "scheduler not running")

    if not keep_containers:
        dev_sh = jobs.PROJECT_ROOT / "scripts" / "dev.sh"
        subprocess.run(["bash", str(dev_sh), "down"], check=False)
        console.print("[yellow]stopped containers — a running watchdog will restart "
                      "them within ~45s[/yellow]")

    _print_status(settings)


@app.command()
def status(
    json_out: bool = typer.Option(
        False, "--json", help="Machine-readable status (for agents and the watchdog)"),
) -> None:
    """Report what's up: containers, serve, and the 8 embed loops. `--json` emits
    the agent/watchdog form with a top-level `up` bool and a `down` list."""
    settings = get_settings()
    if json_out:
        import json as json_mod

        print(json_mod.dumps(_collect_status(settings)))
    else:
        _print_status(settings)


@app.command()
def loop(
    source: str = typer.Argument(..., help=f"one of: {', '.join(EMBED_SOURCES)}"),
    state: str = typer.Argument(..., help="on | off"),
) -> None:
    """Turn an embed loop on or off (desired-state). `off` stops it and keeps it
    off — `up` and the watchdog both honor the flag, so it won't come back until
    you turn it on."""
    from windex.api import service

    if source not in EMBED_SOURCES:
        console.print(f"[red]unknown source '{source}'[/red] — pick from {', '.join(EMBED_SOURCES)}")
        raise typer.Exit(1)
    if state not in ("on", "off"):
        console.print("[red]state must be 'on' or 'off'[/red]")
        raise typer.Exit(1)
    res = service.set_loop_enabled(get_settings(), source, state == "on")
    console.print(f"[green]loop {source} → {'on' if res['enabled'] else 'off'}[/green]")


# Per-source freshness sweep: fetch/discover new content and stage it; the
# always-on embed loops index whatever lands. Keys are the EMBED_SOURCES CLI
# names. Within a source, steps are &&-chained (ingest needs its own sync).
# gh hydrate carries --min-star-events 0: the default (1) silently skips every
# Search-API-sweep candidate (they have star_events=0).
REFRESH_CHAINS = {
    "ccnews": "ccnews sync && ccnews run --no-embed",
    "gh": "gh discover && gh hydrate --min-star-events 0",
    "wiki": "wiki sync && wiki ingest",
    "arxiv": "arxiv harvest --days 7",
    "smallweb": "smallweb sync && smallweb poll",
    "docs": "docs sync && docs ingest",
    "hn": "hn harvest --days 2",
    "hf": "hf sync && hf crawl",
}


def _refresh_script(sources: list[str], wx: str, root: str) -> str:
    """Build the bash sweep: sources run sequentially (gentle on the single box),
    each source's steps &&-chained, sources separated by ; so one source's
    failure doesn't abort the rest. `true WINDEX_REFRESH` tags the process so a
    second `refresh` can detect the sweep is already running via pgrep."""
    def expand(chain: str) -> str:
        return " && ".join(f'"{wx}" {step}' for step in chain.split(" && "))
    # On a source's success, record its ingest timestamp (freshness "last update").
    blocks = [f'echo === refresh {s} === && {expand(REFRESH_CHAINS[s])} && "{wx}" _mark-ingest {s}'
              for s in sources]
    body = " ; ".join(f"{{ {b} ; }}" for b in blocks)
    return f'true WINDEX_REFRESH; cd "{root}" && {body}'


@app.command()
def refresh(
    source: list[str] = typer.Option(
        [], "--source", help="Only these sources (repeatable); default all"),
    foreground: bool = typer.Option(
        False, "--foreground", help="Run the sweep inline (blocks) instead of detaching"),
) -> None:
    """Freshness sweep: check each source for new content, fetch + stage it, and
    let the always-on embed loops index it. Sources run sequentially in one
    detached process; each source's fetch steps are chained, and a per-source
    failure doesn't abort the rest. Idempotent — every job only advances past its
    own watermark, so a re-run with nothing new is a quick no-op."""
    import subprocess

    from windex.api import jobs

    if source:
        unknown = set(source) - set(REFRESH_CHAINS)
        if unknown:
            console.print(f"[red]unknown source(s): {sorted(unknown)}[/red] — "
                          f"pick from {', '.join(REFRESH_CHAINS)}")
            raise typer.Exit(1)
    if jobs._pids("WINDEX_REFRESH"):
        console.print("[yellow]a refresh sweep is already running — skipping[/yellow]")
        raise typer.Exit(0)

    sources = source or list(REFRESH_CHAINS)
    if not source:
        # A bare sweep honors the ingest desired-state; an explicit --source is a
        # manual "check now" that runs regardless of the flag.
        from windex.api import service
        enabled = service.get_ingest_enabled(get_settings())
        sources = [s for s in sources if enabled.get(s, True)]
        if not sources:
            console.print("[yellow]ingest is disabled for every source — nothing to do[/yellow]")
            raise typer.Exit(0)
    script = _refresh_script(sources, str(jobs.VENV_BIN / "windex"), str(jobs.PROJECT_ROOT))
    if foreground:
        raise typer.Exit(subprocess.run(["bash", "-lc", script]).returncode)
    pid = jobs._spawn("refresh", ["bash", "-lc", script])
    console.print(f"[green]refresh sweep started[/green] pid {pid} — sources: {', '.join(sources)}")
    console.print("staged content is indexed by the running embed loops; "
                  "follow ~/.windex/logs/refresh.log")


@app.command("_mark-ingest", hidden=True)
def _mark_ingest(source: str) -> None:
    """Internal: record a successful ingest for a source (ingest_ts_<source>
    control flag) so the freshness 'last update' column is accurate. Appended to
    each refresh chain by _refresh_script."""
    import time as time_mod

    with db.connect(get_settings().pg_dsn) as conn:
        db.set_control(conn, f"ingest_ts_{source}", str(int(time_mod.time())))


@app.command()
def worker(
    slots: int = typer.Option(0, help="Slot subprocesses (0 = WINDEX_WORKER_SLOTS or 4)"),
    lanes: str = typer.Option("", help="Comma-separated lanes to serve (default: all)"),
    slice_seconds: float = typer.Option(0.0, help="Seconds a task holds a slot before yielding"),
    name: str = typer.Option("", help="Pool name recorded in lease_worker ids"),
    inline: bool = typer.Option(False, "--inline",
                                help="Run ONE slot in this process (debugging; no supervisor)"),
) -> None:
    """The worker pool: claim leased tasks from `run_tasks` and run them in slices.

    Replaces the fourteen per-source loop containers with one supervisor and K
    slot subprocesses. A claimed task runs a SLICE, not to completion — it
    commits, yields, and re-enters the queue — so a 20,000-page crawl no longer
    FIFO-blocks every other source for eleven hours, a pause takes effect within
    one slice, and a slot can be recycled to reclaim memory without losing work.

    Never exits on failure: a Postgres blip, a bad task or an OOM-killed slot is
    logged and swept, because nothing supervises this process and an exit would
    leave the whole queue stranded (the failure that stalled indexing ~36 h on
    2026-07-17).
    """
    import logging as _logging

    from windex.worker import config_from_env, default_resolve
    from windex.worker.slot import slot_main
    from windex.worker.supervisor import Pool

    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = get_settings()
    cfg = config_from_env().with_overrides(
        slots=slots or None,
        slice_seconds=slice_seconds or None,
        name=name or None,
        lanes=tuple(x.strip() for x in lanes.split(",") if x.strip()) or None,
    )
    if inline:
        # One slot, no fork: the debugging shape. Preconditions are evaluated
        # once and published so the claim predicate behaves identically to the
        # supervised path — an inline run that silently ignored preconditions
        # would be a debugging tool that lies about production.
        from windex.worker import control as _control
        from windex.worker.preconditions import evaluate

        _control.write(cfg.control_path, satisfied=evaluate(settings),
                       blocked_lanes=(), generation=0)
        raise typer.Exit(slot_main(settings.pg_dsn, default_resolve, cfg, 0))
    console.print(f"[bold]worker pool[/bold] '{cfg.name}' — {cfg.slots} slots, "
                  f"lanes {', '.join(cfg.lanes)}, slice {cfg.slice_seconds:.0f}s")
    Pool(settings.pg_dsn, default_resolve, cfg, settings=settings).run()


@app.command("scheduler2")
def scheduler2(
    interval: float = typer.Option(10.0, help="Seconds between ticks"),
    once: bool = typer.Option(False, "--once", help="Run one tick and exit (cron/debug)"),
    migrate: bool = typer.Option(
        False, "--migrate",
        help="Copy `schedule` rows into `triggers` (UTC-preserving) before ticking"),
    compiler: str = typer.Option(
        "windex.recipe:compile_tasks", "--compiler",
        help="module:attr that compiles a recipe spec into its node list"),
    grace: float = typer.Option(
        90.0, help="Seconds late a fire may be before it counts as a MISSED window"),
) -> None:
    """The trigger scheduler (Phase 9) — successor to `windex scheduler`.

    Named `scheduler2` on purpose: the old command stays until every `schedule`
    row has a `triggers` row and the console reads the new table. Running both is
    safe but redundant — the old one spawns processes, this one writes `runs`
    rows — so the cutover is "start this, stop that", with `--migrate` in between.

    What it does every tick: arm any trigger with no planned instant, then fire
    everything due, each in ONE transaction (run + tasks + watermark). Exactly one
    instance is authoritative, via `pg_try_advisory_lock`; a second instance
    stands by and takes over within one interval if the holder dies. Neither the
    tick nor the loop exits on error — the 2026-07-17 stall (a 25-minute gateway
    blip that a self-healing loop would have ridden out) is the standing argument
    against components that exit when something goes wrong.

    `--compiler` is the seam to the recipe engine. Nothing in `windex.scheduler`
    imports it; the node list arrives as a callable, which is what lets the
    scheduler be built and tested before the compiler exists.
    """
    import importlib
    import logging
    from datetime import datetime

    from windex.scheduler import loop as sched_loop
    from windex.scheduler import migrate_schedule

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()

    mod_name, _, attr = compiler.partition(":")
    try:
        compile_tasks = getattr(importlib.import_module(mod_name), attr)
    except (ImportError, AttributeError) as exc:
        # Fail here, loudly, rather than at 03:00 as a `trigger.failed` row per
        # trigger per night. A scheduler that cannot fan out tasks is not a
        # degraded scheduler, it is a scheduler that queues unstartable runs.
        console.print(f"[red]cannot load the task compiler {compiler!r}: {exc}[/red]")
        console.print("the recipe engine may not be merged yet — pass --compiler "
                      "module:attr to point at one")
        raise typer.Exit(1) from exc

    if migrate:
        with db.connect(settings.pg_dsn) as conn:
            rows = migrate_schedule(conn)
        made = sum(1 for r in rows if r["created"])
        console.print(f"[green]migrated {made}/{len(rows)} schedule rows[/green] "
                      f"→ triggers (timezone=UTC, behaviour preserved)")
        for r in rows:
            console.print(f"  {r['name']:<16} {r['cron']:<14} UTC  "
                          f"{'new' if r['created'] else 'exists'}")

    def report(result) -> None:
        console.print(f"{datetime.now().isoformat(timespec='seconds')} {result.summary()}")

    console.print(f"scheduler2 tick every {interval:g}s "
                  f"(compiler {compiler}, misfire grace {grace:g}s)")
    sched_loop.run_loop(settings.pg_dsn, compile_tasks=compile_tasks,
                        interval=interval, grace_seconds=grace, once=once,
                        on_result=report)


if __name__ == "__main__":
    app()
