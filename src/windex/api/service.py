"""Transport-agnostic search service: both the REST app and the MCP server call
these functions and return the same result objects (the /v1 contract)."""

import hashlib
import threading
import time
from datetime import datetime

import psycopg
import pyarrow.compute as pc
import pyarrow.parquet as pq

from windex import db
from windex.config import Settings
from windex.index.embed_breaker import breaker
from windex.index.search import search as index_search
from windex.metrics import SEARCH_DURATION, SEARCH_REQUESTS

RESULT_FIELDS = ("url", "title", "snippet", "source", "published_at", "outlet",
                 "stars", "language", "topics", "pushed_at", "lang", "incoming_links",
                 "primary_category", "categories", "authors",
                 "framework", "version", "attribution",
                 "points", "num_comments", "author", "target_url",
                 "root", "kind")  # hf: doc root (transformers) and docs|learn|blog


def run_search(
    settings: Settings,
    q: str,
    source: str = "all",
    limit: int = 10,
    mode: str = "hybrid",
    published_after: datetime | None = None,
    published_before: datetime | None = None,
    min_stars: int | None = None,
    language: str | None = None,
    category: str | None = None,
    outlet: str | None = None,
    framework: str | None = None,
    min_points: int | None = None,
    root: str | None = None,
    kind: str | None = None,
) -> dict:
    t0 = time.monotonic()
    try:
        resp = index_search(
            settings, q, source=source, limit=limit, mode=mode,
            published_after=published_after, published_before=published_before,
            min_stars=min_stars, language=language, category=category, outlet=outlet,
            framework=framework, min_points=min_points, root=root, kind=kind,
        )
    except Exception:
        # result=error covers e.g. an explicit mode=dense raised through the open
        # breaker, or Qdrant being unreachable. Still record duration so the
        # histogram isn't silently missing the slow-failure tail.
        SEARCH_REQUESTS.labels(mode=mode, result="error").inc()
        SEARCH_DURATION.observe(time.monotonic() - t0)
        raise
    results = []
    for r in resp["results"]:
        item = {"id": r.get("doc_id"), "score": round(r["score"], 4)}
        item.update({k: r[k] for k in RESULT_FIELDS if r.get(k) is not None})
        results.append(item)
    total_ms = int((time.monotonic() - t0) * 1000)
    response = {
        "query": q,
        "results": results,
        "mode": "lexical (embedder busy — degraded from hybrid)" if resp["degraded"] else mode,
        "timings": {**resp["timings"], "total_ms": total_ms},
        "took_ms": total_ms,
    }
    # Both front doors (REST /v1/search and the MCP search_index tool) call
    # run_search, so recording here — Prometheus counters + the search_metrics
    # row — covers every search path.
    SEARCH_REQUESTS.labels(mode=mode, result="degraded" if resp["degraded"] else "ok").inc()
    SEARCH_DURATION.observe(total_ms / 1000.0)
    _record_search_metric(settings, q, source, mode, resp["degraded"],
                          response["timings"], len(results))
    return response


def _record_search_metric(settings: Settings, q: str, source: str, mode: str,
                          degraded: bool, timings: dict, n_results: int) -> None:
    """Fire-and-forget metric row. Runs on a daemon thread: even a guarded
    inline INSERT can stall for seconds (pool checkout wait + per-checkout
    health check) exactly when postgres is struggling — the moments we most
    want measured without adding user-visible search latency. A lost row
    (process exit mid-write, pg down) is acceptable; a slow search is not."""

    def _write():
        try:
            with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO search_metrics
                           (source, mode_requested, degraded, q_hash,
                            embed_ms, search_ms, total_ms, results)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (source, mode, degraded,
                     hashlib.sha1(q.encode()).hexdigest()[:12],
                     timings.get("embed_query_ms"), timings.get("search_ms"),
                     timings.get("total_ms"), n_results),
                )
        except Exception:
            pass  # metrics must never break search

    try:
        threading.Thread(target=_write, name="search-metric", daemon=True).start()
    except Exception:
        pass


def get_search_metrics(settings: Settings, minutes: int = 60) -> dict:
    """Latency percentiles + degradation counts over a trailing window
    (GET /v1/metrics). An empty window yields zeros, never errors."""
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE degraded),
                   percentile_cont(0.5)  WITHIN GROUP (ORDER BY total_ms),
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY total_ms),
                   percentile_cont(0.99) WITHIN GROUP (ORDER BY total_ms),
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY embed_ms),
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY search_ms)
            FROM search_metrics
            WHERE ts > now() - make_interval(mins => %s)
            """,
            (minutes,),
        )
        searches, degraded, p50, p95, p99, embed_p95, search_p95 = cur.fetchone()
        cur.execute(
            """SELECT source, count(*) FROM search_metrics
               WHERE ts > now() - make_interval(mins => %s) GROUP BY source""",
            (minutes,),
        )
        by_source = dict(cur.fetchall())

    def ms(v):  # percentile_cont returns float, NULL on an empty window
        return round(v) if v is not None else 0

    return {
        "window_minutes": minutes,
        "searches": searches,
        "degraded": degraded,
        "degraded_pct": round(100.0 * degraded / searches, 1) if searches else 0.0,
        "p50_ms": ms(p50),
        "p95_ms": ms(p95),
        "p99_ms": ms(p99),
        "embed_p95_ms": ms(embed_p95),
        "search_p95_ms": ms(search_p95),
        "by_source": by_source,
    }


def prune_search_metrics(conn: psycopg.Connection, days: int = 30) -> int:
    """Retention cap for search_metrics (called by `windex daily`)."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM search_metrics WHERE ts < now() - make_interval(days => %s)",
            (days,),
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


def get_document(settings: Settings, doc_id: str) -> dict | None:
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source, url, title, published_at, lang, status, duplicate_of, text_ref
            FROM documents WHERE id = %s
            """,
            (doc_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    doc = dict(zip(("id", "source", "url", "title", "published_at", "lang",
                    "status", "duplicate_of", "text_ref"), row))
    if doc["published_at"]:
        doc["published_at"] = doc["published_at"].isoformat()
    text_ref = doc.pop("text_ref")
    doc["text"] = None
    if text_ref:
        table = pq.read_table(settings.staging_dir / text_ref, filters=[("id", "==", doc_id)])
        if table.num_rows:
            # arXiv stages `abstract` (metadata only) and HN stages `story_text`
            # (title-only stories stage "") rather than a `text` column
            col = next(
                (c for c in ("text", "abstract", "story_text") if c in table.column_names),
                None,
            )
            if col is not None:
                doc["text"] = table.column(col)[0].as_py()
    return doc


# 1h search p95 + degraded count for the dashboard tile, cached 60s: the SSE
# loop calls get_stats every ~2s per viewer and percentile scans shouldn't
# rerun per tick (the tile only needs minute-fresh numbers).
_metrics_cache: dict = {}
_METRICS_TTL = 60.0


def _search_metrics_summary(settings: Settings) -> dict:
    now = time.monotonic()
    hit = _metrics_cache.get(settings.pg_dsn)
    if hit and now - hit[0] < _METRICS_TTL:
        return hit[1]
    m = get_search_metrics(settings, minutes=60)
    result = {"search_p95_ms": m["p95_ms"], "degraded_recent": m["degraded"],
              "searches_1h": m["searches"]}
    _metrics_cache[settings.pg_dsn] = (now, result)
    return result


# PG aggregates are cached briefly: the dashboard polls every 4s, and some of
# these queries turn into full scans at backfill scale. Two tiers (see
# docs/store-tuning.md): full-heap aggregates refresh at 60s; cheap live
# signals at 10s.
_pg_stats_cache: dict = {}
_PG_STATS_TTL = 10.0
_pg_heavy_cache: dict = {}
_PG_HEAVY_TTL = 600.0

# Index-queue units. Each source's watermark table counts a different unit of
# work — a WARC file is not an arXiv window is not a feed — so these counts are
# NEVER summed or compared across sources; the unit rides along in the payload
# so /v1/stats is self-describing and the dashboard can say so out loud.
QUEUE_UNITS = {
    "news": "WARC files",
    "github": "archive hours",
    "wiki": "dump shards",
    "arxiv": "date windows",
    "smallweb": "feeds",
    "docs": "docsets",
    "hn": "time windows",
    "hf": "doc roots",  # blog posts are a second unit and deliberately not summed in
}


def _pg_heavy(settings: Settings, ttl: float = _PG_HEAVY_TTL) -> dict:
    now = time.monotonic()
    hit = _pg_heavy_cache.get(settings.pg_dsn)
    if hit and now - hit[0] < ttl:
        return hit[1]
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source, status, count(*) FROM documents GROUP BY source, status"
        )
        docs: dict = {}
        for source, status, n in cur.fetchall():
            docs.setdefault(source, {})[status] = n
        # count(*) over GROUP BY, not count(DISTINCT ...): the latter sorts every
        # news row against work_mem and spills ~10MB/s of temp files to the
        # external drive. HashAggregate does the same job without the sort.
        cur.execute(
            """SELECT count(*) FROM (
                   SELECT split_part(split_part(canonical_url, '://', 2), '/', 1)
                   FROM documents WHERE source = 'news' GROUP BY 1) t"""
        )
        outlets = cur.fetchone()[0]
        cur.execute(
            """SELECT min(published_at)::date, max(published_at)::date
               FROM documents WHERE source = 'news' AND status = 'embedded'"""
        )
        cov_min, cov_max = cur.fetchone()
    result = {"docs": docs, "outlets": outlets, "cov": (cov_min, cov_max)}
    _pg_heavy_cache[settings.pg_dsn] = (now, result)
    return result


def _pg_stats(settings: Settings, ttl: float = _PG_STATS_TTL) -> dict:
    now = time.monotonic()
    hit = _pg_stats_cache.get(settings.pg_dsn)
    if hit and now - hit[0] < ttl:
        return hit[1]
    heavy = _pg_heavy(settings)
    docs = heavy["docs"]
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM warc_files GROUP BY status")
        warcs = dict(cur.fetchall())
        cur.execute("SELECT max(path) FROM warc_files WHERE status = 'done'")
        latest_warc = cur.fetchone()[0]
        cur.execute("SELECT status, count(*) FROM gharchive_files GROUP BY status")
        hours = dict(cur.fetchall())
        cur.execute("SELECT max(name) FROM gharchive_files WHERE status = 'done'")
        latest_hour = cur.fetchone()[0]
        cur.execute("SELECT status, count(*) FROM repos GROUP BY status")
        repos = dict(cur.fetchall())
        # "actively indexing" signal: anything landed in the last 2 minutes
        cur.execute(
            "SELECT count(*) FROM documents WHERE indexed_at > now() - interval '2 minutes'"
        )
        indexed_recently = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM documents WHERE indexed_at > now() - interval '10 minutes'"
        )
        indexed_10m = cur.fetchone()[0]
        cur.execute(
            """SELECT
                 (SELECT coalesce(sum(bytes), 0)::bigint FROM warc_files)
               + (SELECT coalesce(sum(bytes), 0)::bigint FROM gharchive_files)"""
        )
        bytes_total = cur.fetchone()[0]
        cur.execute(
            """SELECT
                 (SELECT coalesce(sum(bytes), 0)::bigint FROM warc_files
                  WHERE processed_at > now() - interval '30 minutes')
               + (SELECT coalesce(sum(bytes), 0)::bigint FROM gharchive_files
                  WHERE processed_at > now() - interval '30 minutes')"""
        )
        bytes_30m = cur.fetchone()[0]
        # Index queue: the five watermark tables not already counted above
        # (warc_files/gharchive_files are reused from `warcs`/`hours` rather
        # than re-scanned). All tiny and status-indexed — index-only scans,
        # ~5ms for the whole UNION — so they belong in this 10s tier. feeds is
        # a poll ROTATION, not a pending/done watermark: it is never "done", so
        # its queue is the first-sweep backlog (an active feed never fetched).
        cur.execute(
            """
            SELECT 'wiki'::text, status::text, count(*) FROM wiki_dumps GROUP BY status
            UNION ALL
            SELECT 'arxiv', status::text, count(*) FROM arxiv_windows GROUP BY status
            UNION ALL
            SELECT 'docs', status::text, count(*) FROM docsets GROUP BY status
            UNION ALL
            SELECT 'hn', status::text, count(*) FROM hn_windows GROUP BY status
            UNION ALL
            -- hf's unit is the doc root. 'no_llms'/'partial' roll up to their
            -- nearest queue meaning: a root with no llms.txt can never be
            -- crawled (it is not pending work), a partial one still is.
            SELECT 'hf',
                   (CASE status WHEN 'no_llms' THEN 'done'
                                WHEN 'partial' THEN 'pending'
                                ELSE status END)::text,
                   count(*)
            FROM hf_roots GROUP BY 2
            UNION ALL
            SELECT 'smallweb',
                   (CASE WHEN last_polled IS NULL THEN 'pending' ELSE 'done' END)::text,
                   count(*)
            FROM feeds WHERE status = 'active' GROUP BY 2
            """
        )
        marks: dict = {"news": warcs, "github": hours}
        for src, status, n in cur.fetchall():
            marks.setdefault(src, {})[status] = n
    outlets = heavy["outlets"]
    cov_min, cov_max = heavy["cov"]
    news = docs.get("news", {})
    gh = docs.get("github", {})
    wiki = docs.get("wiki", {})
    arxiv = docs.get("arxiv", {})
    smallweb = docs.get("smallweb", {})
    progdocs = docs.get("docs", {})
    hn = docs.get("hn", {})
    hf = docs.get("hf", {})
    # Embed queue = documents awaiting a vector, per source. This is a pure
    # re-projection of the source/status group-by already cached in _pg_heavy
    # (600s TTL) — it adds no query. Zero-backlog sources are kept here (the
    # contract stays complete); the dashboard is what drops them from the chart.
    embed_backlog = {src: st.get("deduped", 0) for src, st in docs.items()}
    result = {
        "documents": docs,
        "repos": repos,
        "warc_files": warcs,
        "gharchive_files": hours,
        "queues": {
            "embed": {
                "by_source": embed_backlog,
                "total": sum(embed_backlog.values()),
            },
            # Per-source and NEVER totalled: every row counts a different unit.
            # Keyed off QUEUE_UNITS, not off what the query returned: an empty
            # watermark table yields no GROUP BY rows, and a source must report
            # zeros rather than disappear from the contract.
            "index": {
                src: {
                    "unit": unit,
                    "pending": marks.get(src, {}).get("pending", 0),
                    "processing": marks.get(src, {}).get("processing", 0),
                    "failed": marks.get(src, {}).get("failed", 0),
                    "done": marks.get(src, {}).get("done", 0),
                }
                for src, unit in QUEUE_UNITS.items()
            },
        },
        "totals": {
            "indexed_pages": news.get("embedded", 0) + gh.get("embedded", 0)
            + wiki.get("embedded", 0) + arxiv.get("embedded", 0)
            + smallweb.get("embedded", 0) + progdocs.get("embedded", 0)
            + hn.get("embedded", 0) + hf.get("embedded", 0),
            "news_articles": news.get("embedded", 0),
            "github_projects": gh.get("embedded", 0),
            "wiki_articles": wiki.get("embedded", 0),
            "arxiv_papers": arxiv.get("embedded", 0),
            "smallweb_posts": smallweb.get("embedded", 0),
            "docs_pages": progdocs.get("embedded", 0),
            "hn_stories": hn.get("embedded", 0),
            "hf_pages": hf.get("embedded", 0),
            "duplicates_collapsed": news.get("duplicate", 0),
            "news_outlets": outlets,
            "news_coverage": [
                cov_min.isoformat() if cov_min else None,
                cov_max.isoformat() if cov_max else None,
            ],
        },
        "freshness": {
            "news_indexed_through": latest_warc,
            "news_warcs_pending": warcs.get("pending", 0),
            "gharchive_scanned_through": latest_hour,
        },
        "activity": {
            "warcs_processing": warcs.get("processing", 0),
            "indexed_last_2m": indexed_recently,
            "docs_per_min": round(indexed_10m / 10, 1),
            # batch completions attribute bytes chunkily; 30-min window smooths
            "download_mb_per_s": round(bytes_30m / 1800 / 1e6, 2),
            "bytes_downloaded_total": bytes_total,
        },
    }
    _pg_stats_cache[settings.pg_dsn] = (now, result)
    return result


def get_recent(settings: Settings, limit: int = 30) -> list[dict]:
    """Most recently indexed documents — the dashboard's live ticker."""
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source, url, title, indexed_at
            FROM documents WHERE indexed_at IS NOT NULL
            ORDER BY indexed_at DESC LIMIT %s
            """,
            (limit,),
        )
        return [
            {"id": i, "source": s, "url": u, "title": t,
             "indexed_at": ts.isoformat()}
            for i, s, u, t, ts in cur.fetchall()
        ]


def get_worker_activity(settings: Settings) -> dict:
    """Live view into the current extraction batch: datatrove's per-worker task
    logs + completion markers. Powers the Console's batch-workers panel."""
    import re

    with db.pooled(settings.pg_dsn) as conn:
        stage = db.get_control(conn, "news_stage", "idle")
    match = re.search(r"batch (\S+)", stage)
    if not match:
        return {"active": False, "stage": stage}
    bid = match.group(1)
    logdir = settings.news_staging_dir / "logs" / bid
    total = 0
    input_files = logdir / "input_files.txt"
    if input_files.exists():
        total = len(input_files.read_text().splitlines())
    completions = logdir / "completions"
    done = len(list(completions.glob("*"))) if completions.exists() else 0
    workers = []
    for f in sorted((logdir / "logs").glob("task_*.log")) if (logdir / "logs").exists() else []:
        try:
            with open(f, "rb") as fh:
                size = fh.seek(0, 2)
                fh.seek(max(size - 2000, 0))
                lines = [
                    ln.strip() for ln in fh.read().decode(errors="replace").splitlines()
                    if ln.strip()
                ]
            if lines:
                # strip datatrove's timestamp/level prefix for display
                line = re.sub(r"^[\d\-\s:.,|]+\w+\s+\|\s*", "", lines[-1])
                workers.append({"task": f.stem.replace("task_", "worker "),
                                "line": line[-160:]})
        except OSError:
            continue
    return {"active": True, "stage": stage, "batch": bid,
            "tasks_done": done, "tasks_total": total, "workers": workers[:32]}


_timeseries_cache: dict = {}
_TIMESERIES_TTL = 30.0


def get_timeseries(settings: Settings, minutes: int = 60) -> list[dict]:
    """Per-minute indexing and download volumes for the trailing window,
    zero-filled — feeds the dashboard charts. Cached: the created_at half is a
    full scan (the backlog index is partial), and the SSE loop calls this once
    per connected viewer."""
    key = (settings.pg_dsn, minutes)
    now = time.monotonic()
    hit = _timeseries_cache.get(key)
    if hit and now - hit[0] < _TIMESERIES_TTL:
        return hit[1]
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT g.m, coalesce(i.docs, 0)::bigint, coalesce(e.docs, 0)::bigint,
                   coalesce(b.bytes, 0)::bigint
            FROM generate_series(
                date_trunc('minute', now()) - make_interval(mins => %s - 1),
                date_trunc('minute', now()), interval '1 minute') AS g(m)
            LEFT JOIN (
                SELECT date_trunc('minute', created_at) m, count(*) docs
                FROM documents WHERE created_at > now() - make_interval(mins => %s)
                GROUP BY 1) i USING (m)
            LEFT JOIN (
                SELECT date_trunc('minute', indexed_at) m, count(*) docs
                FROM documents WHERE indexed_at > now() - make_interval(mins => %s)
                GROUP BY 1) e USING (m)
            LEFT JOIN (
                SELECT date_trunc('minute', processed_at) m, sum(bytes) bytes
                FROM (
                    SELECT processed_at, bytes FROM warc_files WHERE bytes IS NOT NULL
                    UNION ALL
                    SELECT processed_at, bytes FROM gharchive_files WHERE bytes IS NOT NULL
                ) x WHERE processed_at > now() - make_interval(mins => %s)
                GROUP BY 1) b USING (m)
            ORDER BY g.m
            """,
            (minutes, minutes, minutes, minutes),
        )
        rows = [
            {"t": m.isoformat(), "ingested": ingested, "docs": embeds,
             "mb": round(nbytes / 1e6, 1)}
            for m, ingested, embeds, nbytes in cur.fetchall()
        ]
    _timeseries_cache[key] = (now, rows)
    return rows


def get_control(settings: Settings) -> str:
    with db.pooled(settings.pg_dsn) as conn:
        return db.get_control(conn, "indexing", "running")


def set_embed_profile(settings: Settings, profile: str) -> str:
    with db.pooled(settings.pg_dsn) as conn:
        db.set_control(conn, "embed_profile", profile)
    return profile


def set_control(settings: Settings, value: str) -> str:
    with db.pooled(settings.pg_dsn) as conn:
        db.set_control(conn, "indexing", value)
    return value


# --- per-source embed-loop desired-state (the on/off that must STICK) ---
# A `loop_<source>` control flag is the single source of truth honored by BOTH
# `windex up` (won't start a disabled source) and the watchdog (a disabled+stopped
# loop is not in status --json's `down` list, so it's never auto-restarted). That
# is what stops the supervisor from fighting a manual "off".

def get_loops_enabled(settings: Settings) -> dict[str, bool]:
    """{source: enabled} from the loop_<source> flags (default enabled). DB
    unreachable ⇒ assume enabled — never leave a loop off because a flag read
    failed."""
    from windex.api import jobs

    sources = [j.argv[1] for j in jobs.embed_loop_jobs()]
    try:
        with db.pooled(settings.pg_dsn) as conn:
            return {s: db.get_control(conn, f"loop_{s}", "enabled") != "disabled"
                    for s in sources}
    except Exception:  # noqa: BLE001 — a flag-read failure must not disable loops
        return {s: True for s in sources}


def loop_states(settings: Settings) -> list[dict]:
    """Per-source {source, enabled, running, state, pids} for status + the
    console. state = up (running) | down (enabled but not running — the gap the
    watchdog closes) | disabled (intentionally off)."""
    from windex.api import jobs

    enabled = get_loops_enabled(settings)
    out = []
    for job in jobs.embed_loop_jobs():
        src = job.argv[1]
        pids = jobs._pids(job.pattern)
        en = enabled.get(src, True)
        out.append({
            "source": src, "enabled": en, "running": bool(pids),
            "state": "up" if pids else ("down" if en else "disabled"), "pids": pids,
        })
    return out


def set_loop_enabled(settings: Settings, source: str, enabled: bool) -> dict:
    """Set a source's desired-state and reconcile the process now: enable starts
    the loop (if down); disable stops it and keeps it off. Raises KeyError for an
    unknown source."""
    from windex.api import jobs

    job = next((j for j in jobs.embed_loop_jobs() if j.argv[1] == source), None)
    if job is None:
        raise KeyError(source)
    with db.pooled(settings.pg_dsn) as conn:
        db.set_control(conn, f"loop_{source}", "enabled" if enabled else "disabled")
    if enabled and not jobs._pids(job.pattern):
        jobs.start(job.name, {})
    elif not enabled:
        jobs.stop(job.name)
    return {"source": source, "enabled": enabled, "state": ("up" if enabled else "disabled")}


def supervisor_status(settings: Settings) -> dict:
    """Is the watchdog (supervisor) process alive, and the per-loop states it
    acts on — for the console's supervision panel."""
    from windex.api import jobs

    return {"watchdog_running": bool(jobs._pids("scripts/watchdog.sh")),
            "loops": loop_states(settings)}


def set_all_loops_enabled(settings: Settings, enabled: bool) -> list[dict]:
    """Bulk on/off for every source (the console's 'start all' / 'stop all')."""
    from windex.api import jobs

    return [set_loop_enabled(settings, j.argv[1], enabled) for j in jobs.embed_loop_jobs()]


def _spawn_windex(args: list[str], log_name: str) -> int:
    """Detach a `windex <args>` subprocess (the API can't block on a long
    lifecycle command). Reuses the jobs spawn machinery + log rotation."""
    from windex.api import jobs

    return jobs._spawn(log_name, [str(jobs.VENV_BIN / "windex"), *args])


def system_up(settings: Settings) -> dict:
    """Detached `windex up` — reconcile to desired state (start enabled loops and
    serve that are down). Returns immediately; progress in system-up.log."""
    return {"action": "up", "pid": _spawn_windex(["up"], "system-up")}


def restart_loops(settings: Settings) -> dict:
    """Bounce the loops: stop every one, then detached `windex up` restarts the
    ENABLED ones (disabled stay off)."""
    from windex.api import jobs

    for job in jobs.embed_loop_jobs():
        jobs.stop(job.name)
    return {"action": "restart", "pid": _spawn_windex(["up"], "system-up")}


def run_refresh(settings: Settings, sources: list[str] | None = None) -> dict:
    """Detached freshness sweep (`windex refresh [--source …]`); its own guard
    skips if a sweep is already running."""
    args = ["refresh"]
    for s in sources or []:
        args += ["--source", s]
    return {"action": "refresh", "pid": _spawn_windex(args, "system-refresh")}


def get_stats(settings: Settings, ttl: float = _PG_STATS_TTL) -> dict:
    stats = dict(_pg_stats(settings, ttl=ttl))

    vectors = {}
    try:
        from qdrant_client import QdrantClient

        client = QdrantClient(url=settings.qdrant_url, timeout=5)
        aliases = {a.alias_name: a.collection_name for a in client.get_aliases().aliases}
        for alias, coll in aliases.items():
            vectors[alias] = client.get_collection(coll).points_count
    except Exception:
        vectors = {"error": "qdrant unreachable"}

    stats["vectors"] = vectors
    # live, uncached extras: control flags, pipeline stages, in-flight bytes
    in_flight = 0
    for d in (settings.ccnews_downloads_dir, settings.gharchive_downloads_dir):
        if d.exists():
            in_flight += sum(f.stat().st_size for f in d.iterdir() if f.is_file())
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT key, value FROM control")
        flags = dict(cur.fetchall())
    stats["activity"] = {
        **stats["activity"],
        "control": flags.get("indexing", "running"),
        "embed_profile": flags.get("embed_profile", "env"),
        "stages": {
            "news": flags.get("news_stage", "idle"),
            "github": flags.get("gh_stage", "idle"),
            "wiki": flags.get("wiki_stage", "idle"),
            "arxiv": flags.get("arxiv_stage", "idle"),
            "smallweb": flags.get("smallweb_stage", "idle"),
            "docs": flags.get("docs_stage", "idle"),
            "hn": flags.get("hn_stage", "idle"),
            "hf": flags.get("hf_stage", "idle"),
        },
        "downloading_bytes_on_disk": in_flight,
        # why searches are degrading right now: open = we're skipping the dense
        # leg on purpose. Live (the metrics tile below is a 1h trailing window).
        "embed_breaker": breaker.snapshot(settings),
        # search-performance tile: 1h p95 + degraded fallbacks (60s-cached)
        **_search_metrics_summary(settings),
    }
    # Self-describing outbound links for the console header (e.g. the Grafana
    # that scrapes /metrics). Empty string ⇒ the header hides that link.
    # NB: loop/supervisor state is deliberately NOT here — the control panel
    # polls the lightweight GET /v1/loops instead, so it stays responsive even
    # when this (qdrant + heavy-pg) call is slow/cold.
    stats["links"] = {"grafana": settings.grafana_url}
    return stats
