"""Transport-agnostic search service: both the REST app and the MCP server call
these functions and return the same result objects (the /v1 contract)."""

import hashlib
import threading
import time
from datetime import datetime

import psycopg
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
                 "root", "kind",  # hf: doc root (transformers) and docs|learn|blog
                 "conversation_id", "chunk_index",  # memory: source chat + chunk position
                 "extra")  # custom sources: opaque per-doc blob the pusher attached

# Registered-custom-source names, cached briefly so /v1/search's per-request
# source validation doesn't hit Postgres every query (populated by validate_source).
_source_cache: dict = {}
_SOURCE_TTL = 15.0


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
    conversation_id: str | None = None,
) -> dict:
    t0 = time.monotonic()
    try:
        resp = index_search(
            settings, q, source=source, limit=limit, mode=mode,
            published_after=published_after, published_before=published_before,
            min_stars=min_stars, language=language, category=category, outlet=outlet,
            framework=framework, min_points=min_points, root=root, kind=kind,
            conversation_id=conversation_id,
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


# --- chat-memory source (push-based) -----------------------------------------
# Thin wrappers over memory_source.ingest, opening a pooled connection like every
# other write path. The endpoints are the app's push interface (full-replace per
# conversation), so they delegate straight through — validation lives at the
# route (422) and staging IO failures surface as OSError (mapped to 503 there).

def memory_replace(settings: Settings, conversation_id: str, title: str,
                   chunks: list[dict]) -> dict:
    from windex.memory_source import ingest as mingest

    with db.pooled(settings.pg_dsn) as conn:
        return mingest.replace_conversation(conn, settings, conversation_id, title, chunks)


def memory_delete(settings: Settings, conversation_id: str) -> dict:
    from windex.memory_source import ingest as mingest

    with db.pooled(settings.pg_dsn) as conn:
        return mingest.delete_conversation(conn, settings, conversation_id)


def memory_status(settings: Settings) -> dict:
    from windex.memory_source import ingest as mingest

    with db.pooled(settings.pg_dsn) as conn:
        return mingest.status(conn)


# --- custom sources (push-based; generalized memory) -------------------------
# Thin wrappers over custom_source.registry / .ingest, opening a pooled
# connection like every other write path. Validation and error→status mapping
# live at the route; these delegate straight through.

def custom_create(settings: Settings, name: str, title: str = "",
                  description: str = "", recipe: dict | None = None) -> dict:
    from windex.custom_source import registry

    with db.pooled(settings.pg_dsn) as conn:
        return registry.create(conn, name, title, description, recipe)


def custom_get(settings: Settings, name: str) -> dict | None:
    from windex.custom_source import registry

    with db.pooled(settings.pg_dsn) as conn:
        return registry.get(conn, name)


def custom_list(settings: Settings) -> list[dict]:
    from windex.custom_source import registry

    with db.pooled(settings.pg_dsn) as conn:
        return registry.list_all(conn)


def custom_update(settings: Settings, name: str, **fields) -> dict | None:
    from windex.custom_source import registry

    with db.pooled(settings.pg_dsn) as conn:
        return registry.update(conn, name, **fields)


def custom_push(settings: Settings, name: str, docs: list[dict]) -> dict:
    from windex.custom_source import ingest as cingest

    with db.pooled(settings.pg_dsn) as conn:
        return cingest.upsert_docs(conn, settings, name, docs)


def custom_delete_docs(settings: Settings, name: str, ids: list[str]) -> dict:
    from windex.custom_source import ingest as cingest

    with db.pooled(settings.pg_dsn) as conn:
        return cingest.delete_docs(conn, settings, name, ids)


def custom_delete_source(settings: Settings, name: str) -> dict | None:
    """Full teardown of a custom source (tombstone its docs + drop the Qdrant
    points + drop the registry row + remove staging). Returns None if unknown."""
    from windex.custom_source import ingest as cingest

    with db.pooled(settings.pg_dsn) as conn:
        return cingest.delete_source(conn, settings, name)


_STATIC_SEARCH_SOURCES = {"news", "github", "wiki", "arxiv", "smallweb", "docs",
                          "hn", "hf", "memory", "all"}


def validate_source(settings: Settings, source: str) -> str:
    """Return ``source`` if it is a valid /v1/search source — a built-in static
    source (or ``all``), or a registered custom source name — else raise
    ValueError (the route maps that to 422, preserving the bogus-source contract).
    Custom names are checked against a short module-level TTL cache so per-query
    validation doesn't hit Postgres on every search; a DB blip degrades to the
    last known set rather than rejecting a real source."""
    if source in _STATIC_SEARCH_SOURCES:
        return source
    now = time.monotonic()
    hit = _source_cache.get(settings.pg_dsn)
    if not hit or now - hit[0] > _SOURCE_TTL:
        try:
            from windex.custom_source import registry

            with db.pooled(settings.pg_dsn) as conn:
                names = {i["name"] for i in registry.list_all(conn)}
        except Exception:  # noqa: BLE001 — DB blip: reuse the last known set
            names = hit[1] if hit else set()
        _source_cache[settings.pg_dsn] = (now, names)
        hit = (now, names)
    if source in hit[1]:
        return source
    raise ValueError(f"unknown source: {source}")


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
    # github is the exception: its pending-embed work lives in repos.status=
    # 'hydrated' (repos has no 'deduped' state), and it only writes a documents
    # row once already 'embedded'. Pull its backlog from the repos group-by so a
    # stalled gh-embed loop shows up instead of always reporting 0.
    embed_backlog["github"] = repos.get("hydrated", 0)
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


_RECENT_COLUMNS = {"indexed_at", "created_at"}


def recent_feed(settings: Settings, column: str, limit: int = 25) -> list[dict]:
    """Recent documents by `column`, newest first — the console progress feeds.
    `indexed_at` = recently EMBEDDED (landed in Qdrant); `created_at` = recently
    INDEXED (harvested/staged). `ts` is unix epoch seconds so the console renders
    it with agoTs() directly (same shape for both feeds)."""
    if column not in _RECENT_COLUMNS:  # whitelist: the column is interpolated below
        raise ValueError(f"unsupported recent column: {column}")
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, source, url, title, extract(epoch FROM {column})::bigint
            FROM documents WHERE {column} IS NOT NULL
            ORDER BY {column} DESC LIMIT %s
            """,
            (limit,),
        )
        return [
            {"id": i, "source": s, "url": u, "title": t, "ts": ts}
            for i, s, u, t, ts in cur.fetchall()
        ]


def clear_doc_stats_cache() -> None:
    """Drop the cached document rollups so the next /metrics + /v1/stats scrape
    recomputes from Postgres. Called after a bulk status mutation (cleanup /
    backfill) so dashboards don't show pre-change counts for up to _PG_HEAVY_TTL."""
    _pg_heavy_cache.clear()
    _pg_stats_cache.clear()


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

    import time

    enabled = get_loops_enabled(settings)
    ingest = get_ingest_enabled(settings)
    # Liveness comes from the per-loop Postgres heartbeat (embed_loop writes
    # loop_heartbeat_<source> every cycle), not host pgrep — the loops run in
    # separate containers with their own PID namespaces (and the slim image has no
    # pgrep). A heartbeat within HEARTBEAT_STALE_SECS = the process is alive; the
    # window exceeds the loop's max backoff (300s) so a probing-but-alive loop
    # isn't misreported as down.
    HEARTBEAT_STALE_SECS = 360
    out = []
    with db.pooled(settings.pg_dsn) as conn:
        now = int(time.time())
        for job in jobs.embed_loop_jobs():
            src = job.argv[1]
            try:
                last = int(db.get_control(conn, f"loop_heartbeat_{src}", "0"))
            except ValueError:
                last = 0
            running = (now - last) < HEARTBEAT_STALE_SECS
            en = enabled.get(src, True)
            out.append({
                "source": src, "enabled": en, "running": running,
                "state": "up" if running else ("down" if en else "disabled"),
                "pids": [],  # not meaningful across container PID namespaces
                "ingest_enabled": ingest.get(src, True),
                "log": job.name,  # /v1/logs/{name} key for the row's "read logs" button
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


# --- ingest desired-state (symmetric to the embed loops) ---
# "ingest" is the per-source fetch macro — `windex refresh --source X`: check the
# source for new content → fetch → stage into Postgres → the embed loop queues
# it. There is NO continuous ingest process; this flag gates whether the refresh
# sweep and the scheduler auto-ingest the source. `ingest now` is a manual run.

def get_ingest_enabled(settings: Settings) -> dict[str, bool]:
    """{source: ingest enabled} from ingest_<source> flags (default enabled)."""
    from windex.api import jobs

    sources = [j.argv[1] for j in jobs.embed_loop_jobs()]
    try:
        with db.pooled(settings.pg_dsn) as conn:
            return {s: db.get_control(conn, f"ingest_{s}", "enabled") != "disabled"
                    for s in sources}
    except Exception:  # noqa: BLE001 — a flag-read failure must not disable ingest
        return {s: True for s in sources}


def set_ingest_enabled(settings: Settings, source: str, enabled: bool) -> dict:
    """Toggle whether this source is auto-ingested (by refresh + the scheduler).
    Ingest is on-demand/scheduled — no process to start/stop, just the flag.
    Raises KeyError for an unknown source."""
    from windex.api import jobs

    if source not in {j.argv[1] for j in jobs.embed_loop_jobs()}:
        raise KeyError(source)
    with db.pooled(settings.pg_dsn) as conn:
        db.set_control(conn, f"ingest_{source}", "enabled" if enabled else "disabled")
    return {"source": source, "ingest_enabled": enabled}


def supervisor_status(settings: Settings) -> dict:
    """For the console control panel: supervisor liveness, the global
    pause/resume flag, and the per-loop states."""
    from windex.api import jobs

    try:
        with db.pooled(settings.pg_dsn) as conn:
            paused = db.get_control(conn, "indexing", "running") == "paused"
    except Exception:  # noqa: BLE001
        paused = False
    return {"watchdog_running": bool(jobs._pids("scripts/watchdog.sh")),
            "indexing_paused": paused,
            "loops": loop_states(settings)}


_pg_heavy_warming = False


def _warm_pg_heavy(settings: Settings) -> None:
    """Compute _pg_heavy in a daemon thread (one at a time) so a cold rollup
    never blocks the freshness poll; errors are swallowed (counts just stay 0
    until a later warm succeeds)."""
    global _pg_heavy_warming
    if _pg_heavy_warming:
        return
    _pg_heavy_warming = True

    def _run() -> None:
        global _pg_heavy_warming
        try:
            _pg_heavy(settings)
        except Exception:  # noqa: BLE001
            pass
        finally:
            _pg_heavy_warming = False

    import threading

    threading.Thread(target=_run, daemon=True).start()


def freshness(settings: Settings) -> list[dict]:
    """Per-source freshness for the console table: indexed + pending counts (from
    the 600s-cached docs rollup, so this stays cheap) and last embed-loop
    activity (the loop log's mtime — avoids a per-source max(indexed_at) on the
    13M-row table, which has no (source, indexed_at) index)."""
    from windex.api import jobs

    # Counts come from the 600s-cached docs rollup, but computing it cold is a
    # full 13M-row aggregate. Never block the poll on that: serve the cache if
    # warm, else 0 now and warm it in the background so counts fill in on a later
    # poll. Timestamps (below) never depend on it.
    now = time.monotonic()
    hit = _pg_heavy_cache.get(settings.pg_dsn)
    docs = hit[1].get("docs", {}) if hit else {}
    if not (hit and now - hit[0] < _PG_HEAVY_TTL):
        _warm_pg_heavy(settings)
    canon = {"ccnews": "news", "gh": "github"}  # loop name → corpus source
    # last successful ingest per source (recorded by `windex refresh` via the
    # ingest_ts_<source> control flag) — the "last update" column.
    ingest_ts = {}
    try:
        with db.pooled(settings.pg_dsn) as conn:
            for job in jobs.embed_loop_jobs():
                v = db.get_control(conn, f"ingest_ts_{job.argv[1]}", "")
                ingest_ts[job.argv[1]] = float(v) if v else None
    except Exception:  # noqa: BLE001
        pass
    out = []
    for job in jobs.embed_loop_jobs():
        src = job.argv[1]
        by_status = docs.get(canon.get(src, src), {})
        indexed = int(by_status.get("embedded", 0))
        pending = int(sum(v for k, v in by_status.items() if k != "embedded"))
        last = None
        for name in (f"{job.name}.log", f"embed-{src}.log"):  # jobs name + legacy nohup name
            try:
                last = max(last or 0, (jobs.LOG_DIR / name).stat().st_mtime)
            except OSError:
                pass
        out.append({"source": src, "indexed": indexed, "pending": pending,
                    "last_embed_ts": last, "last_update_ts": ingest_ts.get(src)})
    return out


def dataset_stats(settings: Settings, source: str) -> dict:
    """Per-dataset detail for the freshness row-click: doc counts broken out by
    pipeline status, the total, and the content date span. Counts come from the
    600s-cached rollup (cheap; warmed in the background if cold); the date range
    uses the (source, published_at) index so min/max stays fast. Raises KeyError
    for an unknown source."""
    from windex.api import jobs

    if source not in {j.argv[1] for j in jobs.embed_loop_jobs()}:
        raise KeyError(source)
    corpus = {"ccnews": "news", "gh": "github"}.get(source, source)
    hit = _pg_heavy_cache.get(settings.pg_dsn)
    if not hit:
        _warm_pg_heavy(settings)
    by_status = dict((hit[1].get("docs", {}) if hit else {}).get(corpus, {}))

    content_from = content_to = None
    try:
        with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT min(published_at), max(published_at) FROM documents WHERE source = %s",
                (corpus,))
            row = cur.fetchone()
            if row and row[0]:
                content_from, content_to = row[0].isoformat(), row[1].isoformat()
    except Exception:  # noqa: BLE001 — the panel degrades gracefully without dates
        pass

    return {"source": source, "by_status": by_status, "total": sum(by_status.values()),
            "content_from": content_from, "content_to": content_to}


# --- editable job scheduler (backed by the `schedule` table) ---
# The hardcoded SCHEDULE list is gone: rows live in the DB (seeded by init_db,
# edited via /v1/schedule), and the `windex scheduler` timer loop fires the ones
# that are enabled + due. A 'command' entry maps its target through _SCHED_CMD;
# an 'ingest' entry runs `windex refresh --source <target>`.
_SCHED_CMD = {"daily": ["daily"], "maintain": ["maintain"], "eval": ["eval"]}
_SCHED_LOG = {"daily": "daily", "maintain": "maintain", "eval": "eval"}
# pgrep pattern that means "this entry is running now": command targets match
# their own process; every ingest entry shares the refresh sweep's marker.
_SCHED_PATTERN = {"daily": "windex daily", "maintain": "windex maintain",
                  "eval": "windex eval"}
_WEEKDAYS = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")


def _schedule_sources() -> set[str]:
    """Valid ingest targets — the embed-loop source names (== EMBED_SOURCES),
    minus the push sources. A push source (memory) has an embed loop but no pull
    ingest to schedule, so it must never be an editable `ingest` target."""
    from windex.api import jobs

    return {j.argv[1] for j in jobs.embed_loop_jobs()} - jobs.PUSH_SOURCES


def _read_schedule(settings: Settings) -> list[dict]:
    """Raw schedule rows (last_run as a datetime). Not resilient — callers that
    face the console wrap this; the scheduler tick lets a blip bubble to its
    own catch/back-off."""
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT name, kind, target, hour, minute, weekday, enabled, last_run
               FROM schedule ORDER BY hour, minute, name"""
        )
        rows = cur.fetchall()
    return [
        {"name": n, "kind": k, "target": t, "hour": h, "minute": m,
         "weekday": w, "enabled": en, "last_run": lr}
        for n, k, t, h, m, w, en, lr in rows
    ]


def _get_schedule_entry(settings: Settings, name: str) -> dict | None:
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT name, kind, target, hour, minute, weekday, enabled, last_run
               FROM schedule WHERE name = %s""",
            (name,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {"name": row[0], "kind": row[1], "target": row[2], "hour": row[3],
            "minute": row[4], "weekday": row[5], "enabled": row[6], "last_run": row[7]}


def list_schedule(settings: Settings) -> list[dict]:
    """Read the schedule table (API-facing: last_run as ISO). Resilient to a
    cold/missing DB — returns [] rather than raising."""
    try:
        rows = _read_schedule(settings)
    except Exception:  # noqa: BLE001 — a cold DB must not break the console
        return []
    for r in rows:
        r["last_run"] = r["last_run"].isoformat() if r["last_run"] else None
    return rows


def _dow_sun0(now: datetime) -> int:
    """Day of week with Sunday=0 (the schedule.weekday convention). Python's
    datetime.weekday() is Monday=0."""
    return (now.weekday() + 1) % 7


def _is_due(entry: dict, now: datetime) -> bool:
    """Pure predicate: is this entry due to fire at `now`? Enabled, the hour and
    minute match, the weekday matches (or is NULL = every day), and it has not
    already fired within this same minute (last_run guard against a double-tick)."""
    if not entry["enabled"]:
        return False
    if entry["hour"] != now.hour or entry["minute"] != now.minute:
        return False
    weekday = entry["weekday"]
    if weekday is not None and weekday != _dow_sun0(now):
        return False
    last = entry.get("last_run")
    if last is not None and (last.year, last.month, last.day, last.hour, last.minute) == \
            (now.year, now.month, now.day, now.hour, now.minute):
        return False
    return True


def _cadence(entry: dict) -> str:
    when = f"{entry['hour']:02d}:{entry['minute']:02d}"
    if entry["weekday"] is None:
        return f"daily · {when}"
    return f"{_WEEKDAYS[entry['weekday']]} · {when}"


def _entry_running(entry: dict) -> bool:
    from windex.api import jobs

    if entry["kind"] == "command":
        pattern = _SCHED_PATTERN.get(entry["target"])
        return bool(pattern and jobs._pids(pattern))
    # ingest entries all fire through the refresh sweep, tagged WINDEX_REFRESH
    return bool(jobs._pids("WINDEX_REFRESH"))


def schedule_status(settings: Settings) -> list[dict]:
    """The schedule entries with their editable fields + running state + last
    run — the shape the console schedule editor reads. Resilient to a cold DB."""
    try:
        rows = _read_schedule(settings)
    except Exception:  # noqa: BLE001
        return []
    out = []
    for e in rows:
        last = e["last_run"]
        label = (f"Ingest {e['target']}" if e["kind"] == "ingest"
                 else {"daily": "Daily freshness",
                       "maintain": "Store maintenance"}.get(e["target"], e["target"]))
        out.append({
            "name": e["name"], "kind": e["kind"], "target": e["target"],
            "hour": e["hour"], "minute": e["minute"], "weekday": e["weekday"],
            "enabled": e["enabled"],
            "last_run": last.isoformat() if last else None,
            "last_run_ts": last.timestamp() if last else None,
            "running": _entry_running(e),
            "label": label, "cadence": _cadence(e),
        })
    return out


def _validate_schedule_entry(e: dict) -> None:
    """Raise ValueError if the entry is not a valid, dispatchable row."""
    if e["kind"] not in ("ingest", "command"):
        raise ValueError("kind must be 'ingest' or 'command'")
    if not isinstance(e["hour"], int) or not (0 <= e["hour"] <= 23):
        raise ValueError("hour must be 0-23")
    if not isinstance(e["minute"], int) or not (0 <= e["minute"] <= 59):
        raise ValueError("minute must be 0-59")
    if e["weekday"] is not None and (not isinstance(e["weekday"], int)
                                     or not (0 <= e["weekday"] <= 6)):
        raise ValueError("weekday must be 0-6 (0=Sun) or null")
    if e["kind"] == "command" and e["target"] not in _SCHED_CMD:
        raise ValueError(f"command target must be one of {sorted(_SCHED_CMD)}")
    if e["kind"] == "ingest" and e["target"] not in _schedule_sources():
        raise ValueError(f"ingest target must be one of {sorted(_schedule_sources())}")


def _coerce_bool(value) -> bool:
    """Coerce a schedule 'enabled' value to a real bool. The route accepts an
    untyped JSON body, so a client can send the string "false" — and bool("false")
    is True (any non-empty string is truthy), silently ENABLING an entry meant to
    be off. Accept real bools/ints and the literal string forms; reject the rest
    with a ValueError (→ 422) rather than guessing."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "yes", "on"):
            return True
        if low in ("false", "0", "no", "off", ""):
            return False
    raise ValueError(f"enabled must be a boolean, got {value!r}")


def upsert_schedule(settings: Settings, entry: dict) -> dict:
    """Create or update a schedule row. On an existing row, unspecified fields
    are preserved (partial edit); on a create, kind + target are required and
    hour/minute/weekday/enabled fall back to sensible defaults. Raises
    ValueError (→ 422) for an invalid entry."""
    name = entry.get("name")
    if not name:
        raise ValueError("name is required")
    existing = _get_schedule_entry(settings, name)
    if existing is None:
        merged = {
            "name": name,
            "kind": entry.get("kind"),
            "target": entry.get("target"),
            "hour": entry.get("hour", 0),
            "minute": entry.get("minute", 0),
            "weekday": entry.get("weekday"),
            "enabled": entry.get("enabled", True),
        }
        if merged["kind"] is None or merged["target"] is None:
            raise ValueError("kind and target are required to create a schedule entry")
    else:
        merged = {k: existing[k] for k in
                  ("name", "kind", "target", "hour", "minute", "weekday", "enabled")}
        for k in ("kind", "target", "hour", "minute", "weekday", "enabled"):
            if k in entry:
                merged[k] = entry[k]
    merged["enabled"] = _coerce_bool(merged["enabled"])
    _validate_schedule_entry(merged)
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO schedule (name, kind, target, hour, minute, weekday, enabled)
               VALUES (%(name)s, %(kind)s, %(target)s, %(hour)s, %(minute)s,
                       %(weekday)s, %(enabled)s)
               ON CONFLICT (name) DO UPDATE SET
                   kind = EXCLUDED.kind, target = EXCLUDED.target,
                   hour = EXCLUDED.hour, minute = EXCLUDED.minute,
                   weekday = EXCLUDED.weekday, enabled = EXCLUDED.enabled""",
            merged,
        )
        conn.commit()
    merged["cadence"] = _cadence(merged)
    return merged


def delete_schedule(settings: Settings, name: str) -> dict:
    """Delete a schedule row. Raises KeyError (→ 404) if it does not exist."""
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM schedule WHERE name = %s", (name,))
        deleted = cur.rowcount
        conn.commit()
    if not deleted:
        raise KeyError(name)
    return {"deleted": name}


def dispatch_entry(settings: Settings, entry: dict) -> dict:
    """Fire one schedule entry now (detached), regardless of due-ness: ingest →
    `windex refresh --source <target>`; command → the mapped windex command.
    Raises KeyError for an unknown/unmapped target."""
    kind, target = entry["kind"], entry["target"]
    if kind == "ingest":
        return run_refresh(settings, [target])
    if kind == "command":
        if target not in _SCHED_CMD:
            raise KeyError(target)
        return {"action": target, "pid": _spawn_windex(_SCHED_CMD[target], _SCHED_LOG[target])}
    raise KeyError(kind)


def run_scheduled(settings: Settings, name: str) -> dict:
    """Run a schedule entry now (detached). A manual run ignores the ingest
    desired-state flag (like an explicit `refresh --source`). Raises KeyError
    (→ 404) for an unknown name."""
    entry = _get_schedule_entry(settings, name)
    if entry is None:
        raise KeyError(name)
    return dispatch_entry(settings, entry)


def _mark_ran(settings: Settings, name: str, when: datetime) -> None:
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("UPDATE schedule SET last_run = %s WHERE name = %s", (when, name))
        conn.commit()


def run_due(settings: Settings, now: datetime | None = None) -> list[str]:
    """One scheduler tick: fire every enabled + due entry, stamping last_run.
    Ingest entries additionally skip when the source's ingest_enabled flag is
    off. Returns the names fired. Resilient: a failed table read yields [] so the
    loop backs off and retries; a single entry's dispatch failure is logged and
    does not abort the rest of the tick."""
    now = now or datetime.now()
    try:
        entries = _read_schedule(settings)
    except Exception:  # noqa: BLE001 — DB blip: skip this tick, retry next
        return []
    ingest_enabled: dict[str, bool] | None = None
    fired: list[str] = []
    for e in entries:
        if not _is_due(e, now):
            continue
        if e["kind"] == "ingest":
            if ingest_enabled is None:
                ingest_enabled = get_ingest_enabled(settings)
            if not ingest_enabled.get(e["target"], True):
                continue
        try:
            dispatch_entry(settings, e)
            _mark_ran(settings, e["name"], now)
            fired.append(e["name"])
        except Exception:  # noqa: BLE001 — one bad entry must not stop the tick
            pass
    return fired


def activity(settings: Settings) -> list[dict]:
    """What the console log drawer watches: recurring actions, the embed loops,
    and the services — each with running state, last activity, and (for a stopped
    action) whether its log ended in an error ('crashed'). `name` is the
    /v1/logs/{name} key for tailing."""
    from windex.api import jobs
    from windex.api import logs as logmod

    mtimes = {r["name"]: r["mtime"] for r in logmod.list_logs()}

    def errored(key: str) -> bool:
        try:
            hit = logmod.tail(key, lines=1, level="error")
            return bool(hit.get("available") and hit.get("lines"))
        except Exception:  # noqa: BLE001
            return False

    out = []
    for label, pattern, key in (
        ("Refresh sweep", "WINDEX_REFRESH", "refresh"),
        ("Daily freshness", "windex daily", "daily"),
        ("Store maintenance", "windex maintain", "maintain"),
    ):
        running = bool(jobs._pids(pattern))
        out.append({"name": key, "label": label, "group": "action", "running": running,
                    "last_ts": mtimes.get(key), "error": (not running and errored(key))})
    for job in jobs.embed_loop_jobs():
        out.append({"name": job.name, "label": f"loop · {job.argv[1]}", "group": "loop",
                    "running": bool(jobs._pids(job.pattern)), "last_ts": mtimes.get(job.name),
                    "error": False})
    out.append({"name": "server", "label": "API server", "group": "service",
                "running": jobs.serve_running(), "last_ts": mtimes.get("server"), "error": False})
    out.append({"name": "scheduler", "label": "Job scheduler", "group": "service",
                "running": jobs.scheduler_running(), "last_ts": mtimes.get("scheduler"),
                "error": False})
    out.append({"name": "watchdog", "label": "Supervisor", "group": "service",
                "running": bool(jobs._pids("scripts/watchdog.sh")), "last_ts": mtimes.get("watchdog"),
                "error": False})
    return out


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
