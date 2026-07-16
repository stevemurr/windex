"""Transport-agnostic search service: both the REST app and the MCP server call
these functions and return the same result objects (the /v1 contract)."""

import time
from datetime import datetime

import pyarrow.compute as pc
import pyarrow.parquet as pq

from windex import db
from windex.config import Settings
from windex.index.search import search as index_search

RESULT_FIELDS = ("url", "title", "snippet", "source", "published_at", "outlet",
                 "stars", "language", "topics", "pushed_at", "lang", "incoming_links",
                 "primary_category", "categories", "authors")


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
) -> dict:
    t0 = time.monotonic()
    resp = index_search(
        settings, q, source=source, limit=limit, mode=mode,
        published_after=published_after, published_before=published_before,
        min_stars=min_stars, language=language, category=category, outlet=outlet,
    )
    results = []
    for r in resp["results"]:
        item = {"id": r.get("doc_id"), "score": round(r["score"], 4)}
        item.update({k: r[k] for k in RESULT_FIELDS if r.get(k) is not None})
        results.append(item)
    total_ms = int((time.monotonic() - t0) * 1000)
    return {
        "query": q,
        "results": results,
        "mode": "lexical (embedder busy — degraded from hybrid)" if resp["degraded"] else mode,
        "timings": {**resp["timings"], "total_ms": total_ms},
        "took_ms": total_ms,
    }


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
            # arXiv stages the abstract (metadata only) rather than a `text` column
            col = "text" if "text" in table.column_names else "abstract"
            if col in table.column_names:
                doc["text"] = table.column(col)[0].as_py()
    return doc


# PG aggregates are cached briefly: the dashboard polls every 4s, and some of
# these queries turn into full scans at backfill scale.
_pg_stats_cache: dict = {}
_PG_STATS_TTL = 10.0


def _pg_stats(settings: Settings, ttl: float = _PG_STATS_TTL) -> dict:
    now = time.monotonic()
    hit = _pg_stats_cache.get(settings.pg_dsn)
    if hit and now - hit[0] < ttl:
        return hit[1]
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source, status, count(*) FROM documents GROUP BY source, status"
        )
        docs: dict = {}
        for source, status, n in cur.fetchall():
            docs.setdefault(source, {})[status] = n
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
        cur.execute(
            """SELECT count(DISTINCT split_part(split_part(canonical_url, '://', 2), '/', 1))
               FROM documents WHERE source = 'news'"""
        )
        outlets = cur.fetchone()[0]
        cur.execute(
            """SELECT min(published_at)::date, max(published_at)::date
               FROM documents WHERE source = 'news' AND status = 'embedded'"""
        )
        cov_min, cov_max = cur.fetchone()
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
    news = docs.get("news", {})
    gh = docs.get("github", {})
    wiki = docs.get("wiki", {})
    arxiv = docs.get("arxiv", {})
    smallweb = docs.get("smallweb", {})
    result = {
        "documents": docs,
        "repos": repos,
        "warc_files": warcs,
        "gharchive_files": hours,
        "totals": {
            "indexed_pages": news.get("embedded", 0) + gh.get("embedded", 0)
            + wiki.get("embedded", 0) + arxiv.get("embedded", 0)
            + smallweb.get("embedded", 0),
            "news_articles": news.get("embedded", 0),
            "github_projects": gh.get("embedded", 0),
            "wiki_articles": wiki.get("embedded", 0),
            "arxiv_papers": arxiv.get("embedded", 0),
            "smallweb_posts": smallweb.get("embedded", 0),
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


def get_timeseries(settings: Settings, minutes: int = 60) -> list[dict]:
    """Per-minute indexing and download volumes for the trailing window,
    zero-filled — feeds the dashboard charts."""
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
        return [
            {"t": m.isoformat(), "ingested": ingested, "docs": embeds,
             "mb": round(nbytes / 1e6, 1)}
            for m, ingested, embeds, nbytes in cur.fetchall()
        ]


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
        },
        "downloading_bytes_on_disk": in_flight,
    }
    return stats
