"""Transport-neutral canonical search and document retrieval."""

from __future__ import annotations

import hashlib
import json
import threading
import time
from datetime import datetime

import psycopg
import pyarrow.parquet as pq

from windex import db
from windex.config import Settings
from windex.index.search import search as index_search
from windex.metrics import SEARCH_DURATION, SEARCH_REQUESTS

RESULT_FIELDS = (
    "url", "title", "snippet", "source", "published_at", "outlet", "stars",
    "language", "topics", "pushed_at", "lang", "incoming_links",
    "primary_category", "categories", "authors", "framework", "version",
    "attribution", "points", "num_comments", "author", "target_url", "root",
    "kind", "conversation_id", "chunk_index", "message_range", "extra",
)

_source_cache: dict[str, tuple[float, set[str]]] = {}
_SOURCE_TTL = 15.0


def _extra_object(value: object) -> dict | None:
    """Normalize the opaque custom-Source metadata stored in parquet/Qdrant."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {"value": value}
        return decoded if isinstance(decoded, dict) else {"value": decoded}
    return {"value": value}


def _mode_label(requested: str, response: dict) -> str:
    """Describe the mode actually served without conflating failure causes."""
    if not response["degraded"]:
        return requested

    degradation = response.get("degradation")
    if not isinstance(degradation, dict):
        # Compatibility with older index_search implementations and focused
        # test doubles that only expose the original boolean.
        return "lexical (embedder busy — degraded from hybrid)"

    embedder = bool(degradation.get("embedder"))
    unavailable = sorted({
        str(source) for source in degradation.get("unavailable_sources") or []
    })
    if embedder and not unavailable:
        return "lexical (embedder busy — degraded from hybrid)"

    actual = "lexical" if embedder and requested == "hybrid" else requested
    reasons = []
    if embedder:
        reasons.append("embedder busy — degraded from hybrid")
    if unavailable:
        reasons.append(
            "partial results; unavailable sources: " + ", ".join(unavailable))
    if not reasons:
        return f"{actual} (degraded)"
    if not embedder:
        return f"{actual} (degraded — {reasons[0]})"
    return f"{actual} ({'; '.join(reasons)})"


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
    started = time.monotonic()
    try:
        response = index_search(
            settings, q, source=source, limit=limit, mode=mode,
            published_after=published_after, published_before=published_before,
            min_stars=min_stars, language=language, category=category,
            outlet=outlet, framework=framework, min_points=min_points,
            root=root, kind=kind, conversation_id=conversation_id,
        )
    except Exception:
        SEARCH_REQUESTS.labels(mode=mode, result="error").inc()
        SEARCH_DURATION.observe(time.monotonic() - started)
        raise
    results = []
    for raw in response["results"]:
        item = {"id": raw.get("doc_id"), "score": round(raw["score"], 4)}
        item.update({
            key: raw[key] for key in RESULT_FIELDS if raw.get(key) is not None})
        if "extra" in item:
            item["extra"] = _extra_object(item["extra"])
        results.append(item)
    total_ms = int((time.monotonic() - started) * 1000)
    result = {
        "query": q,
        "results": results,
        "mode": _mode_label(mode, response),
        "timings": {**response["timings"], "total_ms": total_ms},
        "took_ms": total_ms,
    }
    SEARCH_REQUESTS.labels(
        mode=mode, result="degraded" if response["degraded"] else "ok").inc()
    SEARCH_DURATION.observe(total_ms / 1000.0)
    _record_search_metric(
        settings, q, source, mode, response["degraded"],
        result["timings"], len(results))
    return result


def _record_search_metric(
    settings: Settings,
    q: str,
    source: str,
    mode: str,
    degraded: bool,
    timings: dict,
    result_count: int,
) -> None:
    def write() -> None:
        try:
            with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO search_metrics
                           (source, mode_requested, degraded, q_hash,
                            embed_ms, search_ms, total_ms, results)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        source, mode, degraded,
                        hashlib.sha1(q.encode()).hexdigest()[:12],
                        timings.get("embed_query_ms"), timings.get("search_ms"),
                        timings.get("total_ms"), result_count,
                    ),
                )
                conn.commit()
        except Exception:
            pass

    try:
        threading.Thread(
            target=write, name="search-metric", daemon=True).start()
    except Exception:
        pass


def validate_source(settings: Settings, source: str) -> str:
    """Validate public search identity against enabled canonical Sources."""
    if source == "all":
        return source
    now = time.monotonic()
    hit = _source_cache.get(settings.pg_dsn)
    if not hit or now - hit[0] > _SOURCE_TTL:
        try:
            with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    """SELECT search_name FROM sources
                        WHERE archived_at IS NULL AND enabled""")
                names = {row[0] for row in cur.fetchall()}
        except Exception:
            names = hit[1] if hit else set()
        hit = (now, names)
        _source_cache[settings.pg_dsn] = hit
    if source not in hit[1]:
        raise ValueError(f"unknown source: {source}")
    return source


def get_document(settings: Settings, doc_id: str) -> dict | None:
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT id, source, url, title, published_at, lang, status,
                      duplicate_of, text_ref
                 FROM documents WHERE id = %s""",
            (doc_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    document = dict(zip(
        (
            "id", "source", "url", "title", "published_at", "lang", "status",
            "duplicate_of", "text_ref",
        ),
        row,
    ))
    if document["published_at"]:
        document["published_at"] = document["published_at"].isoformat()
    text_ref = document.pop("text_ref")
    document["text"] = None
    document["message_range"] = None
    if text_ref:
        root = settings.staging_dir.resolve()
        path = (root / text_ref).resolve()
        if root not in path.parents or not path.is_file():
            return document
        table = pq.read_table(path, filters=[("id", "==", doc_id)])
        if table.num_rows:
            column = next(
                (name for name in ("text", "abstract", "story_text")
                 if name in table.column_names),
                None,
            )
            if column is not None:
                document["text"] = table.column(column)[0].as_py()
            if "message_range" in table.column_names:
                document["message_range"] = (
                    table.column("message_range")[0].as_py()
                )
    return document


def get_search_metrics(settings: Settings, minutes: int = 60) -> dict:
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT count(*), count(*) FILTER (WHERE degraded),
                      percentile_cont(0.5) WITHIN GROUP (ORDER BY total_ms),
                      percentile_cont(0.95) WITHIN GROUP (ORDER BY total_ms),
                      percentile_cont(0.99) WITHIN GROUP (ORDER BY total_ms)
                 FROM search_metrics
                WHERE ts > now() - make_interval(mins => %s)""",
            (minutes,),
        )
        searches, degraded, p50, p95, p99 = cur.fetchone()
    def milliseconds(value):
        return round(value) if value is not None else 0
    return {
        "window_minutes": minutes,
        "searches": searches,
        "degraded": degraded,
        "degraded_pct": (
            round(100.0 * degraded / searches, 1) if searches else 0.0),
        "p50_ms": milliseconds(p50),
        "p95_ms": milliseconds(p95),
        "p99_ms": milliseconds(p99),
    }


def prune_search_metrics(conn: psycopg.Connection, days: int = 30) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM search_metrics "
            "WHERE ts < now() - make_interval(days => %s)",
            (days,),
        )
        deleted = cur.rowcount or 0
    conn.commit()
    return deleted


__all__ = [
    "get_document", "get_search_metrics", "prune_search_metrics",
    "run_search", "validate_source",
]
