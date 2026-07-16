"""Poll CC-News warc.paths.gz listings and record unseen WARC paths as pending.

This is the freshness watermark: backfill and the daily job are the same code,
differing only in how far back the window reaches.
"""

import gzip
from datetime import date, datetime, timedelta

import httpx
import psycopg

PATHS_URL = "https://data.commoncrawl.org/crawl-data/CC-NEWS/{y:04d}/{m:02d}/warc.paths.gz"
DATA_URL = "https://data.commoncrawl.org/{path}"


def path_date(path: str) -> date:
    # crawl-data/CC-NEWS/2026/07/CC-NEWS-20260714123456-01234.warc.gz
    stamp = path.rsplit("CC-NEWS-", 1)[1][:8]
    return datetime.strptime(stamp, "%Y%m%d").date()


def months_in_window(start: date, end: date) -> list[tuple[int, int]]:
    months = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def list_month(client: httpx.Client, year: int, month: int) -> list[str]:
    resp = client.get(PATHS_URL.format(y=year, m=month))
    if resp.status_code == 404:  # month not published yet
        return []
    resp.raise_for_status()
    return gzip.decompress(resp.content).decode().splitlines()


def sync(conn: psycopg.Connection, days: int, today: date | None = None) -> int:
    """Insert unseen in-window WARC paths as pending. Returns number inserted."""
    today = today or date.today()
    start = today - timedelta(days=days)
    inserted = 0
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for y, m in months_in_window(start, today):
            paths = [p for p in list_month(client, y, m) if path_date(p) >= start]
            if not paths:
                continue
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO warc_files (path) VALUES (%s) ON CONFLICT DO NOTHING",
                    [(p,) for p in paths],
                    returning=False,
                )
                inserted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    conn.commit()
    return inserted


def pending_paths(conn: psycopg.Connection, limit: int, oldest_first: bool = True) -> list[str]:
    order = "ASC" if oldest_first else "DESC"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT path FROM warc_files WHERE status = 'pending' ORDER BY path {order} LIMIT %s",
            (limit,),
        )
        return [r[0] for r in cur.fetchall()]


def mark(
    conn: psycopg.Connection,
    paths: list[str],
    status: str,
    doc_counts: dict | None = None,
    sizes: dict[str, int] | None = None,
) -> None:
    """sizes: downloaded bytes per path (bandwidth accounting)."""
    import json

    with conn.cursor() as cur:
        cur.executemany(
            """UPDATE warc_files SET status = %s, doc_counts = %s::jsonb,
               bytes = coalesce(%s, bytes), processed_at = now() WHERE path = %s""",
            [(status, json.dumps(doc_counts or {}), (sizes or {}).get(p), p) for p in paths],
        )
    conn.commit()
