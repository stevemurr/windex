"""Discover the newest complete Wikipedia CirrusSearch snapshot and record its
shard files as pending. The freshness watermark for Wikipedia.

Snapshots are weekly (Saturdays) and only ~4 weeks are retained upstream. A
snapshot is only ingestible once a ``_SUCCESS`` marker exists in its
``index_name=<wiki>_content/`` directory (no partial-run ingestion). Each
snapshot is a FULL index, so we always re-baseline from the newest complete
date; the documents.text_hash ledger keeps a weekly re-ingest to the delta.
"""

import re

import httpx
import psycopg

ROOT_URL = "https://dumps.wikimedia.org/other/cirrus_search_index/"
CONTENT_DIR_URL = ROOT_URL + "{date}/index_name={wiki}_content/"
SUCCESS_MARKER = "_SUCCESS"


def content_dir_url(date: str, wiki: str) -> str:
    return CONTENT_DIR_URL.format(date=date, wiki=wiki)


def shard_url(date: str, name: str, wiki: str) -> str:
    return content_dir_url(date, wiki) + name


def list_dates(client: httpx.Client) -> list[str]:
    """Snapshot dates (YYYYMMDD), newest first."""
    resp = client.get(ROOT_URL)
    resp.raise_for_status()
    return sorted(set(re.findall(r'href="(\d{8})/"', resp.text)), reverse=True)


def list_content_dir(client: httpx.Client, date: str, wiki: str) -> tuple[bool, list[tuple[str, int]]]:
    """Return (has_success, [(shard_name, bytes), ...]) for one snapshot's
    content dir. Missing dir (404) reads as incomplete."""
    resp = client.get(content_dir_url(date, wiki))
    if resp.status_code == 404:
        return False, []
    resp.raise_for_status()
    has_success = f'href="{SUCCESS_MARKER}"' in resp.text
    name_re = re.compile(
        r'href="(' + re.escape(f"{wiki}_content-{date}-")
        + r'\d{5}\.json\.bz2)">[^<]*</a>\s+\S+\s+\S+\s+(\d+)'
    )
    files = [(m.group(1), int(m.group(2))) for m in name_re.finditer(resp.text)]
    files.sort()
    return has_success, files


def latest_complete(client: httpx.Client, wiki: str) -> tuple[str | None, list[tuple[str, int]]]:
    """Newest snapshot date whose content dir carries a _SUCCESS marker, with
    its shard files. (None, []) when nothing complete is available."""
    for date in list_dates(client):
        has_success, files = list_content_dir(client, date, wiki)
        if has_success and files:
            return date, files
    return None, []


def sync(conn: psycopg.Connection, wiki: str, client: httpx.Client | None = None) -> int:
    """Record the newest complete snapshot's shard files as pending. Returns the
    number of new shard rows inserted (0 when the newest snapshot is already
    recorded, or nothing complete is available)."""
    own = client is None
    client = client or httpx.Client(timeout=60, follow_redirects=True)
    try:
        date, files = latest_complete(client, wiki)
        if not date:
            return 0
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO wiki_dumps (name, dump_date, bytes)
                   VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
                [(name, date, size) for name, size in files],
                returning=False,
            )
            inserted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        conn.commit()
        return inserted
    finally:
        if own:
            client.close()


def pending_shards(conn: psycopg.Connection, limit: int) -> list[tuple[str, str]]:
    """Oldest-first (name, dump_date) pairs still pending."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, dump_date FROM wiki_dumps WHERE status = 'pending' "
            "ORDER BY name LIMIT %s",
            (limit,),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def mark(
    conn: psycopg.Connection,
    names: list[str],
    status: str,
    doc_counts: dict | None = None,
    sizes: dict[str, int] | None = None,
) -> None:
    import json

    with conn.cursor() as cur:
        cur.executemany(
            """UPDATE wiki_dumps SET status = %s, doc_counts = %s::jsonb,
               bytes = coalesce(%s, bytes), processed_at = now() WHERE name = %s""",
            [(status, json.dumps(doc_counts or {}), (sizes or {}).get(n), n) for n in names],
        )
    conn.commit()
