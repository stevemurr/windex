"""Sync the Kagi Small Web feed list into the ``feeds`` table.

Source: ``smallweb.txt`` from github.com/kagisearch/smallweb (MIT) — one
RSS/Atom feed URL per line (~38k feeds, ~37.6k unique hosts, one personal blog
per domain). This is the freshness *seed* for Small Web: it discovers WHICH
feeds exist. Polling them (conditional GET + parse) is poll.py.

Idempotent, like every windex sync: re-running upserts the current list.
Entries that have dropped off the list are marked ``status='removed'`` (never
deleted — a feed that reappears is reactivated, and the row keeps its poll
watermark/etag). Everything list-format-specific (URL-per-line, comment lines)
lives here so a different upstream list only touches this module.
"""

import logging
from urllib.parse import urlsplit

import httpx
import psycopg

from windex.smallweb import USER_AGENT

log = logging.getLogger("windex.smallweb.sync")

LIST_URL = "https://raw.githubusercontent.com/kagisearch/smallweb/main/smallweb.txt"

# A fetched list smaller than this fraction of the feeds we already track is
# treated as a truncated/glitched 200 (raw.githubusercontent.com edge-cache
# hiccup, proxy short-read): inserts/reactivations still apply, but the removal
# step is skipped so a partial fetch can't mark most of ~38k feeds 'removed' and
# stall Small Web ingest until the next successful sync.
_SHRINK_FLOOR = 0.5


def parse_list(text: str) -> list[str]:
    """One http(s) feed URL per line. Blank lines and ``#`` comments are
    skipped; anything that isn't an absolute http(s) URL is ignored. Order is
    preserved and duplicates are collapsed (first occurrence wins)."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if not line.lower().startswith(("http://", "https://")):
            continue
        if line in seen:
            continue
        seen.add(line)
        out.append(line)
    return out


def host_of(url: str) -> str:
    """Feed host (lowercased netloc) — the payload ``outlet`` for its posts."""
    return urlsplit(url).netloc.lower()


def sync(conn: psycopg.Connection, client: httpx.Client | None = None,
         url: str = LIST_URL) -> dict:
    """Fetch the list and reconcile the ``feeds`` table against it.

    Returns ``{"total", "added", "reactivated", "removed"}``. ``total`` is the
    number of feed URLs in the fetched list; the rest are row-count deltas.
    """
    own = client is None
    client = client or httpx.Client(
        timeout=60, follow_redirects=True, headers={"User-Agent": USER_AGENT}
    )
    try:
        resp = client.get(url)
        resp.raise_for_status()
        urls = parse_list(resp.text)
        if not urls:
            # An empty parse from a 200 is almost certainly a truncated/glitched
            # fetch, never a real "every blog vanished". Reconciling would mark
            # every feed 'removed' and halt ingest — refuse loudly so the caller
            # retries instead of silently wiping the table.
            raise RuntimeError(
                "smallweb feed list fetched empty — refusing to reconcile "
                "(a truncated 200 would mark every feed 'removed')"
            )
        pairs = [(u, host_of(u)) for u in urls]
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM feeds WHERE status <> 'removed'")
            existing = cur.fetchone()[0]
            # 1. insert unseen feeds
            cur.executemany(
                "INSERT INTO feeds (url, host) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING",
                pairs,
                returning=False,
            )
            added = max(cur.rowcount or 0, 0)
            # 2. reactivate feeds that dropped off earlier and are back
            cur.execute(
                "UPDATE feeds SET status = 'active', fail_count = 0 "
                "WHERE status = 'removed' AND url = ANY(%s)",
                (urls,),
            )
            reactivated = cur.rowcount or 0
            # 3. mark feeds no longer on the list as removed (idempotent — the
            #    row and its poll watermark survive so a reappearance is cheap),
            #    but skip this entirely on an implausibly-small fetch: a partial
            #    200 must not wipe most of the table (inserts/reactivations above
            #    are harmless either way).
            if existing and len(urls) < existing * _SHRINK_FLOOR:
                log.warning(
                    "smallweb sync: fetched %d feeds vs %d tracked (<%.0f%%) — "
                    "skipping removals, list looks truncated",
                    len(urls), existing, _SHRINK_FLOOR * 100,
                )
                removed = 0
            else:
                cur.execute(
                    "UPDATE feeds SET status = 'removed' "
                    "WHERE status <> 'removed' AND NOT (url = ANY(%s))",
                    (urls,),
                )
                removed = cur.rowcount or 0
        conn.commit()
        return {"total": len(urls), "added": added,
                "reactivated": reactivated, "removed": removed}
    finally:
        if own:
            client.close()
