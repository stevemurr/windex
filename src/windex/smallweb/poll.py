"""The Small Web poller — windex's only FETCH-based ingest.

Each active feed gets a conditional GET (stored ETag / Last-Modified sent back;
a 304 is a near-free skip — personal blogs post rarely). The response is parsed
with feedparser; the newest N items are taken. An item whose body is already in
the feed (``content:encoded`` / Atom ``content``, or a long ``<description>``)
goes straight to extraction — no page fetch at all. A summary-only item's post
page IS fetched, and that fetch is the polite part:

  * robots.txt is honored per host (cached with a TTL; a failed robots fetch
    defaults to allow, logged),
  * a per-host minimum interval throttles repeat hits to one host,
  * a global concurrency cap bounds total in-flight work,
  * an honest descriptive User-Agent identifies windex (a default UA drew 403s),
  * responses are size- and content-type-bounded (HTML only, ~2MB cap).

Feeds accrue a consecutive ``fail_count``; after N failures a feed is marked
``dead`` and dropped from the rotation (reset to 0 on any success/304). The
dashboard pause flag is honored between feed batches. Exact dedup (canonical URL
id + text_hash, via the documents ledger) is essential here because feeds
re-serve the same items every poll; near-dup MinHash is deliberately NOT used
(see the module note in the tests / README): the corpus is ~one blog per host,
cross-blog syndication is rare, so text_hash-only keeps this simple.
"""

import concurrent.futures as cf
import hashlib
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import httpx
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from windex import db
from windex.ccnews.dedup import canonical_url, text_hash
from windex.config import Settings
from windex.smallweb import USER_AGENT, extract

console = Console()

# Product token used for robots.txt matching (matches '*' and any 'windex' rule).
ROBOT_AGENT = "windex"

CLEAN_SCHEMA = pa.schema(
    [
        ("id", pa.string()),            # stable doc id: smallweb:<sha1[:20] of canonical url>
        ("url", pa.string()),
        ("canonical_url", pa.string()),
        ("title", pa.string()),
        ("published_at", pa.string()),
        ("outlet", pa.string()),        # feed host
        ("lang", pa.string()),
        ("text", pa.string()),
    ]
)


def doc_id(canon: str) -> str:
    """Same recipe as the news doc_id, ``smallweb:`` prefixed."""
    return "smallweb:" + hashlib.sha1(canon.encode()).hexdigest()[:20]


# --- feed parsing (feedparser accessor seam) -------------------------------

def entry_link(entry) -> str | None:
    return ((entry.get("link") or "").strip()) or None


def entry_title(entry) -> str | None:
    return ((entry.get("title") or "").strip()) or None


def entry_published(entry) -> str | None:
    """RFC3339 timestamp from the entry's published/updated date, or None.
    feedparser normalizes dates to a UTC ``struct_time``."""
    for key in ("published_parsed", "updated_parsed"):
        t = entry.get(key)
        if t:
            return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
    return None


def item_body(entry, inline_summary_min: int) -> tuple[str | None, bool]:
    """Return (html, is_inline). Full-text feeds carry the post body in
    ``content`` (both RSS ``content:encoded`` and Atom ``content`` land there);
    a sufficiently long ``<description>``/``summary`` is treated as inline too.
    A short teaser summary means (None, False) → the caller fetches the page."""
    for c in (entry.get("content") or []):
        val = c.get("value")
        if val:
            return val, True
    summary = entry.get("summary") or ""
    if len(summary) >= inline_summary_min:
        return summary, True
    return None, False


def newest_entries(parsed, limit: int) -> list:
    """Up to ``limit`` newest entries. Sorted by date when any entry carries
    one (feedparser struct_times are tuple-comparable); otherwise feed order."""
    entries = list(getattr(parsed, "entries", []) or [])

    def keyf(e):
        return e.get("published_parsed") or e.get("updated_parsed") or ()

    if any(keyf(e) for e in entries):
        entries = sorted(entries, key=keyf, reverse=True)
    return entries[:limit]


# --- politeness: robots cache, per-host interval, bounded page fetch -------

class RobotsCache:
    """Per-host robots.txt, cached with a TTL. A failed robots fetch defaults
    to allow (logged) rather than blocking a whole host on a transient error.
    Thread-safe (the fetch happens outside the lock, so distinct hosts fetch
    concurrently; a rare duplicate fetch for one host is harmless)."""

    def __init__(self, client: httpx.Client, ttl: float, agent: str = ROBOT_AGENT,
                 clock=time.monotonic, logger=None, user_agent: str = USER_AGENT):
        self.client = client
        self.ttl = ttl
        self.agent = agent
        self._clock = clock
        self._log = logger or console.print
        self.user_agent = user_agent
        self._cache: dict[str, tuple[RobotFileParser | None, float]] = {}
        self._lock = threading.Lock()

    def _fetch(self, scheme: str, netloc: str) -> RobotFileParser | None:
        robots_url = urlunsplit((scheme or "https", netloc, "/robots.txt", "", ""))
        rp = RobotFileParser()
        try:
            resp = self.client.get(robots_url, headers={"User-Agent": self.user_agent})
        except Exception as exc:
            self._log(f"[yellow]smallweb: robots fetch failed for {netloc} "
                      f"({exc}); allowing[/yellow]")
            return None
        if resp.status_code >= 400:
            rp.parse([])  # no robots.txt (404 etc.) → allow all
        else:
            rp.parse(resp.text.splitlines())
        return rp

    def get(self, url: str) -> RobotFileParser | None:
        parts = urlsplit(url)
        host = parts.netloc.lower()
        now = self._clock()
        with self._lock:
            hit = self._cache.get(host)
            if hit and now - hit[1] < self.ttl:
                return hit[0]
        rp = self._fetch(parts.scheme, parts.netloc)
        with self._lock:
            self._cache[host] = (rp, self._clock())
        return rp

    def allowed(self, url: str) -> bool:
        rp = self.get(url)
        if rp is None:
            return True  # fetch failed → default allow (already logged)
        try:
            return rp.can_fetch(self.agent, url)
        except Exception:
            return True


class HostRateLimiter:
    """A per-host minimum interval between hits. Thread-safe; the clock/sleep
    are injectable so tests drive it deterministically."""

    def __init__(self, interval: float, clock=time.monotonic, sleep=time.sleep):
        self.interval = interval
        self._clock = clock
        self._sleep = sleep
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, host: str) -> None:
        while True:
            with self._lock:
                now = self._clock()
                last = self._last.get(host)
                if last is None or (now - last) >= self.interval:
                    self._last[host] = now
                    return
                delay = self.interval - (now - last)
            self._sleep(delay)


class PageFetcher:
    """Fetch a page politely: robots → per-host interval → bounded GET
    (content-type allowlist, ~max_bytes cap). Returns the decoded body or None.

    The keyword overrides exist for windex's OTHER fetch-based source (hf/),
    which points at a single host and therefore needs different politeness
    *configuration* while reusing this exact machinery — see hf/fetch.py. The
    defaults are smallweb's and are unchanged:

      * ``allowed_types`` — HTML-only by default. HF serves its doc pages as
        ``text/markdown`` and llms.txt as ``text/plain``; the old hardcoded
        `"html" not in content-type` test dropped those silently, which would
        have discarded 3,175 of that source's 4,014 pages without an error.
      * ``limiter`` — smallweb's plain interval limiter suits ~37.6k hosts; a
        one-host crawl wants one that reads the host's published rate-limit
        headers (hf.fetch.PagesRateLimiter).
      * ``on_response`` — a hook called once the response headers are in, so
        such a limiter can observe the budget it just spent.
      * ``user_agent`` — the UA this fetcher identifies as, for both the page
        and its robots.txt. Every windex source declares its own honest,
        descriptive UA constant; hardcoding smallweb's here would quietly make
        another source's constant dead code that still looks live.
    """

    def __init__(self, client: httpx.Client, settings: Settings, *,
                 robots_ttl: float | None = None, host_interval: float | None = None,
                 max_bytes: int | None = None,
                 allowed_types: tuple[str, ...] = ("html",),
                 limiter: "HostRateLimiter | None" = None,
                 on_response=None, user_agent: str = USER_AGENT):
        self.client = client
        self.user_agent = user_agent
        self.robots = RobotsCache(
            client, settings.smallweb_robots_ttl if robots_ttl is None else robots_ttl,
            user_agent=user_agent,
        )
        self.limiter = limiter or HostRateLimiter(
            settings.smallweb_host_interval if host_interval is None else host_interval
        )
        self.max_bytes = settings.smallweb_max_page_bytes if max_bytes is None else max_bytes
        self.allowed_types = allowed_types
        self.on_response = on_response

    def fetch(self, url: str) -> str | None:
        if not self.robots.allowed(url):
            return None
        self.limiter.wait(urlsplit(url).netloc.lower())
        try:
            with self.client.stream("GET", url, headers={"User-Agent": self.user_agent}) as resp:
                if self.on_response is not None:
                    # Before the status check: a 429/5xx carries the rate-limit
                    # headers too, and that is exactly when they matter most.
                    self.on_response(resp)
                if resp.status_code != 200:
                    return None
                ctype = resp.headers.get("content-type", "").lower()
                if not any(t in ctype for t in self.allowed_types):
                    return None
                clen = resp.headers.get("content-length")
                if clen and clen.isdigit() and int(clen) > self.max_bytes:
                    return None
                buf = bytearray()
                for chunk in resp.iter_bytes(1 << 16):
                    buf.extend(chunk)
                    if len(buf) > self.max_bytes:
                        return None  # oversize body: stop reading, skip
                return buf.decode(resp.encoding or "utf-8", errors="replace")
        except Exception:
            return None


# --- feed watermark --------------------------------------------------------

def active_feeds(conn: psycopg.Connection, limit: int,
                 polled_before=None) -> list[tuple[str, str, str | None, str | None]]:
    """(url, host, etag, last_modified) for the least-recently-polled active
    feeds (never-polled first). The watermark advances as we mark them polled,
    so successive batches walk the whole list.

    polled_before caps a run to feeds not yet polled *this* run (last_polled is
    NULL or older than the cutoff). Without it, marking a feed polled just rotates
    it to the back of the same never-empty queue, so a full 'all active' pass
    (max_feeds=None) would re-poll forever and never terminate."""
    with conn.cursor() as cur:
        cutoff = "" if polled_before is None else "AND (last_polled IS NULL OR last_polled < %s) "
        params = (limit,) if polled_before is None else (polled_before, limit)
        cur.execute(
            "SELECT url, host, etag, last_modified FROM feeds WHERE status = 'active' "
            + cutoff +
            "ORDER BY last_polled ASC NULLS FIRST, url LIMIT %s",
            params,
        )
        return [(r[0], r[1], r[2], r[3]) for r in cur.fetchall()]


def mark_feed_ok(conn: psycopg.Connection, url: str, etag: str | None,
                 last_modified: str | None, status_code: int, items: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE feeds SET last_polled = now(), fail_count = 0, status = 'active', "
            "etag = %s, last_modified = %s, last_status = %s, items_seen = items_seen + %s "
            "WHERE url = %s",
            (etag, last_modified, status_code, items, url),
        )
    conn.commit()


def mark_feed_not_modified(conn: psycopg.Connection, url: str, status_code: int = 304) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE feeds SET last_polled = now(), fail_count = 0, last_status = %s WHERE url = %s",
            (status_code, url),
        )
    conn.commit()


def mark_feed_failure(conn: psycopg.Connection, url: str, max_fail: int,
                      status_code: int | None = None) -> bool:
    """Bump the consecutive fail count; flip to 'dead' at the threshold. Returns
    True if this failure crossed into 'dead'."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE feeds SET last_polled = now(), fail_count = fail_count + 1, "
            "last_status = coalesce(%s, last_status), "
            "status = CASE WHEN fail_count + 1 >= %s THEN 'dead' ELSE status END "
            "WHERE url = %s RETURNING status",
            (status_code, max_fail, url),
        )
        row = cur.fetchone()
    conn.commit()
    return bool(row) and row[0] == "dead"


# --- staging (exact dedup → clean parquet → ledger) ------------------------

def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _existing_ids(cur: psycopg.Cursor, ids: list[str]) -> set[str]:
    if not ids:
        return set()
        # No `source =` predicate: ids are namespaced (hn:, wiki:, …) so an id
        # list can't match another source. Including it makes the planner pick
        # documents_source_published_idx (est. rows=1 — rare sources are absent
        # from the MCV list) and scan every row of the source: 244s vs 63ms.
    cur.execute("SELECT id FROM documents WHERE id = ANY(%s)", (ids,))
    return {r[0] for r in cur.fetchall()}


def _existing_hashes(cur: psycopg.Cursor, hashes: list[str]) -> set[str]:
    if not hashes:
        return set()
    cur.execute(
        "SELECT text_hash FROM documents WHERE source = 'smallweb' AND text_hash = ANY(%s)",
        (hashes,),
    )
    return {r[0] for r in cur.fetchall()}


def _clean_batch(rows: list[dict]) -> pa.RecordBatch:
    return pa.record_batch(
        [
            pa.array([r["id"] for r in rows]),
            pa.array([r["url"] for r in rows]),
            pa.array([r["canon"] for r in rows]),
            pa.array([r["title"] for r in rows]),
            pa.array([r["published"] for r in rows]),
            pa.array([r["outlet"] for r in rows]),
            pa.array([r["lang"] for r in rows]),
            pa.array([r["text"] for r in rows]),
        ],
        schema=CLEAN_SCHEMA,
    )


def stage_batch(conn: psycopg.Connection, settings: Settings, items: list[dict],
                text_ref: str) -> dict:
    """Exact-dedup a batch of extracted posts (in-batch, then against the
    ledger) and stage survivors to one clean parquet + ledger rows. Feeds
    re-serve items, so the ledger check is what keeps re-polls from re-staging.
    The parquet is renamed into place only after a full write; the ledger insert
    commits once, after the rename (text_ref never points at a partial file)."""
    stats = {"items": len(items), "staged": 0, "dup_batch": 0, "dup_ledger": 0}
    prepared: list[dict] = []
    seen_ids: set[str] = set()
    seen_hashes: set[str] = set()
    for it in items:
        canon = canonical_url(it["url"]) if it.get("url") else ""
        if not canon:
            continue
        did = doc_id(canon)
        thash = text_hash(it["text"])
        if did in seen_ids or thash in seen_hashes:
            stats["dup_batch"] += 1
            continue
        seen_ids.add(did)
        seen_hashes.add(thash)
        prepared.append({
            "id": did, "url": it["url"], "canon": canon,
            "title": it.get("title") or "", "published": it.get("date"),
            "outlet": it.get("outlet") or "", "lang": it.get("lang") or "en",
            "thash": thash, "text": it["text"],
        })
    if not prepared:
        return stats

    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")
    writer: pq.ParquetWriter | None = None
    try:
        with conn.cursor() as cur:
            existing_ids = _existing_ids(cur, [p["id"] for p in prepared])
            existing_hashes = _existing_hashes(cur, [p["thash"] for p in prepared])
            survivors = [p for p in prepared
                         if p["id"] not in existing_ids and p["thash"] not in existing_hashes]
            stats["dup_ledger"] = len(prepared) - len(survivors)
            if not survivors:
                return stats
            writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)
            writer.write_batch(_clean_batch(survivors))
            writer.close()
            writer = None
            tmp_path.rename(clean_path)
            cur.executemany(
                """INSERT INTO documents
                     (id, source, url, canonical_url, title, published_at, lang,
                      text_hash, status, text_ref)
                   VALUES (%s, 'smallweb', %s, %s, %s, %s, %s, %s, 'deduped', %s)
                   ON CONFLICT (id) DO NOTHING""",
                # sorted by id: the embed loop UPDATEs these same rows, and
                # locking them in a different order deadlocks (killed two wiki
                # shards 2026-07-16). Every batch writer to `documents` locks
                # in id order.
                sorted(
                    (p["id"], p["url"], p["canon"], p["title"], _parse_ts(p["published"]),
                     p["lang"], p["thash"], text_ref) for p in survivors
                ),
            )
            stats["staged"] = len(survivors)
        conn.commit()
    except Exception:
        if writer is not None:
            writer.close()
        tmp_path.unlink(missing_ok=True)
        conn.rollback()
        raise
    return stats


# --- polling ---------------------------------------------------------------

def poll_feed(feed: tuple[str, str, str | None, str | None], client: httpx.Client,
              fetcher: PageFetcher, settings: Settings) -> dict:
    """Poll one feed. Never raises: any error is reported as outcome='error'
    so a bad feed can't abort a batch. Runs in a worker thread; touches no DB."""
    import feedparser

    url, host, etag, last_modified = feed
    headers = {}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    try:
        resp = client.get(url, headers=headers)
    except Exception as exc:
        return {"url": url, "outcome": "error", "status_code": None, "error": str(exc)}
    if resp.status_code == 304:
        return {"url": url, "outcome": "not_modified", "status_code": 304}
    if resp.status_code >= 400:
        return {"url": url, "outcome": "error", "status_code": resp.status_code}
    try:
        parsed = feedparser.parse(resp.content)
        entries = newest_entries(parsed, settings.smallweb_max_items)
    except Exception as exc:
        return {"url": url, "outcome": "error", "status_code": resp.status_code, "error": str(exc)}

    # network only in this (worker-thread) function: extraction runs on the
    # caller's thread — the quality filters share a spaCy tokenizer that is NOT
    # thread-safe (vocab corruption crashed the first production poll, E064)
    raw_items: list[dict] = []
    for entry in entries:
        link = entry_link(entry)
        if not link:
            continue
        body, inline = item_body(entry, settings.smallweb_inline_summary_min)
        if not inline:
            body = fetcher.fetch(link)
            if body is None:
                continue
        raw_items.append({
            "link": link, "host": host, "body": body, "inline": inline,
            "feed_title": entry_title(entry), "feed_published": entry_published(entry),
        })
    return {"url": url, "outcome": "ok", "status_code": resp.status_code,
            "etag": resp.headers.get("ETag"), "last_modified": resp.headers.get("Last-Modified"),
            "raw_items": raw_items}


def extract_items(raw_items: list[dict], filters) -> list[dict]:
    """Main-thread extraction over fetched bodies (single-threaded on purpose —
    see the thread-safety note in poll_feed)."""
    items = []
    for raw in raw_items:
        extracted = extract.extract_post(
            raw["body"], raw["link"], feed_title=raw["feed_title"],
            feed_published=raw["feed_published"], filters=filters, wrap=raw["inline"],
        )
        if extracted is None:
            continue
        items.append({
            "url": raw["link"], "outlet": raw["host"], "title": extracted["title"],
            "date": extracted["date"], "lang": extracted["lang"], "text": extracted["text"],
        })
    return items


def _poll_feeds(feeds, client, fetcher, settings) -> list[dict]:
    """Poll a batch of feeds, up to the global concurrency cap. Distinct hosts
    run in parallel; same-host page fetches serialize on the per-host interval."""
    workers = max(settings.smallweb_concurrency, 1)
    if workers == 1 or len(feeds) <= 1:
        return [poll_feed(f, client, fetcher, settings) for f in feeds]
    with cf.ThreadPoolExecutor(min(workers, len(feeds))) as pool:
        return list(pool.map(
            lambda f: poll_feed(f, client, fetcher, settings), feeds
        ))


def _apply_feed_result(conn: psycopg.Connection, settings: Settings, res: dict,
                       totals: dict) -> None:
    url = res["url"]
    if res["outcome"] == "not_modified":
        mark_feed_not_modified(conn, url, res.get("status_code", 304))
        totals["not_modified"] += 1
    elif res["outcome"] == "error":
        went_dead = mark_feed_failure(conn, url, settings.smallweb_max_fail,
                                      res.get("status_code"))
        totals["errors"] += 1
        totals["dead"] += int(went_dead)
    else:
        mark_feed_ok(conn, url, res.get("etag"), res.get("last_modified"),
                     res.get("status_code", 200), len(res.get("raw_items", [])))


def poll(conn: psycopg.Connection, settings: Settings, max_feeds: int | None = None,
         filters=None, client: httpx.Client | None = None,
         pause_poll_seconds: float = 10.0) -> dict:
    """Poll active feeds in pause-checked batches, staging new posts. Returns
    aggregate stats. DB writes stay on this thread; only the network fetch +
    extraction fan out to the worker pool."""
    totals = {"feeds": 0, "not_modified": 0, "errors": 0, "dead": 0,
              "items": 0, "staged": 0, "dup_batch": 0, "dup_ledger": 0}
    own = client is None
    client = client or httpx.Client(
        timeout=httpx.Timeout(settings.smallweb_request_timeout),
        follow_redirects=True, headers={"User-Agent": USER_AGENT},
    )
    fetcher = PageFetcher(client, settings)
    if filters is None:
        filters = extract.build_quality_filters(min_chars=settings.smallweb_min_chars)
    run_started = datetime.now(timezone.utc)
    run_id = run_started.strftime("%Y%m%dT%H%M%SZ")
    batch_idx = 0
    try:
        while max_feeds is None or totals["feeds"] < max_feeds:
            # dashboard pause: honor it between batches, never mid-batch.
            while db.get_control(conn, "indexing", "running") == "paused":
                db.set_control(conn, "smallweb_stage", "paused")
                time.sleep(pause_poll_seconds)

            remaining = None if max_feeds is None else max_feeds - totals["feeds"]
            n = settings.smallweb_poll_batch if remaining is None \
                else min(settings.smallweb_poll_batch, remaining)
            # polled_before=run_started makes this one pass over each active feed:
            # feeds marked polled this run drop out, so 'all active' terminates
            # instead of re-polling the same rows forever.
            feeds = active_feeds(conn, n, polled_before=run_started)
            if not feeds:
                break
            db.set_control(conn, "smallweb_stage", f"polling {len(feeds)} feeds")
            results = _poll_feeds(feeds, client, fetcher, settings)

            # not_modified/error carry no new content — mark them now. 'ok' results
            # (which advance etag/last_modified PAST freshly-fetched posts) are held
            # until their posts are durably staged: marking first would let a stage
            # failure lose the posts (the next conditional GET 304s them away).
            items: list[dict] = []
            ok_results: list[dict] = []
            for res in results:
                if res["outcome"] in ("not_modified", "error"):
                    _apply_feed_result(conn, settings, res, totals)
                else:
                    ok_results.append(res)
                    items.extend(extract_items(res.get("raw_items", []), filters))
            totals["feeds"] += len(feeds)
            totals["items"] += len(items)
            if items:
                text_ref = f"smallweb/clean/{run_id}_{batch_idx:04d}.parquet"
                stats = stage_batch(conn, settings, items, text_ref)
                totals["staged"] += stats["staged"]
                totals["dup_batch"] += stats["dup_batch"]
                totals["dup_ledger"] += stats["dup_ledger"]
            # Posts are staged (or there were none): now it is safe to advance each
            # ok feed's conditional-GET watermark.
            for res in ok_results:
                _apply_feed_result(conn, settings, res, totals)
            batch_idx += 1
    finally:
        db.set_control(conn, "smallweb_stage", "idle")
        if own:
            client.close()
    return totals
