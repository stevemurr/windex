"""Harvest Hacker News stories from the Algolia HN Search API, stage clean
parquet, and insert documents-ledger rows.

Source: https://hn.algolia.com/api/v1/search_by_date (free, no auth,
~10k req/hr/IP). Verified live 2026-07-16: ``tags=story`` excludes comments and
dead/deleted items; ``numericFilters=created_at_i>=X,created_at_i<Y`` gives
clean epoch windows; any query is hard-capped at 1000 hits (page*hitsPerPage —
past it the API returns 200 with empty hits and a message, never an error), and
busy days exceed it (2026-07-15 = 1,172 stories). A window is therefore fetched
by recursively halving over-cap sub-ranges until each fits, but staged and
watermarked as one unit (``hn_windows``: months for the backfill, a rolling
trailing-days window for the tail).

One doc per STORY, never comments: id ``hn:<item_id>``, canonical url the HN
discussion page (the stable link target), the external link as payload field
``target_url``. Text = title + story_text (Ask/Show/self posts only; HTML
entities/tags stripped). Change detection reuses the documents.text_hash ledger:
an unchanged story is never re-staged or re-embedded, but its points /
num_comments — which drift constantly while the text stays identical — are
refreshed in the Qdrant payload in place (set_payload, see hn/embed_index.py).
That is why the incremental window trails a couple of days and is re-armed on
every run.

Everything Algolia-specific (params, hit shape, the 1000-cap recursion) lives
here; the open-index parquet fast path (hn/backfill.py) feeds the same
stage_stories() flow.
"""

import html
import re
import time
from datetime import datetime, timedelta, timezone

import httpx
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from windex import db
from windex.ccnews.dedup import text_hash
from windex.config import Settings
from windex.hn import USER_AGENT

console = Console()

MAX_HITS = 1000  # Algolia hard cap per query (page * hitsPerPage)
HN_EPOCH = (2006, 10)  # first item (id 1) is 2006-10-09

CLEAN_SCHEMA = pa.schema(
    [
        ("id", pa.string()),           # stable doc id: hn:<item_id>
        ("url", pa.string()),          # canonical: the HN discussion page
        ("target_url", pa.string()),   # external link target (null on self posts)
        ("title", pa.string()),
        ("story_text", pa.string()),   # Ask/Show/self text, HTML stripped ("" when absent)
        ("author", pa.string()),
        ("points", pa.int64()),
        ("num_comments", pa.int64()),
        ("created_at", pa.string()),   # RFC3339 UTC
    ]
)


def doc_id(item_id: int | str) -> str:
    return f"hn:{item_id}"


def item_url(item_id: int | str) -> str:
    return f"https://news.ycombinator.com/item?id={item_id}"


_P_RE = re.compile(r"(?i)<p[^>]*>")
_TAG_RE = re.compile(r"<[^>]+>")


def clean_title(raw: str | None) -> str:
    """Titles skip the HTML path but still land in a PG text column, which
    cannot hold NUL (0x00). str.split() doesn't drop it (NUL isn't whitespace),
    so a single real story in 2023-07 failed that whole month's window
    permanently. Both ingest paths must call this — text_hash consistency
    between them depends on identical normalization."""
    return " ".join((raw or "").replace("\x00", "").split())


def clean_text(fragment: str | None) -> str:
    """HN item text is a small HTML fragment: <p> paragraph breaks, <i>/<a>,
    entity-encoded quotes (&#x27;, &#34; — verified live on both Algolia and the
    open-index mirror). Lossy fragment -> plain text; same normalization on both
    ingest paths so text_hash stays consistent between them."""
    if not fragment:
        return ""
    text = _P_RE.sub("\n\n", fragment.replace("\x00", ""))
    text = _TAG_RE.sub("", text)
    text = html.unescape(text)
    lines = [" ".join(line.split()) for line in text.split("\n")]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _rfc3339(epoch: int) -> str:
    return (
        datetime.fromtimestamp(int(epoch), tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def story_from_hit(hit: dict) -> dict:
    """Normalize one Algolia hit into the staging row shape. Self posts carry
    no external url (verified live: field is null) — target_url stays None and
    the canonical HN discussion page is the doc url either way."""
    item_id = str(hit["objectID"])
    title = clean_title(hit.get("title"))
    text = clean_text(hit.get("story_text"))
    return {
        "id": doc_id(item_id),
        "url": item_url(item_id),
        "target_url": hit.get("url") or None,
        "title": title,
        "story_text": text,
        "author": hit.get("author") or "",
        "points": int(hit.get("points") or 0),
        "num_comments": int(hit.get("num_comments") or 0),
        "created_at": _rfc3339(hit["created_at_i"]),
        "thash": text_hash(title + "\n\n" + text),
    }


# --- window watermark ------------------------------------------------------

def month_epochs(year: int, month: int) -> tuple[int, int]:
    """[first-of-month, first-of-next-month) as unix UTC epochs."""
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    ny, nm = (year + 1, 1) if month == 12 else (year, month + 1)
    end = datetime(ny, nm, 1, tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


def month_range(from_year: int, from_month: int, to_year: int, to_month: int):
    y, m = from_year, from_month
    while (y, m) <= (to_year, to_month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def plan_backfill(
    conn: psycopg.Connection,
    from_year: int = HN_EPOCH[0],
    from_month: int | None = None,
    to_year: int | None = None,
    to_month: int | None = None,
    now: datetime | None = None,
) -> int:
    """Insert one pending per-month window for each month in range (default:
    HN's first month 2006-10 through the current month). Idempotent: already-
    recorded months are left as-is. The same windows serve both engines —
    hn/backfill.py drains them from the parquet mirror, harvest() from Algolia."""
    now = now or datetime.now(timezone.utc)
    from_month = from_month or (HN_EPOCH[1] if from_year == HN_EPOCH[0] else 1)
    to_year = to_year or now.year
    to_month = to_month or (now.month if to_year == now.year else 12)
    rows = [month_epochs(y, m) for y, m in month_range(from_year, from_month, to_year, to_month)]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO hn_windows (from_ts, until_ts) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING",
            rows,
            returning=False,
        )
        inserted = max(cur.rowcount or 0, 0)
    conn.commit()
    return inserted


def plan_incremental(conn: psycopg.Connection, days: int, now: datetime | None = None) -> tuple[int, int]:
    """Arm a rolling trailing window [today-days 00:00 UTC, tomorrow 00:00 UTC).
    Day-aligned so every run within a UTC day hits the same row; a completed or
    failed window of the same span is re-armed back to pending so the re-pull
    picks up new stories AND refreshes points on unchanged ones. An in-flight
    window is left untouched. Returns (from_ts, until_ts)."""
    now = now or datetime.now(timezone.utc)
    day0 = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    frm = int((day0 - timedelta(days=days)).timestamp())
    until = int((day0 + timedelta(days=1)).timestamp())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO hn_windows (from_ts, until_ts) VALUES (%s, %s) "
            "ON CONFLICT (from_ts, until_ts) DO UPDATE SET "
            "status = 'pending', processed_at = NULL "
            "WHERE hn_windows.status IN ('done', 'failed')",
            (frm, until),
        )
    conn.commit()
    return frm, until


def pending_windows(conn: psycopg.Connection, limit: int) -> list[tuple[int, int]]:
    """Oldest-first (from_ts, until_ts) pairs still pending."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT from_ts, until_ts FROM hn_windows WHERE status = 'pending' "
            "ORDER BY from_ts, until_ts LIMIT %s",
            (limit,),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def mark_window(
    conn: psycopg.Connection,
    frm: int,
    until: int,
    status: str,
    stats: dict | None = None,
) -> None:
    stats = stats or {}
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE hn_windows SET status = %s,
               queries = coalesce(%s, queries), hits = coalesce(%s, hits),
               staged = coalesce(%s, staged), refreshed = coalesce(%s, refreshed),
               processed_at = CASE WHEN %s IN ('done', 'failed') THEN now() ELSE processed_at END
               WHERE from_ts = %s AND until_ts = %s""",
            (status, stats.get("queries"), stats.get("hits"),
             stats.get("staged"), stats.get("refreshed"), status, frm, until),
        )
    conn.commit()


def window_label(epoch: int) -> str:
    """Filename-safe UTC label: bare date at midnight, full timestamp otherwise."""
    dt = datetime.fromtimestamp(int(epoch), tz=timezone.utc)
    if (dt.hour, dt.minute, dt.second) == (0, 0, 0):
        return dt.strftime("%Y%m%d")
    return dt.strftime("%Y%m%dT%H%M%SZ")


# --- fetching ----------------------------------------------------------------

def fetch_window_stories(
    client: httpx.Client,
    url: str,
    from_ts: int,
    until_ts: int,
    on_request=None,
    max_hits: int = MAX_HITS,
) -> tuple[list[dict], int]:
    """All story hits in [from_ts, until_ts), recursively halving any sub-range
    whose nbHits exceeds the per-query hit cap (with hitsPerPage=cap a
    within-cap range fits entirely in page 0 — past the cap Algolia still
    returns 200 with truncated hits, so nbHits is the split signal). on_request
    runs before every HTTP call — the pacing + pause hook. Returns
    (hits, queries_issued)."""
    params = {
        "tags": "story",
        "numericFilters": f"created_at_i>={from_ts},created_at_i<{until_ts}",
        "hitsPerPage": max_hits,
    }
    # rate-limit aware: pacing keeps us ~9k req/hr (under Algolia's ~10k/hr),
    # but a 429 (shared IP, upstream change) backs off per Retry-After instead
    # of failing the window
    for attempt in range(5):
        if on_request:
            on_request()
        resp = client.get(url, params=params)
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = int(resp.headers.get("Retry-After", 0)) or min(2**attempt * 5, 120)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        resp.raise_for_status()  # exhausted retries: surface the last error
    body = resp.json()
    nb_hits = int(body.get("nbHits") or 0)
    if nb_hits > max_hits and until_ts - from_ts > 1:
        mid = (from_ts + until_ts) // 2
        left, ql = fetch_window_stories(client, url, from_ts, mid, on_request, max_hits)
        right, qr = fetch_window_stories(client, url, mid, until_ts, on_request, max_hits)
        return left + right, 1 + ql + qr
    return list(body.get("hits") or []), 1


def _algolia_client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(30, read=60), follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )


# --- staging (shared with hn/backfill.py) ------------------------------------

def _existing(cur: psycopg.Cursor, ids: list[str]) -> dict[str, tuple[str, str]]:
    """id -> (text_hash, status) for existing hn ledger rows."""
    if not ids:
        return {}
        # No `source =` predicate: ids are namespaced (hn:, wiki:, …) so an id
        # list can't match another source. Including it makes the planner pick
        # documents_source_published_idx (est. rows=1 — rare sources are absent
        # from the MCV list) and scan every row of the source: 244s vs 63ms.
    cur.execute(
        "SELECT id, text_hash, status FROM documents WHERE id = ANY(%s)",
        (ids,),
    )
    return {r[0]: (r[1], r[2]) for r in cur.fetchall()}


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _story_batch(rows: list[dict]) -> pa.RecordBatch:
    return pa.record_batch(
        [
            pa.array([r["id"] for r in rows]),
            pa.array([r["url"] for r in rows]),
            pa.array([r["target_url"] for r in rows]),
            pa.array([r["title"] for r in rows]),
            pa.array([r["story_text"] for r in rows]),
            pa.array([r["author"] for r in rows]),
            pa.array([r["points"] for r in rows], pa.int64()),
            pa.array([r["num_comments"] for r in rows], pa.int64()),
            pa.array([r["created_at"] for r in rows]),
        ],
        schema=CLEAN_SCHEMA,
    )


def stage_stories(
    conn: psycopg.Connection,
    settings: Settings,
    from_ts: int,
    until_ts: int,
    stories: list[dict],
) -> dict:
    """Stage one window's stories. Full-replace semantics per window (like the
    docs source): the WHOLE story set is rewritten to the window's clean parquet
    — the same trailing window is re-pulled run after run, so unchanged stories
    must stay readable at their text_ref (and their staged points stay fresh for
    a future reindex) — while the ledger upsert's text_hash guard keeps
    re-embedding to the changed-text delta. Unchanged stories that are already
    embedded get points/num_comments refreshed in the Qdrant payload IN PLACE —
    a score drift updates the payload without touching the vector. The parquet
    is written to a temp path and renamed only after the full set is written."""
    # dedupe by id (split sub-ranges partition cleanly, but be safe), stable order
    by_id: dict[str, dict] = {}
    for s in sorted(stories, key=lambda s: (s["created_at"], s["id"])):
        by_id[s["id"]] = s
    stories = list(by_id.values())

    text_ref = f"hn/clean/{window_label(from_ts)}_{window_label(until_ts)}.parquet"
    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")

    stats = {"hits": len(stories), "staged": 0, "skipped": 0, "refreshed": 0}
    writer: pq.ParquetWriter | None = None
    refresh_rows: list[dict] = []
    try:
        with conn.cursor() as cur:
            existing = _existing(cur, [s["id"] for s in stories])
            delta = [s for s in stories if existing.get(s["id"], (None, None))[0] != s["thash"]]
            for s in stories:
                prior = existing.get(s["id"])
                if prior and prior[0] == s["thash"] and prior[1] == "embedded":
                    refresh_rows.append(s)
            stats["skipped"] = len(stories) - len(delta)
            if stories:
                writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)
                writer.write_batch(_story_batch(stories))
                writer.close()
                writer = None
                tmp_path.rename(clean_path)
                stats["staged"] = len(delta)

            # Change-aware ledger upsert: unchanged stories never reach here
            # (pre-filtered); the WHERE guards a race re-embedding an identical row.
            cur.executemany(
                """
                INSERT INTO documents
                    (id, source, url, title, published_at, text_hash, status, text_ref)
                VALUES (%s, 'hn', %s, %s, %s, %s, 'deduped', %s)
                ON CONFLICT (id) DO UPDATE SET
                    url = EXCLUDED.url, title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at, text_hash = EXCLUDED.text_hash,
                    text_ref = EXCLUDED.text_ref, status = 'deduped',
                    embedded_model = NULL, indexed_at = NULL
                WHERE documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                """,
                # sorted by id: the embed loop UPDATEs these same rows, and
                # locking them in a different order deadlocks (killed two wiki
                # shards 2026-07-16). Every batch writer to `documents` locks
                # in id order.
                sorted(
                    (s["id"], s["url"], s["title"], _parse_date(s["created_at"]),
                     s["thash"], text_ref)
                    for s in delta
                ),
            )
        conn.commit()
    except Exception:
        if writer is not None:
            writer.close()
        tmp_path.unlink(missing_ok=True)
        conn.rollback()
        raise

    # payload-only refresh is best-effort (mirrors the tombstone path): a down
    # index leaves points stale until the next trailing re-pull, nothing more.
    try:
        from windex.hn.embed_index import refresh_payloads

        stats["refreshed"] = refresh_payloads(settings, refresh_rows)
    except Exception as exc:
        console.print(f"[yellow]hn: payload refresh skipped ({exc})[/yellow]")
    return stats


# --- harvest -----------------------------------------------------------------

def harvest_window(
    conn: psycopg.Connection,
    settings: Settings,
    from_ts: int,
    until_ts: int,
    client: httpx.Client,
    request_interval: float | None = None,
    pause_poll_seconds: float = 10.0,
) -> dict:
    """Fetch one window from Algolia (splitting as needed) and stage it. The
    dashboard pause flag is honored between requests, never mid-request."""
    request_interval = settings.hn_request_interval if request_interval is None else request_interval
    label = f"{window_label(from_ts)}..{window_label(until_ts)}"
    first = True

    def on_request():
        nonlocal first
        while db.get_control(conn, "indexing", "running") == "paused":
            db.set_control(conn, "hn_stage", "paused")
            time.sleep(pause_poll_seconds)
        if not first and request_interval:
            time.sleep(request_interval)  # polite pacing, well under 10k req/hr
        first = False
        db.set_control(conn, "hn_stage", label)

    hits, queries = fetch_window_stories(
        client, settings.hn_algolia_url, from_ts, until_ts, on_request=on_request
    )
    stats = stage_stories(conn, settings, from_ts, until_ts,
                          [story_from_hit(h) for h in hits])
    stats["queries"] = queries
    return stats


def harvest(
    conn: psycopg.Connection,
    settings: Settings,
    max_windows: int | None = None,
    max_consecutive_failures: int = 3,
    client: httpx.Client | None = None,
    request_interval: float | None = None,
    pause_poll_seconds: float = 10.0,
) -> dict:
    """Process pending windows oldest-first via Algolia. Returns aggregate
    stats. A single failed window is marked failed and skipped so a long
    backfill survives it; repeated back-to-back failures still abort."""
    totals = {"windows": 0, "queries": 0, "hits": 0, "staged": 0, "skipped": 0, "refreshed": 0}
    consecutive_failures = 0
    own = client is None
    client = client or _algolia_client()
    try:
        while max_windows is None or totals["windows"] < max_windows:
            pending = pending_windows(conn, 1)
            if not pending:
                break
            frm, until = pending[0]
            mark_window(conn, frm, until, "processing")
            console.print(f"[bold]window[/bold] {window_label(frm)}..{window_label(until)}")
            try:
                stats = harvest_window(
                    conn, settings, frm, until, client,
                    request_interval=request_interval, pause_poll_seconds=pause_poll_seconds,
                )
                mark_window(conn, frm, until, "done", stats)
                for k in ("queries", "hits", "staged", "skipped", "refreshed"):
                    totals[k] += stats[k]
                totals["windows"] += 1
                console.print(f"  {stats}")
                consecutive_failures = 0
            except Exception as exc:
                conn.rollback()
                mark_window(conn, frm, until, "failed")
                consecutive_failures += 1
                console.print(f"[red]window {window_label(frm)}..{window_label(until)} "
                              f"failed[/red] ({exc}); continuing")
                if consecutive_failures >= max_consecutive_failures:
                    raise
    finally:
        db.set_control(conn, "hn_stage", "idle")
        if own:
            client.close()
    return totals
