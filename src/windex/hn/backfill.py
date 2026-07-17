"""Fast-path Hacker News backfill from the open-index/hacker-news parquet
mirror (Hugging Face, ODC-By 1.0, no auth) — download-and-filter, zero Algolia
load.

One monthly file per (year, month):
``.../data/YYYY/YYYY-MM.parquet`` (~240 files, 12.2GB total for the full 48.9M-
item mirror). Live-verified 2026-07-16 against data/2006/2006-10.parquet:
``type`` is int8 with 1=story and 2=comment, ``deleted``/``dead`` are uint8
0/1 flags, ``time`` is timestamp[ms, UTC], string columns are non-null (""
when absent — a self post has url ""), ``text`` carries the same HTML-entity
fragments as Algolia's story_text, and a month can legitimately hold 0 rows
(2007-01 exists and is empty). The filter also tolerates a string ``type``
column ('story') and boolean flags in case the single-maintainer mirror's
schema shifts.

This module drains the SAME ``hn_windows`` month watermarks that
harvest.plan_backfill() plans and feeds the same stage_stories() flow, so the
two engines are interchangeable per window: run `windex hn backfill` for the
bulk, and the Algolia harvester for anything left over (and the daily tail).
Non-month-aligned windows (the incremental trailing window) are left for
Algolia.
"""

import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from rich.console import Console

from windex import db
from windex.ccnews.dedup import text_hash
from windex.config import Settings
from windex.hn import USER_AGENT
from windex.hn.harvest import (
    clean_text,
    clean_title,
    doc_id,
    item_url,
    mark_window,
    month_epochs,
    pending_windows,
    stage_stories,
)

console = Console()

_COLUMNS = ("id", "type", "dead", "deleted", "by", "time", "text", "url", "score",
            "title", "descendants")


def month_url(base: str, year: int, month: int) -> str:
    return f"{base.rstrip('/')}/{year}/{year}-{month:02d}.parquet"


def month_of_window(from_ts: int, until_ts: int) -> tuple[int, int] | None:
    """(year, month) when the window spans exactly one calendar month; None
    otherwise (e.g. the incremental trailing window — Algolia's job)."""
    start = datetime.fromtimestamp(from_ts, tz=timezone.utc)
    if (start.day, start.hour, start.minute, start.second) != (1, 0, 0, 0):
        return None
    if month_epochs(start.year, start.month) != (from_ts, until_ts):
        return None
    return start.year, start.month


def _flag_is_zero(col: pa.ChunkedArray) -> pa.ChunkedArray:
    """True where a dead/deleted flag is unset — handles uint8 0/1 (live schema)
    and bool alike."""
    return pc.equal(pc.cast(col, pa.int64()), 0)


def filter_stories(table: pa.Table) -> pa.Table:
    """Live stories only: type==1 (int8, verified live; 'story' tolerated for a
    string column) AND NOT dead AND NOT deleted."""
    ty = table["type"]
    if pa.types.is_integer(ty.type):
        keep = pc.equal(ty, 1)
    else:
        keep = pc.equal(ty, "story")
    for col in ("dead", "deleted"):
        if col in table.column_names:
            keep = pc.and_(keep, _flag_is_zero(table[col]))
    return table.filter(keep)


def stories_from_table(table: pa.Table, from_ts: int, until_ts: int) -> list[dict]:
    """Normalize mirror rows to the same shape harvest.story_from_hit() emits,
    so text_hash and staging are identical across both ingest paths."""
    table = filter_stories(table.select([c for c in _COLUMNS if c in table.column_names]))
    out = []
    for row in table.to_pylist():
        t = row.get("time")
        epoch = int(t.timestamp()) if isinstance(t, datetime) else int(t or 0)
        if not (from_ts <= epoch < until_ts):
            continue  # defensive: a mirror file should already be month-scoped
        title = clean_title(row.get("title"))
        text = clean_text(row.get("text"))
        out.append({
            "id": doc_id(row["id"]),
            "url": item_url(row["id"]),
            "target_url": row.get("url") or None,   # "" (self post) -> None
            "title": title,
            "story_text": text,
            "author": row.get("by") or "",
            "points": int(row.get("score") or 0),
            "num_comments": int(row.get("descendants") or 0),
            "created_at": datetime.fromtimestamp(epoch, tz=timezone.utc)
                          .isoformat().replace("+00:00", "Z"),
            "thash": text_hash(title + "\n\n" + text),
        })
    return out


def fetch_month(client: httpx.Client, base_url: str, year: int, month: int,
                dest_dir: Path) -> Path:
    """Stream one monthly parquet to disk (files run KBs to ~200MB) — never
    buffered through the response object."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / f"{year}-{month:02d}.parquet"
    tmp = path.with_suffix(".parquet.part")
    with client.stream("GET", month_url(base_url, year, month)) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in resp.iter_bytes(1 << 20):
                f.write(chunk)
    tmp.rename(path)
    return path


def _mirror_client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(30, read=600), follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )


def backfill(
    conn: psycopg.Connection,
    settings: Settings,
    max_windows: int | None = None,
    max_consecutive_failures: int = 3,
    client: httpx.Client | None = None,
    keep: bool = False,
    pause_poll_seconds: float = 10.0,
) -> dict:
    """Drain pending month-aligned hn_windows from the mirror, oldest-first.
    Same idempotence story as the Algolia harvester (it IS the same staging +
    ledger flow); the dashboard pause flag is honored between months."""
    totals = {"windows": 0, "hits": 0, "staged": 0, "skipped": 0, "refreshed": 0}
    consecutive_failures = 0
    own = client is None
    client = client or _mirror_client()
    try:
        done: set[tuple[int, int]] = set()
        while max_windows is None or totals["windows"] < max_windows:
            window = next(
                ((f, u) for f, u in pending_windows(conn, 1000)
                 if (f, u) not in done and month_of_window(f, u)),
                None,
            )
            if window is None:
                break
            frm, until = window
            done.add(window)
            year, month = month_of_window(frm, until)

            while db.get_control(conn, "indexing", "running") == "paused":
                db.set_control(conn, "hn_stage", "paused")
                time.sleep(pause_poll_seconds)

            mark_window(conn, frm, until, "processing")
            db.set_control(conn, "hn_stage", f"backfill {year}-{month:02d}")
            console.print(f"[bold]month[/bold] {year}-{month:02d}")
            try:
                path = fetch_month(client, settings.hn_mirror_url, year, month,
                                   settings.hn_downloads_dir)
                try:
                    stories = stories_from_table(pq.read_table(path), frm, until)
                finally:
                    if not keep:
                        path.unlink(missing_ok=True)
                stats = stage_stories(conn, settings, frm, until, stories)
                stats["queries"] = 0  # zero Algolia load — that's the point
                mark_window(conn, frm, until, "done", stats)
                for k in ("hits", "staged", "skipped", "refreshed"):
                    totals[k] += stats[k]
                totals["windows"] += 1
                console.print(f"  {stats}")
                consecutive_failures = 0
            except Exception as exc:
                conn.rollback()
                mark_window(conn, frm, until, "failed")
                consecutive_failures += 1
                console.print(f"[red]month {year}-{month:02d} failed[/red] ({exc}); continuing")
                if consecutive_failures >= max_consecutive_failures:
                    raise
    finally:
        db.set_control(conn, "hn_stage", "idle")
        if own:
            client.close()
    return totals
