"""GH Archive hourly-file scanner. Bootstrap and the daily freshness tail are
the same code: enumerate hour files into gharchive_files, then stream each one
counting WatchEvents per repo (candidate signal — true star counts come from
hydration). Files are deleted after counting unless keep=True."""

import concurrent.futures as cf
import gzip
import orjson
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
import psycopg

HOUR_URL = "https://data.gharchive.org/{name}"


def hour_names(start: date, end: date) -> list[str]:
    """All hourly file names in [start, end), e.g. 2026-07-14-23.json.gz."""
    names = []
    d = start
    while d < end:
        names.extend(f"{d:%Y-%m-%d}-{h}.json.gz" for h in range(24))
        d += timedelta(days=1)
    return names


def sync_hours(
    conn: psycopg.Connection,
    days: int | None = None,
    today: date | None = None,
    start: date | None = None,
    end: date | None = None,
) -> int:
    """Trailing window (days) or explicit [start, end) range.

    NOTE (measured 2026-07): GitHub's 2025-10-07 Events API change collapsed
    WatchEvents in the public feed (~5100/hr → ~39/hr). Star-signal scans must
    target the rich window (pre 2025-10); after that, star discovery comes from
    the Search API sweep (github/discover.py). The tail remains useful for
    PushEvent-based staleness detection.
    """
    today = today or date.today()
    if start is None:
        start = today - timedelta(days=days or 365)
    names = hour_names(start, end or today)
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO gharchive_files (name) VALUES (%s) ON CONFLICT DO NOTHING",
            [(n,) for n in names],
            returning=False,
        )
        inserted = max(cur.rowcount or 0, 0)
    conn.commit()
    return inserted


def pending_hours(conn: psycopg.Connection, limit: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM gharchive_files WHERE status = 'pending' ORDER BY name LIMIT %s",
            (limit,),
        )
        return [r[0] for r in cur.fetchall()]


def download_hour(client: httpx.Client, name: str, dest_dir: Path) -> Path | None:
    """Returns None for hours the archive never published (rare gaps)."""
    dest = dest_dir / name
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    part = dest.with_suffix(".part")
    for attempt in range(3):
        try:
            with client.stream("GET", HOUR_URL.format(name=name)) as resp:
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in resp.iter_bytes(1 << 20):
                        f.write(chunk)
            part.rename(dest)
            return dest
        except httpx.HTTPError:
            part.unlink(missing_ok=True)
            if attempt == 2:
                raise
    return None


def count_watch_events(path: Path) -> dict[int, tuple[str, int]]:
    """repo_id → (full_name, star_event_count) for one hourly file."""
    counts: dict[int, tuple[str, int]] = {}
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            # cheap pre-filter before json parse: WatchEvent is ~2-4% of events
            if '"WatchEvent"' not in line:
                continue
            try:
                ev = orjson.loads(line)  # 1.8x stdlib on archive events (measured 2026-07-19)
            except orjson.JSONDecodeError:
                continue
            if ev.get("type") != "WatchEvent":
                continue
            repo = ev.get("repo") or {}
            rid, name = repo.get("id"), repo.get("name")
            if rid is None or not name:
                continue
            prev = counts.get(rid)
            counts[rid] = (name, (prev[1] + 1) if prev else 1)
    return counts


_UPSERT_SQL = """
    INSERT INTO repos (repo_id, full_name, star_events)
    VALUES (%s, %s, %s)
    ON CONFLICT (repo_id) DO UPDATE SET
        star_events = repos.star_events + EXCLUDED.star_events,
        full_name = EXCLUDED.full_name
    """


def upsert_counts(conn: psycopg.Connection, counts: dict[int, tuple[str, int]]) -> None:
    rows = [(rid, name, n) for rid, (name, n) in counts.items()]
    with conn.cursor() as cur:
        for row in rows:
            # Per-row SAVEPOINT: a full_name collision (rename + recreate) rolls
            # back only THIS row, not the whole hour's accumulated star_events.
            # conn.rollback() aborted the entire transaction, silently discarding
            # every earlier repo's increments — and scan() then marked the hour
            # done, so they were lost for good.
            cur.execute("SAVEPOINT r")
            try:
                cur.execute(_UPSERT_SQL, row)
            except psycopg.errors.UniqueViolation:
                cur.execute("ROLLBACK TO SAVEPOINT r")
                # full_name reused by a different repo_id: the newer event stream
                # wins the name; the incumbent is #stale-suffixed.
                cur.execute(
                    "UPDATE repos SET full_name = full_name || '#stale:' || repo_id "
                    "WHERE full_name = %s AND repo_id <> %s",
                    (row[1], row[0]),
                )
                cur.execute(_UPSERT_SQL, row)
            cur.execute("RELEASE SAVEPOINT r")
    conn.commit()


def scan(
    conn: psycopg.Connection,
    dest_dir: Path,
    max_files: int | None = None,
    keep: bool = False,
    download_concurrency: int = 4,
    batch: int = 24,
) -> dict:
    """Process pending hour files. Returns aggregate stats."""
    from windex import db as wdb

    dest_dir.mkdir(parents=True, exist_ok=True)
    stats = {"files": 0, "missing": 0, "watch_events": 0, "repos_touched": 0}
    with wdb.stage(conn, "gh_stage", "scanning event hours"), httpx.Client(
        timeout=httpx.Timeout(30, read=120), follow_redirects=True
    ) as client:
        while max_files is None or stats["files"] + stats["missing"] < max_files:
            limit = batch if max_files is None else min(batch, max_files - stats["files"] - stats["missing"])
            names = pending_hours(conn, limit)
            if not names:
                break
            with cf.ThreadPoolExecutor(download_concurrency) as pool:
                local = list(pool.map(lambda n: download_hour(client, n, dest_dir), names))
            for name, path in zip(names, local):
                if path is None:
                    stats["missing"] += 1
                    _mark_hour(conn, name, "missing")
                    continue
                nbytes = path.stat().st_size
                counts = count_watch_events(path)
                upsert_counts(conn, counts)
                _mark_hour(conn, name, "done", nbytes)
                stats["files"] += 1
                stats["watch_events"] += sum(n for _, n in counts.values())
                stats["repos_touched"] += len(counts)
                if not keep:
                    path.unlink(missing_ok=True)
    return stats


def _mark_hour(conn: psycopg.Connection, name: str, status: str, nbytes: int | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE gharchive_files SET status = %s, bytes = coalesce(%s, bytes),
               processed_at = now() WHERE name = %s""",
            (status, nbytes, name),
        )
    conn.commit()
