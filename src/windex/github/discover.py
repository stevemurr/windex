"""Date-sharded GitHub Search API sweep: enumerate repos with stars >= T by
creation-date shard, splitting any shard that approaches the API's 1000-result
window. This is the durable star-discovery path after GitHub's 2025-10-07
Events API change collapsed WatchEvents in the public feed (measured ~5100/hr
→ ~39/hr), and the only source for repos created after that date. Search
results carry full repo objects, so candidates arrive pre-filled with stars and
metadata; READMEs still come from hydration."""

import logging
import time
from collections import deque
from datetime import date, timedelta
from typing import TYPE_CHECKING

import httpx
import psycopg

if TYPE_CHECKING:
    from windex import db as wdb

log = logging.getLogger("windex.github.discover")

SEARCH = "https://api.github.com/search/repositories"
PAGE = 100
CAP = 1000  # search API hard result window per query
RETRY_BUDGET = 30 * 60  # cumulative seconds of retry waiting before giving up
# A completed leaf shard is skipped on re-run within this window: long enough
# to resume a crashed sweep, short enough that a later periodic sweep re-checks
# the window (repos created then can cross the star threshold afterwards).
RESUME_DAYS = 7


def _get(client: httpx.Client, token: str, params: dict, budget: float = RETRY_BUDGET) -> dict:
    """GET with rate-limit-aware retries. Primary exhaustion (remaining=0)
    self-heals at x-ratelimit-reset (≤60s). A *secondary* abuse limit survives
    the reset boundary and signals via retry-after — it needs long escalating
    waits, so retries are bounded by cumulative wait time, not attempt count
    (a fixed attempt count burned itself out in seconds on 2026-07-16)."""
    waited = 0.0
    sec_hits = 0
    attempt = 0
    while True:
        try:
            resp = client.get(
                SEARCH,
                params=params,
                headers={
                    "Authorization": f"bearer {token}",
                    "Accept": "application/vnd.github+json",
                },
            )
        except httpx.HTTPError as exc:
            # Connection-level failure (dropped/half-closed socket, DNS blip, read
            # timeout): retry with backoff instead of crashing the whole sweep —
            # hydrate._post guards this same class; _get did not. Budget-bounded
            # like the 5xx branch so a persistent outage still gives up.
            wait = min(2**attempt * 5, 300)
            log.warning("github search transport error (attempt %d): %r -> waiting %.0fs",
                        attempt, exc, wait)
            if waited + wait > budget:
                raise RuntimeError(
                    f"search request failed after {waited:.0f}s of retry waiting "
                    f"(transport error: {exc!r})"
                ) from exc
            time.sleep(wait)
            waited += wait
            attempt += 1
            continue
        if resp.status_code in (403, 429):
            retry_after = int(resp.headers.get("retry-after", 0) or 0)
            remaining = resp.headers.get("x-ratelimit-remaining")
            reset = int(resp.headers.get("x-ratelimit-reset", 0) or 0)
            if remaining == "0":
                # primary bucket empty: refills at reset, no escalation needed
                wait = max(reset - time.time(), 0) + 1
            else:
                # secondary/abuse limit: honor retry-after, floor 60s, double
                # on each consecutive hit — short waits don't clear it
                wait = min(max(retry_after, 60) * (2**sec_hits), 900)
                sec_hits += 1
            log.warning(
                "github search 403/429 (attempt %d): remaining=%s reset=%s "
                "retry-after=%s -> waiting %.0fs; body=%r",
                attempt, remaining, reset, retry_after, wait, resp.text[:200],
            )
        elif resp.status_code >= 500:
            wait = min(2**attempt * 5, 300)
            log.warning(
                "github search %d (attempt %d): waiting %.0fs",
                resp.status_code, attempt, wait,
            )
        else:
            resp.raise_for_status()
            return resp.json()
        if waited + wait > budget:
            log.error(
                "github search retry budget exhausted (%.0fs waited, last status %d)",
                waited, resp.status_code,
            )
            raise RuntimeError(
                f"search request failed after {waited:.0f}s of retry waiting "
                f"(last status {resp.status_code})"
            )
        time.sleep(wait)
        waited += wait
        attempt += 1


def _upsert(conn: psycopg.Connection, items: list[dict]) -> int:
    new = 0
    with conn.cursor() as cur:
        for it in items:
            cur.execute(
                """
                INSERT INTO repos (repo_id, full_name, stars, description,
                                   primary_language, pushed_at, status, discovered_at)
                VALUES (%s, %s, %s, %s, %s, %s, 'candidate', now())
                ON CONFLICT (repo_id) DO UPDATE SET
                    stars = EXCLUDED.stars,
                    full_name = EXCLUDED.full_name,
                    description = EXCLUDED.description,
                    primary_language = EXCLUDED.primary_language,
                    pushed_at = EXCLUDED.pushed_at
                RETURNING (xmax = 0)
                """,
                (
                    it["id"],
                    it["full_name"],
                    it["stargazers_count"],
                    it.get("description"),
                    it.get("language"),
                    it.get("pushed_at"),
                ),
            )
            if cur.fetchone()[0]:
                new += 1
    conn.commit()
    return new


def _shard_done(conn: psycopg.Connection, a: date, b: date, threshold: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT 1 FROM gh_shards
               WHERE from_date = %s AND to_date = %s AND star_threshold = %s
                 AND processed_at > now() - make_interval(days => %s)""",
            (a, b, threshold, RESUME_DAYS),
        )
        return cur.fetchone() is not None


def _mark_shard(conn: psycopg.Connection, a: date, b: date, threshold: int, repos: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO gh_shards (from_date, to_date, star_threshold, repos)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (from_date, to_date, star_threshold)
               DO UPDATE SET repos = EXCLUDED.repos, processed_at = now()""",
            (a, b, threshold, repos),
        )
    conn.commit()


def _clear_shards(conn: psycopg.Connection, threshold: int, a: date, b: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """DELETE FROM gh_shards
               WHERE star_threshold = %s AND from_date >= %s AND to_date <= %s""",
            (threshold, a, b),
        )
    conn.commit()


def sweep(
    conn: "psycopg.Connection | wdb.Reconnecting",
    tokens: list[str],
    star_threshold: int,
    created_from: date,
    created_to: date | None = None,
    fresh: bool = False,
) -> dict:
    """Sweep the GitHub Search API for repos ≥ star_threshold.

    `conn` may be a plain connection or a db.Reconnecting. With the latter (the
    production path — see cli.gh_discover), every DB op runs through run() so a
    transient postgres disconnect (a host↔container port-forward blip) is retried
    on a fresh connection instead of crashing the whole sweep (2026-07-17
    incident). The ops are idempotent — a read and two ON CONFLICT upserts — and
    the sweep is resumable regardless via the gh_shards leaf-shard ledger, so
    re-running any op is safe."""
    if not tokens:
        raise ValueError("no GitHub tokens configured (WINDEX_GITHUB_TOKENS)")
    created_to = created_to or date.today()
    from windex import db as wdb

    # run(fn) executes fn(connection), transparently reconnecting+retrying when
    # conn is a Reconnecting; a plain connection just runs it once (tests).
    run = conn.run if isinstance(conn, wdb.Reconnecting) else (lambda fn: fn(conn))

    pace = 2.1 / max(len(tokens), 1)  # 30 search req/min/token
    if fresh:
        run(lambda c: _clear_shards(c, star_threshold, created_from, created_to))

    stats = {"shards": 0, "shards_skipped": 0, "repos_seen": 0, "repos_new": 0,
             "capped_shards": 0}
    shards: deque[tuple[date, date]] = deque([(created_from, created_to)])
    tok_i = 0
    with wdb.stage(conn, "gh_stage", "discovery sweep (search API)"), httpx.Client(
        timeout=30
    ) as client:
        while shards:
            a, b = shards.popleft()
            if run(lambda c: _shard_done(c, a, b, star_threshold)):
                stats["shards_skipped"] += 1
                continue
            token = tokens[tok_i % len(tokens)]
            tok_i += 1
            q = f"stars:>={star_threshold} created:{a}..{b}"
            first = _get(client, token, {"q": q, "per_page": PAGE, "page": 1})
            # Pace EVERY request, split decisions included: the BFS descent of
            # a cold sweep fires ~100+ page-1 queries, and unpaced they burst
            # past 30/min and trip GitHub's secondary limit (the 2026-07-16
            # crash). Leaf pagination below paces the same way.
            time.sleep(pace)
            total = first.get("total_count", 0)
            if total > CAP and (b - a).days >= 1:
                mid = a + (b - a) / 2
                shards.append((a, mid))
                shards.append((mid + timedelta(days=1), b))
                continue
            if total > CAP:
                stats["capped_shards"] += 1  # single day > 1000: overflow dropped
            stats["shards"] += 1
            items = list(first.get("items", []))
            pages = min((min(total, CAP) + PAGE - 1) // PAGE, CAP // PAGE)
            for page in range(2, pages + 1):
                token = tokens[tok_i % len(tokens)]
                tok_i += 1
                items.extend(
                    _get(client, token, {"q": q, "per_page": PAGE, "page": page}).get("items", [])
                )
                time.sleep(pace)
            stats["repos_seen"] += len(items)
            stats["repos_new"] += run(lambda c: _upsert(c, items))
            run(lambda c: _mark_shard(c, a, b, star_threshold, len(items)))
    return stats
