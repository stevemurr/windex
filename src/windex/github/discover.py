"""Date-sharded GitHub Search API sweep: enumerate repos with stars >= T by
creation-date shard, splitting any shard that approaches the API's 1000-result
window. This is the durable star-discovery path after GitHub's 2025-10-07
Events API change collapsed WatchEvents in the public feed (measured ~5100/hr
→ ~39/hr), and the only source for repos created after that date. Search
results carry full repo objects, so candidates arrive pre-filled with stars and
metadata; READMEs still come from hydration."""

import time
from collections import deque
from datetime import date, timedelta

import httpx
import psycopg

SEARCH = "https://api.github.com/search/repositories"
PAGE = 100
CAP = 1000  # search API hard result window per query


def _get(client: httpx.Client, token: str, params: dict, retries: int = 5) -> dict:
    for attempt in range(retries):
        resp = client.get(
            SEARCH,
            params=params,
            headers={
                "Authorization": f"bearer {token}",
                "Accept": "application/vnd.github+json",
            },
        )
        if resp.status_code in (403, 429):
            reset = int(resp.headers.get("x-ratelimit-reset", 0))
            wait = max(reset - time.time(), 0) if reset else 2**attempt * 5
            time.sleep(min(wait + 1, 120))
            continue
        if resp.status_code >= 500:
            time.sleep(2**attempt)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("search request failed after retries")


def _upsert(conn: psycopg.Connection, items: list[dict]) -> int:
    new = 0
    with conn.cursor() as cur:
        for it in items:
            cur.execute(
                """
                INSERT INTO repos (repo_id, full_name, stars, description,
                                   primary_language, pushed_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'candidate')
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


def sweep(
    conn: psycopg.Connection,
    tokens: list[str],
    star_threshold: int,
    created_from: date,
    created_to: date | None = None,
) -> dict:
    if not tokens:
        raise ValueError("no GitHub tokens configured (WINDEX_GITHUB_TOKENS)")
    created_to = created_to or date.today()
    stats = {"shards": 0, "repos_seen": 0, "repos_new": 0, "capped_shards": 0}
    shards: deque[tuple[date, date]] = deque([(created_from, created_to)])
    tok_i = 0
    with httpx.Client(timeout=30) as client:
        while shards:
            a, b = shards.popleft()
            token = tokens[tok_i % len(tokens)]
            tok_i += 1
            q = f"stars:>={star_threshold} created:{a}..{b}"
            first = _get(client, token, {"q": q, "per_page": PAGE, "page": 1})
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
                time.sleep(2.1 / max(len(tokens), 1))  # 30 search req/min/token
            stats["repos_seen"] += len(items)
            stats["repos_new"] += _upsert(conn, items)
            time.sleep(2.1 / max(len(tokens), 1))
    return stats
