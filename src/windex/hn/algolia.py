"""Hacker News normalization and Algolia window fetching."""

import html
import re
import time
from datetime import UTC, datetime

import httpx

from windex.ccnews.identity import text_hash

MAX_HITS = 1000
_P_RE = re.compile(r"(?i)<p[^>]*>")
_TAG_RE = re.compile(r"<[^>]+>")


def doc_id(item_id: int | str) -> str:
    return f"hn:{item_id}"


def item_url(item_id: int | str) -> str:
    return f"https://news.ycombinator.com/item?id={item_id}"


def clean_title(raw: str | None) -> str:
    """Normalize a story title for text storage and stable hashing."""
    return " ".join((raw or "").replace("\x00", "").split())


def clean_text(fragment: str | None) -> str:
    """Convert the small HTML fragment in HN item text to plain text."""
    if not fragment:
        return ""
    text = _P_RE.sub("\n\n", fragment.replace("\x00", ""))
    text = _TAG_RE.sub("", text)
    lines = [
        " ".join(line.split())
        for line in html.unescape(text).split("\n")
    ]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _rfc3339(epoch: int) -> str:
    return (
        datetime.fromtimestamp(int(epoch), tz=UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )


def story_from_hit(hit: dict) -> dict:
    """Normalize one Algolia hit to the epoch-2 extractor's row shape."""
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


def fetch_window_stories(
    client: httpx.Client,
    url: str,
    from_ts: int,
    until_ts: int,
    on_request=None,
    max_hits: int = MAX_HITS,
) -> tuple[list[dict], int]:
    """Fetch a complete Algolia time window, splitting capped ranges."""
    params = {
        "tags": "story",
        "numericFilters": f"created_at_i>={from_ts},created_at_i<{until_ts}",
        "hitsPerPage": max_hits,
    }
    for attempt in range(5):
        if on_request:
            on_request()
        response = client.get(url, params=params)
        if response.status_code == 429 or response.status_code >= 500:
            wait = int(response.headers.get("Retry-After", 0))
            time.sleep(wait or min(2**attempt * 5, 120))
            continue
        response.raise_for_status()
        break
    else:
        response.raise_for_status()

    body = response.json()
    count = int(body.get("nbHits") or 0)
    if count > max_hits and until_ts - from_ts > 1:
        midpoint = (from_ts + until_ts) // 2
        left, left_queries = fetch_window_stories(
            client,
            url,
            from_ts,
            midpoint,
            on_request,
            max_hits,
        )
        right, right_queries = fetch_window_stories(
            client,
            url,
            midpoint,
            until_ts,
            on_request,
            max_hits,
        )
        return left + right, 1 + left_queries + right_queries
    return list(body.get("hits") or []), 1
