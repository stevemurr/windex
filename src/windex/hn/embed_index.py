"""Embed staged Hacker News stories from clean parquet and upsert into Qdrant,
plus the points-refresh path for unchanged stories.

The embed driver is shared (windex.embed.pipeline); this is the HN-specific
part. Source 'hn' lands in the "hn" collection behind the hn_current alias. The
embedded text is title + story_text (Ask/Show/self posts; most stories are
title-only) and the snippet is the title. points rides in the payload under an
integer index — the future ranking boost and today's min_points filter.

refresh_payloads() is the other half of the trailing re-pull design: a story
whose text is unchanged but whose points/num_comments drifted gets a
set_payload — never a re-embed, never a full upsert (which would zero the
vectors) — so score freshness costs no embedding work at all.
"""

import psycopg
from qdrant_client import QdrantClient

from windex.config import Settings
from windex.embed.pipeline import SourceSpec, embed_pending as _embed_pending, point_id
from windex.index import qdrant as qidx


def refresh_payloads(settings: Settings, stories: list[dict]) -> int:
    """Update points/num_comments in place for already-embedded stories whose
    text is unchanged. set_payload merges the given keys into the existing
    payload; the vectors are untouched. Callers treat this as best-effort
    (stale points self-heal on the next trailing re-pull)."""
    if not stories:
        return 0
    client = QdrantClient(url=settings.qdrant_url, timeout=30)
    alias = qidx.alias_name("hn")
    for s in stories:
        client.set_payload(
            collection_name=alias,
            payload={"points": int(s["points"]), "num_comments": int(s["num_comments"])},
            points=[point_id(s["id"])],
            wait=True,  # refreshed points should be visible on return
        )
    return len(stories)


SPEC = SourceSpec(
    source="hn",
    collection="hn",
    columns=("id", "url", "target_url", "title", "story_text", "author",
             "points", "num_comments", "created_at"),
    text_field="story_text",
    payload=lambda r: {
        "url": r["url"],                    # the HN discussion page
        "target_url": r["target_url"],      # external link (None on self posts)
        "title": r["title"],
        "snippet": r["title"][:400],        # the title IS the story
        "published_at": r["created_at"],
        "points": int(r["points"] or 0),
        "num_comments": int(r["num_comments"] or 0),
        "author": r["author"],
    },
)


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    return _embed_pending(conn, settings, SPEC, limit)
