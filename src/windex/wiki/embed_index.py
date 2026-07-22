"""Embed staged Wikipedia articles from clean parquet and upsert into Qdrant.

The driver is shared (windex.embed.pipeline); this is the wiki-specific part.
Source 'wiki' lands in the "wiki" collection behind the wiki_current alias.
The snippet prefers the dump's own opening_text over a blind text prefix, and
incoming_links rides in the payload as a popularity signal.
"""

import psycopg
from qdrant_client import QdrantClient

from windex.config import Settings
from windex.embed.pipeline import SourceSpec, embed_pending as _embed_pending, point_id
from windex.index import qdrant as qidx


def refresh_payloads(settings: Settings, articles: list[dict]) -> int:
    """Update url/title/snippet/published_at/incoming_links in place for already-
    embedded articles whose text is unchanged but whose page was moved (title/url
    changed). set_payload merges the keys into the existing payload; the vectors
    are untouched — a rename costs no embedding work. Best-effort (a moved page's
    payload self-heals on the next reindex if the index is briefly down)."""
    if not articles:
        return 0
    client = QdrantClient(url=settings.qdrant_url, timeout=30)
    alias = qidx.alias_name("wiki")
    try:
        for a in articles:
            client.set_payload(
                collection_name=alias,
                payload={
                    "url": a["url"],
                    "title": a["title"],
                    "snippet": (a.get("opening_text") or a["text"][:400])[:400],
                    "published_at": a["revision_ts"],
                    "incoming_links": int(a["incoming_links"]),
                },
                points=[point_id(a["id"])],
                wait=True,  # a refreshed title/url should be visible on return
            )
    finally:
        client.close()
    return len(articles)


SPEC = SourceSpec(
    source="wiki",
    collection="wiki",
    columns=("id", "url", "title", "revision_ts", "incoming_links", "opening_text", "text"),
    text_field="text",
    payload=lambda r: {
        "url": r["url"],
        "title": r["title"],
        "snippet": (r["opening_text"] or r["text"][:400])[:400],
        "published_at": r["revision_ts"],  # current revision timestamp
        "incoming_links": r["incoming_links"],  # popularity signal
    },
)


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    return _embed_pending(conn, settings, SPEC, limit)
