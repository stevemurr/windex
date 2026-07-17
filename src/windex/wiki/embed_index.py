"""Embed staged Wikipedia articles from clean parquet and upsert into Qdrant.

The driver is shared (windex.embed.pipeline); this is the wiki-specific part.
Source 'wiki' lands in the "wiki" collection behind the wiki_current alias.
The snippet prefers the dump's own opening_text over a blind text prefix, and
incoming_links rides in the payload as a popularity signal.
"""

import psycopg

from windex.config import Settings
from windex.embed.pipeline import SourceSpec, embed_pending as _embed_pending

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
