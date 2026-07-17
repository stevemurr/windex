"""Embed staged programming-docs pages from clean parquet and upsert into Qdrant.

The driver is shared (windex.embed.pipeline); this is the docs-specific part.
Source 'docs' lands in the "docs" collection behind the docs_current alias. The
embedded text is title + page body; the payload carries the framework (slug
base), upstream version, and the docset's upstream license attribution (plain
text, truncated — the full string lives in ``docsets``).
"""

import psycopg

from windex.config import Settings
from windex.embed.pipeline import SourceSpec, embed_pending as _embed_pending

ATTRIBUTION_CHARS = 200

SPEC = SourceSpec(
    source="docs",
    collection="docs",
    columns=("id", "url", "title", "framework", "version", "attribution", "text"),
    text_field="text",
    payload=lambda r: {
        "url": r["url"],
        "title": r["title"],
        "snippet": (r["text"] or "")[:400],
        "framework": r["framework"],
        "version": r["version"],
        "attribution": (r["attribution"] or "")[:ATTRIBUTION_CHARS],
    },
)


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    return _embed_pending(conn, settings, SPEC, limit)
