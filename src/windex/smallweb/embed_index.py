"""Embed staged Small Web posts from clean parquet and upsert into Qdrant.

The driver is shared (windex.embed.pipeline); this is the smallweb-specific
part. Source 'smallweb' lands in the "smallweb" collection behind the
smallweb_current alias. The embedded text is title + post body; the payload
``outlet`` is the feed host.
"""

import psycopg

from windex.config import Settings
from windex.embed.pipeline import SourceSpec, embed_pending as _embed_pending


def _published_at(value: str | None) -> str | None:
    """Normalize to RFC3339 so Qdrant's datetime index / DatetimeRange accept it.
    Feed dates already carry a time; a bare trafilatura date (YYYY-MM-DD) gets a
    midnight-UTC suffix."""
    if not value:
        return None
    return value if "T" in value else f"{value}T00:00:00Z"


SPEC = SourceSpec(
    source="smallweb",
    collection="smallweb",
    columns=("id", "url", "title", "published_at", "outlet", "text"),
    text_field="text",
    payload=lambda r: {
        "url": r["url"],
        "title": r["title"],
        "snippet": (r["text"] or "")[:400],
        "published_at": _published_at(r["published_at"]),
        "outlet": r["outlet"],
    },
)


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    return _embed_pending(conn, settings, SPEC, limit)
