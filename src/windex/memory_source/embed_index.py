"""Embed staged chat-memory chunks from clean parquet and upsert into Qdrant.

The driver is shared (windex.embed.pipeline); this is the memory-specific part.
Source 'memory' lands in the "memory" collection behind the memory_current
alias. The embedded text is the conversation title + chunk body; the payload
carries the conversation id and chunk index (so a result can fetch its n±1
neighbours) and the chunk's end time as published_at for date-windowed recall.
"""

import psycopg

from windex.config import Settings
from windex.embed.pipeline import SourceSpec, embed_pending as _embed_pending


def _published_at(value) -> str | None:
    """Normalize the parquet value to RFC3339 so Qdrant's datetime index /
    DatetimeRange accept it. The column is timestamp[us, tz=UTC], so pyarrow
    hands back a tz-aware datetime; a str (defensive) is passed through, adding a
    midnight-UTC suffix only if it carries no time component."""
    if value is None:
        return None
    if isinstance(value, str):
        return value if "T" in value else f"{value}T00:00:00Z"
    return value.isoformat()


SPEC = SourceSpec(
    source="memory",
    collection="memory",
    columns=("id", "url", "title", "conversation_id", "chunk_index", "published_at", "text"),
    text_field="text",
    payload=lambda r: {
        "url": r["url"],
        "title": r["title"],
        "snippet": (r["text"] or "")[:400],
        "conversation_id": r["conversation_id"],
        "chunk_index": r["chunk_index"],
        "published_at": _published_at(r["published_at"]),
    },
)


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    return _embed_pending(conn, settings, SPEC, limit)
