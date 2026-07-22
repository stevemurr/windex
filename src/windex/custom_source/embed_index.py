"""Embed staged custom-source docs from clean parquet into Qdrant.

Each registered custom source lands in its own ``<name>`` collection behind the
``<name>_current`` alias, using the shared embed driver (``windex.embed.pipeline``)
exactly like the built-in parquet-backed sources — nothing here computes
embeddings. The embedded text is the doc title + body; the payload carries
url/title/snippet, published_at normalized to RFC3339 for the datetime index, and
the opaque ``extra`` blob the pusher attached (parsed back from its stored orjson
string so a search result surfaces it as structured JSON).

One module-level ``embed_pending`` drains ALL registered custom sources, so the
single ``custom-embed`` loop (jobs.py) covers every source without a per-source
loop process. ``spec_for(name)`` is the per-source factory the driver needs.
"""

from __future__ import annotations

import orjson
import psycopg

from windex.config import Settings
from windex.embed.pipeline import SourceSpec
from windex.embed.pipeline import embed_pending as _embed_pending


def _published_at(value) -> str | None:
    """Normalize the parquet value to RFC3339 so Qdrant's datetime index /
    DatetimeRange accept it (memory's rule). The column is timestamp[us, tz=UTC],
    so pyarrow hands back a tz-aware datetime; a str is passed through, adding a
    midnight-UTC suffix only if it carries no time component."""
    if value is None:
        return None
    if isinstance(value, str):
        return value if "T" in value else f"{value}T00:00:00Z"
    return value.isoformat()


def _extra(value):
    """The `extra` column is stored as an orjson string (ingest); parse it back so
    the payload — and thus a search result — carries structured JSON. Anything
    unparseable degrades to None rather than sinking the batch."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return orjson.loads(value)
    except Exception:  # noqa: BLE001 — a malformed blob must not fail the embed
        return None


def spec_for(name: str) -> SourceSpec:
    """The driver's per-source description for one custom source: source and
    collection are the source name; the staged columns include the opaque
    ``extra`` blob, surfaced in the payload."""
    return SourceSpec(
        source=name,
        collection=name,
        columns=("id", "url", "title", "published_at", "text", "extra"),
        text_field="text",
        payload=lambda r: {
            "url": r["url"],
            "title": r["title"],
            "snippet": (r["text"] or "")[:400],
            "published_at": _published_at(r["published_at"]),
            "extra": _extra(r["extra"]),
        },
    )


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    """Embed every registered custom source's pending docs; return the total
    marked 'embedded'. One pass over the registry so the single custom-embed loop
    drains all of them; each source's driver run is the same idempotent,
    durable-before-commit pass as the built-in sources."""
    from windex.custom_source import registry

    total = 0
    for info in registry.list_all(conn):
        total += _embed_pending(conn, settings, spec_for(info["name"]), limit)
    return total
