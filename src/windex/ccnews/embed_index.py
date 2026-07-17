"""Embed deduped news docs from clean parquet and upsert into Qdrant.

Dense vectors come from the user's Embedder; sparse BM25 from fastembed. The
driver (stream parquet → embed → upsert → commit status, with the pause check
and the runtime throughput profile) is shared: windex.embed.pipeline. This
module is just what makes 'news' different from the other sources.

point_id is re-exported here because it has always been importable from this
module — the other sources and the tests import it from here.
"""

import psycopg

from windex.config import Settings
from windex.embed.pipeline import SourceSpec, embed_pending as _embed_pending, point_id

__all__ = ["point_id", "embed_pending", "SPEC"]

SPEC = SourceSpec(
    source="news",
    collection="news",
    columns=("id", "url", "canonical_url", "title", "published_at", "lang", "text"),
    text_field="text",
    payload=lambda r: {
        "url": r["url"],
        "title": r["title"],
        "snippet": r["text"][:400],
        "published_at": r["published_at"],
        "lang": r["lang"],
        "outlet": (r["canonical_url"].split("/")[2] if r["canonical_url"] else None),
    },
    default_limit=50_000,
)


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 50_000) -> int:
    return _embed_pending(conn, settings, SPEC, limit)
