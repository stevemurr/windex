"""Embed staged arXiv papers from clean parquet and upsert into Qdrant.

The driver is shared (windex.embed.pipeline); this is the arXiv-specific part.
Source 'arxiv' lands in the "arxiv" collection behind the arxiv_current alias.
The embedded text is title + abstract — metadata only; arXiv metadata is CC0 and
full text is never harvested.
"""

import psycopg

from windex.config import Settings
from windex.embed.pipeline import SourceSpec, embed_pending as _embed_pending


def format_authors(authors: list[str], n: int = 3) -> str:
    """First `n` authors, "et al." when more — the compact byline for payloads."""
    if not authors:
        return ""
    head = ", ".join(authors[:n])
    return f"{head}, et al." if len(authors) > n else head


def _published_at(created: str | None) -> str | None:
    """arXiv `created` is a bare date (YYYY-MM-DD); normalize to RFC3339 so the
    Qdrant datetime index / DatetimeRange filter accept it."""
    if not created:
        return None
    return created if "T" in created else f"{created}T00:00:00Z"


SPEC = SourceSpec(
    source="arxiv",
    collection="arxiv",
    columns=("id", "url", "title", "abstract", "authors", "primary_category",
             "categories", "created"),
    text_field="abstract",
    payload=lambda r: {
        "url": r["url"],
        "title": r["title"],
        "snippet": (r["abstract"] or "")[:400],
        "published_at": _published_at(r["created"]),  # submission date
        "primary_category": r["primary_category"],
        "categories": list(r["categories"] or []),
        "authors": format_authors(list(r["authors"] or [])),
    },
)


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    return _embed_pending(conn, settings, SPEC, limit)
