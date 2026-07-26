"""Stable document identity helpers for crawl Modules."""

from __future__ import annotations

from urllib.parse import urlsplit

from windex.ccnews.dedup import text_hash


def document_suffix(url: str, seed: str = "") -> str:
    """Return a bounded, stable path/query suffix for a crawled URL."""
    del seed  # reserved for future host-relative identity policies
    parts = urlsplit(url)
    tail = parts.path.lstrip("/") + (f"?{parts.query}" if parts.query else "")
    tail = tail or "index"
    if len(tail) > 200:
        tail = f"{tail[:160]}~{text_hash(url)[:16]}"
    return tail


__all__ = ["document_suffix"]
