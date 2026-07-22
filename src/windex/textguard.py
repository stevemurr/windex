"""Detect text that must never be embedded.

Motivation (2026-07-22): an empty (or whitespace/invisible-only) composed
document — a title-less, body-less row — makes ``compose_text`` return "".
Embedding servers accept "" and return a (meaningless) vector, and there is no
HTTP 4xx for ``embed_isolating`` to catch, so without this guard the empty doc
silently upserts a junk vector (observed live: 7 empty hn vectors, 13,569 empty
hn docs).

This is a WHITESPACE check, NOT a length threshold. Legitimately short docs (e.g.
HN title-only link posts, median ~50 chars) are valid, indexable content and must
be kept — only a doc with no visible content at all is dropped.
"""

from windex.sanitize import strip_smuggled


def is_empty_text(text: str | None) -> bool:
    """True when ``text`` is empty or only whitespace/invisible code points."""
    return not strip_smuggled(text or "").strip()
