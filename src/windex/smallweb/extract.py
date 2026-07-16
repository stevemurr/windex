"""Extraction + a deliberately LIGHT quality gate for Small Web blog posts.

Reuses the news extraction approach — trafilatura ``bare_extraction`` keeps the
title/date metadata alongside the text (stock Trafilatura drops titles), exactly
like ``ccnews.pipeline.NewsExtractor``. What is deliberately DIFFERENT is the
quality gate.

The feasibility research flags FineWeb/C4 strictness as the main quality risk
for this corpus: those filters over-reject legitimate short and idiosyncratic
personal blog posts. So the Small Web gate is only:

    minimum length  →  English  →  Gopher repetition

and explicitly NOT GopherQuality / C4 / FineWeb. The gate is assembled by
``build_quality_filters`` (returning a list of named predicates over a datatrove
``Document``) so it stays *per-source tunable* — the news pipeline keeps its own
heavier chain in ``ccnews/pipeline.py``; this is not a copy of it, and neither is
locked to the other.
"""

from collections.abc import Callable

# A quality filter is (name, predicate) where predicate(doc) -> keep?
Filter = tuple[str, Callable[[object], bool]]


def _wrap(datatrove_filter) -> Callable[[object], bool]:
    """Adapt a datatrove BaseFilter to a keep-predicate. datatrove filters
    return ``bool`` or ``(bool, reason)`` and may annotate ``doc.metadata`` (the
    language filter sets ``language``), so we call ``.filter`` for its side
    effects and normalize the verdict."""
    def keep(doc) -> bool:
        res = datatrove_filter.filter(doc)
        return res[0] if isinstance(res, tuple) else bool(res)
    return keep


def build_quality_filters(
    language: str = "en",
    min_chars: int = 200,
    include_language: bool = True,
) -> list[Filter]:
    """The Small Web gate, ordered cheap → expensive. ``include_language`` is a
    tuning/test seam: the real language filter loads a fastText model, so callers
    that can't (or shouldn't) pay that — unit tests — pass ``False`` and rely on
    length + repetition only. Callers that want a different corpus policy build
    their own list; nothing here is hard-wired to Small Web beyond the defaults.
    """
    from datatrove.pipeline.filters import GopherRepetitionFilter

    filters: list[Filter] = [
        ("min_length", lambda doc: len(doc.text) >= min_chars),
    ]
    if include_language:
        from datatrove.pipeline.filters import LanguageFilter

        filters.append(("language", _wrap(LanguageFilter(languages=[language]))))
    filters.append(("repetition", _wrap(GopherRepetitionFilter())))
    return filters


def _maybe_wrap(html: str, wrap: bool) -> str:
    """Inline feed content (``content:encoded`` / Atom ``content``) is often a
    bare body fragment — e.g. a single ``<p>`` — which trafilatura discards as
    "not a document". Wrap such fragments in a minimal skeleton so extraction
    succeeds. Full fetched pages (wrap=False) and fragments that already look
    like a document are passed through untouched."""
    if not wrap:
        return html
    low = html.lower()
    if "<html" in low or "<body" in low or "<article" in low:
        return html
    return f"<html><body><article>{html}</article></body></html>"


def extract_html(html: str, url: str | None) -> tuple[str, dict] | None:
    """trafilatura bare_extraction → (text, metadata dict), or None when there
    is no usable text. Works on both a full fetched page and an inline feed
    content fragment."""
    import trafilatura

    try:
        res = trafilatura.bare_extraction(
            html, url=url, include_comments=False, with_metadata=True
        )
    except Exception:
        return None
    if res is not None and not isinstance(res, dict):  # trafilatura >= 1.9 Document
        res = res.as_dict() if hasattr(res, "as_dict") else vars(res)
    res = res or {}
    text = res.get("text") or ""
    if not text:
        return None
    return text, res


def extract_post(
    html: str,
    url: str,
    feed_title: str | None = None,
    feed_published: str | None = None,
    filters: list[Filter] | None = None,
    wrap: bool = False,
) -> dict | None:
    """Extract one post and apply the light gate. Returns
    ``{"text", "title", "date", "lang"}`` or None if it fails extraction or a
    filter. ``wrap=True`` marks inline feed content (a body fragment) so it is
    wrapped into a document first. The feed's own title/date are preferred over
    trafilatura's guesses (feed metadata is reliable; trafilatura on an inline
    fragment often isn't)."""
    from datatrove.data import Document

    parsed = extract_html(_maybe_wrap(html, wrap), url)
    if parsed is None:
        return None
    text, meta = parsed
    filters = build_quality_filters() if filters is None else filters
    doc = Document(text=text, id=url, metadata={"url": url})
    for _name, keep in filters:
        if not keep(doc):
            return None
    return {
        "text": doc.text,
        "title": (feed_title or meta.get("title") or "").strip(),
        "date": feed_published or meta.get("date") or None,
        "lang": doc.metadata.get("language") or "en",
    }
