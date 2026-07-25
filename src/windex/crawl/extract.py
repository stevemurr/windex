"""Page → document text, with a fallback for what trafilatura drops.

trafilatura is the primary extractor (same as ``ccnews`` and ``smallweb``) and
stays primary: it is better at separating prose from chrome, and it returns the
title/date metadata a bare text dump cannot.

But it makes a document/not-a-document judgement, and on doc-site pages that
judgement is sometimes wrong. Measured on the Claude cookbook (2026-07-24): 2 of
84 pages — `claude-agent-sdk-07-hosting-the-agent` and `misc-using-citations` —
returned None from `bare_extraction` despite carrying ~4,700 words of real prose
each. Both are code-heavy notebook-style pages, exactly the shape
``hf/crawl.py`` warns that generic filters over-reject.

Silently losing 2.4% of a curated cluster is not acceptable for a corpus whose
whole selling point is that it was curated. So when trafilatura declines, we fall
back to a structural extraction: pick the main content container, drop the known
chrome elements, and take the text. It is coarser — some nav text can survive —
but a slightly noisy document beats a missing one, and the alternative is a page
that is silently unsearchable.

The fallback NEVER overrides trafilatura; it only runs where trafilatura produced
nothing usable.
"""

from __future__ import annotations

# Elements that are chrome on essentially every site. Removed before taking text
# in the fallback path (trafilatura does its own, better, version of this).
_STRIP_TAGS = ("script", "style", "nav", "header", "footer", "aside", "noscript",
               "form", "svg", "button")

# Containers most likely to hold the document body, best first. Falling through
# to <body> is the last resort, not the expectation.
_MAIN_XPATH = (
    "//main", "//article", "//*[@role='main']",
    "//*[contains(@class,'content')]", "//body",
)


def _clean_text(node) -> str:
    """Text of an lxml node with chrome removed and whitespace normalized.

    `drop_tree()` rather than `parent.remove()`: removing an element also discards
    its *tail* text, which is the prose sitting between it and the next sibling —
    dropping that silently glues neighbouring words together. drop_tree preserves
    the tail, which is exactly why lxml.html provides it.
    """
    for el in node.xpath(".//" + " | .//".join(_STRIP_TAGS)):
        el.drop_tree()
    text = node.text_content()
    lines = [ln.strip() for ln in text.splitlines()]
    return "\n".join(ln for ln in lines if ln)


def fallback_extract(html: str) -> tuple[str, str] | None:
    """Structural extraction: ``(text, title)`` or None.

    Used only when trafilatura declines. Picks the densest of the candidate
    containers rather than the first match — a page with an empty <main> and the
    real content in a content div would otherwise extract to nothing.
    """
    import lxml.html

    try:
        doc = lxml.html.fromstring(html)
    except Exception:
        return None

    title = ""
    for el in doc.xpath("//title")[:1]:
        title = (el.text or "").strip()
    if not title:
        for el in doc.xpath("//h1")[:1]:
            title = (el.text_content() or "").strip()

    best = ""
    for xpath in _MAIN_XPATH:
        for node in doc.xpath(xpath)[:1]:
            text = _clean_text(node)
            if len(text) > len(best):
                best = text
        if len(best) > 2000:
            break  # a good container was found; don't bother widening to <body>
    return (best, title) if best else None


def declared_canonical(html: str) -> str | None:
    """The URL the page says it lives at: ``rel=canonical``, else ``og:url``.

    Why this matters for a link crawl: an index page often links its articles
    with a referrer tag (`/posts/x/?from=research`), so the fetched URL is not
    the page's identity. Indexing that URL gives ugly doc ids and, worse, forks
    the same article into several documents when it is linked from several
    places. ``hf/crawl.py`` already made this call for versioned HF doc URLs —
    "index what you link" — and the reasoning is identical here.

    Returned raw; the caller decides whether to trust it (a canonical pointing
    off-host or out of scope must not silently redirect what we index).
    """
    import lxml.html

    try:
        doc = lxml.html.fromstring(html)
    except Exception:
        return None
    for el in doc.xpath("//link[@rel='canonical'][@href]")[:1]:
        href = (el.get("href") or "").strip()
        if href:
            return href
    for el in doc.xpath("//meta[@property='og:url'][@content]")[:1]:
        content = (el.get("content") or "").strip()
        if content:
            return content
    return None


def extract_page(html: str, url: str, policy) -> dict | None:
    """Page HTML → ``{title, text, published_at}``, or None if unusable.

    trafilatura first (it also yields title/date); the structural fallback only
    where that produced nothing. Quality filters are off unless the policy asks —
    a curated cluster's scope decision IS its quality gate, the same call
    ``hf/crawl.py`` and ``docs_source`` made, and FineWeb/C4-style gates
    over-reject exactly the short code-heavy pages a doc site is made of.
    """
    from windex.smallweb.extract import extract_html

    title, published = "", ""
    parsed = extract_html(html, url)
    if parsed is not None:
        text, meta = parsed
        title = (meta.get("title") or "").strip()
        published = (meta.get("date") or "") or ""
    else:
        text = ""

    if len(text) < policy.extract.min_chars:
        rescued = fallback_extract(html)
        if rescued is not None and len(rescued[0]) > len(text):
            text, fb_title = rescued
            title = title or fb_title
    if len(text) < policy.extract.min_chars:
        return None

    if policy.extract.quality_filters:
        from datatrove.data import Document

        from windex.smallweb.extract import build_quality_filters

        doc = Document(text=text, id=url, metadata={"url": url})
        for _name, keep in build_quality_filters(min_chars=policy.extract.min_chars):
            if not keep(doc):
                return None
        text = doc.text
    return {"title": title, "text": text, "published_at": published}
