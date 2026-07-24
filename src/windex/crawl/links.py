"""Pull outbound links out of a fetched page.

Separate from ``smallweb.extract`` (which produces the *document text*) because
the two answer different questions and fail independently: a page can extract to
excellent text and expose no links, or be a bare nav index with no prose and
every link that matters. The crawl needs both answers about the same fetch.

lxml rather than a regex: `href` appears in comments, in inline scripts, and in
attributes that are not links, and the base-URL join has to respect `<base href>`.
"""

from __future__ import annotations

from urllib.parse import urljoin, urlsplit

from windex.crawl.scope import canonicalize

# Attributes that hold a navigable document URL. `src` is deliberately absent —
# images/scripts are not documents, and following them would be the fastest way
# to fill a crawl budget with assets.
_LINK_XPATH = "//a[@href]"


def extract_links(html: str, base_url: str) -> list[str]:
    """Absolute, canonicalized, de-duplicated links from one page.

    Order is preserved (first appearance wins) so a BFS visits a page's links in
    document order, which keeps a truncated crawl biased toward the top of an
    index page rather than an arbitrary hash order.
    """
    import lxml.html

    try:
        doc = lxml.html.fromstring(html)
    except Exception:
        return []  # unparseable markup is a page with no links, not a crash

    # <base href> changes what relative links resolve against; ignoring it
    # silently produces 404s on any site that uses one.
    base = base_url
    for el in doc.xpath("//base[@href]")[:1]:
        base = urljoin(base_url, el.get("href", "").strip())

    seen: set[str] = set()
    out: list[str] = []
    for el in doc.xpath(_LINK_XPATH):
        href = (el.get("href") or "").strip()
        if not href or href.startswith(("#", "mailto:", "javascript:", "data:", "tel:")):
            continue
        try:
            absolute = urljoin(base, href)
        except ValueError:
            continue
        if not urlsplit(absolute).hostname:
            continue
        canon = canonicalize(absolute)
        if canon in seen:
            continue
        seen.add(canon)
        out.append(canon)
    return out
