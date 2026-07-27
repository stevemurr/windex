"""DevDocs page identity and HTML extraction helpers."""

import re
from html.parser import HTMLParser

_ATTR_LINK_RE = re.compile(
    r'<a\b[^>]*?href="([^"]+)"[^>]*?class="_attribution-link"'
)
_ATTR_DIV = '<div class="_attribution">'
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.DOTALL | re.IGNORECASE)
_SKIP_TAGS = {"script", "style", "template", "iframe", "svg"}
_BLOCK_TAGS = {
    "p", "div", "section", "article", "aside", "header", "footer", "nav",
    "h1", "h2", "h3", "h4", "h5", "h6", "li", "ul", "ol", "dl", "dt", "dd",
    "table", "tr", "caption", "pre", "blockquote", "figure", "figcaption",
    "details", "summary", "br", "hr",
}
_WS = re.compile(r"[ \t\r\f\v]+")
_NL = re.compile(r"\n\s*\n\s*(\s*\n)+")


def doc_id(slug: str, path: str) -> str:
    return f"docs:{slug}/{path}"


def framework_of(slug: str) -> str:
    """Return the version-free framework name."""
    return slug.split("~", 1)[0]


class _TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in _SKIP_TAGS:
            self._skip += 1
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")
        elif tag in ("td", "th"):
            self.parts.append(" ")

    def handle_endtag(self, tag):
        if tag in _SKIP_TAGS:
            self._skip = max(self._skip - 1, 0)
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self._skip and data:
            self.parts.append(data)


def html_to_text(html: str) -> str:
    parser = _TextExtractor()
    parser.feed(html)
    text = _WS.sub(" ", "".join(parser.parts))
    text = "\n".join(line.strip() for line in text.split("\n"))
    return _NL.sub("\n\n", text).strip()


def upstream_url(html: str) -> str | None:
    """Return the exact scraped-from URL recorded by DevDocs, if present."""
    match = _ATTR_LINK_RE.search(html)
    return match.group(1) if match else None


def page_title(html: str, fallback: str | None = None) -> str:
    match = _H1_RE.search(html)
    if match:
        title = html_to_text(match.group(1)).replace("\n", " ").strip()
        if title:
            return title
    return (fallback or "").strip()


def strip_html(body: str) -> str:
    return html_to_text(body).replace("\n", " ").strip()


def page_titles_from_index(index: dict) -> dict[str, str]:
    """Return page-level index titles, excluding anchor-only entries."""
    titles: dict[str, str] = {}
    for entry in index.get("entries", []):
        path = entry.get("path") or ""
        name = entry.get("name") or ""
        if path and name and "#" not in path:
            titles.setdefault(path, name)
    return titles
