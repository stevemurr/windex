"""Pure parsers for Hugging Face sitemap and ``llms.txt`` catalogs."""

import hashlib
import re
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

from windex.hf import BASE_URL

WANTED_SHARDS = ("sitemap-doc.xml", "sitemap-blog.xml")
_SM_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
_LINK_RE = re.compile(r"^\s*-\s*\[([^\]]*)\]\((\S+?)\)\s*$", re.M)
_VER_RE = re.compile(r"v\d+(?:\.\w+)+")


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()


def parse_sitemap_index(xml: str) -> list[str]:
    """Return the enumerable doc/blog shard URLs from a sitemap index."""
    root = ET.fromstring(xml)
    locations = [
        element.text.strip()
        for element in root.iter(f"{_SM_NS}loc")
        if element.text
    ]
    return [
        url for url in locations
        if url.rsplit("/", 1)[-1] in WANTED_SHARDS
    ]


def parse_urlset(xml: str) -> list[tuple[str, str]]:
    """Return ``(location, last_modified)`` pairs from a sitemap shard."""
    root = ET.fromstring(xml)
    entries = []
    for element in root.iter(f"{_SM_NS}url"):
        location = element.findtext(f"{_SM_NS}loc") or ""
        if location.strip():
            entries.append((
                location.strip(),
                (element.findtext(f"{_SM_NS}lastmod") or "").strip(),
            ))
    return entries


def root_key(url: str) -> str:
    return urlsplit(url).path.strip("/")


def kind_of(root: str) -> str:
    return root.split("/", 1)[0]


def blog_slug(url: str) -> str:
    path = urlsplit(url).path.strip("/")
    return path[len("blog/"):] if path.startswith("blog/") else path


def parse_llms(
    text: str,
    root: str,
    base_url: str = BASE_URL,
) -> list[dict]:
    """Parse stable page paths, titles, and version pins from ``llms.txt``."""
    prefix = f"{base_url}/{root}/"
    pages: list[dict] = []
    seen: set[str] = set()
    for title, url in _LINK_RE.findall(text):
        if not url.startswith(prefix) or not url.endswith(".md"):
            continue
        rest = url[len(prefix):-len(".md")]
        head, _, tail = rest.partition("/")
        if tail and _VER_RE.fullmatch(head):
            version, path = head, tail
        else:
            version, path = "", rest
        if not path or path in seen:
            continue
        seen.add(path)
        pages.append({
            "path": path,
            "title": title.strip(),
            "version": version,
        })
    return pages


def llms_url(root: str, base_url: str = BASE_URL) -> str:
    return f"{base_url}/{root}/llms.txt"


def root_version(pages: list[dict]) -> str:
    return next(
        (str(page["version"]) for page in pages if page["version"]),
        "",
    )
