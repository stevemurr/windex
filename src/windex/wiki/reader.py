"""The single place that knows the Wikipedia dump on-disk format.

Source: Wikimedia CirrusSearch *index* dumps
(https://dumps.wikimedia.org/other/cirrus_search_index/). A weekly snapshot for
`enwiki_content` is 64 bzip2 shards of Elasticsearch bulk JSON-lines: each
document is TWO lines — an action-metadata line ``{"index": {"_id": <page_id>}}``
followed by the document line. Content dumps carry PRE-EXTRACTED plain text in
the ``text`` field, so there is no HTML extraction step.

Verified against the 2026-07-12 enwiki_content dump (shard 00000): the action
line has no ``_type`` field and ``_id`` is an integer; the parser tolerates the
string / ``_type``-bearing variant too. Everything format-specific (bulk
pairing, field names, bzip2, URL derivation) lives here so a different upstream
(e.g. a return to the old cirrussearch gzip layout) only touches this module.
"""

import bz2
import io
import json
from collections.abc import Iterable, Iterator
from urllib.parse import quote

CONTENT_NAMESPACE = 0
WIKI_URL_BASE = "https://en.wikipedia.org/wiki/"


def wiki_url(title: str) -> str:
    """Canonical article URL: spaces → underscores, then percent-encode
    (keeping ':' for namespaces and '/' for subpages, like Wikipedia does)."""
    return WIKI_URL_BASE + quote(title.replace(" ", "_"), safe=":/")


class _StreamReader(io.RawIOBase):
    """Adapt an iterator of byte chunks (e.g. httpx's streamed response) into a
    read-only file object, so a multi-GB shard is decompressed lazily and never
    held in memory."""

    def __init__(self, chunks: Iterable[bytes]):
        self._it = iter(chunks)
        self._buf = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        while not self._buf:
            try:
                self._buf = next(self._it)
            except StopIteration:
                return 0
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


def open_dump_lines(chunks: Iterable[bytes]) -> Iterator[str]:
    """Decode a streamed bzip2 shard into UTF-8 text lines, lazily. Handles
    multi-stream bzip2 (BZ2File does) and decodes leniently."""
    raw = _StreamReader(chunks)
    bz = bz2.BZ2File(raw, mode="rb")
    text = io.TextIOWrapper(bz, encoding="utf-8", errors="replace")
    return iter(text)


def _page_id(action: dict, doc: dict) -> int | None:
    pid = doc.get("page_id")
    if pid is None:
        pid = (action.get("index") or {}).get("_id")
    try:
        return int(pid) if pid is not None else None
    except (TypeError, ValueError):
        return None


def parse_pair(action: dict, doc: dict, namespace: int = CONTENT_NAMESPACE) -> dict | None:
    """Map one (action, document) pair to a normalized article record, or None
    if it isn't a content-namespace article carrying text."""
    if doc.get("namespace") != namespace:
        return None
    text = doc.get("text")
    if not text:
        return None
    page_id = _page_id(action, doc)
    if page_id is None:
        return None
    title = doc.get("title") or ""
    incoming = doc.get("incoming_links")
    return {
        "id": f"wiki:{page_id}",
        "page_id": page_id,
        "url": wiki_url(title),
        "title": title,
        "revision_ts": doc.get("timestamp"),        # current revision timestamp
        "incoming_links": int(incoming) if isinstance(incoming, int) else 0,
        "opening_text": doc.get("opening_text") or "",  # lead summary → snippet
        "text": text,
    }


def iter_articles(lines: Iterable[str], namespace: int = CONTENT_NAMESPACE) -> Iterator[dict]:
    """Yield normalized article records from CirrusSearch bulk JSON-lines.

    Consumes the line iterator strictly in (action, document) pairs, so the
    dump is streamed, never materialized. Malformed or truncated trailing
    lines are skipped rather than raising."""
    it = iter(lines)
    while True:
        try:
            action_line = next(it)
        except StopIteration:
            return
        try:
            doc_line = next(it)
        except StopIteration:
            return  # dangling action at a truncation boundary
        try:
            action = json.loads(action_line)
            doc = json.loads(doc_line)
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(action, dict) or "index" not in action:
            continue
        rec = parse_pair(action, doc, namespace=namespace)
        if rec is not None:
            yield rec


def iter_articles_from_bytes(
    chunks: Iterable[bytes], namespace: int = CONTENT_NAMESPACE
) -> Iterator[dict]:
    """Convenience: bzip2 byte stream → normalized article records."""
    return iter_articles(open_dump_lines(chunks), namespace=namespace)
