"""Ingest pending DevDocs docsets: fetch index.json + db.json, stage clean
parquet, and reconcile the documents ledger.

Per docset: ``documents.devdocs.io/<slug>/index.json`` (entries with
name/path/type, incl. real upstream ``#anchor``s) and ``db.json``
(``{path: cleaned HTML}`` — the page-level doc unit). db.json runs 1-70MB, so
it is STREAMED to a temp file and parsed from disk, never buffered through the
response object.

Full-replace semantics per slug (there are no per-page deltas upstream): the
whole page set is rewritten to ``docs/clean/<slug>.parquet`` — so text_ref
stays valid for unchanged pages — while the ledger upsert's text_hash guard
keeps re-embedding to the changed-page delta. Pages present in the ledger but
absent from the new bundle are tombstoned ('deleted' + best-effort Qdrant
point delete, mirroring the arXiv tombstone path). ``ingested_mtime`` only
advances when a slug completes, so an interrupted docset stays pending.

Everything DevDocs-format-specific (manifest fields live in sync.py; page HTML
shape, attribution block, path conventions here) stays in this package so a
different docs upstream only touches these modules.
"""

import json
import re
import tempfile
import time
from html.parser import HTMLParser
from pathlib import Path

import httpx
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from windex import db
from windex.ccnews.dedup import text_hash
from windex.config import Settings
from windex.docs_source import USER_AGENT
from windex.docs_source.canonical import canonical_url

console = Console()

CDN_URL = "https://documents.devdocs.io"

CLEAN_SCHEMA = pa.schema(
    [
        ("id", pa.string()),          # stable doc id: docs:<slug>/<path>
        ("url", pa.string()),         # canonical upstream (official docs) URL
        ("title", pa.string()),
        ("framework", pa.string()),   # slug base, e.g. python, vue
        ("version", pa.string()),     # manifest release, e.g. 3.14.6
        ("attribution", pa.string()),  # upstream license, plain text
        ("text", pa.string()),
    ]
)

# DevDocs' attribution filter appends the exact scraped URL to every page:
# <a href="<url>" class="_attribution-link"> (see canonical.py for why this
# beats path reconstruction). The block itself is license boilerplate on every
# page — extract the link, then cut the block before text extraction.
_ATTR_LINK_RE = re.compile(r'<a\b[^>]*?href="([^"]+)"[^>]*?class="_attribution-link"')
_ATTR_DIV = '<div class="_attribution">'
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.DOTALL | re.IGNORECASE)


def doc_id(slug: str, path: str) -> str:
    return f"docs:{slug}/{path}"


def framework_of(slug: str) -> str:
    """The version-free framework name: python~3.14 -> python."""
    return slug.split("~", 1)[0]


# --- HTML -> plain text ------------------------------------------------------

_SKIP_TAGS = {"script", "style", "template", "iframe", "svg"}
_BLOCK_TAGS = {
    "p", "div", "section", "article", "aside", "header", "footer", "nav",
    "h1", "h2", "h3", "h4", "h5", "h6", "li", "ul", "ol", "dl", "dt", "dd",
    "table", "tr", "caption", "pre", "blockquote", "figure", "figcaption",
    "details", "summary", "br", "hr",
}


class _TextExtractor(HTMLParser):
    """Lossy HTML -> text, in the spirit of github/clean.py: the goal is clean
    embedding/snippet text, not fidelity. Block tags break lines, table cells
    get a space, script/style/svg subtrees are dropped, code text is KEPT
    (code examples are exactly what doc searches hit on)."""

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


_WS = re.compile(r"[ \t\r\f\v]+")
_NL = re.compile(r"\n\s*\n\s*(\s*\n)+")


def html_to_text(html: str) -> str:
    parser = _TextExtractor()
    parser.feed(html)
    text = "".join(parser.parts)
    text = _WS.sub(" ", text)
    text = "\n".join(line.strip() for line in text.split("\n"))
    text = _NL.sub("\n\n", text)
    return text.strip()


def upstream_url(html: str) -> str | None:
    """The page's exact scraped-from URL, when DevDocs recorded one."""
    m = _ATTR_LINK_RE.search(html)
    return m.group(1) if m else None


def page_title(html: str, fallback: str | None = None) -> str:
    """First <h1> text; falls back to the page's anchor-less index-entry name
    (passed in by the caller), then to nothing."""
    m = _H1_RE.search(html)
    if m:
        title = html_to_text(m.group(1)).replace("\n", " ").strip()
        if title:
            return title
    return (fallback or "").strip()


def strip_html(body: str) -> str:
    """Attribution strings are small HTML fragments; payloads want plain text."""
    return html_to_text(body).replace("\n", " ").strip()


def page_titles_from_index(index: dict) -> dict[str, str]:
    """path -> entry name, for entries that address a whole page (no #anchor) —
    the h1 fallback. First entry wins (index order is DevDocs' own)."""
    titles: dict[str, str] = {}
    for e in index.get("entries", []):
        path, name = e.get("path") or "", e.get("name") or ""
        if path and name and "#" not in path:
            titles.setdefault(path, name)
    return titles


# --- fetching ---------------------------------------------------------------

def fetch_index(client: httpx.Client, slug: str, cdn_url: str = CDN_URL) -> dict:
    resp = client.get(f"{cdn_url}/{slug}/index.json")
    resp.raise_for_status()
    return resp.json()


def fetch_db(client: httpx.Client, slug: str, dest_dir: Path,
             cdn_url: str = CDN_URL) -> dict:
    """Stream db.json (1-70MB) to a temp file, then parse from disk — the
    response body is never accumulated in memory alongside the parsed dict."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=dest_dir, suffix=".db.json", delete=True) as tmp:
        with client.stream("GET", f"{cdn_url}/{slug}/db.json") as resp:
            resp.raise_for_status()
            for chunk in resp.iter_bytes(1 << 20):
                tmp.write(chunk)
        tmp.flush()
        tmp.seek(0)
        return json.load(tmp)


# --- ledger reconciliation ---------------------------------------------------

def _existing_hashes(cur: psycopg.Cursor, ids: list[str]) -> dict[str, str]:
    """id -> text_hash for live ledger rows. Tombstoned rows are deliberately
    excluded so a page that reappears (even byte-identical) re-stages."""
    if not ids:
        return {}
    cur.execute(
        "SELECT id, text_hash FROM documents "
        # No `source =` predicate: ids are namespaced (hn:, wiki:, …) so an id
        # list can't match another source. Including it makes the planner pick
        # documents_source_published_idx (est. rows=1 — rare sources are absent
        # from the MCV list) and scan every row of the source: 244s vs 63ms.
        "WHERE status <> 'deleted' AND id = ANY(%s)",
        (ids,),
    )
    return dict(cur.fetchall())


def _ledger_ids_for_slug(cur: psycopg.Cursor, slug: str) -> set[str]:
    cur.execute(
        "SELECT id FROM documents WHERE source = 'docs' "
        "AND status <> 'deleted' AND starts_with(id, %s)",
        (f"docs:{slug}/",),
    )
    return {r[0] for r in cur.fetchall()}


def apply_tombstones(conn: psycopg.Connection, settings: Settings, doc_ids: list[str]) -> int:
    """Mark vanished-page ledger rows status='deleted' and drop their Qdrant
    points. Qdrant removal is best-effort: a down index still leaves the ledger
    tombstoned (the point is dropped on the next reindex). Returns rows marked."""
    if not doc_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE documents SET status = 'deleted', embedded_model = NULL, "
            "indexed_at = NULL WHERE id = ANY(%s)",  # see note above: no source predicate
            (doc_ids,),
        )
        marked = cur.rowcount or 0
    conn.commit()
    try:
        from qdrant_client import QdrantClient
        from qdrant_client import models as qm

        from windex.ccnews.embed_index import point_id
        from windex.index import qdrant as qidx

        client = QdrantClient(url=settings.qdrant_url, timeout=30)
        client.delete(
            collection_name=qidx.alias_name("docs"),
            points_selector=qm.PointIdsList(points=[point_id(i) for i in doc_ids]),
            wait=True,  # tombstones are rare; deletion should be visible on return
        )
    except Exception as exc:  # index absent/unreachable: ledger tombstone stands
        console.print(f"[yellow]docs tombstone: qdrant delete skipped ({exc})[/yellow]")
    return marked


# --- ingest -----------------------------------------------------------------

def _chunked(seq: list, n: int):
    for start in range(0, len(seq), n):
        yield seq[start : start + n]


def stage_docset(
    conn: psycopg.Connection,
    settings: Settings,
    docset: dict,
    client: httpx.Client,
    cdn_url: str = CDN_URL,
    chunk_rows: int = 500,
) -> dict:
    """Fetch one docset and full-replace its staging partition. The parquet is
    written to a temp path and renamed into place only after the whole set is
    processed, so text_ref never points at a partial file; the ledger upsert is
    deferred and committed once, after the rename."""
    slug = docset["slug"]
    fw = framework_of(slug)
    version = docset.get("release") or ""
    attribution = strip_html(docset.get("attribution") or "")

    index = fetch_index(client, slug, cdn_url)
    pages = fetch_db(client, slug, settings.docs_downloads_dir, cdn_url)
    titles = page_titles_from_index(index)

    text_ref = f"docs/clean/{slug}.parquet"
    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")

    stats = {"pages": len(pages), "staged": 0, "skipped": 0, "deleted": 0}
    writer: pq.ParquetWriter | None = None
    doc_rows: list[tuple] = []
    current_ids: set[str] = set()
    try:
        with conn.cursor() as cur:
            # Guard a truncated/glitched db.json (a 200 with a {} or short body):
            # `current_ids` would stay empty, so EVERY previously-ingested page
            # falls into `missing` and gets tombstoned + its Qdrant point dropped,
            # and the docset is then banked 'done' at the new mtime — never
            # retried. Refuse when the fetch is empty but the ledger is not, so
            # ingest() marks the docset failed and retries without wiping it.
            if not pages and _ledger_ids_for_slug(cur, slug):
                raise RuntimeError(
                    f"docs {slug}: db.json fetched empty but the docset already has "
                    "ingested pages — refusing to tombstone (likely a truncated fetch)"
                )
            for chunk in _chunked(sorted(pages.items()), chunk_rows):
                rows = []
                for path, html in chunk:
                    upstream = upstream_url(html)
                    body = html.split(_ATTR_DIV, 1)[0]  # license boilerplate off the text
                    text = html_to_text(body)
                    title = page_title(body, titles.get(path))
                    rows.append({
                        "id": doc_id(slug, path),
                        "url": canonical_url(slug, path, upstream),
                        "title": title,
                        "text": text,
                        "thash": text_hash(title + "\n\n" + text),
                    })
                current_ids.update(r["id"] for r in rows)
                # The FULL page set goes to parquet (full-replace semantics —
                # unchanged pages must stay readable at this text_ref); only
                # the changed delta is queued for the ledger -> re-embed.
                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)
                writer.write_batch(
                    pa.record_batch(
                        [
                            pa.array([r["id"] for r in rows]),
                            pa.array([r["url"] for r in rows]),
                            pa.array([r["title"] for r in rows]),
                            pa.array([fw] * len(rows)),
                            pa.array([version] * len(rows)),
                            pa.array([attribution] * len(rows)),
                            pa.array([r["text"] for r in rows]),
                        ],
                        schema=CLEAN_SCHEMA,
                    )
                )
                existing = _existing_hashes(cur, [r["id"] for r in rows])
                delta = [r for r in rows if existing.get(r["id"]) != r["thash"]]
                stats["skipped"] += len(rows) - len(delta)
                for r in delta:
                    doc_rows.append((r["id"], r["url"], r["title"], r["thash"], text_ref))
                stats["staged"] += len(delta)

            missing = sorted(_ledger_ids_for_slug(cur, slug) - current_ids)

            if writer is not None:
                writer.close()
                writer = None
                tmp_path.rename(clean_path)

            # Change-aware ledger upsert: unchanged pages never reach here
            # (pre-filtered); the WHERE guards a race re-embedding an identical
            # row, while still resurrecting a tombstoned page that reappeared.
            cur.executemany(
                """
                INSERT INTO documents
                    (id, source, url, canonical_url, title, text_hash, status, text_ref)
                VALUES (%s, 'docs', %s, %s, %s, %s, 'deduped', %s)
                ON CONFLICT (id) DO UPDATE SET
                    url = EXCLUDED.url, canonical_url = EXCLUDED.canonical_url,
                    title = EXCLUDED.title, text_hash = EXCLUDED.text_hash,
                    text_ref = EXCLUDED.text_ref, status = 'deduped',
                    embedded_model = NULL, indexed_at = NULL
                WHERE documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                   OR documents.status = 'deleted'
                """,
                # sorted by id: the embed loop UPDATEs these same rows, and
                # locking them in a different order deadlocks (killed two wiki
                # shards 2026-07-16). Every batch writer to `documents` locks
                # in id order.
                sorted([(i, u, u, t, h, ref) for (i, u, t, h, ref) in doc_rows]),
            )
        conn.commit()
    except Exception:
        if writer is not None:
            writer.close()
        tmp_path.unlink(missing_ok=True)
        conn.rollback()
        raise
    stats["deleted"] = apply_tombstones(conn, settings, missing)
    return stats


def ingest(
    conn: psycopg.Connection,
    settings: Settings,
    max_docsets: int | None = None,
    max_consecutive_failures: int = 3,
    client: httpx.Client | None = None,
    cdn_url: str | None = None,
    pause_poll_seconds: float = 10.0,
) -> dict:
    """Process pending docsets one at a time, in seed-list order. Returns
    aggregate stats. A single failed docset is marked failed and skipped so a
    long refresh survives it; repeated back-to-back failures still abort. The
    dashboard pause flag is honored between docsets, never mid-docset."""
    cdn_url = cdn_url or settings.docs_cdn_url
    totals = {"docsets": 0, "pages": 0, "staged": 0, "skipped": 0, "deleted": 0}
    consecutive_failures = 0
    own = client is None
    client = client or httpx.Client(
        timeout=httpx.Timeout(30, read=300), follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )
    try:
        from windex.docs_source import sync as dsync

        pending = dsync.pending_docsets(conn, settings.docs_slug_list())
        if max_docsets is not None:
            pending = pending[:max_docsets]
        for docset in pending:
            # dashboard pause: honor it between docsets, never mid-docset (a
            # docset stages in one transaction; there is nothing to leave
            # half-applied while we wait here).
            while db.get_control(conn, "indexing", "running") == "paused":
                db.set_control(conn, "docs_stage", "paused")
                time.sleep(pause_poll_seconds)

            slug = docset["slug"]
            dsync.mark(conn, slug, "processing")
            db.set_control(conn, "docs_stage", f"ingesting {slug}")
            console.print(f"[bold]docset[/bold] {slug} ({docset.get('release') or 'unversioned'})")
            try:
                stats = stage_docset(conn, settings, docset, client, cdn_url)
                # ingested_mtime advances only now — a crash above leaves the
                # docset pending and the next run re-ingests it (idempotent:
                # the text_hash guard makes the re-run a no-op delta).
                dsync.mark(conn, slug, "done", stats, ingested_mtime=docset["mtime"])
                for k in ("pages", "staged", "skipped", "deleted"):
                    totals[k] += stats[k]
                totals["docsets"] += 1
                console.print(f"  {stats}")
                consecutive_failures = 0
            except Exception as exc:
                conn.rollback()
                dsync.mark(conn, slug, "failed")
                consecutive_failures += 1
                console.print(f"[red]docset {slug} failed[/red] ({exc}); continuing")
                if consecutive_failures >= max_consecutive_failures:
                    raise
    finally:
        db.set_control(conn, "docs_stage", "idle")
        if own:
            client.close()
    return totals
