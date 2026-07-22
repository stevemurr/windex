"""Crawl the pending Hugging Face doc roots and blog posts into clean parquet.

Two shapes, one source:

  * **Docs / courses.** HF serves every doc page as CLEAN MARKDOWN
    (`/docs/transformers/quicktour.md` → 200, text/markdown, `# Quickstart…`),
    so there is no extraction to do and no trafilatura in this path — the body
    goes straight to staging. llms.txt is the enumeration (BFS link-crawling
    cannot work: the sidebar toctree is client-rendered, a page's HTML exposes
    21 links and 17 are JS chunks). Full-replace per root, like the DevDocs
    source: the whole page set is rewritten to one parquet so text_ref stays
    valid for unchanged pages, while the ledger's text_hash guard keeps
    re-embedding to the changed-page delta.
  * **Blog.** No `.md` (verified 404) → HTML + trafilatura, which extracts
    cleanly (~5k chars). Staged in batches like smallweb, keyed on the
    sitemap's lastmod.

NO QUALITY FILTERS, deliberately. docs/smallweb-source.md already warns the
FineWeb/C4 filters "over-reject short/idiosyncratic" text, and an API reference
page is exactly that shape. This corpus is curated by construction — the same
call the DevDocs source made. **The quality filter here IS the scope decision**
(hf/__init__.py), not a text classifier. Skipping them also dissolves the spaCy
main-thread constraint for docs: nothing is extracted, so there is no shared
tokenizer to corrupt.

VERSIONS. llms.txt links are version-pinned but the page declares
`rel=canonical` pointing at the UNVERSIONED URL, which serves byte-identical
content (13,126 B both ways, verified). We fetch the unversioned URL and link to
it: index what you link. The version is recorded in the payload but kept OUT of
the doc id, so a version bump upserts the same document instead of forking one.

CHUNKING — we do not. Pages are staged WHOLE and the shared embed driver bounds
the embedded text at `embed_max_tokens`, exactly as it does for wiki articles
and DevDocs pages. See embed_index.py for why.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import httpx
import psycopg
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from rich.console import Console

from windex import db
from windex.ccnews.dedup import text_hash
from windex.config import Settings
from windex.hf import BASE_URL, USER_AGENT
from windex.hf import sync as hsync

console = Console()

# One schema for both shapes so a single SourceSpec reads every hf text_ref.
CLEAN_SCHEMA = pa.schema(
    [
        ("id", pa.string()),            # hf:docs/transformers/quicktour | hf:blog/<slug>
        ("url", pa.string()),           # canonical, unversioned
        ("title", pa.string()),
        ("kind", pa.string()),          # docs | learn | blog
        ("root", pa.string()),          # transformers | agents-course | blog
        ("version", pa.string()),       # observed vX.Y.Z (recorded, never in the id)
        ("license", pa.string()),       # per-root upstream license ("" = unchecked)
        ("published_at", pa.string()),  # blog only ("" for docs — reference pages aren't dated)
        ("text", pa.string()),
    ]
)


def doc_id(root: str, path: str) -> str:
    """`docs/transformers` + `quicktour` -> `hf:docs/transformers/quicktour`.

    The id is `hf:` + the canonical URL path. Namespaced (several probes rely on
    an id's prefix matching its source) and version-free on purpose.
    """
    return f"hf:{root}/{path}"


def blog_doc_id(slug: str) -> str:
    return f"hf:blog/{slug}"


def page_url(root: str, path: str, base_url: str = BASE_URL) -> str:
    """The canonical (unversioned) page URL — what rel=canonical declares."""
    return f"{base_url}/{root}/{path}"


def md_url(root: str, path: str, base_url: str = BASE_URL) -> str:
    return page_url(root, path, base_url) + ".md"


def root_name(root: str) -> str:
    """`docs/transformers` -> `transformers` — the payload's `root` filter value."""
    return root.split("/", 1)[-1]


def text_ref_for(root: str) -> str:
    """One parquet per root. The key contains a slash (`docs/transformers`), so
    it is flattened rather than nesting a directory per kind."""
    return f"hf/clean/{root.replace('/', '__')}.parquet"


def md_title(body: str, fallback: str = "") -> str:
    """First markdown `# ` heading; falls back to llms.txt's link text (which is
    human-written and already good)."""
    for line in body.splitlines():
        line = line.strip()
        if line.startswith("# "):
            title = line[2:].strip()
            if title:
                return title
        if line and not line.startswith("#"):
            break  # past the front matter: no leading h1
    return fallback.strip()


def _batch(rows: list[dict]) -> pa.RecordBatch:
    cols = ["id", "url", "title", "kind", "root", "version", "license", "published_at", "text"]
    return pa.record_batch(
        [pa.array([r[c] for r in rows]) for c in cols], schema=CLEAN_SCHEMA
    )


# --- ledger helpers (mirroring docs_source/ingest.py) -----------------------

def _existing_hashes(cur: psycopg.Cursor, ids: list[str]) -> dict[str, str]:
    """id -> text_hash for live ledger rows. Tombstoned rows are excluded so a
    page that reappears (even byte-identical) re-stages."""
    if not ids:
        return {}
    # No `source =` predicate: ids are namespaced (hf:, hn:, wiki:, …) so an id
    # list can't match another source. Including it makes the planner pick
    # documents_source_published_idx and scan every row of the source
    # (244s vs 63ms — see docs_source/ingest.py).
    cur.execute(
        "SELECT id, text_hash FROM documents WHERE status <> 'deleted' AND id = ANY(%s)",
        (ids,),
    )
    return dict(cur.fetchall())


def _ledger_ids_for_root(cur: psycopg.Cursor, root: str) -> set[str]:
    cur.execute(
        "SELECT id FROM documents WHERE source = 'hf' AND status <> 'deleted' "
        "AND starts_with(id, %s)",
        (f"hf:{root}/",),
    )
    return {r[0] for r in cur.fetchall()}


def apply_tombstones(conn: psycopg.Connection, settings: Settings,
                     doc_ids: list[str]) -> int:
    """Mark vanished-page ledger rows deleted and drop their Qdrant points.
    Qdrant removal is best-effort: a down index still leaves the ledger
    tombstoned (the point goes on the next reindex)."""
    if not doc_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE documents SET status = 'deleted', embedded_model = NULL, "
            "indexed_at = NULL WHERE id = ANY(%s)",  # see note above: no source predicate
            (sorted(doc_ids),),
        )
        marked = cur.rowcount or 0
    conn.commit()
    try:
        from qdrant_client import QdrantClient
        from qdrant_client import models as qm

        from windex.embed.pipeline import point_id
        from windex.index import qdrant as qidx

        client = QdrantClient(url=settings.qdrant_url, timeout=30)
        client.delete(
            collection_name=qidx.alias_name("hf"),
            points_selector=qm.PointIdsList(points=[point_id(i) for i in doc_ids]),
            wait=True,  # tombstones are rare; deletion should be visible on return
        )
    except Exception as exc:  # index absent/unreachable: ledger tombstone stands
        console.print(f"[yellow]hf tombstone: qdrant delete skipped ({exc})[/yellow]")
    return marked


def _carry_forward(text_ref_path, ids: list[str], columns: list[str]) -> list[dict]:
    """Rows for `ids` from the EXISTING staged parquet, if it has them.

    Why this exists: staging is full-replace per root, so a page that fails to
    fetch this run would vanish from the new parquet while its ledger row still
    points at that text_ref — the embed reader would then find nothing for it and
    it would sit 'deduped' forever, embedded by no one and erroring for no one.
    Carrying the previous text forward keeps the parquet complete for every page
    that still exists upstream. (The root also stays pending — see stage_root —
    so the real page is retried next run.)
    """
    if not ids or not text_ref_path.exists():
        return []
    # Do NOT swallow a read failure here. An existing file with no matching ids
    # legitimately returns an empty table (no exception); the only thing an except
    # would catch is a genuine read failure (corrupt/truncated file, staging drive
    # detached). Swallowing it as "nothing to carry" drops the failed page from
    # the full-replace rewrite while its ledger row still points at this text_ref,
    # leaving it permanently unreadable. Let it propagate so stage_root aborts this
    # root's rewrite (leaving the previous parquet+ledger consistent) and the
    # caller retries — exactly what embed/pipeline._reader does for the same drive.
    table = ds.dataset(text_ref_path, format="parquet").to_table(
        columns=columns, filter=ds.field("id").isin(ids)
    )
    return table.to_pylist()


# --- docs -------------------------------------------------------------------

def stage_root(conn: psycopg.Connection, settings: Settings, root: dict,
               fetcher, base_url: str = BASE_URL) -> dict:
    """Fetch one doc root's `.md` pages and full-replace its staging partition.

    Returns stats incl. `failed` (pages listed by llms.txt we could not fetch).
    The caller only advances `ingested_hash` when `failed == 0`, so a partial
    root stays pending and retries — the hash gate makes the retry a near-no-op
    for the pages that already landed (text_hash guard → no re-embed).
    """
    key = root["root"]
    name, kind = root_name(key), root["kind"]
    license_ = root["license"] or ""

    # Re-fetch llms.txt rather than trusting sync's snapshot: it costs ONE
    # request against a root that is about to cost hundreds, and it means the
    # hash we record as ingested is the hash of the list we actually ingested —
    # no race where llms.txt moved between sync and crawl and we bank a stale
    # watermark.
    listing = fetcher.fetch(hsync.llms_url(key, base_url))
    if listing is None:
        return {"pages": 0, "fetched": 0, "staged": 0, "skipped": 0,
                "failed": 0, "deleted": 0, "llms_hash": None}
    llms_hash = hsync.sha1(listing)
    pages = hsync.parse_llms(listing, key, base_url)
    version = hsync.root_version(pages)

    text_ref = text_ref_for(key)
    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")

    stats = {"pages": len(pages), "fetched": 0, "staged": 0, "skipped": 0,
             "failed": 0, "deleted": 0, "llms_hash": llms_hash}
    rows: list[dict] = []
    failed_ids: list[str] = []
    for page in pages:
        path = page["path"]
        did = doc_id(key, path)
        body = fetcher.fetch(md_url(key, path, base_url))
        if body is None:
            failed_ids.append(did)
            stats["failed"] += 1
            continue
        rows.append({
            "id": did,
            "url": page_url(key, path, base_url),  # canonical: unversioned
            "title": md_title(body, page["title"]),
            "kind": kind,
            "root": name,
            "version": page["version"] or version,
            "license": license_,
            "published_at": "",  # reference pages aren't dated
            "text": body,
        })
        stats["fetched"] += 1

    if not pages:
        # llms.txt fetched but lists no pages for this root. If the root already
        # has ingested pages, this is almost certainly a truncated/glitched
        # listing or an upstream format change — NOT a real "every page vanished".
        # Mark it failed (non-zero) so the caller keeps the root pending and
        # retries, rather than banking the empty hash as 'done' (falsely synced,
        # never revisited) or tombstoning the whole root (mass-wipe on a glitch).
        # A genuinely-empty root with nothing prior is fine to record as-is.
        with conn.cursor() as cur:
            had_pages = bool(_ledger_ids_for_root(cur, key))
        if had_pages:
            stats["failed"] = 1
        return stats

    listed_ids = {doc_id(key, p["path"]) for p in pages}
    carried = _carry_forward(clean_path, failed_ids, [f.name for f in CLEAN_SCHEMA])
    all_rows = sorted(rows + carried, key=lambda r: r["id"])

    writer: pq.ParquetWriter | None = None
    try:
        with conn.cursor() as cur:
            missing = sorted(_ledger_ids_for_root(cur, key) - listed_ids)
            if all_rows:
                writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)
                writer.write_batch(_batch(all_rows))
                writer.close()
                writer = None
                tmp_path.rename(clean_path)

            # Only pages we actually fetched reach the ledger: a carried-forward
            # row's ledger entry is already correct and its text_ref name is
            # unchanged, so touching it would only churn a re-embed.
            fresh = [dict(r, thash=text_hash(r["title"] + "\n\n" + r["text"])) for r in rows]
            existing = _existing_hashes(cur, [r["id"] for r in fresh])
            delta = [r for r in fresh if existing.get(r["id"]) != r["thash"]]
            stats["skipped"] = len(fresh) - len(delta)
            stats["staged"] = len(delta)
            cur.executemany(
                """
                INSERT INTO documents
                    (id, source, url, canonical_url, title, text_hash, status, text_ref)
                VALUES (%s, 'hf', %s, %s, %s, %s, 'deduped', %s)
                ON CONFLICT (id) DO UPDATE SET
                    url = EXCLUDED.url, canonical_url = EXCLUDED.canonical_url,
                    title = EXCLUDED.title, text_hash = EXCLUDED.text_hash,
                    text_ref = EXCLUDED.text_ref, status = 'deduped',
                    embedded_model = NULL, indexed_at = NULL
                WHERE documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                   OR documents.status = 'deleted'
                """,
                # sorted by id: the embed loop UPDATEs these same rows, and
                # locking them in a different order deadlocks (it killed two wiki
                # shards on 2026-07-16, losing 5% of that corpus). Every batch
                # writer to `documents` locks in id order.
                sorted(
                    (r["id"], r["url"], r["url"], r["title"], r["thash"], text_ref)
                    for r in delta
                ),
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


# --- blog -------------------------------------------------------------------

def extract_post(html: str, url: str) -> dict | None:
    """Blog HTML -> {title, text, published_at} via trafilatura, or None.

    Reuses smallweb's extraction seam (`extract_html` = bare_extraction with
    metadata) but NOT its quality gate: `build_quality_filters` is skipped
    entirely here — see the module docstring.
    """
    from windex.smallweb.extract import extract_html

    parsed = extract_html(html, url)
    if parsed is None:
        return None
    text, meta = parsed
    return {
        "title": (meta.get("title") or "").strip(),
        "text": text,
        "published_at": (meta.get("date") or "") or "",
    }


def stage_posts(conn: psycopg.Connection, settings: Settings, posts: list[dict],
                fetcher, text_ref: str) -> dict:
    """Fetch + extract a batch of blog posts and stage them to one parquet.

    Change-aware like the docs path (a post can be edited — its sitemap lastmod
    moves, the text_hash guard decides whether that costs a re-embed).
    """
    stats = {"posts": len(posts), "fetched": 0, "staged": 0, "skipped": 0, "failed": 0}
    rows: list[dict] = []
    for post in posts:
        html = fetcher.fetch(post["url"])
        if html is None:
            stats["failed"] += 1
            continue
        extracted = extract_post(html, post["url"])
        if extracted is None:
            stats["failed"] += 1
            continue
        stats["fetched"] += 1
        rows.append({
            "id": blog_doc_id(post["slug"]),
            "url": post["url"],
            "title": extracted["title"],
            "kind": "blog",
            "root": "blog",
            "version": "",
            "license": "",  # 829 posts, HF staff + community orgs, no blanket license
            "published_at": extracted["published_at"] or post["lastmod"] or "",
            "text": extracted["text"],
            "slug": post["slug"],
            "lastmod": post["lastmod"],
        })
    if not rows:
        return stats

    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")
    writer: pq.ParquetWriter | None = None
    try:
        with conn.cursor() as cur:
            fresh = [dict(r, thash=text_hash(r["title"] + "\n\n" + r["text"])) for r in rows]
            existing = _existing_hashes(cur, [r["id"] for r in fresh])
            delta = [r for r in fresh if existing.get(r["id"]) != r["thash"]]
            stats["skipped"] = len(fresh) - len(delta)
            # The parquet holds only the delta: unlike a doc root there is no
            # full-replace contract per file (blog batches are append-only
            # partitions), so an unchanged post keeps pointing at the older
            # parquet that still holds its text.
            if delta:
                writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)
                writer.write_batch(_batch(sorted(delta, key=lambda r: r["id"])))
                writer.close()
                writer = None
                tmp_path.rename(clean_path)
                cur.executemany(
                    """
                    INSERT INTO documents
                        (id, source, url, canonical_url, title, published_at,
                         text_hash, status, text_ref)
                    VALUES (%s, 'hf', %s, %s, %s, %s, %s, 'deduped', %s)
                    ON CONFLICT (id) DO UPDATE SET
                        url = EXCLUDED.url, canonical_url = EXCLUDED.canonical_url,
                        title = EXCLUDED.title, published_at = EXCLUDED.published_at,
                        text_hash = EXCLUDED.text_hash, text_ref = EXCLUDED.text_ref,
                        status = 'deduped', embedded_model = NULL, indexed_at = NULL
                    WHERE documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                       OR documents.status = 'deleted'
                    """,
                    # sorted by id — same deadlock contract as every other writer.
                    sorted(
                        (r["id"], r["url"], r["url"], r["title"],
                         _parse_ts(r["published_at"]), r["thash"], text_ref)
                        for r in delta
                    ),
                )
                stats["staged"] = len(delta)
        conn.commit()
    except Exception:
        if writer is not None:
            writer.close()
        tmp_path.unlink(missing_ok=True)
        conn.rollback()
        raise
    # Watermark AFTER the text is durably staged: a crash above leaves these
    # posts pending, and re-fetching them is a text_hash no-op.
    for r in rows:
        hsync.mark_post(conn, r["slug"], "done", ingested_lastmod=r["lastmod"])
    return stats


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


# --- entry point ------------------------------------------------------------

def crawl(conn: psycopg.Connection, settings: Settings, max_roots: int | None = None,
          max_posts: int | None = None, client: httpx.Client | None = None,
          pause_poll_seconds: float = 10.0, max_consecutive_failures: int = 3) -> dict:
    """Crawl pending doc roots, then pending blog posts.

    ~3.3h cold (4,014 pages at HF's 1 req/3s), minutes warm — the llms.txt hash
    gate means an unchanged root costs ONE request, not 727. Idempotent and
    resumable: work is selected from the watermarks, every root/post advances
    its watermark only after its text is staged, and a killed run leaves
    everything it hadn't finished pending (pending-ness never consults `status`
    — see sync.pending_roots).
    """
    from windex.hf.fetch import build_fetcher

    totals = {"roots": 0, "pages": 0, "staged": 0, "skipped": 0, "failed": 0,
              "deleted": 0, "posts": 0, "posts_staged": 0, "posts_failed": 0}
    own = client is None
    client = client or httpx.Client(
        timeout=httpx.Timeout(settings.hf_request_timeout, read=60),
        follow_redirects=True, headers={"User-Agent": USER_AGENT},
    )
    consecutive_failures = 0
    try:
        fetcher = build_fetcher(client, settings)
        pending = hsync.pending_roots(conn, settings.hf_root_list())
        if max_roots is not None:
            pending = pending[:max_roots]
        for root in pending:
            # dashboard pause: honored between roots, never mid-root (a root
            # stages in one transaction; there is nothing to leave half-applied
            # while we wait here).
            while db.get_control(conn, "indexing", "running") == "paused":
                db.set_control(conn, "hf_stage", "paused")
                time.sleep(pause_poll_seconds)

            key = root["root"]
            hsync.mark_root(conn, key, "processing")
            db.set_control(conn, "hf_stage", f"crawling {key}")
            console.print(f"[bold]hf root[/bold] {key} ({root['version'] or 'unversioned'})")
            try:
                stats = stage_root(conn, settings, root, fetcher, settings.hf_base_url)
                if stats["failed"] or stats["llms_hash"] is None:
                    # Partial: some listed page didn't come back. ingested_hash
                    # does NOT advance, so this root is still pending and the
                    # next run retries only what's missing (staged pages are a
                    # text_hash no-op).
                    hsync.mark_root(conn, key, "partial", stats)
                else:
                    hsync.mark_root(conn, key, "done", stats,
                                    ingested_hash=stats["llms_hash"])
                for k in ("pages", "staged", "skipped", "failed", "deleted"):
                    totals[k] += stats[k]
                totals["roots"] += 1
                console.print(f"  {stats}")
                consecutive_failures = 0
            except Exception as exc:
                conn.rollback()
                hsync.mark_root(conn, key, "failed")
                consecutive_failures += 1
                console.print(f"[red]hf root {key} failed[/red] ({exc}); continuing")
                if consecutive_failures >= max_consecutive_failures:
                    raise

        # --- blog
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        batch_idx = 0
        # A post that fails to fetch keeps its watermark (we WANT it retried on
        # a later run — a 502 is not a reason to drop a post forever), which
        # means pending_posts would hand back the same failures until the end of
        # time. Bound the run instead of the post: a slug attempted here is not
        # attempted again in THIS run, so the loop drains and exits.
        attempted: set[str] = set()
        while max_posts is None or totals["posts"] < max_posts:
            while db.get_control(conn, "indexing", "running") == "paused":
                db.set_control(conn, "hf_stage", "paused")
                time.sleep(pause_poll_seconds)

            remaining = None if max_posts is None else max_posts - totals["posts"]
            n = settings.hf_blog_batch if remaining is None else min(
                settings.hf_blog_batch, remaining)
            posts = [p for p in hsync.pending_posts(conn, n + len(attempted))
                     if p["slug"] not in attempted][:n]
            if not posts:
                break
            attempted.update(p["slug"] for p in posts)
            db.set_control(conn, "hf_stage", f"crawling {len(posts)} blog posts")
            stats = stage_posts(conn, settings, posts, fetcher,
                                f"hf/clean/blog/{run_id}_{batch_idx:04d}.parquet")
            totals["posts"] += len(posts)
            totals["posts_staged"] += stats["staged"]
            totals["posts_failed"] += stats["failed"]
            console.print(f"[bold]hf blog[/bold] batch {batch_idx}: {stats}")
            batch_idx += 1
    finally:
        db.set_control(conn, "hf_stage", "idle")
        if own:
            client.close()
    return totals
