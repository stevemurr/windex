"""Sync huggingface.co's enumerable surface into the hf_roots / hf_posts
watermarks.

THE SITEMAP TRAP. ``sitemap.xml`` is an index of 7 shards, and only two of them
are catalogs — the rest are recency windows wearing a sitemap's clothes:

    sitemap-doc.xml       52 URLs   2023-07-28 → today   COMPLETE (doc roots)
    sitemap-blog.xml     829 URLs   2020-02-14 → today   COMPLETE (whole archive)
    sitemap-models.xml     6,026 of 2.9M (0.2%), sorted by recency
    sitemap-datasets.xml   6,729 spanning EIGHT DAYS
    sitemap-spaces.xml     ~10k cap
    sitemap-papers.xml     exactly 10,000 (capped)

Treating one of the bottom four as a frontier would silently index a random
recent slice of the Hub while looking like it works. So this module reads the
index and takes ONLY the doc and blog shards, by name, and ignores the rest
loudly (see WANTED_SHARDS). If HF ever adds a shard, it is ignored until someone
decides it is a catalog.

Freshness, mirroring ``docsets`` exactly:

  * Doc roots: the per-root ``llms.txt`` (a titled index of every page as a .md
    link) is fetched and HASHED here. ``llms_hash`` is the upstream watermark;
    ``ingested_hash`` is what the crawl last completed. A root is pending when
    they differ. This is why a refresh is ~55 requests instead of a 3.3-hour
    re-sweep, and the reason the gate is a hash rather than a 304: a 304 still
    SPENDS a request against the pages bucket, so conditional GETs would not
    make the refresh cheap — the hash gate is load-bearing, not an optimization.
  * Blog posts: ``sitemap-blog.xml``'s ``lastmod`` is the watermark; a post is
    pending when lastmod advances past ``ingested_lastmod``.

Everything sitemap/llms.txt-format-specific lives here so a different upstream
only touches this module.
"""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

import httpx
import psycopg

from windex.hf import BASE_URL, USER_AGENT, license_for

SITEMAP_URL = f"{BASE_URL}/sitemap.xml"

# The only two shards that can enumerate anything. See the module docstring:
# this allowlist IS the scope decision, so widening it is a deliberate act.
WANTED_SHARDS = ("sitemap-doc.xml", "sitemap-blog.xml")

_SM_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"

# `- [Quickstart](https://huggingface.co/docs/transformers/v5.14.0/quicktour.md)`
_LINK_RE = re.compile(r"^\s*-\s*\[([^\]]*)\]\((\S+?)\)\s*$", re.M)
# A version segment is `v` + digit + version-ish chars: v5.14.0, v0.35.2.
# Anchored on `v\d` on purpose — a page legitimately named `main_classes` or
# `visualization` must NOT be mistaken for a version and swallowed.
_VER_RE = re.compile(r"v\d[\w.]*")


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()


# --- sitemap ---------------------------------------------------------------

def parse_sitemap_index(xml: str) -> list[str]:
    """Shard URLs from the sitemap index, filtered to WANTED_SHARDS."""
    root = ET.fromstring(xml)
    locs = [e.text.strip() for e in root.iter(f"{_SM_NS}loc") if e.text]
    return [u for u in locs if u.rsplit("/", 1)[-1] in WANTED_SHARDS]


def parse_urlset(xml: str) -> list[tuple[str, str]]:
    """(loc, lastmod) pairs from a sitemap shard; lastmod is "" when absent."""
    root = ET.fromstring(xml)
    out = []
    for url in root.iter(f"{_SM_NS}url"):
        loc = url.findtext(f"{_SM_NS}loc") or ""
        if not loc.strip():
            continue
        out.append((loc.strip(), (url.findtext(f"{_SM_NS}lastmod") or "").strip()))
    return out


def root_key(url: str) -> str:
    """`https://huggingface.co/docs/transformers` -> `docs/transformers`.

    The root key is the URL path, which makes doc ids (`hf:` + path + `/` +
    page) literally the canonical URL path — no slug→base_url rule table, none
    of docs_source/canonical.py's pain.
    """
    return urlsplit(url).path.strip("/")


def kind_of(root: str) -> str:
    """`docs/transformers` -> `docs`; `learn/agents-course` -> `learn`."""
    return root.split("/", 1)[0]


def blog_slug(url: str) -> str:
    """`https://huggingface.co/blog/nvidia/foo` -> `nvidia/foo`.

    Slugs are NOT always flat: org-authored posts are namespaced, so a slug can
    contain a slash. Ids carry it verbatim (`hf:blog/nvidia/foo`).
    """
    path = urlsplit(url).path.strip("/")
    return path[len("blog/"):] if path.startswith("blog/") else path


# --- llms.txt --------------------------------------------------------------

def parse_llms(text: str, root: str, base_url: str = BASE_URL) -> list[dict]:
    """`[{path, title, version}]` for the .md links llms.txt lists under `root`.

    Links are version-pinned (`/v5.14.0/quicktour.md`); the version is split out
    and recorded, never kept in the doc id — a version bump must UPSERT the same
    document, not fork a new one. Links outside this root (or non-.md) are
    dropped: llms.txt is a published index, not a promise about its own shape.
    First occurrence of a path wins.
    """
    prefix = f"{base_url}/{root}/"
    out: list[dict] = []
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
        out.append({"path": path, "title": title.strip(), "version": version})
    return out


def llms_url(root: str, base_url: str = BASE_URL) -> str:
    return f"{base_url}/{root}/llms.txt"


def root_version(pages: list[dict]) -> str:
    """The version the root's links are pinned to (first non-empty wins)."""
    for p in pages:
        if p["version"]:
            return p["version"]
    return ""


# --- watermark upserts -----------------------------------------------------

def upsert_roots(conn: psycopg.Connection, roots: list[tuple[str, str]]) -> dict:
    """Upsert (loc, lastmod) doc-root pairs into hf_roots. Ingest state
    (ingested_hash / status) is never touched here — freshness is decided by
    llms_hash vs ingested_hash, not by status."""
    added = 0
    with conn.cursor() as cur:
        for loc, lastmod in roots:
            root = root_key(loc)
            if not root:
                continue
            cur.execute(
                """
                INSERT INTO hf_roots (root, kind, url, lastmod, license)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (root) DO UPDATE SET
                    url = EXCLUDED.url, lastmod = EXCLUDED.lastmod,
                    license = EXCLUDED.license
                RETURNING (xmax = 0) AS inserted
                """,
                (root, kind_of(root), loc, lastmod or None, license_for(root)),
            )
            row = cur.fetchone()
            added += int(bool(row and row[0]))
    conn.commit()
    return {"roots": len(roots), "added": added}


def upsert_posts(conn: psycopg.Connection, posts: list[tuple[str, str]]) -> dict:
    """Upsert (loc, lastmod) blog pairs into hf_posts. A post whose lastmod
    advances becomes pending again — that is the whole blog freshness gate."""
    added = updated = 0
    with conn.cursor() as cur:
        for loc, lastmod in posts:
            slug = blog_slug(loc)
            if not slug:
                continue
            cur.execute(
                """
                INSERT INTO hf_posts (slug, url, lastmod) VALUES (%s, %s, %s)
                ON CONFLICT (slug) DO UPDATE SET
                    url = EXCLUDED.url, lastmod = EXCLUDED.lastmod
                WHERE hf_posts.lastmod IS DISTINCT FROM EXCLUDED.lastmod
                RETURNING (xmax = 0) AS inserted
                """,
                (slug, loc, lastmod or ""),
            )
            row = cur.fetchone()
            if row is not None:
                added += int(row[0])
                updated += int(not row[0])
    conn.commit()
    return {"posts": len(posts), "added": added, "updated": updated}


def mark_root_llms(conn: psycopg.Connection, root: str, llms_hash: str | None,
                   pages: int, version: str, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE hf_roots SET llms_hash = %s, pages = %s, version = %s, status = %s "
            "WHERE root = %s",
            (llms_hash, pages, version, status, root),
        )
    conn.commit()


def refresh_llms(conn: psycopg.Connection, fetcher, roots: list[str],
                 base_url: str = BASE_URL) -> dict:
    """Fetch + hash each root's llms.txt — the freshness sweep. ~52 requests
    (~3 min at 1 req/3s); a quiet day costs this and little else.

    A root whose llms.txt 404s gets status='no_llms' and a NULL llms_hash, which
    keeps it permanently out of the crawl's pending set. That is deliberate: the
    docs nav is client-rendered (a page's HTML exposes 21 links, 17 of them JS
    chunks), so for a root without llms.txt there is no enumeration path at all
    — an HTML fallback could only ever fetch the landing page. 5 of 52 roots,
    ~2% of the corpus. Skipping beats indexing one orphan page per root.
    """
    stats = {"checked": 0, "with_llms": 0, "no_llms": 0, "pages": 0}
    for root in roots:
        body = fetcher.fetch(llms_url(root, base_url))
        stats["checked"] += 1
        if body is None:
            mark_root_llms(conn, root, None, 0, "", "no_llms")
            stats["no_llms"] += 1
            continue
        pages = parse_llms(body, root, base_url)
        mark_root_llms(conn, root, sha1(body), len(pages), root_version(pages), "pending")
        stats["with_llms"] += 1
        stats["pages"] += len(pages)
    return stats


# --- pending selection -----------------------------------------------------

def configured_roots(conn: psycopg.Connection, wanted: list[str]) -> list[str]:
    """The roots to crawl: the configured list, or every synced root when the
    list is empty (the default — all 52 roots is only ~3,175 pages)."""
    with conn.cursor() as cur:
        if wanted:
            cur.execute("SELECT root FROM hf_roots WHERE root = ANY(%s) ORDER BY root",
                        (wanted,))
        else:
            cur.execute("SELECT root FROM hf_roots ORDER BY root")
        return [r[0] for r in cur.fetchall()]


def pending_roots(conn: psycopg.Connection, wanted: list[str]) -> list[dict]:
    """Roots whose llms.txt hash has moved past what was last fully ingested.

    NOTE what this does NOT look at: `status`. Pending-ness is decided purely by
    llms_hash vs ingested_hash, exactly as docsets decides on mtime vs
    ingested_mtime. That is what makes a killed crawl safe — a row left in
    'processing' forever is still pending here and gets re-crawled, so there is
    no stale claim to reclaim and no way for a SIGKILL to strand a root the way
    a status-gated queue once stranded 3 years of arXiv. `status` is progress
    reporting; the hash is the truth.
    """
    sql = """
        SELECT root, kind, llms_hash, version, license FROM hf_roots
        WHERE llms_hash IS NOT NULL
          AND (ingested_hash IS NULL OR llms_hash IS DISTINCT FROM ingested_hash)
    """
    params: tuple = ()
    if wanted:
        sql += " AND root = ANY(%s)"
        params = (wanted,)
    sql += " ORDER BY root"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [
            {"root": r[0], "kind": r[1], "llms_hash": r[2], "version": r[3], "license": r[4]}
            for r in cur.fetchall()
        ]


def pending_posts(conn: psycopg.Connection, limit: int) -> list[dict]:
    """Blog posts whose sitemap lastmod advanced past what was ingested.
    Newest first — a fresh post is worth more than a 2020 backfill page."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT slug, url, lastmod FROM hf_posts
            WHERE ingested_lastmod IS NULL OR lastmod > ingested_lastmod
            ORDER BY lastmod DESC, slug LIMIT %s
            """,
            (limit,),
        )
        return [{"slug": r[0], "url": r[1], "lastmod": r[2]} for r in cur.fetchall()]


def mark_root(conn: psycopg.Connection, root: str, status: str,
              stats: dict | None = None, ingested_hash: str | None = None) -> None:
    """Record a crawl-run transition. ``ingested_hash`` only advances on a
    complete root, so an interrupted or partial root stays pending."""
    import json

    with conn.cursor() as cur:
        cur.execute(
            """UPDATE hf_roots SET status = %s,
               doc_counts = coalesce(%s::jsonb, doc_counts),
               ingested_hash = coalesce(%s, ingested_hash),
               processed_at = CASE WHEN %s IN ('done', 'failed', 'partial')
                                   THEN now() ELSE processed_at END
               WHERE root = %s""",
            (status, json.dumps(stats) if stats else None, ingested_hash, status, root),
        )
    conn.commit()


def mark_post(conn: psycopg.Connection, slug: str, status: str,
              ingested_lastmod: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE hf_posts SET status = %s,
               ingested_lastmod = coalesce(%s, ingested_lastmod),
               processed_at = now() WHERE slug = %s""",
            (status, ingested_lastmod, slug),
        )
    conn.commit()


# --- entry point -----------------------------------------------------------

def sync(conn: psycopg.Connection, settings, client: httpx.Client | None = None,
         url: str | None = None, refresh: bool = True) -> dict:
    """Fetch the sitemap index → doc roots + blog posts, then (refresh=True)
    re-hash every configured root's llms.txt.

    Idempotent: re-running upserts the current sitemap and re-hashes. ~55
    requests, ~3 minutes at HF's 1-req/3s. Returns aggregate stats.
    """
    from windex.hf.fetch import build_fetcher

    own = client is None
    client = client or httpx.Client(
        timeout=httpx.Timeout(settings.hf_request_timeout, read=60),
        follow_redirects=True, headers={"User-Agent": USER_AGENT},
    )
    try:
        fetcher = build_fetcher(client, settings)
        index_url = url or settings.hf_sitemap_url
        body = fetcher.fetch(index_url)
        if body is None:
            raise RuntimeError(f"sitemap index unreachable: {index_url}")
        # Nested per shard: both upserts report an "added", and flattening them
        # into one dict made 52 roots report 829 added.
        out: dict = {"doc": {}, "blog": {}}
        for shard in parse_sitemap_index(body):
            shard_body = fetcher.fetch(shard)
            if shard_body is None:
                raise RuntimeError(f"sitemap shard unreachable: {shard}")
            entries = parse_urlset(shard_body)
            if shard.endswith("sitemap-doc.xml"):
                out["doc"] = upsert_roots(conn, entries)
            elif shard.endswith("sitemap-blog.xml"):
                out["blog"] = upsert_posts(conn, entries)
        if refresh:
            roots = configured_roots(conn, settings.hf_root_list())
            out["llms"] = refresh_llms(conn, fetcher, roots, settings.hf_base_url)
        return out
    finally:
        if own:
            client.close()
