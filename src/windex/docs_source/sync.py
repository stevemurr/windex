"""Sync the DevDocs manifest into the ``docsets`` watermark table.

Source: ``https://devdocs.io/docs.json`` (a 302 to the current manifest;
~363KB, 819 docsets, verified live 2026-07-16). Each entry carries ``slug``,
``release`` (upstream version), ``mtime`` (THE per-docset freshness
watermark), ``db_size``, and ``attribution`` (the upstream license HTML —
stored and carried into search payloads).

Idempotent, like every windex sync: re-running upserts the current manifest.
A docset is *pending* when its slug is in the configured seed list and its
manifest mtime has advanced past ``ingested_mtime`` (NULL = never ingested).
Everything manifest-format-specific lives here so a different upstream only
touches this module.
"""

import httpx
import psycopg

from windex.docs_source import USER_AGENT

MANIFEST_URL = "https://devdocs.io/docs.json"


def parse_manifest(data: list[dict]) -> list[dict]:
    """Normalize manifest entries to the columns we keep. Entries without a
    slug or mtime are dropped (nothing to watermark against)."""
    out = []
    for d in data:
        slug, mtime = d.get("slug"), d.get("mtime")
        if not slug or mtime is None:
            continue
        out.append({
            "slug": slug,
            "release": d.get("release") or "",
            "mtime": int(mtime),
            "db_size": int(d.get("db_size") or 0),
            "attribution": d.get("attribution") or "",
        })
    return out


def sync(conn: psycopg.Connection, client: httpx.Client | None = None,
         url: str = MANIFEST_URL) -> dict:
    """Fetch the manifest and upsert the ``docsets`` table. Returns
    ``{"total", "added", "updated"}`` (updated = rows whose mtime advanced).
    Ingest state (status / ingested_mtime / doc_counts) is never touched here —
    freshness is decided by mtime vs ingested_mtime, not by status."""
    own = client is None
    client = client or httpx.Client(
        timeout=60, follow_redirects=True, headers={"User-Agent": USER_AGENT}
    )
    try:
        resp = client.get(url)
        resp.raise_for_status()
        rows = parse_manifest(resp.json())
        added = updated = 0
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO docsets (slug, release, mtime, db_size, attribution)
                    VALUES (%(slug)s, %(release)s, %(mtime)s, %(db_size)s, %(attribution)s)
                    ON CONFLICT (slug) DO UPDATE SET
                        release = EXCLUDED.release, mtime = EXCLUDED.mtime,
                        db_size = EXCLUDED.db_size, attribution = EXCLUDED.attribution
                    WHERE docsets.mtime IS DISTINCT FROM EXCLUDED.mtime
                       OR docsets.release IS DISTINCT FROM EXCLUDED.release
                    RETURNING (xmax = 0) AS inserted
                    """,
                    r,
                )
                row = cur.fetchone()
                if row is not None:
                    added += int(row[0])
                    updated += int(not row[0])
        conn.commit()
        return {"total": len(rows), "added": added, "updated": updated}
    finally:
        if own:
            client.close()


def pending_docsets(conn: psycopg.Connection, slugs: list[str]) -> list[dict]:
    """Seed-list docsets whose upstream mtime has advanced past what was last
    fully ingested, in seed-list order (the user's chosen priority). Slugs not
    yet synced into the table are simply absent (run ``docs sync`` first)."""
    if not slugs:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT slug, release, mtime, attribution FROM docsets
            WHERE slug = ANY(%s)
              AND (ingested_mtime IS NULL OR mtime > ingested_mtime)
            """,
            (slugs,),
        )
        rows = {
            r[0]: {"slug": r[0], "release": r[1], "mtime": r[2], "attribution": r[3]}
            for r in cur.fetchall()
        }
    return [rows[s] for s in slugs if s in rows]


def mark(
    conn: psycopg.Connection,
    slug: str,
    status: str,
    stats: dict | None = None,
    ingested_mtime: int | None = None,
) -> None:
    """Record an ingest-run transition. ``ingested_mtime`` is only advanced on
    completion ('done'), so an interrupted docset stays pending."""
    import json

    with conn.cursor() as cur:
        cur.execute(
            """UPDATE docsets SET status = %s,
               doc_counts = coalesce(%s::jsonb, doc_counts),
               ingested_mtime = coalesce(%s, ingested_mtime),
               processed_at = CASE WHEN %s IN ('done', 'failed') THEN now() ELSE processed_at END
               WHERE slug = %s""",
            (status, json.dumps(stats) if stats else None, ingested_mtime, status, slug),
        )
    conn.commit()
