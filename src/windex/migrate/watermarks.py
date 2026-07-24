"""Backfill `source_units` from the twelve legacy watermark tables.

WHY THIS IS THE CAREFUL PHASE. Getting a watermark wrong has two failure modes
with wildly different costs:

  * **Re-ingest** — a `done` unit looks pending. Expensive in bandwidth and CPU,
    but the `text_hash`-guarded ledger makes it a document-level no-op. Recoverable.
  * **Skip** — a `pending` unit looks done. Silent data loss, discovered weeks
    later when someone notices a gap. **Unacceptable.**

So this is a copy, never a move; `verify()` compares the two models' PENDING SETS
element-by-element (not just counts, which can agree while the membership differs);
and any divergence is reported with a named reason rather than smoothed over.

THE ENCODING. The new model has one freshness gate for every source:

    upstream IS DISTINCT FROM ingested   ->  this unit needs work

Each legacy table expressed that differently, and the mapping below reproduces
each one. Three shapes appear:

  * **status-gated** (warc_files, gharchive_files, wiki_dumps, arxiv_windows,
    hn_windows) — the only signal is "have we finished this once". Encoded with a
    sentinel: `upstream = {"seen": 1}` always, `ingested = {"seen": 1}` iff the
    row is done. Distinct exactly when not done.
  * **token-gated** (docsets, hf_roots, hf_posts) — an upstream token compared
    against the last fully-ingested one. Encoded under the SAME key on both sides,
    or the two would always be distinct and every unit would look pending forever.
  * **not gated at all** (feeds, gh_shards) — selected by rotation or re-arm
    rather than by content change. `upstream` and `ingested` are both left `{}` so
    the gate reads false, and the recipe's `rotate`/`rearm` predicate does the
    selecting. Their real state lives in `processed_at` / `attrs`.

`repos` and `crawl_urls` are deliberately NOT migrated: they keep dedicated tables
behind a store adapter (`repos` is a wide typed table with millions of rows and
`ORDER BY stars DESC`; `crawl_urls` is run-scoped and cascade-deletes), and
`minhash_bands` is a dedup index, not a watermark.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import psycopg


@dataclass(frozen=True)
class Mapping:
    """One legacy table's projection into `source_units`."""

    recipe: str          # source_units.source — the recipe that will own these
    store: str           # store name within that recipe
    table: str           # legacy table
    insert: str          # INSERT ... SELECT, idempotent
    legacy_pending: str  # SELECT unit_key ... — the predicate the LIVE code uses
    selector: str        # token | rotate | rearm — how the new model selects
    # Divergences we accept, with the reason. Empty means the two predicates are
    # expected to agree exactly, and verify() treats any difference as a failure.
    tolerated: str = ""
    # A predicate over the LEGACY table selecting rows that may legitimately be
    # pending in the new model only. Checked per row rather than tolerated in
    # bulk, so an unexpected divergence still fails even where a known one exists.
    redo_ok: str = ""


# `_UPSERT` keeps the backfill re-runnable: re-running after more ingest has
# happened refreshes the projection rather than erroring or double-counting.
_UPSERT = """
ON CONFLICT (source, store, unit_key) DO UPDATE SET
    ord = EXCLUDED.ord, status = EXCLUDED.status,
    upstream = EXCLUDED.upstream, ingested = EXCLUDED.ingested,
    counts = EXCLUDED.counts, bytes = EXCLUDED.bytes, attrs = EXCLUDED.attrs,
    processed_at = EXCLUDED.processed_at, updated_at = now()
"""

# The status-gated sentinel. Anything other than 'done' stays pending, which also
# means a FAILED unit re-arms for free — `ingested` only advances on a clean
# completion, so "retry the failures" needs no code at all.
_SEEN = "jsonb_build_object('seen', 1)"


def _status_gated(recipe: str, store: str, table: str, key: str, ord_expr: str,
                  counts: str = "'{}'::jsonb", bytes_expr: str = "NULL",
                  attrs: str = "'{}'::jsonb") -> Mapping:
    return Mapping(
        recipe=recipe, store=store, table=table, selector="token",
        insert=f"""
            INSERT INTO source_units
                (source, store, unit_key, ord, status, upstream, ingested,
                 counts, bytes, attrs, processed_at)
            SELECT '{recipe}', '{store}', {key}, {ord_expr}, status,
                   {_SEEN},
                   CASE WHEN status = 'done' THEN {_SEEN} ELSE '{{}}'::jsonb END,
                   {counts}, {bytes_expr}, {attrs}, processed_at
            FROM {table}
            {_UPSERT}
        """,
        legacy_pending=f"SELECT {key} FROM {table} WHERE status = 'pending'",
        # A FAILED unit becomes pending again in the new model, by construction:
        # `ingested` only advances on a clean completion, so "still needs work" and
        # "failed last time" are the same state. That is deliberate — it deletes
        # the need for a separate re-arm path (`ccnews retry-failed`, wiki's
        # explicit failed-row reset) — but it IS a behaviour change, so it is
        # checked per row rather than waved through.
        redo_ok=f"SELECT {key} FROM {table} WHERE status = 'failed'",
    )


MAPPINGS: tuple[Mapping, ...] = (
    _status_gated("ccnews", "warc", "warc_files", "path", "path",
                  counts="coalesce(doc_counts, '{}'::jsonb)"),
    _status_gated("gh", "hour", "gharchive_files", "name", "name"),
    _status_gated("wiki", "shard", "wiki_dumps", "name", "name",
                  counts="coalesce(doc_counts, '{}'::jsonb)", bytes_expr="bytes",
                  attrs="jsonb_build_object('dump_date', dump_date)"),
    _status_gated("arxiv", "window", "arxiv_windows",
                  "from_date || '..' || until_date", "from_date",
                  counts="jsonb_build_object('pages', pages, 'records', records,"
                         " 'staged', staged, 'deleted', deleted)",
                  attrs="jsonb_build_object('token', token)"),
    _status_gated("hn", "window", "hn_windows",
                  "from_ts::text || '..' || until_ts::text", "lpad(from_ts::text, 20, '0')",
                  counts="jsonb_build_object('queries', queries, 'hits', hits,"
                         " 'staged', staged, 'refreshed', refreshed)"),

    # --- token-gated -------------------------------------------------------
    Mapping(
        recipe="docs", store="docset", table="docsets", selector="token",
        # Same key both sides, or the gate would never read equal.
        insert=f"""
            INSERT INTO source_units
                (source, store, unit_key, ord, status, upstream, ingested,
                 counts, bytes, attrs, processed_at)
            SELECT 'docs', 'docset', slug, slug, status,
                   jsonb_build_object('mtime', mtime),
                   jsonb_build_object('mtime', ingested_mtime),
                   coalesce(doc_counts, '{{}}'::jsonb), db_size,
                   jsonb_build_object('release', release, 'attribution', attribution),
                   processed_at
            FROM docsets
            {_UPSERT}
        """,
        legacy_pending="SELECT slug FROM docsets "
                       "WHERE ingested_mtime IS NULL OR mtime > ingested_mtime",
        tolerated="a docset whose upstream mtime moved BACKWARDS: legacy uses "
                  "`mtime > ingested_mtime` so it stays done, the generic gate "
                  "uses IS DISTINCT FROM so it re-ingests. Safe direction "
                  "(re-ingest is a text_hash no-op) and arguably more correct.",
    ),
    Mapping(
        recipe="hf", store="root", table="hf_roots", selector="token",
        # A root with no llms.txt is NOT pending in the live code
        # (`llms_hash IS NOT NULL`). Mirror that by making both sides equal when
        # the hash is absent, rather than leaving a null-vs-value mismatch that
        # would make every no_llms root look permanently pending.
        insert=f"""
            INSERT INTO source_units
                (source, store, unit_key, ord, status, upstream, ingested,
                 counts, bytes, attrs, processed_at)
            SELECT 'hf', 'root', root, root, status,
                   jsonb_build_object('hash', llms_hash),
                   CASE WHEN llms_hash IS NULL
                        THEN jsonb_build_object('hash', llms_hash)
                        ELSE jsonb_build_object('hash', ingested_hash) END,
                   coalesce(doc_counts, '{{}}'::jsonb), NULL,
                   jsonb_build_object('kind', kind, 'url', url, 'lastmod', lastmod,
                                      'version', version, 'license', license,
                                      'pages', pages),
                   processed_at
            FROM hf_roots
            {_UPSERT}
        """,
        legacy_pending="SELECT root FROM hf_roots WHERE llms_hash IS NOT NULL "
                       "AND (ingested_hash IS NULL OR llms_hash IS DISTINCT FROM ingested_hash)",
    ),
    Mapping(
        recipe="hf", store="post", table="hf_posts", selector="token",
        insert=f"""
            INSERT INTO source_units
                (source, store, unit_key, ord, status, upstream, ingested,
                 counts, bytes, attrs, processed_at)
            SELECT 'hf', 'post', slug, lastmod, status,
                   jsonb_build_object('lastmod', lastmod),
                   jsonb_build_object('lastmod', ingested_lastmod),
                   '{{}}'::jsonb, NULL,
                   jsonb_build_object('url', url),
                   processed_at
            FROM hf_posts
            {_UPSERT}
        """,
        legacy_pending="SELECT slug FROM hf_posts "
                       "WHERE ingested_lastmod IS NULL OR lastmod > ingested_lastmod",
        tolerated="a post whose lastmod moved backwards — same safe-direction "
                  "divergence as docsets.",
    ),

    # --- not content-gated -------------------------------------------------
    Mapping(
        recipe="smallweb", store="feed", table="feeds", selector="rotate",
        # Feeds are polled on ROTATION, not because content changed: there is no
        # cheap upstream signal for 38k blogs. etag/last_modified are conditional-GET
        # validators for the fetcher, so they live in attrs; both gate sides stay
        # empty so the token gate reads false and `rotate` does the selecting.
        insert=f"""
            INSERT INTO source_units
                (source, store, unit_key, ord, status, upstream, ingested,
                 counts, bytes, attrs, attempts, processed_at)
            SELECT 'smallweb', 'feed', url, url, status,
                   '{{}}'::jsonb, '{{}}'::jsonb,
                   jsonb_build_object('items_seen', items_seen), NULL,
                   jsonb_build_object('host', host, 'etag', etag,
                                      'last_modified', last_modified,
                                      'last_status', last_status),
                   least(fail_count, 32767), last_polled
            FROM feeds
            {_UPSERT}
        """,
        legacy_pending="SELECT url FROM feeds WHERE status = 'active'",
    ),
    Mapping(
        recipe="gh", store="gh_shard", table="gh_shards", selector="rearm",
        # Re-armed on age (RESUME_DAYS=7), not content. Gate stays false; the
        # recipe's `rearm` predicate reads processed_at.
        insert=f"""
            INSERT INTO source_units
                (source, store, unit_key, ord, status, upstream, ingested,
                 counts, bytes, attrs, processed_at)
            SELECT 'gh', 'gh_shard',
                   from_date::text || '..' || to_date::text || '@' || star_threshold::text,
                   from_date::text, 'done',
                   '{{}}'::jsonb, '{{}}'::jsonb,
                   jsonb_build_object('repos', repos), NULL,
                   jsonb_build_object('from_date', from_date, 'to_date', to_date,
                                      'star_threshold', star_threshold),
                   processed_at
            FROM gh_shards
            {_UPSERT}
        """,
        legacy_pending="SELECT from_date::text || '..' || to_date::text || '@' "
                       "|| star_threshold::text FROM gh_shards "
                       "WHERE processed_at IS NULL "
                       "OR processed_at <= now() - make_interval(days => 7)",
    ),
)

# How the new model decides "pending", per selector. `token` is the generic gate;
# the other two deliberately ignore it (see the module docstring).
_NEW_PENDING = {
    "token": "upstream IS DISTINCT FROM ingested",
    "rotate": "status = 'active'",
    "rearm": "processed_at IS NULL OR processed_at <= now() - make_interval(days => 7)",
}


def migrate(conn: psycopg.Connection) -> list[dict]:
    """Project every legacy watermark table into `source_units`. Idempotent."""
    out = []
    with conn.cursor() as cur:
        for m in MAPPINGS:
            cur.execute(f"SELECT to_regclass('{m.table}')")
            if cur.fetchone()[0] is None:      # table absent (fresh install)
                out.append({"table": m.table, "store": m.store, "rows": 0,
                            "skipped": "table does not exist"})
                continue
            cur.execute(m.insert)
            rows = cur.rowcount or 0
            out.append({"table": m.table, "recipe": m.recipe, "store": m.store,
                        "rows": rows})
    conn.commit()
    return out


def verify(conn: psycopg.Connection) -> list[dict]:
    """Compare the two models. The pending SET is the assertion that matters.

    Counts alone are not enough: two sets of the same size can have different
    membership, and the failure that costs us is one specific unit silently
    looking done. So this diffs the keys both ways.
    """
    results = []
    with conn.cursor() as cur:
        for m in MAPPINGS:
            cur.execute(f"SELECT to_regclass('{m.table}')")
            if cur.fetchone()[0] is None:
                results.append({"table": m.table, "store": m.store, "ok": True,
                                "note": "table does not exist"})
                continue

            cur.execute(f"SELECT count(*) FROM {m.table}")
            legacy_rows = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM source_units WHERE source = %s AND store = %s",
                        (m.recipe, m.store))
            new_rows = cur.fetchone()[0]

            new_pending = (
                f"SELECT unit_key FROM source_units "
                f"WHERE source = '{m.recipe}' AND store = '{m.store}' "
                f"AND ({_NEW_PENDING[m.selector]})"
            )
            # Symmetric difference, both directions, with the dangerous one named.
            cur.execute(f"SELECT count(*) FROM ({m.legacy_pending}) l(k)")
            legacy_pending = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM ({new_pending}) n(k)")
            new_pending_n = cur.fetchone()[0]
            cur.execute(
                f"SELECT count(*) FROM ({m.legacy_pending}) l(k) "
                f"WHERE NOT EXISTS (SELECT 1 FROM ({new_pending}) n(k) WHERE n.k = l.k)")
            would_skip = cur.fetchone()[0]          # THE dangerous direction
            redo_sql = (f"SELECT n.k FROM ({new_pending}) n(k) "
                        f"WHERE NOT EXISTS (SELECT 1 FROM ({m.legacy_pending}) l(k) "
                        f"WHERE l.k = n.k)")
            cur.execute(f"SELECT count(*) FROM ({redo_sql}) x(k)")
            would_redo = cur.fetchone()[0]          # safe: re-ingest is a no-op

            # Of those, how many are NOT explained by the mapping's declared
            # exception? Any unexplained row is a real divergence and fails.
            unexplained = would_redo
            if would_redo and m.redo_ok:
                cur.execute(f"SELECT count(*) FROM ({redo_sql}) x(k) "
                            f"WHERE NOT EXISTS (SELECT 1 FROM ({m.redo_ok}) o(k) "
                            f"WHERE o.k = x.k)")
                unexplained = cur.fetchone()[0]
            elif would_redo and m.tolerated:
                unexplained = 0

            ok = legacy_rows == new_rows and would_skip == 0 and unexplained == 0
            results.append({
                "table": m.table, "recipe": m.recipe, "store": m.store, "ok": ok,
                "rows": legacy_rows, "rows_new": new_rows,
                "pending": legacy_pending, "pending_new": new_pending_n,
                "would_skip": would_skip, "would_redo": would_redo,
                "unexplained": unexplained,
                "tolerated": (m.tolerated or "failed units re-arm by construction")
                             if would_redo else "",
            })
    return results
