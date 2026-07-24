"""The dual-write mirror.

The point of doing this with triggers rather than a helper called from each
source's mark() is that application code can forget a call and a trigger cannot.
So these tests write to the legacy tables the way the pipeline does — and, more
importantly, the way it doesn't: raw SQL that no shim would ever intercept.
"""

import pytest

from windex.migrate import dualwrite, watermarks


@pytest.fixture()
def mirroring(pg):
    dualwrite.enable(pg)
    yield pg
    dualwrite.disable(pg)


def _unit(pg, source, store, key):
    with pg.cursor() as cur:
        cur.execute("SELECT status, upstream IS DISTINCT FROM ingested "
                    "FROM source_units WHERE source=%s AND store=%s AND unit_key=%s",
                    (source, store, key))
        return cur.fetchone()


def test_insert_is_mirrored(mirroring):
    with mirroring.cursor() as cur:
        cur.execute("INSERT INTO warc_files (path, status) VALUES ('a.gz', 'pending')")
    mirroring.commit()
    assert _unit(mirroring, "ccnews", "warc", "a.gz") == ("pending", True)


def test_update_is_mirrored(mirroring):
    """The write that matters: a unit completing must stop being pending in BOTH
    models, or the new one hands out work that is already done."""
    with mirroring.cursor() as cur:
        cur.execute("INSERT INTO warc_files (path, status) VALUES ('a.gz', 'pending')")
        cur.execute("UPDATE warc_files SET status='done' WHERE path='a.gz'")
    mirroring.commit()
    assert _unit(mirroring, "ccnews", "warc", "a.gz") == ("done", False)


def test_delete_is_mirrored(mirroring):
    """A mirror that only ever grows is a slow-motion divergence. gh_shards is the
    table whose sweep actually prunes rows."""
    with mirroring.cursor() as cur:
        cur.execute("INSERT INTO gh_shards (from_date, to_date, star_threshold, repos) "
                    "VALUES ('2025-01-01', '2025-02-01', 10, 5)")
    mirroring.commit()
    assert _unit(mirroring, "gh", "gh_shard", "2025-01-01..2025-02-01@10") is not None

    with mirroring.cursor() as cur:
        cur.execute("DELETE FROM gh_shards WHERE star_threshold = 10")
    mirroring.commit()
    assert _unit(mirroring, "gh", "gh_shard", "2025-01-01..2025-02-01@10") is None


def test_token_gated_update_is_mirrored(mirroring):
    with mirroring.cursor() as cur:
        cur.execute("INSERT INTO docsets (slug, mtime, ingested_mtime, status) "
                    "VALUES ('rust', 100, 100, 'done')")
    mirroring.commit()
    assert _unit(mirroring, "docs", "docset", "rust") == ("done", False)

    with mirroring.cursor() as cur:      # upstream publishes a new release
        cur.execute("UPDATE docsets SET mtime = 200 WHERE slug = 'rust'")
    mirroring.commit()
    assert _unit(mirroring, "docs", "docset", "rust") == ("done", True)


def test_mirror_is_transactional(pg):
    """It commits with the write that caused it, or not at all — no window where
    the two models disagree because a shim ran after the transaction."""
    dualwrite.enable(pg)
    try:
        with pg.cursor() as cur:
            cur.execute("INSERT INTO warc_files (path, status) VALUES ('r.gz', 'pending')")
        pg.rollback()
        assert _unit(pg, "ccnews", "warc", "r.gz") is None
    finally:
        dualwrite.disable(pg)


def test_parity_holds_after_writes_the_backfill_never_saw(mirroring):
    """The whole point: ingest that happens AFTER the backfill must keep the two
    models in agreement, without anyone re-running the migration."""
    with mirroring.cursor() as cur:
        cur.executemany("INSERT INTO warc_files (path, status) VALUES (%s, %s)",
                        [("a.gz", "pending"), ("b.gz", "done"), ("c.gz", "failed")])
        cur.execute("INSERT INTO docsets (slug, mtime, ingested_mtime, status) "
                    "VALUES ('go', 7, 7, 'done')")
    mirroring.commit()

    for r in watermarks.verify(mirroring):
        assert r.get("would_skip", 0) == 0, r
        assert r.get("unexplained", 0) == 0, r
        assert r["ok"], r


# --- lifecycle --------------------------------------------------------------

def test_enable_is_idempotent_and_disable_is_complete(pg):
    dualwrite.enable(pg)
    dualwrite.enable(pg)                       # must not error or double-fire
    with pg.cursor() as cur:
        cur.execute("INSERT INTO warc_files (path, status) VALUES ('a.gz', 'pending')")
    pg.commit()
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM source_units WHERE unit_key = 'a.gz'")
        assert cur.fetchone()[0] == 1

    assert all(r["mirroring"] for r in dualwrite.status(pg))
    dualwrite.disable(pg)
    assert not any(r["mirroring"] for r in dualwrite.status(pg))

    # ...and once off, legacy writes stop propagating — the undo is real
    with pg.cursor() as cur:
        cur.execute("INSERT INTO warc_files (path, status) VALUES ('z.gz', 'pending')")
    pg.commit()
    assert _unit(pg, "ccnews", "warc", "z.gz") is None


def test_disable_leaves_the_legacy_tables_untouched(pg):
    dualwrite.enable(pg)
    with pg.cursor() as cur:
        cur.execute("INSERT INTO docsets (slug, mtime, ingested_mtime, status) "
                    "VALUES ('py', 1, 1, 'done')")
    pg.commit()
    dualwrite.disable(pg)
    with pg.cursor() as cur:
        cur.execute("SELECT slug, mtime, ingested_mtime, status FROM docsets")
        assert cur.fetchall() == [("py", 1, 1, "done")]
