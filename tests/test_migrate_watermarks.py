"""The legacy → source_units projection.

These are almost entirely about ONE property: a unit the live code would process
must never look done in the new model. That direction is silent data loss, and it
is the reason the migration is a copy with a parity gate rather than a rewrite.
The opposite direction (re-ingest) is a `text_hash` no-op and merely wasteful.
"""

import pytest

from windex.migrate import watermarks


def _units(pg, source, store):
    with pg.cursor() as cur:
        cur.execute("SELECT unit_key, upstream IS DISTINCT FROM ingested "
                    "FROM source_units WHERE source = %s AND store = %s ORDER BY unit_key",
                    (source, store))
        return dict(cur.fetchall())


def _verdict(pg, table):
    return next(r for r in watermarks.verify(pg) if r["table"] == table)


# --- the status-gated shape -------------------------------------------------

def test_status_gate_maps_done_pending_and_failed(pg):
    """`done` is the only status that means "no work left". The sentinel encoding
    has to reproduce that, and re-arm a failed unit as a side effect."""
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO warc_files (path, status) VALUES (%s, %s)",
            [("a.warc.gz", "done"), ("b.warc.gz", "pending"),
             ("c.warc.gz", "failed"), ("d.warc.gz", "processing")])
    pg.commit()
    watermarks.migrate(pg)

    pending = _units(pg, "ccnews", "warc")
    assert pending == {"a.warc.gz": False, "b.warc.gz": True,
                       "c.warc.gz": True, "d.warc.gz": True}


def test_failed_units_rearm_and_verify_calls_it_expected(pg):
    """A failed unit is pending in the new model but not the old. That is a real
    behaviour change — it replaces `ccnews retry-failed` — so verify must classify
    it rather than either failing or silently ignoring it."""
    with pg.cursor() as cur:
        cur.executemany("INSERT INTO warc_files (path, status) VALUES (%s, %s)",
                        [("ok.gz", "done"), ("bad.gz", "failed")])
    pg.commit()
    watermarks.migrate(pg)

    v = _verdict(pg, "warc_files")
    assert v["would_skip"] == 0          # the direction that must never happen
    assert v["would_redo"] == 1          # the failed unit
    assert v["unexplained"] == 0         # ...and it is accounted for
    assert v["ok"] is True


def test_verify_fails_on_an_unexplained_divergence(pg):
    """The gate has to actually bite. Corrupt one projected row so a unit the live
    code WOULD process looks done, and verify must refuse."""
    with pg.cursor() as cur:
        cur.execute("INSERT INTO warc_files (path, status) VALUES ('x.gz', 'pending')")
    pg.commit()
    watermarks.migrate(pg)
    assert _verdict(pg, "warc_files")["ok"] is True

    with pg.cursor() as cur:      # pretend the mapping got it wrong
        cur.execute("UPDATE source_units SET ingested = upstream "
                    "WHERE source = 'ccnews' AND unit_key = 'x.gz'")
    pg.commit()

    v = _verdict(pg, "warc_files")
    assert v["would_skip"] == 1
    assert v["ok"] is False


# --- the token-gated shape --------------------------------------------------

@pytest.mark.parametrize("mtime,ingested,expect_pending", [
    (100, None, True),    # never ingested
    (100, 100, False),    # unchanged — the case that must NOT be pending, or
                          # every docset re-ingests on every pass forever
    (200, 100, True),     # upstream advanced
])
def test_docset_token_gate(pg, mtime, ingested, expect_pending):
    with pg.cursor() as cur:
        cur.execute("INSERT INTO docsets (slug, mtime, ingested_mtime, status) "
                    "VALUES ('python~3.14', %s, %s, 'done')", (mtime, ingested))
    pg.commit()
    watermarks.migrate(pg)
    assert _units(pg, "docs", "docset")["python~3.14"] is expect_pending
    assert _verdict(pg, "docsets")["would_skip"] == 0


def test_hf_root_without_llms_txt_is_not_pending(pg):
    """`llms_hash IS NOT NULL` in the live query. Encoded naively, a root with no
    llms.txt would have upstream {"hash": null} against a non-null ingested hash
    and look pending forever — a permanent busy-loop on a source that has nothing
    to fetch."""
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO hf_roots (root, kind, url, llms_hash, ingested_hash, status) "
            "VALUES (%s, 'docs', 'https://hf.co/x', %s, %s, 'done')",
            [("none", None, "stale-hash"), ("fresh", "h2", "h1"), ("same", "h3", "h3")])
    pg.commit()
    watermarks.migrate(pg)

    assert _units(pg, "hf", "root") == {"none": False, "fresh": True, "same": False}
    assert _verdict(pg, "hf_roots")["ok"] is True


# --- properties of the migration itself -------------------------------------

def test_migration_is_idempotent_and_refreshes(pg):
    """Re-runnable is not optional: this runs against production repeatedly while
    the dual-write phase settles, and must pick up ingest that happened since."""
    with pg.cursor() as cur:
        cur.execute("INSERT INTO warc_files (path, status) VALUES ('a.gz', 'pending')")
    pg.commit()
    watermarks.migrate(pg)
    assert _units(pg, "ccnews", "warc") == {"a.gz": True}

    with pg.cursor() as cur:      # the live pipeline finishes it
        cur.execute("UPDATE warc_files SET status = 'done' WHERE path = 'a.gz'")
    pg.commit()
    watermarks.migrate(pg)

    assert _units(pg, "ccnews", "warc") == {"a.gz": False}
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM source_units WHERE source = 'ccnews'")
        assert cur.fetchone()[0] == 1          # refreshed, not duplicated


def test_migration_never_writes_to_the_legacy_tables(pg):
    """The whole rollback story is that the legacy tables stay authoritative and
    untouched until reads are flipped."""
    with pg.cursor() as cur:
        cur.execute("INSERT INTO docsets (slug, mtime, ingested_mtime, status) "
                    "VALUES ('rust', 42, 41, 'pending')")
    pg.commit()
    watermarks.migrate(pg)

    with pg.cursor() as cur:
        cur.execute("SELECT slug, mtime, ingested_mtime, status FROM docsets")
        assert cur.fetchall() == [("rust", 42, 41, "pending")]


def test_empty_database_migrates_cleanly(pg):
    """A fresh install has every legacy table present and empty."""
    assert all(r["rows"] == 0 for r in watermarks.migrate(pg) if "rows" in r)
    assert all(r["ok"] for r in watermarks.verify(pg))
