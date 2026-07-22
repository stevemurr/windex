"""One-time hn cleanups: tombstone fully-empty docs, backfill exact duplicates.
_drop_points is stubbed so no test ever touches a live *_current Qdrant alias."""

import pyarrow as pa
import pyarrow.parquet as pq

from windex.hn import cleanup


def _seed(cur, rows):
    """rows: (id, text_hash, status, created_at)."""
    cur.executemany(
        "INSERT INTO documents (id, source, url, text_hash, status, created_at, text_ref) "
        "VALUES (%s, 'hn', 'u', %s, %s, %s, 'r')",
        rows,
    )


def _write_hn_parquet(settings, ref, ids, titles, texts):
    path = settings.staging_dir / ref
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"id": ids, "title": titles, "story_text": texts}), path)


def test_tombstone_empty_marks_deleted_and_drops_only_true_empties(pg, settings, monkeypatch):
    ref = "hn/clean/w.parquet"
    _write_hn_parquet(settings, ref, ["hn:1", "hn:2", "hn:3"],
                      ["", "", "Real Title"], ["", "", ""])
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO documents (id, source, url, status, text_ref) VALUES (%s,'hn','u',%s,%s)",
            [("hn:1", "embedded", ref), ("hn:2", "deduped", ref), ("hn:3", "embedded", ref)],
        )
    pg.commit()
    dropped = []
    monkeypatch.setattr(cleanup, "_drop_points", lambda s, ids: dropped.extend(ids))

    assert cleanup.tombstone_empty_stories(pg, settings) == 2
    with pg.cursor() as cur:
        cur.execute("SELECT id, status FROM documents WHERE source='hn' ORDER BY id")
        rows = dict(cur.fetchall())
    assert rows == {"hn:1": "deleted", "hn:2": "deleted", "hn:3": "embedded"}  # title-only kept
    assert dropped == ["hn:1"]  # only the embedded empty had a vector to drop


def test_tombstone_empty_is_idempotent(pg, settings, monkeypatch):
    ref = "hn/clean/w.parquet"
    _write_hn_parquet(settings, ref, ["hn:1"], [""], [""])
    with pg.cursor() as cur:
        cur.execute("INSERT INTO documents (id, source, url, status, text_ref) "
                    "VALUES ('hn:1','hn','u','deduped',%s)", (ref,))
    pg.commit()
    monkeypatch.setattr(cleanup, "_drop_points", lambda s, ids: None)
    assert cleanup.tombstone_empty_stories(pg, settings) == 1
    assert cleanup.tombstone_empty_stories(pg, settings) == 0  # nothing left


def test_backfill_exact_duplicates_marks_all_but_earliest(pg, settings, monkeypatch):
    with pg.cursor() as cur:
        _seed(cur, [
            ("hn:1", "h", "embedded", "2026-07-01"),
            ("hn:2", "h", "deduped", "2026-07-02"),
            ("hn:3", "h", "embedded", "2026-07-03"),
            ("hn:4", "other", "deduped", "2026-07-01"),
        ])
    pg.commit()
    dropped = []
    monkeypatch.setattr(cleanup, "_drop_points", lambda s, ids: dropped.extend(ids))

    out = cleanup.backfill_exact_duplicates(pg, settings)
    with pg.cursor() as cur:
        cur.execute("SELECT id, status, duplicate_of FROM documents WHERE source='hn' ORDER BY id")
        rows = cur.fetchall()
    assert rows == [
        ("hn:1", "embedded", None),    # earliest of hash 'h' -> canonical
        ("hn:2", "duplicate", "hn:1"),
        ("hn:3", "duplicate", "hn:1"),
        ("hn:4", "deduped", None),     # distinct hash -> untouched
    ]
    assert out == {"marked_duplicate": 2, "vectors_dropped": 1}
    assert dropped == ["hn:3"]  # only the embedded dup's vector dropped


def test_backfill_exact_duplicates_is_idempotent_and_skips_deleted(pg, settings, monkeypatch):
    with pg.cursor() as cur:
        _seed(cur, [
            ("hn:1", "h", "deduped", "2026-07-01"),
            ("hn:2", "h", "deduped", "2026-07-02"),
            ("hn:9", "h", "deleted", "2026-06-01"),  # tombstoned: not an anchor, untouched
        ])
    pg.commit()
    monkeypatch.setattr(cleanup, "_drop_points", lambda s, ids: None)

    assert cleanup.backfill_exact_duplicates(pg, settings)["marked_duplicate"] == 1  # hn:2 -> hn:1
    assert cleanup.backfill_exact_duplicates(pg, settings)["marked_duplicate"] == 0  # idempotent
    with pg.cursor() as cur:
        cur.execute("SELECT status, duplicate_of FROM documents WHERE id='hn:2'")
        assert cur.fetchone() == ("duplicate", "hn:1")
        cur.execute("SELECT status FROM documents WHERE id='hn:9'")
        assert cur.fetchone()[0] == "deleted"  # the earlier tombstoned row is not a canonical
