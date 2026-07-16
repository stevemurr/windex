from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from windex.ccnews import dedup as dd

DAY = date(2026, 7, 13)

STORY_A = (
    "The city council voted on Tuesday to approve the new transit plan, which "
    "includes dedicated bus lanes, expanded night service, and a pilot program "
    "for fare-free rides on weekends starting this autumn across every district. "
) * 4
STORY_A_SYNDICATED = STORY_A.replace("Tuesday", "Wednesday", 1) + "Additional reporting by AP."
STORY_B = (
    "Quarterly earnings at the semiconductor firm beat analyst expectations, "
    "driven by strong datacenter demand and improving margins in mobile chips, "
    "while guidance for the coming quarter remained notably conservative overall. "
) * 4


def _write_extracted(dir: Path, rows: list[dict]) -> None:
    dir.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "text": [r["text"] for r in rows],
            "id": [r.get("rec_id", f"rec-{i}") for i, r in enumerate(rows)],
            "metadata": [
                {"url": r["url"], "title": r.get("title", "t"), "date": "2026-07-13T00:00:00",
                 "language": "en"}
                for r in rows
            ],
        }
    )
    pq.write_table(table, dir / "000.parquet")


def _run(pg, tmp_path, rows, batch="b1", day=DAY):
    extracted = tmp_path / "extracted" / batch
    _write_extracted(extracted, rows)
    clean = tmp_path / "clean" / f"{batch}.parquet"
    return dd.run_dedup(pg, extracted, clean, f"clean/{batch}.parquet", day), clean


def test_batch_exact_and_near_dups_collapse(pg, tmp_path):
    rows = [
        {"text": STORY_A, "url": "https://a.com/story"},
        {"text": STORY_A, "url": "https://a.com/story?utm_source=feed"},  # same canonical
        {"text": STORY_A_SYNDICATED, "url": "https://b.com/wire-copy"},   # near-dup
        {"text": STORY_B, "url": "https://c.com/earnings"},
    ]
    stats, clean = _run(pg, tmp_path, rows)
    assert stats["dup_batch_exact"] == 1
    assert stats["dup_near"] == 1
    assert stats["clean_out"] == 2
    ids = pq.read_table(clean).column("id").to_pylist()
    assert len(ids) == 2
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE status = 'duplicate'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT duplicate_of FROM documents WHERE status = 'duplicate'")
        assert cur.fetchone()[0] in ids


def test_cross_batch_dedup_via_ledger_and_bands(pg, tmp_path):
    _run(pg, tmp_path, [{"text": STORY_A, "url": "https://a.com/story"}], batch="b1")
    stats, _ = _run(
        pg,
        tmp_path,
        [
            {"text": STORY_A, "url": "https://a.com/story"},          # same id → skip
            {"text": STORY_A_SYNDICATED, "url": "https://d.com/x"},   # near-dup via bands
        ],
        batch="b2",
        day=date(2026, 7, 14),
    )
    assert stats["already_indexed"] == 1
    assert stats["dup_near"] == 1
    # a batch containing only a byte-identical mirror at a new URL → ledger text-hash
    stats, _ = _run(
        pg,
        tmp_path,
        [
            {"text": STORY_A, "url": "https://mirror.com/repost"},
            {"text": STORY_B, "url": "https://c.com/earnings"},
        ],
        batch="b3",
        day=date(2026, 7, 14),
    )
    assert stats["dup_db_exact"] == 1
    assert stats["clean_out"] == 1


def test_prune_bands_respects_window(pg, tmp_path):
    _run(pg, tmp_path, [{"text": STORY_A, "url": "https://a.com/1"}], batch="old", day=date(2026, 6, 1))
    _run(pg, tmp_path, [{"text": STORY_B, "url": "https://b.com/2"}], batch="new", day=date(2026, 7, 13))
    deleted = dd.prune_bands(pg, window_days=14)
    assert deleted == 14  # one doc's bands aged out
    with pg.cursor() as cur:
        cur.execute("SELECT count(DISTINCT doc_id) FROM minhash_bands")
        assert cur.fetchone()[0] == 1


def test_failed_batch_rolls_back_atomically(pg, tmp_path, monkeypatch):
    rows = [{"text": STORY_A, "url": "https://a.com/story"}]
    extracted = tmp_path / "extracted" / "boom"
    _write_extracted(extracted, rows)
    clean = tmp_path / "clean" / "boom.parquet"
    monkeypatch.setattr(dd, "band_hashes", lambda sig: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        dd.run_dedup(pg, extracted, clean, "clean/boom.parquet", DAY)
    assert not clean.exists()
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents")
        assert cur.fetchone()[0] == 0
