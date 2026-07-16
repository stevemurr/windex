"""Wiki ingest: streamed bz2 shard → clean parquet + documents ledger, with
change detection (text_hash) keeping weekly re-ingests to the delta. Uses the pg
fixture and a fake httpx client so no network/bz2 file is needed on disk."""

import bz2
import json

import pyarrow.parquet as pq

from windex.wiki import ingest as wingest


def _article(page_id, title, text, ns=0, ts="2026-07-12T00:00:00Z", incoming=0):
    return {"page_id": page_id, "namespace": ns, "title": title, "text": text,
            "timestamp": ts, "opening_text": text[:50], "incoming_links": incoming}


def _make_dump(articles):
    lines = []
    for a in articles:
        lines.append(json.dumps({"index": {"_id": a["page_id"]}}))
        lines.append(json.dumps(a))
    return bz2.compress(("\n".join(lines) + "\n").encode())


class _FakeStream:
    def __init__(self, data):
        self.data = data

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        pass

    def iter_bytes(self, n=65536):
        for i in range(0, len(self.data), n):
            yield self.data[i : i + n]


class _FakeClient:
    def __init__(self, by_name):
        self.by_name = by_name

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def stream(self, method, url):
        for name, data in self.by_name.items():
            if url.endswith(name):
                return _FakeStream(data)
        raise AssertionError(f"no fake shard for {url}")


def _seed_pending(pg, names, date="20260712"):
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO wiki_dumps (name, dump_date, bytes) VALUES (%s, %s, 100)",
            [(n, date) for n in names],
        )
    pg.commit()


def test_ingest_stages_parquet_and_ledger(pg, settings, monkeypatch):
    name = "enwiki_content-20260712-00000.json.bz2"
    dump = _make_dump([
        _article(12, "Anarchism", "Anarchism is a political philosophy. " * 5, incoming=1200),
        _article(39, "Autism", "Autism is a condition. " * 5, incoming=800),
        _article(290, "A", "The letter A. " * 5),
        _article(999, "Talk:A", "chatter", ns=1),  # namespace filtered out
    ])
    monkeypatch.setattr(wingest.httpx, "Client", lambda *a, **k: _FakeClient({name: dump}))
    _seed_pending(pg, [name])

    totals = wingest.ingest(pg, settings, max_files=1, chunk_rows=2)  # 2 chunks over 3 arts
    assert totals == {"files": 1, "articles": 3, "staged": 3, "skipped": 0}

    text_ref = "wiki/clean/enwiki_content-20260712-00000.parquet"
    table = pq.read_table(settings.staging_dir / text_ref)
    assert table.num_rows == 3
    assert set(table.column_names) == {
        "id", "url", "title", "revision_ts", "incoming_links", "opening_text", "text"
    }
    row0 = table.slice(0, 1).to_pylist()[0]
    assert row0["id"] == "wiki:12" and row0["incoming_links"] == 1200

    with pg.cursor() as cur:
        cur.execute(
            """SELECT id, source, url, title, status, text_ref, published_at, text_hash
               FROM documents WHERE source='wiki' ORDER BY id"""
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == ["wiki:12", "wiki:290", "wiki:39"]
    for r in rows:
        assert r[1] == "wiki" and r[4] == "deduped" and r[5] == text_ref
        assert r[6] is not None and r[7]  # published_at + text_hash populated
    doc12 = next(r for r in rows if r[0] == "wiki:12")
    assert doc12[2] == "https://en.wikipedia.org/wiki/Anarchism"

    with pg.cursor() as cur:
        cur.execute("SELECT status, doc_counts FROM wiki_dumps WHERE name=%s", (name,))
        status, counts = cur.fetchone()
    assert status == "done" and counts["staged"] == 3


def test_reingest_skips_unchanged_articles(pg, settings, monkeypatch):
    articles = [
        _article(12, "Anarchism", "Original anarchism text. " * 5),
        _article(39, "Autism", "Original autism text. " * 5),
        _article(290, "A", "Original letter A text. " * 5),
    ]
    s0 = "enwiki_content-20260712-00000.json.bz2"
    s1 = "enwiki_content-20260719-00000.json.bz2"  # next weekly snapshot, same pages
    # article 39 rewritten in the newer snapshot; 12 and 290 unchanged
    changed = [
        articles[0],
        _article(39, "Autism", "REWRITTEN autism text with new content. " * 5),
        articles[2],
    ]
    dumps = {s0: _make_dump(articles), s1: _make_dump(changed)}
    monkeypatch.setattr(wingest.httpx, "Client", lambda *a, **k: _FakeClient(dumps))

    _seed_pending(pg, [s0], date="20260712")
    wingest.ingest(pg, settings, max_files=1, chunk_rows=8)
    # simulate the baseline having been embedded
    with pg.cursor() as cur:
        cur.execute(
            "UPDATE documents SET status='embedded', embedded_model='m', indexed_at=now() "
            "WHERE source='wiki'"
        )
    pg.commit()

    _seed_pending(pg, [s1], date="20260719")
    totals = wingest.ingest(pg, settings, max_files=1, chunk_rows=8)
    assert totals == {"files": 1, "articles": 3, "staged": 1, "skipped": 2}

    with pg.cursor() as cur:
        cur.execute("SELECT id, status, text_ref FROM documents WHERE source='wiki' ORDER BY id")
        rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    # unchanged articles stay embedded (never re-queued); changed one is re-queued
    assert rows["wiki:12"][0] == "embedded"
    assert rows["wiki:290"][0] == "embedded"
    assert rows["wiki:39"][0] == "deduped"
    assert rows["wiki:39"][1] == "wiki/clean/enwiki_content-20260719-00000.parquet"
    # the new shard's clean parquet holds only the changed article
    new_table = pq.read_table(settings.staging_dir / rows["wiki:39"][1])
    assert new_table.num_rows == 1
