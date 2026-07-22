"""Wikipedia reader (format seam) + dump-discovery watermark.

The reader tests are pure (no services); the sync tests use the pg fixture and
monkeypatch the HTTP listing so no network is touched.
"""

import bz2
import json

from windex.wiki import reader
from windex.wiki import sync as wsync


def _article(page_id, title, text, ns=0, ts="2026-07-12T00:00:00Z",
             opening=None, incoming=0):
    return {
        "page_id": page_id, "namespace": ns, "title": title, "text": text,
        "timestamp": ts, "opening_text": opening or text[:60], "incoming_links": incoming,
    }


def _make_dump(articles, action=None):
    """Build a bz2 CirrusSearch-style shard from article dicts. `action` builds
    the index line per article (default: real observed shape {"index":{"_id":int}})."""
    action = action or (lambda a: {"index": {"_id": a["page_id"]}})
    lines = []
    for a in articles:
        lines.append(json.dumps(action(a)))
        lines.append(json.dumps(a))
    return bz2.compress(("\n".join(lines) + "\n").encode())


def _chunks(data, n=64):
    for i in range(0, len(data), n):
        yield data[i : i + n]


def test_reader_parses_three_articles_and_filters_namespace():
    dump = _make_dump([
        _article(12, "Anarchism", "Anarchism is a political philosophy. " * 5,
                 opening="Anarchism is a political philosophy.", incoming=1200),
        _article(39, "Autism", "Autism is a neurodevelopmental condition. " * 5),
        _article(290, "A", "The letter A. " * 5),
        _article(999, "Talk:Anarchism", "talk page chatter", ns=1),  # dropped
    ])
    recs = list(reader.iter_articles_from_bytes(_chunks(dump)))
    assert [r["id"] for r in recs] == ["wiki:12", "wiki:39", "wiki:290"]
    first = recs[0]
    assert first["url"] == "https://en.wikipedia.org/wiki/Anarchism"
    assert first["title"] == "Anarchism"
    assert first["revision_ts"] == "2026-07-12T00:00:00Z"
    assert first["incoming_links"] == 1200
    assert first["opening_text"] == "Anarchism is a political philosophy."


def test_reader_url_encoding():
    assert reader.wiki_url("C++") == "https://en.wikipedia.org/wiki/C%2B%2B"
    assert reader.wiki_url("A/B testing") == "https://en.wikipedia.org/wiki/A/B_testing"
    assert reader.wiki_url("Saint-Étienne") == "https://en.wikipedia.org/wiki/Saint-%C3%89tienne"


def test_reader_tolerates_type_field_and_string_id():
    # The coordinator-described variant: action carries _type and a string _id,
    # and the document omits page_id — the reader must fall back to _id.
    doc = {"namespace": 0, "title": "Fallback", "text": "body " * 20,
           "timestamp": "2026-07-12T00:00:00Z", "opening_text": "body"}
    dump = _make_dump([doc], action=lambda a: {"index": {"_type": "page", "_id": "4242"}})
    recs = list(reader.iter_articles_from_bytes(_chunks(dump)))
    assert len(recs) == 1 and recs[0]["id"] == "wiki:4242" and recs[0]["page_id"] == 4242


def test_reader_skips_documents_without_text():
    dump = _make_dump([
        {"page_id": 1, "namespace": 0, "title": "Empty", "text": "", "timestamp": None},
        _article(2, "Real", "has text " * 10),
    ])
    recs = list(reader.iter_articles_from_bytes(_chunks(dump)))
    assert [r["id"] for r in recs] == ["wiki:2"]


def _content_dir(date, count, success=True):
    return success, [(f"enwiki_content-{date}-{i:05d}.json.bz2", 1000 + i) for i in range(count)]


def test_sync_records_newest_complete_and_is_idempotent(pg, monkeypatch):
    monkeypatch.setattr(wsync, "list_dates", lambda c: ["20260712", "20260705"])
    monkeypatch.setattr(
        wsync, "list_content_dir",
        lambda c, date, wiki: _content_dir(date, 3 if date == "20260712" else 5),
    )
    n = wsync.sync(pg, "enwiki")
    assert n == 3
    pending = wsync.pending_shards(pg, 10)
    assert [name for name, _ in pending] == [
        f"enwiki_content-20260712-{i:05d}.json.bz2" for i in range(3)
    ]
    assert all(d == "20260712" for _, d in pending)
    assert wsync.sync(pg, "enwiki") == 0  # idempotent: newest snapshot already recorded


def test_sync_requires_success_marker(pg, monkeypatch):
    # newest date has no _SUCCESS yet → fall back to the older complete snapshot
    monkeypatch.setattr(wsync, "list_dates", lambda c: ["20260719", "20260712"])

    def content_dir(c, date, wiki):
        if date == "20260719":
            return False, []  # snapshot still uploading
        return _content_dir(date, 2)

    monkeypatch.setattr(wsync, "list_content_dir", content_dir)
    n = wsync.sync(pg, "enwiki")
    assert n == 2
    assert all(d == "20260712" for _, d in wsync.pending_shards(pg, 10))


def test_sync_no_complete_snapshot_is_noop(pg, monkeypatch):
    monkeypatch.setattr(wsync, "list_dates", lambda c: ["20260719"])
    monkeypatch.setattr(wsync, "list_content_dir", lambda c, date, wiki: (False, []))
    assert wsync.sync(pg, "enwiki") == 0
    assert wsync.pending_shards(pg, 10) == []


def test_mark_updates_status_and_counts(pg, monkeypatch):
    monkeypatch.setattr(wsync, "list_dates", lambda c: ["20260712"])
    monkeypatch.setattr(wsync, "list_content_dir", lambda c, date, wiki: _content_dir(date, 1))
    wsync.sync(pg, "enwiki")
    name = "enwiki_content-20260712-00000.json.bz2"
    wsync.mark(pg, [name], "done", {"staged": 7})
    with pg.cursor() as cur:
        cur.execute("SELECT status, doc_counts->>'staged' FROM wiki_dumps WHERE name=%s", (name,))
        assert cur.fetchone() == ("done", "7")
    assert wsync.pending_shards(pg, 10) == []


def test_sync_rearms_a_failed_shard_but_not_a_done_one(pg, monkeypatch):
    """A shard marked 'failed' by a transient ingest error must be retried on the
    next scheduled `wiki sync` (ON CONFLICT DO NOTHING left it failed forever, so
    its ~112k articles were silently never ingested for that snapshot). A 'done'
    shard must NOT be re-armed."""
    monkeypatch.setattr(wsync, "list_dates", lambda c: ["20260712"])
    monkeypatch.setattr(wsync, "list_content_dir", lambda c, date, wiki: _content_dir(date, 2))
    wsync.sync(pg, "enwiki")
    failed = "enwiki_content-20260712-00000.json.bz2"
    done = "enwiki_content-20260712-00001.json.bz2"
    wsync.mark(pg, [failed], "failed")
    wsync.mark(pg, [done], "done")
    assert wsync.pending_shards(pg, 10) == []  # neither is pending

    wsync.sync(pg, "enwiki")  # re-run for the same still-newest snapshot
    assert [n for n, _ in wsync.pending_shards(pg, 10)] == [failed]  # only the failed one re-armed


def test_reclaim_stale_frees_processing_shards_from_a_killed_run(pg):
    """A killed ingest leaves its shard 'processing'; pending_shards() only selects
    'pending', so it is silently skipped forever. Reclaim on age; never steal a
    shard a live worker just claimed."""
    with pg.cursor() as cur:
        cur.execute("INSERT INTO wiki_dumps (name, dump_date, status, processed_at) "
                    "VALUES ('s-stale', '20260712', 'processing', now() - interval '3 hours')")
        cur.execute("INSERT INTO wiki_dumps (name, dump_date, status, processed_at) "
                    "VALUES ('s-live', '20260712', 'processing', now())")
    pg.commit()

    assert wsync.reclaim_stale(pg, older_than_minutes=60) == 1
    assert [n for n, _ in wsync.pending_shards(pg, 10)] == ["s-stale"]
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM wiki_dumps WHERE name='s-live'")
        assert cur.fetchone()[0] == "processing", "stole a shard from a live worker"
