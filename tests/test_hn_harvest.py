"""Hacker News harvest + backfill: recursive window splitting against a
fabricated over-cap Algolia, watermark idempotency, staged harvest → parquet +
ledger with text_hash delta detection, the points-refresh-without-re-embed
path, and the open-index parquet fast path (type/dead/deleted filtering). Uses
the pg fixture and fake clients so the network is never touched."""

from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from windex.hn import backfill as hbackfill
from windex.hn import harvest as hharvest

# --- fabricated Algolia -------------------------------------------------------


def _epoch(iso: str) -> int:
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp())


def _hit(item_id, ts, title=None, url="https://example.com/x", story_text=None,
         points=1, num_comments=0):
    return {"objectID": str(item_id), "title": title or f"Story {item_id}",
            "url": url, "story_text": story_text, "points": points,
            "num_comments": num_comments, "author": f"user{item_id}",
            "created_at_i": ts}


class _FakeResp:
    def __init__(self, body, status_code=200):
        self.body = body
        self.status_code = status_code
        self.headers = {}

    def json(self):
        return self.body

    def raise_for_status(self):
        pass


class _FakeAlgolia:
    """Serves a fixed story universe the way the real API does (verified live):
    nbHits is the true in-range count, but any single query returns at most
    `cap` hits — past the cap the response is still 200 with truncated hits."""

    def __init__(self, hits, cap=hharvest.MAX_HITS):
        self.universe = sorted(hits, key=lambda h: h["created_at_i"])
        self.cap = cap
        self.calls = []

    def get(self, url, params=None):
        import re

        nf = params["numericFilters"]
        frm = int(re.search(r">=(\d+)", nf).group(1))
        until = int(re.search(r"<(\d+)", nf).group(1))
        self.calls.append((frm, until))
        sel = [h for h in self.universe if frm <= h["created_at_i"] < until]
        return _FakeResp({"nbHits": len(sel), "hits": sel[: self.cap]})

    def close(self):
        pass


# --- pure shaping -------------------------------------------------------------


def test_clean_text_strips_html_fragments():
    frag = "I &#x27;quoted&#x27; this.<p>Second   paragraph with <a href=\"https://x\">a link</a>.<p><i>em</i>"
    assert hharvest.clean_text(frag) == "I 'quoted' this.\n\nSecond paragraph with a link.\n\nem"
    assert hharvest.clean_text(None) == ""
    assert hharvest.clean_text("") == ""


def test_story_from_hit_shapes_doc():
    s = hharvest.story_from_hit(_hit(4492, _epoch("2007-03-15T23:13:32"),
                                     title=" A  title ", points=57, num_comments=15))
    assert s["id"] == "hn:4492"
    assert s["url"] == "https://news.ycombinator.com/item?id=4492"  # canonical: the discussion
    assert s["target_url"] == "https://example.com/x"
    assert s["title"] == "A title" and s["points"] == 57 and s["num_comments"] == 15
    assert s["created_at"] == "2007-03-15T23:13:32Z"
    assert s["thash"]
    # self post: url is null upstream → target_url None, text stripped + hashed in
    ask = hharvest.story_from_hit(_hit(9, 1174000000, url=None,
                                       story_text="Ask HN: why&#x27;s that?"))
    assert ask["target_url"] is None
    assert ask["story_text"] == "Ask HN: why's that?"
    assert ask["thash"] != s["thash"]


# --- window watermark ---------------------------------------------------------


def test_plan_backfill_months_idempotent(pg):
    now = datetime(2007, 2, 10, tzinfo=timezone.utc)
    assert hharvest.plan_backfill(pg, now=now) == 5  # 2006-10 .. 2007-02
    assert hharvest.plan_backfill(pg, now=now) == 0  # already recorded
    windows = hharvest.pending_windows(pg, 10)
    assert windows[0] == hharvest.month_epochs(2006, 10)
    assert windows[-1] == hharvest.month_epochs(2007, 2)
    assert [w for w in windows] == sorted(windows)
    # explicit month bounds work too (Dec→Jan rollover)
    assert hharvest.plan_backfill(pg, from_year=2007, from_month=12,
                                  to_year=2008, to_month=1) == 2


def test_plan_incremental_rearms_completed_window(pg):
    now = datetime(2026, 7, 16, 12, 30, tzinfo=timezone.utc)
    frm, until = hharvest.plan_incremental(pg, 2, now=now)
    assert (frm, until) == (_epoch("2026-07-14"), _epoch("2026-07-17"))
    hharvest.mark_window(pg, frm, until, "done")
    assert hharvest.pending_windows(pg, 10) == []
    # a later run the same UTC day re-arms the same span (points re-pull)
    hharvest.plan_incremental(pg, 2, now=now)
    assert hharvest.pending_windows(pg, 10) == [(frm, until)]
    # an in-flight window is left alone
    hharvest.mark_window(pg, frm, until, "processing")
    hharvest.plan_incremental(pg, 2, now=now)
    assert hharvest.pending_windows(pg, 10) == []


def test_reclaim_stale_frees_windows_from_a_killed_run(pg):
    """A killed harvest/backfill leaves its window 'processing'; pending_windows()
    only selects 'pending', so that month is silently absent from the index and
    nothing ever retries it (the arxiv failure class, unported to hn until now).
    Reclaim on age; never steal a window a live worker just claimed."""
    with pg.cursor() as cur:
        cur.execute("INSERT INTO hn_windows (from_ts, until_ts, status, processed_at) "
                    "VALUES (100, 200, 'processing', now() - interval '3 hours')")  # stale
        cur.execute("INSERT INTO hn_windows (from_ts, until_ts, status, processed_at) "
                    "VALUES (300, 400, 'processing', now())")  # live worker
        cur.execute("INSERT INTO hn_windows (from_ts, until_ts, status) "
                    "VALUES (500, 600, 'done')")
    pg.commit()

    assert hharvest.reclaim_stale(pg, older_than_minutes=60) == 1
    assert (100, 200) in hharvest.pending_windows(pg, 10)
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM hn_windows WHERE from_ts=300")
        assert cur.fetchone()[0] == "processing", "stole a window from a live worker"


def test_reclaim_ignores_a_window_claimed_via_mark_window(pg):
    """Regression: mark_window('processing') must stamp processed_at at claim time
    so a freshly-claimed window (processed_at not NULL, not old) is never
    reclaimed out from under a running harvest."""
    hharvest.plan_backfill(pg, from_year=2020, from_month=1, to_year=2020, to_month=1)
    frm, until = hharvest.pending_windows(pg, 1)[0]
    hharvest.mark_window(pg, frm, until, "processing")
    with pg.cursor() as cur:
        cur.execute("SELECT processed_at FROM hn_windows WHERE from_ts=%s", (frm,))
        assert cur.fetchone()[0] is not None, "claim did not stamp processed_at"
    assert hharvest.reclaim_stale(pg, older_than_minutes=60) == 0
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM hn_windows WHERE from_ts=%s", (frm,))
        assert cur.fetchone()[0] == "processing"


# --- recursive cap splitting ----------------------------------------------------


def test_fetch_window_splits_recursively_on_cap():
    day0 = _epoch("2026-07-15")
    universe = [_hit(i, day0 + i * 3600) for i in range(8)]  # spread over the day
    fake = _FakeAlgolia(universe, cap=3)
    hits, queries = hharvest.fetch_window_stories(fake, "u", day0, day0 + 86400, max_hits=3)
    assert sorted(h["objectID"] for h in hits) == sorted(str(i) for i in range(8))
    assert len(hits) == 8  # complete, no duplicates
    assert queries == len(fake.calls)
    # the first call saw the whole over-cap day and split; every returned page
    # was within cap
    assert fake.calls[0] == (day0, day0 + 86400)
    for frm, until in fake.calls[1:]:
        assert day0 <= frm < until <= day0 + 86400
    over_cap = [c for c in fake.calls
                if len([h for h in universe if c[0] <= h["created_at_i"] < c[1]]) > 3]
    for c in over_cap:  # over-cap ranges were never taken as-is: both halves queried
        mid = (c[0] + c[1]) // 2
        assert (c[0], mid) in fake.calls and (mid, c[1]) in fake.calls


# --- harvest → parquet + ledger -------------------------------------------------


def test_harvest_stages_parquet_and_ledger(pg, settings):
    now = datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc)
    day0 = _epoch("2026-07-15")
    fake = _FakeAlgolia([
        _hit(101, day0 + 10, title="Show HN: windex", points=42, num_comments=7),
        _hit(102, day0 + 20, title="Ask HN: parquet?", url=None,
             story_text="Is <i>parquet</i> good?", points=5),
    ])
    frm, until = hharvest.plan_incremental(pg, 1, now=now)
    totals = hharvest.harvest(pg, settings, client=fake, request_interval=0)
    assert totals == {"windows": 1, "queries": 1, "hits": 2, "staged": 2,
                      "skipped": 0, "refreshed": 0}

    text_ref = "hn/clean/20260715_20260717.parquet"
    table = pq.read_table(settings.staging_dir / text_ref)
    assert table.num_rows == 2
    assert set(table.column_names) == {"id", "url", "target_url", "title", "story_text",
                                       "author", "points", "num_comments", "created_at"}
    ask = table.filter(pc.equal(table["id"], "hn:102")).to_pylist()[0]
    assert ask["target_url"] is None and ask["story_text"] == "Is parquet good?"
    show = table.filter(pc.equal(table["id"], "hn:101")).to_pylist()[0]
    assert show["points"] == 42 and show["num_comments"] == 7
    assert show["url"] == "https://news.ycombinator.com/item?id=101"

    with pg.cursor() as cur:
        cur.execute(
            "SELECT id, source, url, status, text_ref, published_at, text_hash "
            "FROM documents WHERE source='hn' ORDER BY id"
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == ["hn:101", "hn:102"]
    for r in rows:
        assert r[1] == "hn" and r[3] == "deduped" and r[4] == text_ref
        assert r[5] is not None and r[6]  # published_at + text_hash populated
    assert rows[0][2] == "https://news.ycombinator.com/item?id=101"

    with pg.cursor() as cur:
        cur.execute("SELECT status, queries, hits, staged FROM hn_windows "
                    "WHERE from_ts=%s AND until_ts=%s", (frm, until))
        assert cur.fetchone() == ("done", 1, 2, 2)


def test_reharvest_unchanged_skips_but_refreshes_points(pg, settings, qclient, fake_embedder, monkeypatch):
    import windex.hn.embed_index as hn_embed
    from windex.ccnews.embed_index import point_id

    now = datetime(2026, 7, 16, tzinfo=timezone.utc)
    day0 = _epoch("2026-07-15")
    base = [_hit(201, day0 + 10, title="Stable title", points=10, num_comments=1),
            _hit(202, day0 + 20, title="Will change", points=3)]
    hharvest.plan_incremental(pg, 1, now=now)
    hharvest.harvest(pg, settings, client=_FakeAlgolia(base), request_interval=0)

    # embed into the pytest collection, then pin the alias at it — once real
    # collections exist, hn_current must never resolve to production here
    import windex.embed.pipeline as embed_pipeline

    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    assert hn_embed.embed_pending(pg, settings, limit=10) == 2
    coll = "hn__pytest-model"
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda source: coll)

    pid = point_id("hn:201")
    before = qclient.retrieve(coll, ids=[pid], with_payload=True, with_vectors=True)[0]
    assert before.payload["points"] == 10

    # trailing re-pull: same text, drifted points on 201; new text on 202
    bumped = [_hit(201, day0 + 10, title="Stable title", points=99, num_comments=44),
              _hit(202, day0 + 20, title="Will change (edited)", points=5)]
    hharvest.plan_incremental(pg, 1, now=now)  # re-arms the done window
    totals = hharvest.harvest(pg, settings, client=_FakeAlgolia(bumped), request_interval=0)
    assert totals["staged"] == 1 and totals["skipped"] == 1 and totals["refreshed"] == 1

    with pg.cursor() as cur:
        cur.execute("SELECT id, status FROM documents WHERE source='hn' ORDER BY id")
        rows = dict(cur.fetchall())
    assert rows["hn:201"] == "embedded"   # untouched — no re-embed queued
    assert rows["hn:202"] == "deduped"    # text changed — re-queued

    after = qclient.retrieve(coll, ids=[pid], with_payload=True, with_vectors=True)[0]
    assert after.payload["points"] == 99 and after.payload["num_comments"] == 44
    assert after.payload["title"] == "Stable title"          # rest of payload kept
    assert after.vector["dense"] == before.vector["dense"]   # vector never touched


def test_refresh_skips_stories_not_yet_embedded(pg, settings):
    # both harvests while the doc is still 'deduped': nothing to refresh, and
    # no Qdrant access is attempted (stories list is empty before the client
    # would be built for zero rows)
    now = datetime(2026, 7, 16, tzinfo=timezone.utc)
    day0 = _epoch("2026-07-15")
    hits = [_hit(301, day0 + 10, points=1)]
    hharvest.plan_incremental(pg, 1, now=now)
    hharvest.harvest(pg, settings, client=_FakeAlgolia(hits), request_interval=0)
    hharvest.plan_incremental(pg, 1, now=now)
    totals = hharvest.harvest(
        pg, settings,
        client=_FakeAlgolia([_hit(301, day0 + 10, points=50)]), request_interval=0,
    )
    assert totals["skipped"] == 1 and totals["refreshed"] == 0 and totals["staged"] == 0


# --- open-index parquet fast path ------------------------------------------------


class _FakeStreamResp:
    def __init__(self, payload: bytes):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        pass

    def iter_bytes(self, n):
        yield self.payload


class _FakeMirror:
    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.urls = []

    def stream(self, method, url):
        self.urls.append(url)
        return _FakeStreamResp(self.files[url])

    def close(self):
        pass


def _mirror_parquet(tmp_path, rows, type_dtype=pa.int8()):
    """Fabricate a monthly mirror file with the live-verified dtypes:
    id uint32, type int8 (1=story, 2=comment), dead/deleted uint8 0/1,
    time timestamp[ms, UTC], non-null strings ("" when absent)."""
    table = pa.table({
        "id": pa.array([r["id"] for r in rows], pa.uint32()),
        "deleted": pa.array([r.get("deleted", 0) for r in rows], pa.uint8()),
        "type": pa.array([r["type"] for r in rows], type_dtype),
        "by": pa.array([r.get("by", "u") for r in rows], pa.string()),
        "time": pa.array(
            [datetime.fromtimestamp(r["time"], tz=timezone.utc) for r in rows],
            pa.timestamp("ms", tz="UTC"),
        ),
        "text": pa.array([r.get("text", "") for r in rows], pa.string()),
        "dead": pa.array([r.get("dead", 0) for r in rows], pa.uint8()),
        "url": pa.array([r.get("url", "") for r in rows], pa.string()),
        "score": pa.array([r.get("score", 0) for r in rows], pa.int32()),
        "title": pa.array([r.get("title", "") for r in rows], pa.string()),
        "descendants": pa.array([r.get("descendants", 0) for r in rows], pa.int32()),
    })
    path = tmp_path / "fabricated.parquet"
    pq.write_table(table, path)
    return path.read_bytes()


def test_backfill_month_filters_types_and_flags(pg, settings, tmp_path):
    frm, until = hharvest.month_epochs(2006, 10)
    t = frm + 1000
    payload = _mirror_parquet(tmp_path, [
        {"id": 1, "type": 1, "time": t, "url": "http://ycombinator.com",
         "title": "Y Combinator", "score": 57, "descendants": 15, "by": "pg"},
        {"id": 2, "type": 2, "time": t + 1, "text": "a comment"},            # comment: skipped
        {"id": 3, "type": 1, "time": t + 2, "dead": 1, "title": "Dead"},     # dead: skipped
        {"id": 4, "type": 1, "time": t + 3, "deleted": 1, "title": "Gone"},  # deleted: skipped
        {"id": 5, "type": 1, "time": t + 4, "title": "Ask HN: hi?",          # self post
         "text": "It&#39;s <i>fine</i>."},
    ])
    fake = _FakeMirror({hbackfill.month_url(settings.hn_mirror_url, 2006, 10): payload})

    hharvest.plan_backfill(pg, now=datetime(2006, 10, 15, tzinfo=timezone.utc))
    totals = hbackfill.backfill(pg, settings, client=fake)
    assert totals == {"windows": 1, "hits": 2, "staged": 2, "skipped": 0, "refreshed": 0}
    assert fake.urls == [f"{settings.hn_mirror_url}/2006/2006-10.parquet"]

    with pg.cursor() as cur:
        cur.execute("SELECT id FROM documents WHERE source='hn' ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == ["hn:1", "hn:5"]
        cur.execute("SELECT status, hits, staged FROM hn_windows WHERE from_ts=%s", (frm,))
        assert cur.fetchone() == ("done", 2, 2)

    table = pq.read_table(settings.staging_dir / "hn/clean/20061001_20061101.parquet")
    rows = {r["id"]: r for r in table.to_pylist()}
    assert rows["hn:1"]["target_url"] == "http://ycombinator.com"
    assert rows["hn:1"]["points"] == 57 and rows["hn:1"]["num_comments"] == 15
    assert rows["hn:5"]["target_url"] is None            # "" self-post url → None
    assert rows["hn:5"]["story_text"] == "It's fine."    # entities + tags stripped
    # downloaded month file is removed after staging (keep=False default)
    assert not any(settings.hn_downloads_dir.glob("*.parquet"))


def test_backfill_tolerates_string_type_and_skips_non_month_windows(pg, settings, tmp_path):
    frm, until = hharvest.month_epochs(2007, 3)
    payload = _mirror_parquet(tmp_path, [
        {"id": 10, "type": "story", "time": frm + 5, "title": "S"},
        {"id": 11, "type": "comment", "time": frm + 6, "text": "c"},
    ], type_dtype=pa.string())
    fake = _FakeMirror({hbackfill.month_url(settings.hn_mirror_url, 2007, 3): payload})

    hharvest.plan_backfill(pg, from_year=2007, from_month=3, to_year=2007, to_month=3)
    # the trailing incremental window must be left for Algolia, not the mirror
    inc = hharvest.plan_incremental(pg, 1, now=datetime(2007, 3, 20, tzinfo=timezone.utc))
    totals = hbackfill.backfill(pg, settings, client=fake)
    assert totals["windows"] == 1 and totals["staged"] == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM hn_windows WHERE from_ts=%s AND until_ts=%s", inc)
        assert cur.fetchone()[0] == "pending"  # untouched by the mirror path


def test_month_of_window():
    assert hbackfill.month_of_window(*hharvest.month_epochs(2006, 10)) == (2006, 10)
    assert hbackfill.month_of_window(*hharvest.month_epochs(2026, 12)) == (2026, 12)
    frm, until = hharvest.month_epochs(2026, 7)
    assert hbackfill.month_of_window(frm, until - 3600) is None      # short of a month
    assert hbackfill.month_of_window(frm + 86400, until) is None     # not the 1st


def test_clean_title_and_text_strip_nul_bytes():
    """Regression (2026-07-17): a real story in the 2023-07 mirror window carries
    a NUL in its title. PG text columns cannot hold 0x00, so the INSERT raised and
    the whole month's window failed permanently on every retry. NUL is not
    whitespace, so the old " ".join(raw.split()) idiom passed it straight through."""
    from windex.hn.harvest import clean_text, clean_title

    assert clean_title("Show HN:\x00 a thing") == "Show HN: a thing"
    assert "\x00" not in clean_title("\x00\x00bad\x00")
    assert clean_title(None) == ""
    assert "\x00" not in clean_text("<p>hello\x00 world</p>")
    # normalization must stay identical on both ingest paths or text_hash
    # diverges between harvest (Algolia) and backfill (parquet mirror)
    assert clean_title("  spaced   out  ") == "spaced out"
