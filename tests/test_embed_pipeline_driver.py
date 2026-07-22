"""Driver-level guards for the shared streaming embed pipeline.

These cover the properties the per-source tests (test_embed_pipelines.py) don't:
the prefetch/overlap must not drop or duplicate documents, the
upsert→status-commit ordering must hold across a mid-pass crash, pause must
still cancel, and the runtime profile must still bound in-flight work.

All Qdrant interaction here is against pytest-model collections or a fake
client. NOTHING in this file may touch a *_current alias — a test once deleted
through the live arxiv_current alias and hit production.
"""

import threading
import time

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import windex.ccnews.embed_index as news_embed
import windex.embed.pipeline as embed_pipeline
from windex import db


class FakeQdrant:
    """Records upserts. wait= is captured so the ordering test can assert it."""

    def __init__(self, on_upsert=None):
        self.upserted: list[list] = []
        self.waits: list[object] = []
        self.on_upsert = on_upsert
        self.lock = threading.Lock()

    def upsert(self, collection_name, points, wait=None, **kw):
        if self.on_upsert:
            self.on_upsert(points)  # may raise: simulates Qdrant dying mid-pass
        with self.lock:
            self.upserted.append(list(points))
            self.waits.append(wait)

    def close(self):
        pass


def _stage(pg, settings, n, text_ref="news/clean/drv.parquet", prefix="d"):
    """n news docs in one clean parquet + matching 'deduped' ledger rows.
    `prefix` keeps doc ids unique when a test stages more than one text_ref."""
    path = settings.staging_dir / text_ref
    path.parent.mkdir(parents=True, exist_ok=True)
    ids = [f"news:{prefix}{i:04d}" for i in range(n)]
    pq.write_table(
        pa.table({
            "id": ids,
            "url": [f"https://x/{i}" for i in range(n)],
            "canonical_url": [f"https://x/{i}" for i in range(n)],
            "title": [f"Doc {i}" for i in range(n)],
            "published_at": [None] * n,
            "lang": ["en"] * n,
            "text": [f"body {i} " * 20 for i in range(n)],
        }),
        path,
        # many small row groups: exercises the streaming reader's batching
        row_group_size=8,
    )
    with pg.cursor() as cur:
        cur.executemany(
            """INSERT INTO documents (id, source, url, title, status, text_ref)
               VALUES (%s, 'news', %s, %s, 'deduped', %s)""",
            [(i, f"https://x/{k}", f"Doc {k}", text_ref) for k, i in enumerate(ids)],
        )
    pg.commit()
    return ids


def _stage_texts(pg, settings, items, text_ref="news/clean/e.parquet"):
    """Stage news docs with explicit (id_suffix, title, text) triples so a test
    controls exactly which docs compose to empty. Clean parquet + 'deduped' ledger."""
    path = settings.staging_dir / text_ref
    path.parent.mkdir(parents=True, exist_ok=True)
    ids = [f"news:{s}" for s, _, _ in items]
    pq.write_table(
        pa.table({
            "id": ids,
            "url": [f"https://x/{i}" for i in range(len(items))],
            "canonical_url": [f"https://x/{i}" for i in range(len(items))],
            "title": [t for _, t, _ in items],
            "published_at": [None] * len(items),
            "lang": ["en"] * len(items),
            "text": [x for _, _, x in items],
        }),
        path, row_group_size=8,
    )
    with pg.cursor() as cur:
        cur.executemany(
            """INSERT INTO documents (id, source, url, title, status, text_ref)
               VALUES (%s, 'news', %s, %s, 'deduped', %s)""",
            [(i, f"https://x/{k}", items[k][1], text_ref) for k, i in enumerate(ids)],
        )
    pg.commit()
    return ids


def _status_of(pg, doc_id):
    with pg.cursor() as cur:
        cur.execute(
            "SELECT status, indexed_at, embedded_model FROM documents WHERE id = %s", (doc_id,)
        )
        return cur.fetchone()


def _embedded(pg) -> set[str]:
    with pg.cursor() as cur:
        cur.execute("SELECT id FROM documents WHERE status = 'embedded'")
        return {r[0] for r in cur.fetchall()}


def test_compose_text_bounds_the_whole_string_not_just_the_body():
    """title + body must be bounded TOGETHER: slicing only the body and then
    prepending an unbounded title lets a long title blow past the char≈token cap
    (the exact 400-and-retry-forever failure sanitize.py exists to prevent)."""
    row = {"title": "T" * 500, "text": "b" * 5000}
    out = embed_pipeline.compose_text(row, "text", max_chars=2048)
    assert len(out) <= 2048
    # a short title is still fully preserved ahead of the (bounded) body
    row2 = {"title": "Short", "text": "b" * 5000}
    out2 = embed_pipeline.compose_text(row2, "text", max_chars=2048)
    assert out2.startswith("Short\n\n") and len(out2) <= 2048


def test_poison_doc_is_isolated_and_marked_failed(pg, settings, monkeypatch):
    """A permanently-rejected (EmbedRejected) document must not wedge the shared
    driver: it is isolated out and marked 'failed' so the pass advances, while
    every good doc still embeds. Before the fix EmbedRejected propagated out and
    the same batch was re-selected and re-crashed forever."""
    from windex.embed.base import EmbedRejected

    class RejectingEmbedder:
        model_id = "pytest-model"
        dim = 8

        def embed_batch(self, texts):
            if any("POISON" in t for t in texts):
                raise EmbedRejected(400, "context length exceeded")
            return [[0.1] * 8 for _ in texts]

        def ping(self):
            return True

        def close(self):
            pass

    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: RejectingEmbedder())
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")

    path = settings.staging_dir / "news/clean/poison.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    ids = [f"news:p{i}" for i in range(5)]
    bodies = [f"clean body {i} " * 5 for i in range(5)]
    bodies[2] = "POISON " * 20  # the one document the server rejects outright
    pq.write_table(
        pa.table({
            "id": ids,
            "url": [f"https://x/{i}" for i in range(5)],
            "canonical_url": [f"https://x/{i}" for i in range(5)],
            "title": [f"Doc {i}" for i in range(5)],
            "published_at": [None] * 5,
            "lang": ["en"] * 5,
            "text": bodies,
        }),
        path, row_group_size=8,
    )
    with pg.cursor() as cur:
        cur.executemany(
            """INSERT INTO documents (id, source, url, title, status, text_ref)
               VALUES (%s, 'news', %s, %s, 'deduped', 'news/clean/poison.parquet')""",
            [(i, f"https://x/{k}", f"Doc {k}") for k, i in enumerate(ids)],
        )
    pg.commit()

    n = news_embed.embed_pending(pg, settings, limit=100)

    assert n == 4  # the four good docs
    assert _embedded(pg) == {i for j, i in enumerate(ids) if j != 2}
    with pg.cursor() as cur:
        cur.execute("SELECT id FROM documents WHERE status = 'failed'")
        assert {r[0] for r in cur.fetchall()} == {"news:p2"}
        cur.execute("SELECT count(*) FROM documents WHERE status = 'deduped'")
        assert cur.fetchone()[0] == 0  # nothing left wedged for a re-crash
        # the failed doc landed NO Qdrant point, so it must not be stamped as
        # 'indexed' — that would surface it in the recent-ticker / throughput
        # dashboards as if it had just been embedded (api/service.py reads
        # indexed_at as "landed in Qdrant").
        cur.execute("SELECT indexed_at FROM documents WHERE id = 'news:p2'")
        assert cur.fetchone()[0] is None


def test_overlap_embeds_every_doc_exactly_once(pg, settings, fake_embedder, monkeypatch):
    """Prefetch + no per-ref barrier must not drop or duplicate work, including
    across text_ref boundaries and a ragged final batch."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")

    a = _stage(pg, settings, 37, "news/clean/a.parquet", prefix="a")  # not a batch multiple
    b = _stage(pg, settings, 23, "news/clean/b.parquet", prefix="b")
    expected = set(a) | set(b)
    assert len(expected) == 60

    n = news_embed.embed_pending(pg, settings, limit=1000)

    assert n == len(expected)
    assert _embedded(pg) == expected
    upserted = [p.payload["doc_id"] for batch in fake.upserted for p in batch]
    assert sorted(upserted) == sorted(expected)  # no duplicates, none dropped


def test_status_never_commits_before_vectors_land(pg, pg_dsn, settings, fake_embedder,
                                                  monkeypatch):
    """The ordering contract, tested directly: hold every upsert open and assert
    that nothing has been marked 'embedded' while the vectors are still in
    flight. This is the property wait=False would destroy — with a non-durable
    upsert the status commit races ahead, and a crash in that window strands
    documents marked embedded whose vectors do not exist."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    gate = threading.Event()
    landed: list[str] = []
    lock = threading.Lock()

    class GatedQdrant:
        def upsert(self, collection_name, points, wait=None, **kw):
            assert wait is True, "upserts must be durable before the status commit"
            gate.wait(timeout=30)  # vectors are NOT durable until this releases
            with lock:
                landed.extend(p.payload["doc_id"] for p in points)

        def close(self):
            pass

    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: GatedQdrant())
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")
    ids = _stage(pg, settings, 8)

    out: dict = {}
    t = threading.Thread(
        target=lambda: out.update(n=news_embed.embed_pending(pg, settings, limit=100))
    )
    t.start()
    try:
        # Long enough for the (fake, instant) embedder to finish and every batch
        # to be sitting in the gated upsert.
        time.sleep(1.5)
        with psycopg.connect(pg_dsn) as obs, obs.cursor() as cur:
            cur.execute("SELECT count(*) FROM documents WHERE status = 'embedded'")
            committed = cur.fetchone()[0]
        assert committed == 0, (
            f"{committed} docs were marked 'embedded' while their vectors were "
            "still in flight — the upsert→status ordering is broken"
        )
    finally:
        gate.set()
        t.join(timeout=30)

    assert out["n"] == len(ids)
    assert _embedded(pg) == set(ids) == set(landed)


def test_crash_mid_pass_leaves_no_doc_embedded_without_vectors(pg, settings, fake_embedder,
                                                               monkeypatch):
    """Crash recovery: when Qdrant dies mid-pass, no document may be marked
    'embedded' whose vectors never landed, and the remainder must stay
    re-runnable."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    seen: list[str] = []

    def die_after_two(points):
        if len(seen) >= 2:
            raise RuntimeError("qdrant died mid-pass")
        seen.append("ok")

    fake = FakeQdrant(on_upsert=die_after_two)
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")

    _stage(pg, settings, 40)

    with pytest.raises(RuntimeError, match="qdrant died"):
        news_embed.embed_pending(pg, settings, limit=1000)

    # Every doc marked embedded must have vectors durably in Qdrant.
    landed = {p.payload["doc_id"] for batch in fake.upserted for p in batch}
    assert _embedded(pg) <= landed, "a doc was marked embedded without landed vectors"
    # The crash is recoverable: the rest stays 'deduped' for the next pass.
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE status = 'deduped'")
        assert cur.fetchone()[0] > 0


def test_unreadable_staging_raises_instead_of_silently_short_passing(
    pg, settings, fake_embedder, monkeypatch
):
    """The staging drive has detached mid-run before. Reading now happens on a
    background thread, so its failure must still reach the caller — embed-loop
    backs off and re-probes on a raise, but would read a pass that quietly
    returns 0 as a drained queue and stop making progress."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")

    _stage(pg, settings, 8)
    # the ledger points at a text_ref whose parquet is gone (drive detached)
    (settings.staging_dir / "news/clean/drv.parquet").unlink()

    with pytest.raises(Exception):  # noqa: B017 — pyarrow's error type is its own
        news_embed.embed_pending(pg, settings, limit=100)


def test_upserts_always_use_wait_true(pg, settings, fake_embedder, monkeypatch):
    """wait=False would make the status commit a lie. Guard it explicitly."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")

    _stage(pg, settings, 12)
    news_embed.embed_pending(pg, settings, limit=1000)

    assert fake.waits and all(w is True for w in fake.waits)


def test_pause_stops_the_pass_and_leaves_the_rest_pending(pg, settings, fake_embedder,
                                                          monkeypatch):
    """The dashboard pause button must still take effect mid-pass, and what it
    interrupts must remain re-runnable."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")
    ids = _stage(pg, settings, 400)

    # Pause is observed via db.get_control; trip it once the pass is under way.
    real_get_control = db.get_control
    calls = {"n": 0}

    def flaky(conn, key, default):
        if key == "indexing":
            calls["n"] += 1
            return "paused" if calls["n"] >= 2 else "running"
        return real_get_control(conn, key, default)

    monkeypatch.setattr(embed_pipeline.db, "get_control", flaky)

    n = news_embed.embed_pending(pg, settings, limit=1000)

    assert 0 < n < len(ids)  # stopped early, but committed what it finished
    assert len(_embedded(pg)) == n
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE status = 'deduped'")
        assert cur.fetchone()[0] == len(ids) - n  # remainder still re-runnable


def test_runtime_profile_bounds_inflight_batches(pg, settings, fake_embedder, monkeypatch):
    """Throttle profiles must still cap concurrency: the prefetch is sized from
    embed_concurrency, so 'polite' must not become a firehose."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")
    db.set_control(pg, "embed_profile", "polite")

    live = {"n": 0, "peak": 0}
    lock = threading.Lock()
    real_embed = fake_embedder.embed_batch

    def counting(texts):
        with lock:
            live["n"] += 1
            live["peak"] = max(live["peak"], live["n"])
        try:
            return real_embed(texts)
        finally:
            with lock:
                live["n"] -= 1

    monkeypatch.setattr(fake_embedder, "embed_batch", counting)
    # Real 'polite' is concurrency 2 / batch 16 / throttle 1.0s. Keep the
    # concurrency (that's what's under test), drop the sleep so the test is fast.
    import windex.embed as embed_pkg

    monkeypatch.setitem(embed_pkg.PROFILES, "polite",
                        {"embed_concurrency": 2, "embed_batch_size": 16,
                         "embed_throttle_seconds": 0.0})
    _stage(pg, settings, 200)

    news_embed.embed_pending(pg, settings, limit=1000)

    assert live["peak"] <= 2, f"polite profile exceeded its concurrency: {live['peak']}"


def test_empty_doc_is_isolated_and_marked_empty_not_embedded(pg, settings, fake_embedder,
                                                             monkeypatch):
    """A fully-empty composed doc (blank title AND body) must never be embedded: it
    lands status='empty' with no Qdrant point and no indexed_at, while every real
    doc in the batch still embeds. An empty string is NOT a server rejection, so
    without the guard it upserts a junk vector (the 7 empty hn vectors)."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")

    _stage_texts(pg, settings, [
        ("good0", "Doc 0", "real body " * 10),
        ("empty1", "", ""),
        ("good2", "Doc 2", "real body " * 10),
        ("good3", "Doc 3", "real body " * 10),
    ])

    n = news_embed.embed_pending(pg, settings, limit=100)

    assert n == 3
    assert _embedded(pg) == {"news:good0", "news:good2", "news:good3"}
    status, indexed_at, model = _status_of(pg, "news:empty1")
    assert status == "empty" and indexed_at is None and model is None
    upserted = {p.payload["doc_id"] for batch in fake.upserted for p in batch}
    assert "news:empty1" not in upserted
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE status = 'deduped'")
        assert cur.fetchone()[0] == 0  # nothing left wedged for a re-crash


def test_title_only_doc_is_not_treated_as_empty(pg, settings, fake_embedder, monkeypatch):
    """~91% of hn is a legitimate title-only link post: a present title with an
    empty body must embed normally. The guard is whitespace-only, NOT a length
    threshold — this is the load-bearing regression guard."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")

    _stage_texts(pg, settings, [("t", "Latvia Startups", "")])

    n = news_embed.embed_pending(pg, settings, limit=100)

    assert n == 1
    assert _embedded(pg) == {"news:t"}
    upserted = {p.payload["doc_id"] for batch in fake.upserted for p in batch}
    assert "news:t" in upserted


def test_whitespace_only_doc_is_treated_as_empty(pg, settings, fake_embedder, monkeypatch):
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")

    _stage_texts(pg, settings, [("ws", "   ", "  \n\t ")])

    n = news_embed.embed_pending(pg, settings, limit=100)

    assert n == 0
    assert _status_of(pg, "news:ws")[0] == "empty"
    assert fake.upserted == []


def test_all_empty_batch_never_calls_the_embedder(pg, settings, monkeypatch):
    """An all-empty batch must be short-circuited before the embedder is touched —
    proves the network call is skipped, not just the vector discarded afterward."""

    class BoomEmbedder:
        model_id = "pytest-model"
        dim = 8

        def embed_batch(self, texts):
            raise AssertionError("embedder called on an all-empty batch")

        def ping(self):
            return True

        def close(self):
            pass

    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: BoomEmbedder())
    fake = FakeQdrant()
    monkeypatch.setattr(embed_pipeline, "QdrantClient", lambda **kw: fake)
    monkeypatch.setattr(embed_pipeline.qidx, "ensure_collection", lambda *a, **k: "c")

    _stage_texts(pg, settings, [("e0", "", ""), ("e1", "  ", "")])

    n = news_embed.embed_pending(pg, settings, limit=100)

    assert n == 0
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE status = 'empty'")
        assert cur.fetchone()[0] == 2
    assert fake.upserted == []
