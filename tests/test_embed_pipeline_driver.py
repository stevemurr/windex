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


def _embedded(pg) -> set[str]:
    with pg.cursor() as cur:
        cur.execute("SELECT id FROM documents WHERE status = 'embedded'")
        return {r[0] for r in cur.fetchall()}


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
