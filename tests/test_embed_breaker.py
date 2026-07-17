"""Query-embed circuit breaker: once the embedding GPU is saturated by a
backfill, hybrid searches must stop paying the 8s timeout to rediscover it.

Covers the state machine directly plus its live wiring through search(), which
is the only call site (the bulk embed path must never be breakered)."""

import threading
import time

import pytest

from windex.config import Settings
from windex.index import qdrant as qidx
from windex.index import search as searchmod
from windex.index.embed_breaker import CLOSED, HALF_OPEN, OPEN, EmbedBreakerOpen, QueryEmbedBreaker


@pytest.fixture()
def brk():
    return QueryEmbedBreaker()


@pytest.fixture()
def bset(settings):
    """Fast cooldown so recovery is testable without sleeping for 30s."""
    return settings.model_copy(
        update={"embed_breaker_threshold": 3, "embed_breaker_cooldown": 0.2}
    )


def _fail(brk, settings, n=1, exc=None):
    for _ in range(n):
        assert brk.allow(settings)
        brk.record_failure(exc or TimeoutError("embed timed out"), settings)


# --- state machine ---------------------------------------------------------


def test_threshold_trips_breaker_and_short_circuits(brk, bset):
    _fail(brk, bset, n=2)
    assert brk.snapshot(bset)["state"] == CLOSED  # under threshold: still trying
    assert brk.allow(bset) is True
    brk.record_failure(TimeoutError("embed timed out"), bset)
    snap = brk.snapshot(bset)
    assert snap["state"] == OPEN
    assert snap["trips"] == 1
    assert brk.allow(bset) is False  # subsequent queries skip the dense leg
    assert brk.allow(bset) is False
    assert brk.snapshot(bset)["short_circuited"] == 2


def test_success_resets_consecutive_failures(brk, bset):
    _fail(brk, bset, n=2)
    assert brk.allow(bset)
    brk.record_success()
    assert brk.snapshot(bset)["consecutive_failures"] == 0
    # a lone failure after a success must not carry the earlier two forward
    _fail(brk, bset, n=2)
    assert brk.snapshot(bset)["state"] == CLOSED


def test_half_open_probe_recovers_and_closes(brk, bset):
    _fail(brk, bset, n=3)
    assert brk.snapshot(bset)["state"] == OPEN
    assert brk.allow(bset) is False
    time.sleep(0.25)  # cooldown elapses
    assert brk.allow(bset) is True  # this caller is the probe
    assert brk.snapshot(bset)["state"] == HALF_OPEN
    brk.record_success()
    snap = brk.snapshot(bset)
    assert snap["state"] == CLOSED and snap["last_error"] is None
    assert brk.allow(bset) is True  # hybrid restored, no restart involved


def test_only_one_probe_passes_while_half_open(brk, bset):
    _fail(brk, bset, n=3)
    time.sleep(0.25)
    assert brk.allow(bset) is True   # takes the probe slot
    assert brk.allow(bset) is False  # no thundering herd onto a recovering GPU
    assert brk.allow(bset) is False


def test_failed_probe_reopens_for_another_cooldown(brk, bset):
    _fail(brk, bset, n=3)
    time.sleep(0.25)
    assert brk.allow(bset) is True
    brk.record_failure(TimeoutError("still saturated"), bset)
    assert brk.snapshot(bset)["state"] == OPEN
    assert brk.allow(bset) is False  # cooldown restarted, not retried immediately
    time.sleep(0.25)
    assert brk.allow(bset) is True   # ...and it probes again after it


def test_breaker_never_latches_open(brk, bset):
    """Recovery must not depend on anything but time passing."""
    _fail(brk, bset, n=3)
    for _ in range(3):  # repeated failed probes
        time.sleep(0.25)
        assert brk.allow(bset) is True
        brk.record_failure(TimeoutError("down"), bset)
    time.sleep(0.25)
    assert brk.allow(bset) is True
    brk.record_success()
    assert brk.snapshot(bset)["state"] == CLOSED


def test_abandoned_probe_does_not_wedge_half_open(brk, bset):
    """A probe whose caller dies without reporting must not hold the slot."""
    _fail(brk, bset, n=3)
    time.sleep(0.25)
    assert brk.allow(bset) is True  # probe taken, never reported
    assert brk.allow(bset) is False
    time.sleep(0.25)  # probe is now older than the cooldown → treat as abandoned
    assert brk.allow(bset) is True
    brk.record_success()
    assert brk.snapshot(bset)["state"] == CLOSED


def test_timeout_and_connection_refused_treated_alike(brk, bset):
    """Both mean 'no dense vector'; only last_error distinguishes them."""
    _fail(brk, bset, n=2, exc=TimeoutError("deadline"))
    assert brk.allow(bset)
    brk.record_failure(ConnectionRefusedError("no listener"), bset)
    snap = brk.snapshot(bset)
    assert snap["state"] == OPEN
    assert "ConnectionRefusedError" in snap["last_error"]


def test_threshold_zero_disables_breaker(brk, settings):
    off = settings.model_copy(update={"embed_breaker_threshold": 0})
    for _ in range(10):
        assert brk.allow(off) is True
        brk.record_failure(TimeoutError("x"), off)
    assert brk.snapshot(off)["state"] == CLOSED  # pre-breaker behavior preserved


def test_snapshot_reports_retry_countdown(brk, bset):
    _fail(brk, bset, n=3)
    retry_in = brk.snapshot(bset)["retry_in_s"]
    assert 0 < retry_in <= bset.embed_breaker_cooldown


def test_defaults_are_sane():
    s = Settings(_env_file=None)
    assert s.embed_breaker_threshold == 3
    assert s.embed_breaker_cooldown == 30.0


# --- thread safety ---------------------------------------------------------


def test_concurrent_failures_trip_breaker_exactly_once(brk, bset):
    """20 threads failing at once must not produce 20 trips or a torn state."""
    barrier = threading.Barrier(20)

    def worker():
        barrier.wait()
        if brk.allow(bset):
            brk.record_failure(TimeoutError("saturated"), bset)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    snap = brk.snapshot(bset)
    assert snap["state"] == OPEN
    assert snap["trips"] == 1  # closed→open happened once, not once per thread


def test_concurrent_probes_elect_a_single_winner(brk, bset):
    _fail(brk, bset, n=3)
    time.sleep(0.25)
    barrier = threading.Barrier(16)
    allowed = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        ok = brk.allow(bset)
        with lock:
            allowed.append(ok)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(allowed) == 1  # exactly one probe reaches the GPU


def test_breaker_does_not_serialize_concurrent_searches(brk, bset):
    """The lock must never be held across the embed call. Two searches embed at
    the same time; if allow() serialized them the barrier would never fill."""
    barrier = threading.Barrier(2, timeout=5)
    errors = []

    def worker():
        try:
            assert brk.allow(bset)
            barrier.wait()  # only passes if both are inside the "embed" at once
            brk.record_success()
        except Exception as exc:  # BrokenBarrierError on timeout == serialized
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


# --- wiring through search() ----------------------------------------------


@pytest.fixture()
def seeded(qclient, fake_embedder, monkeypatch):
    """Own collection (news__pytest-model-breaker), addressed directly.

    Two deliberate safeguards: a model name of its own, so these docs can't be
    ranked against the ones test_index_and_search.py seeds into the shared
    news__pytest-model; and alias_name monkeypatched for every search, so
    nothing here can read or write through a live *_current alias.
    """
    from fastembed import SparseTextEmbedding
    from qdrant_client import models as qm

    from windex.ccnews.embed_index import point_id

    # 'pytest-model' substring keeps the qclient fixture's session cleanup;
    # news_current already exists in every real deployment, so ensure_collection
    # never creates (and cleanup never removes) an alias here.
    name = qidx.ensure_collection(qclient, "news", "pytest-model-breaker", dim=8)
    bm25 = SparseTextEmbedding("Qdrant/bm25")
    docs = [("news:brk1", "transit bus lanes approved by city council"),
            ("news:brk2", "qdrant hybrid search vector database")]
    dense = fake_embedder.embed_batch([t for _, t in docs])
    sparse = list(bm25.embed([t for _, t in docs]))
    qclient.upsert(
        collection_name=name,
        points=[
            qm.PointStruct(
                id=point_id(did),
                vector={qidx.DENSE: dense[i],
                        qidx.SPARSE: qm.SparseVector(indices=sparse[i].indices.tolist(),
                                                     values=sparse[i].values.tolist())},
                payload={"doc_id": did, "source": "news", "url": f"https://x/{did}",
                         "title": text[:20], "snippet": text, "lang": "en"},
            )
            for i, (did, text) in enumerate(docs)
        ],
        wait=True,
    )
    monkeypatch.setattr(searchmod.qidx, "alias_name", lambda source: name)
    return name


@pytest.fixture()
def dead_embed(bset):
    """Settings whose embed endpoint refuses connections, fast cooldown."""
    return bset.model_copy(update={"embed_endpoint": "http://127.0.0.1:1"})


def test_search_trips_breaker_then_short_circuits_fast(seeded, dead_embed, monkeypatch):
    from windex.index.embed_breaker import breaker

    calls = []

    def boom(settings, timeout=None):
        calls.append(timeout)
        raise TimeoutError("embedding server saturated")

    monkeypatch.setattr("windex.embed.build_embedder", boom)
    for _ in range(3):
        resp = searchmod.search(dead_embed, "transit bus lanes", source="news", mode="hybrid")
        assert resp["degraded"] is True
    assert len(calls) == 3
    assert breaker.snapshot(dead_embed)["state"] == OPEN

    # 4th search: breaker open — no embedder is built at all, results still served
    t0 = time.monotonic()
    resp = searchmod.search(dead_embed, "transit bus lanes", source="news", mode="hybrid")
    elapsed = (time.monotonic() - t0) * 1000
    assert len(calls) == 3  # the doomed round trip never happened
    assert resp["results"] and resp["results"][0]["doc_id"] == "news:brk1"
    assert resp["degraded"] is True  # short-circuited IS degraded — stays truthful
    assert resp["timings"]["embed_query_ms"] == 0  # honest: we never called it
    assert elapsed < 1000  # lexical-fast, not an 8s timeout


def test_open_breaker_still_raises_for_explicit_dense(seeded, dead_embed, monkeypatch):
    monkeypatch.setattr(
        "windex.embed.build_embedder",
        lambda s, timeout=None: (_ for _ in ()).throw(TimeoutError("saturated")),
    )
    for _ in range(3):
        searchmod.search(dead_embed, "transit", source="news", mode="hybrid")
    from windex.index.embed_breaker import breaker

    assert breaker.snapshot(dead_embed)["state"] == OPEN
    # must fail loudly rather than silently serve lexical results for mode=dense
    with pytest.raises(EmbedBreakerOpen):
        searchmod.search(dead_embed, "transit", source="news", mode="dense")


def test_dense_raises_underlying_error_when_breaker_closed(seeded, dead_embed, monkeypatch):
    monkeypatch.setattr(
        "windex.embed.build_embedder",
        lambda s, timeout=None: (_ for _ in ()).throw(TimeoutError("saturated")),
    )
    with pytest.raises(TimeoutError):  # unchanged pre-breaker semantics
        searchmod.search(dead_embed, "transit", source="news", mode="dense")


def test_search_recovers_hybrid_after_cooldown(seeded, dead_embed, monkeypatch, fake_embedder):
    from windex.index.embed_breaker import breaker

    state = {"up": False}

    def build(settings, timeout=None):
        if not state["up"]:
            raise TimeoutError("saturated")
        return fake_embedder

    monkeypatch.setattr("windex.embed.build_embedder", build)
    for _ in range(3):
        searchmod.search(dead_embed, "qdrant hybrid", source="news", mode="hybrid")
    assert breaker.snapshot(dead_embed)["state"] == OPEN

    state["up"] = True  # backfill finished, GPU free
    resp = searchmod.search(dead_embed, "qdrant hybrid", source="news", mode="hybrid")
    assert resp["degraded"] is True  # still cooling down, no premature probe
    time.sleep(0.25)
    resp = searchmod.search(dead_embed, "qdrant hybrid", source="news", mode="hybrid")
    assert resp["degraded"] is False  # probe succeeded: hybrid back, no restart
    assert breaker.snapshot(dead_embed)["state"] == CLOSED
    assert searchmod.search(dead_embed, "qdrant hybrid", source="news",
                            mode="dense")["results"]  # dense works again too


def test_lexical_search_never_touches_breaker(seeded, dead_embed, monkeypatch):
    from windex.index.embed_breaker import breaker

    def boom(settings, timeout=None):
        raise AssertionError("lexical must not embed")

    monkeypatch.setattr("windex.embed.build_embedder", boom)
    resp = searchmod.search(dead_embed, "transit bus lanes", source="news", mode="lexical")
    assert resp["results"] and resp["degraded"] is False
    assert breaker.snapshot(dead_embed)["state"] == CLOSED


def test_stats_exposes_breaker_state(settings, pg, monkeypatch):
    """Operators need to see *why* searches degrade, live."""
    import windex.api.service as service_mod
    from windex.index.embed_breaker import breaker

    service_mod._pg_stats_cache.clear()
    service_mod._metrics_cache.clear()
    for _ in range(settings.embed_breaker_threshold):
        assert breaker.allow(settings)
        breaker.record_failure(TimeoutError("saturated"), settings)
    act = service_mod.get_stats(settings)["activity"]
    assert act["embed_breaker"]["state"] == OPEN
    assert act["embed_breaker"]["trips"] == 1
    assert "TimeoutError" in act["embed_breaker"]["last_error"]
