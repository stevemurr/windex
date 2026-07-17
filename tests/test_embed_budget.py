"""The fleet-wide embed budget (src/windex/embed/budget.py).

The bug it exists for: embed_concurrency is per-process, so N jobs multiply it.
These tests must therefore prove the cap holds across *processes*, not just
threads — a thread-only test would pass on an implementation that doesn't fix
the actual bug.
"""

import multiprocessing as mp
import threading
import time

import pytest

from windex.config import Settings
from windex.embed import PROFILES, build_embedder
from windex.embed.budget import BudgetedEmbedder, embed_slot, slot_dir


@pytest.fixture()
def endpoint(tmp_path, monkeypatch):
    # Isolate slot files per test; the real root is ~/.windex/embed-slots.
    monkeypatch.setattr("windex.embed.budget.SLOT_ROOT", tmp_path / "slots")
    return "http://test-endpoint:4000"


def _peak_concurrency(endpoint, budget, workers, hold=0.05):
    live = 0
    peak = 0
    lock = threading.Lock()

    def run():
        nonlocal live, peak
        with embed_slot(endpoint, budget):
            with lock:
                live += 1
                peak = max(peak, live)
            time.sleep(hold)
            with lock:
                live -= 1

    threads = [threading.Thread(target=run) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return peak


def test_budget_caps_in_flight_workers(endpoint):
    assert _peak_concurrency(endpoint, budget=2, workers=10) <= 2


def test_budget_of_one_serializes(endpoint):
    assert _peak_concurrency(endpoint, budget=1, workers=6) == 1


def test_budget_zero_disables_the_cap(endpoint):
    # 0 must be a true no-op: an escape hatch that can't stall the pipeline.
    assert _peak_concurrency(endpoint, budget=0, workers=5) > 1
    assert not slot_dir(endpoint).exists()  # no files created when disabled


def test_all_slots_are_usable(endpoint):
    # Regression: an off-by-one in the slot loop would silently cap at budget-1
    # and halve throughput while looking like it works.
    assert _peak_concurrency(endpoint, budget=4, workers=8, hold=0.15) == 4


def _child(endpoint, root, started, peak, lock, budget):
    import windex.embed.budget as b

    b.SLOT_ROOT = root
    with b.embed_slot(endpoint, budget):
        with lock:
            started.value += 1
            peak.value = max(peak.value, started.value)
        time.sleep(0.4)
        with lock:
            started.value -= 1


def test_budget_holds_across_processes(endpoint, tmp_path):
    """The whole point: embed_concurrency is per-process, so a cap that only
    works within one process fixes nothing. Six real processes, budget 2."""
    ctx = mp.get_context("fork")  # inherit the patched SLOT_ROOT
    started, peak = ctx.Value("i", 0), ctx.Value("i", 0)
    lock = ctx.Lock()
    root = slot_dir(endpoint).parent
    procs = [ctx.Process(target=_child, args=(endpoint, root, started, peak, lock, 2))
             for _ in range(6)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)
    assert peak.value <= 2, f"cap leaked across processes: peak={peak.value}"
    assert peak.value >= 1


def test_slot_released_when_holder_is_killed(endpoint):
    """Crash safety is the reason this is a file lock: these jobs get SIGKILLed
    routinely, and a counter table would leak the slot forever."""
    import os
    import signal

    ctx = mp.get_context("fork")
    root = slot_dir(endpoint).parent
    holding = ctx.Event()

    def hold():
        import windex.embed.budget as b

        b.SLOT_ROOT = root
        with b.embed_slot(endpoint, 1):
            holding.set()
            time.sleep(60)

    p = ctx.Process(target=hold)
    p.start()
    assert holding.wait(timeout=10), "child never acquired the slot"
    os.kill(p.pid, signal.SIGKILL)  # no cleanup handlers run
    p.join(timeout=10)
    # The kernel must have dropped the lock; budget=1 is free again.
    t0 = time.monotonic()
    with embed_slot(endpoint, 1, wait_timeout=15) as got:
        assert got, "slot never freed after the holder was SIGKILLed"
    assert time.monotonic() - t0 < 10


def test_fails_open_rather_than_stalling_forever(endpoint):
    """A throttle that can wedge the backfill is worse than no throttle."""
    with embed_slot(endpoint, 1):  # hold the only slot
        t0 = time.monotonic()
        with embed_slot(endpoint, 1, wait_timeout=0.3, poll=0.05) as got:
            assert got is False  # proceeded unbudgeted rather than blocking
        assert time.monotonic() - t0 < 5


def test_endpoints_get_independent_budgets(endpoint, monkeypatch):
    """A second embedding server must not share the first one's budget."""
    other = "http://second-model:4000"
    assert slot_dir(endpoint) != slot_dir(other)
    with embed_slot(endpoint, 1):
        with embed_slot(other, 1, wait_timeout=2) as got:
            assert got is True  # not blocked by the other endpoint's holder


class _Inner:
    model_id = "m"
    dim = 3

    def __init__(self):
        self.calls = []

    def embed_batch(self, texts):
        self.calls.append(list(texts))
        return [[0.1, 0.2, 0.3] for _ in texts]

    def ping(self):
        return True


def test_budgeted_embedder_preserves_the_interface(endpoint):
    inner = _Inner()
    e = BudgetedEmbedder(inner, endpoint, budget=2)
    assert e.model_id == "m" and e.dim == 3
    assert e.embed_batch(["a", "b"]) == [[0.1, 0.2, 0.3]] * 2
    assert inner.calls == [["a", "b"]]
    assert e.ping() is True  # health checks must not queue behind a backfill


def test_query_path_is_never_budgeted():
    """Live search must walk past the bulk queue, not wait in it."""
    s = Settings(_env_file=None, embed_backend="http-openai", embed_dim=4,
                 embed_global_budget=8)
    assert not isinstance(build_embedder(s, timeout=8.0), BudgetedEmbedder)
    assert isinstance(build_embedder(s, bulk=True), BudgetedEmbedder)
    # and the escape hatch disables it even for bulk
    s0 = s.model_copy(update={"embed_global_budget": 0})
    assert not isinstance(build_embedder(s0, bulk=True), BudgetedEmbedder)


def test_profiles_carry_a_global_budget():
    # The dashboard throttle is the user-facing control; it only bounds the
    # fleet if every profile sets the budget.
    for name, prof in PROFILES.items():
        assert "embed_global_budget" in prof, f"{name} profile has no fleet budget"
    assert PROFILES["polite"]["embed_global_budget"] < PROFILES["full"]["embed_global_budget"]


def test_bulk_and_query_sign_with_their_tier_keys():
    """The gateway enforces different limits per key: bulk is capped at 6
    concurrent (429s past it), interactive queues and never 429s. Crossing the
    keys silently breaks one side or the other."""
    s = Settings(_env_file=None, embed_backend="http-openai", embed_dim=4,
                 embed_api_key="legacy", embed_bulk_api_key="bulk-key",
                 embed_query_api_key="query-key")
    bulk = build_embedder(s, bulk=True)
    assert bulk.inner._client.headers["authorization"] == "Bearer bulk-key"
    query = build_embedder(s, timeout=8.0)
    assert query._client.headers["authorization"] == "Bearer query-key"


def test_tier_keys_fall_back_to_the_single_key():
    # A single-key server (WINDEX_EMBED_API_KEY only) must keep working on
    # both paths.
    s = Settings(_env_file=None, embed_backend="http-openai", embed_dim=4,
                 embed_api_key="only-key")
    assert (build_embedder(s, bulk=True).inner._client.headers["authorization"]
            == "Bearer only-key")
    assert (build_embedder(s, timeout=8.0)._client.headers["authorization"]
            == "Bearer only-key")
