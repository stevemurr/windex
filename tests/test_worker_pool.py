"""Supervisor, slots, and the memory ceiling.

The end-to-end tests here really fork. That is the point: the pool's shape is
"one supervisor, K subprocesses" for three specific reasons (memory reclaim, the
GIL, killability), and a test that ran the slot loop in a thread would verify a
design nobody is shipping. The runners are module-level functions because a
forked child inherits them by memory image — which is also exactly how the real
`resolve` reaches a slot.
"""

import os
import signal
import time
import uuid

import pytest

from windex.worker import claim as C
from windex.worker import control as ctrlfile
from windex.worker import dag, memory, preconditions
from windex.worker.config import DEFAULT_LANE_CAPS, PoolConfig, config_from_env
from windex.worker.protocol import PermanentTaskError, SliceResult
from windex.worker.slot import slot_main, worker_id
from windex.worker.supervisor import Pool

GIB = 1024 ** 3


# --- runners the forked slots will execute ----------------------------------

def drain_units(ctx) -> SliceResult:
    """Mark every pending unit done, one batch of two at a time."""
    done = 0
    while True:
        with ctx.conn.cursor() as cur:
            cur.execute("SELECT unit_key FROM task_units WHERE task_id = %s "
                        "AND state = 'pending' ORDER BY seq LIMIT 2", (ctx.task_id,))
            batch = [r[0] for r in cur.fetchall()]
        if not batch:
            return SliceResult(units_done=done, exhausted=True)
        with ctx.conn.cursor() as cur:
            cur.execute("UPDATE task_units SET state = 'done', "
                        "seq = nextval('task_unit_seq') WHERE task_id = %s "
                        "AND unit_key = ANY(%s)", (ctx.task_id, batch))
        ctx.conn.commit()
        done += len(batch)
        ctx.heartbeat(done, 0, {})
        if ctx.should_yield():
            return SliceResult(units_done=done)


def sleep_forever(ctx) -> SliceResult:
    while not ctx.should_yield():
        time.sleep(0.05)
    return SliceResult(units_done=0)


def resolve(module):
    if module == "test.drain":
        return drain_units
    if module == "test.sleep":
        return sleep_forever
    raise PermanentTaskError(f"unknown module {module!r}")


# --- fixtures ---------------------------------------------------------------

@pytest.fixture()
def cfg(tmp_path):
    return PoolConfig(
        name="testpool", slots=2, tick_seconds=0.2, claim_idle_seconds=0.05,
        heartbeat_seconds=0.2, slice_seconds=5.0, max_tasks_per_slot=50,
        state_dir=tmp_path / "worker", stop_grace_seconds=5.0,
        # Every lane is fair game and uncapped for these tests: the caps have
        # their own tests, and a cap here would silently serialize the pool and
        # make a fairness failure look like a timeout.
        lane_caps={lane: 4 for lane in DEFAULT_LANE_CAPS},
    )


def submit(conn, *, recipe, source, nodes, priority=50):
    run_id = dag.submit_run(conn, recipe=recipe, source=source, spec={}, tasks=nodes,
                            priority=priority, dedupe_key=f"{recipe}-{uuid.uuid4()}")
    assert run_id is not None
    return run_id


def node(name, *, module="test.drain", lane="io", **kw):
    return {"node": name, "module": module, "lane": lane, "kind": "fetch", **kw}


def seed_units(conn, run_id, node_name, n):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM run_tasks WHERE run_id = %s AND node = %s",
                    (run_id, node_name))
        task_id = cur.fetchone()[0]
        for i in range(n):
            cur.execute("INSERT INTO task_units (run_id, task_id, unit_key) "
                        "VALUES (%s, %s, %s)", (run_id, task_id, f"u{i}"))
    conn.commit()
    return task_id


def run_states(conn, run_ids):
    with conn.cursor() as cur:
        cur.execute("SELECT id, state FROM runs WHERE id = ANY(%s)", (list(run_ids),))
        return dict(cur.fetchall())


def wait_until(pred, timeout=45.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


# --- end to end -------------------------------------------------------------

def test_the_pool_runs_a_dag_to_completion(pg, pg_dsn, cfg):
    """Two chained nodes per run, three runs, two slots. Everything finishes.

    This covers the whole spine at once: fan-out, claim, slice, terminal
    transition, inline DAG advance, run rollup.
    """
    run_ids = []
    for i in range(3):
        run_id = submit(pg, recipe=f"r{i}", source=f"s{i}",
                        nodes=[node("fetch"), node("load", depends_on=["fetch"])])
        seed_units(pg, run_id, "fetch", 5)
        seed_units(pg, run_id, "load", 3)
        run_ids.append(run_id)

    pool = Pool(pg_dsn, resolve, cfg, precond=set)
    done = []

    def finished():
        states = run_states(pg, run_ids)
        pg.commit()
        if all(s == "succeeded" for s in states.values()):
            done.append(True)
        return bool(done)

    pool.run(until=finished)
    assert done, f"runs did not finish: {run_states(pg, run_ids)}"
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM task_units WHERE state <> 'done'")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM run_tasks WHERE state <> 'succeeded'")
        assert cur.fetchone()[0] == 0


def test_a_slot_advances_the_dag_inline(pg, pg_dsn, cfg):
    """One slot, one run, two chained nodes, max_tasks_per_slot=2: the second
    node must become claimable without any supervisor tick at all."""
    run_id = submit(pg, recipe="chain", source="s",
                    nodes=[node("fetch"), node("load", depends_on=["fetch"])])
    seed_units(pg, run_id, "fetch", 2)
    seed_units(pg, run_id, "load", 2)
    from dataclasses import replace
    code = slot_main(pg_dsn, resolve, replace(cfg, max_tasks_per_slot=2), 0)
    assert code == 0
    pg.commit()
    with pg.cursor() as cur:
        cur.execute("SELECT node, state FROM run_tasks WHERE run_id = %s ORDER BY node",
                    (run_id,))
        assert dict(cur.fetchall()) == {"fetch": "succeeded", "load": "succeeded"}


def test_a_killed_slot_has_its_lease_released_at_once(pg, pg_dsn, cfg):
    """The reaper does not wait for lease expiry for a death it watched happen:
    300 s of a dead lane is exactly the stall the pool exists to remove."""
    from dataclasses import replace

    run_id = submit(pg, recipe="sleepy", source="s",
                    nodes=[node("hang", module="test.sleep")])
    # One slot: with two, the survivor would re-claim the released task within
    # milliseconds and the reclaim would be unobservable rather than absent.
    pool = Pool(pg_dsn, resolve, replace(cfg, slots=1), precond=set)
    try:
        pool.tick()                            # forks the slot
        assert wait_until(lambda: _leased(pg, run_id) is not None, timeout=20), \
            "no slot claimed the task"
        worker = _leased(pg, run_id)
        pid = int(worker.split("/")[-1])
        os.kill(pid, signal.SIGKILL)
        pool._stopping = True                  # no replacement, for the same reason
        # A killed child is a zombie until its parent waits on it, so "is it
        # gone" is the supervisor's question to answer, not ours: tick until it
        # has noticed. Ticking also re-forks, and the replacement must not be
        # able to re-claim before the reclaim lands.
        assert wait_until(lambda: pool.tick() and _state(pg, run_id)[1] is None,
                          timeout=20)
        state, lease_worker, attempts = _state(pg, run_id)
        assert (state, lease_worker) == ("ready", None)
        assert attempts == 1, "a reclaim must consume an attempt"
    finally:
        pool._stopping = True
        pool.shutdown()


def _leased(conn, run_id):
    with conn.cursor() as cur:
        cur.execute("SELECT lease_worker FROM run_tasks WHERE run_id = %s AND "
                    "state = 'running'", (run_id,))
        row = cur.fetchone()
    conn.commit()
    return row[0] if row else None


def _state(conn, run_id):
    with conn.cursor() as cur:
        cur.execute("SELECT state, lease_worker, attempts FROM run_tasks "
                    "WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
    conn.commit()
    return row


def test_shutdown_yields_running_work_rather_than_losing_it(pg, pg_dsn, cfg):
    """SIGTERM to the pool must leave the task claimable and its units intact —
    a deploy is a yield, not a rollback."""
    run_id = submit(pg, recipe="sleepy", source="s",
                    nodes=[node("hang", module="test.sleep")])
    pool = Pool(pg_dsn, resolve, cfg, precond=set)
    pool.tick()
    assert wait_until(lambda: _leased(pg, run_id) is not None, timeout=20)
    pool._stopping = True
    pool.shutdown()
    pg.commit()
    with pg.cursor() as cur:
        cur.execute("SELECT state, lease_worker FROM run_tasks WHERE run_id = %s", (run_id,))
        assert cur.fetchone() == ("ready", None)


def test_a_slot_over_the_high_water_mark_is_recycled(pg, pg_dsn, cfg):
    """The memory ceiling's second layer, end to end.

    An absurdly low high-water mark makes every slot instantly over budget, so
    the supervisor must retire it and fork a replacement. Recycling the *process*
    is the only thing that actually returns memory to the OS, so a supervisor
    that cannot complete this cycle has no memory ceiling at all — just a log
    line about one.
    """
    from dataclasses import replace

    if memory.rss_bytes(os.getpid()) is None:
        pytest.skip("no procfs: the RSS layers disable themselves here")
    pool = Pool(pg_dsn, resolve, replace(cfg, slots=1, rss_high_water_bytes=1,
                                         rss_hard_bytes=1 << 60), precond=set)
    try:
        pool.tick()
        first = pool.slots[0].pid
        assert wait_until(lambda: pool.tick() and pool.slots[0].pid != first, timeout=25)
    finally:
        pool._stopping = True
        pool.shutdown()


def test_a_retire_signal_lost_in_the_fork_window_is_re_sent(pg, pg_dsn, cfg):
    """A slot forked microseconds ago still carries the SUPERVISOR's inherited
    SIGTERM disposition, so the first retire signal can be swallowed rather than
    honoured. Sending once would leave the pool running the very process it
    decided to replace — so retirement is re-sent every tick until the slot
    actually exits."""
    from dataclasses import replace

    pool = Pool(pg_dsn, resolve, replace(cfg, slots=1), precond=set)
    try:
        pool.tick()
        index = 0
        pool._retire(index, "test")
        first = pool.slots[index].pid
        sent = []
        original = pool._signal
        pool._signal = lambda pid, sig: (sent.append((pid, sig)), original(pid, sig))[1]
        pool._press_retirements()
        pool._press_retirements()
        assert sent.count((first, signal.SIGTERM)) >= 2
        pool._signal = original
        assert wait_until(lambda: pool.tick() and pool.slots[0].pid != first, timeout=25)
    finally:
        pool._stopping = True
        pool.shutdown()


def test_the_pool_restores_signal_handlers_it_installed(pg, pg_dsn, cfg):
    """Left behind, this handler is inherited by every later fork in the process
    — and a slot that inherits it swallows its own retire signal."""
    before = signal.getsignal(signal.SIGTERM)
    Pool(pg_dsn, resolve, cfg, precond=set).run(until=lambda: True)
    assert signal.getsignal(signal.SIGTERM) is before


def test_the_supervisor_publishes_the_control_file(pg, pg_dsn, cfg):
    pool = Pool(pg_dsn, resolve, cfg, precond=lambda: {"gateway"})
    try:
        pool.tick()
        published = ctrlfile.read(cfg.control_path, stale_seconds=60)
        assert published.satisfied == frozenset({"gateway"})
        assert published.stale is False
        assert published.generation >= 1
    finally:
        pool._stopping = True
        pool.shutdown()


def test_a_task_whose_module_is_unknown_fails_permanently(pg, pg_dsn, cfg):
    from dataclasses import replace

    run_id = submit(pg, recipe="bogus", source="s", nodes=[node("x", module="nope.nope")])
    slot_main(pg_dsn, resolve, replace(cfg, max_tasks_per_slot=1), 0)
    pg.commit()
    with pg.cursor() as cur:
        cur.execute("SELECT state, attempts, error FROM run_tasks WHERE run_id = %s",
                    (run_id,))
        state, attempts, error = cur.fetchone()
        cur.execute("SELECT state FROM runs WHERE id = %s", (run_id,))
        run_state = cur.fetchone()[0]
    pg.commit()
    # One attempt, not three: an unknown module is unknown on the retry too.
    assert (state, attempts, run_state) == ("failed", 1, "failed")
    assert "nope.nope" in error


# --- the memory ceiling -----------------------------------------------------

def test_rss_is_readable_for_this_process():
    rss = memory.rss_bytes(os.getpid())
    if rss is None:
        pytest.skip("no procfs on this platform (the RSS layers disable themselves)")
    assert rss > 1024 * 1024


def test_an_unreadable_pid_reads_as_unknown_not_zero():
    """Zero would make every ceiling look comfortably satisfied."""
    assert memory.rss_bytes(2 ** 22 - 1) is None
    assert memory.read_all([2 ** 22 - 1]) == {}


def test_classify_splits_retire_from_kill():
    readings = {1: 1 * GIB, 2: 7 * GIB, 3: 11 * GIB}
    retire, kill = memory.classify(readings, high_water=6 * GIB, hard=10 * GIB)
    assert retire == [2] and kill == [3]


def test_backpressure_blocks_only_the_expensive_lanes():
    assert memory.backpressure_lanes(27 * GIB, 40 * GIB, 0.70, ("cpu_heavy",)) == []
    assert memory.backpressure_lanes(29 * GIB, 40 * GIB, 0.70, ("cpu_heavy",)) \
        == ["cpu_heavy"]
    # Disabled when there is no limit to be a fraction of.
    assert memory.backpressure_lanes(99 * GIB, 0, 0.70, ("cpu_heavy",)) == []


def test_backpressure_reaches_the_claim_through_the_control_file(pg, tmp_path, cfg):
    """The full path: supervisor decides, file carries it, slot narrows its lanes."""
    ctrlfile.write(cfg.control_path, satisfied={"storage:staging"},
                   blocked_lanes=("cpu_heavy",), generation=3)
    published = ctrlfile.read(cfg.control_path, stale_seconds=60)
    assert published.lanes(("cpu_heavy", "io", "net")) == ["io", "net"]

    submit(pg, recipe="heavy", source="s", nodes=[node("x", lane="cpu_heavy")])
    assert C.claim_task(pg, worker="w", lanes=published.lanes(cfg.lanes),
                        caps=cfg.lane_caps, satisfied=published.satisfied_for_claim(),
                        default_cap=4) is None
    assert C.claim_task(pg, worker="w", lanes=list(cfg.lanes), caps=cfg.lane_caps,
                        satisfied=published.satisfied_for_claim(),
                        default_cap=4) is not None


# --- the control file -------------------------------------------------------

def test_a_missing_or_stale_control_file_is_fail_closed(tmp_path):
    missing = ctrlfile.read(tmp_path / "nope.json", stale_seconds=60)
    assert missing.stale and missing.satisfied_for_claim() == frozenset()

    path = tmp_path / "pool.json"
    ctrlfile.write(path, satisfied={"gateway", "storage:staging"}, blocked_lanes=(),
                   generation=1)
    fresh = ctrlfile.read(path, stale_seconds=60)
    assert fresh.satisfied_for_claim() == frozenset({"gateway", "storage:staging"})
    # Old enough to distrust: a precondition nobody has checked is not satisfied.
    stale = ctrlfile.read(path, stale_seconds=-1)
    assert stale.stale and stale.satisfied_for_claim() == frozenset()


# --- preconditions ----------------------------------------------------------

class StubSettings:
    def __init__(self, root, min_free=0, tokens=()):
        self.staging_dir = root / "staging"
        self.downloads_dir = root / "downloads"
        self.storage_min_free_bytes = min_free
        self.github_token_list = list(tokens)


def test_storage_precondition_tracks_presence_and_free_space(tmp_path):
    s = StubSettings(tmp_path)
    # Neither directory exists yet: a vanished staging tree is not satisfied.
    assert preconditions.evaluate(s, names=("storage:staging",)) == set()
    s.staging_dir.mkdir(parents=True)
    assert "storage:staging" in preconditions.evaluate(s, names=("storage:staging",))
    # An unreachable reserve fails the check even though the path is fine — the
    # case the old staging_mount check structurally could not catch.
    s.storage_min_free_bytes = 1 << 60
    assert preconditions.evaluate(s, names=("storage:staging",)) == set()


def test_the_legacy_staging_mount_alias_is_published(tmp_path):
    s = StubSettings(tmp_path)
    s.staging_dir.mkdir(parents=True)
    got = preconditions.evaluate(s, names=("storage:staging",))
    assert "staging_mount" in got and "storage:staging" in got


def test_gh_token_precondition(tmp_path):
    assert preconditions.evaluate(StubSettings(tmp_path), names=("gh_token",)) == set()
    assert preconditions.evaluate(StubSettings(tmp_path, tokens=("ghp_x",)),
                                  names=("gh_token",)) == {"gh_token"}


def test_precondition_cache_respects_its_ttl():
    calls = []

    def evaluator():
        calls.append(1)
        return {"gateway"}

    cache = preconditions.Cache(None, ttl=3600, evaluator=evaluator)
    assert cache.get() == {"gateway"}
    assert cache.get() == {"gateway"}
    assert len(calls) == 1, "the gateway was pinged twice inside one TTL"
    cache.get(force=True)
    assert len(calls) == 2


def test_a_raising_precondition_is_unsatisfied_not_fatal():
    def boom():
        raise RuntimeError("gateway exploded")

    assert preconditions.evaluate(None, names=("custom",), extra={"custom": boom}) == set()


# --- config -----------------------------------------------------------------

def test_env_overrides_are_read(monkeypatch):
    monkeypatch.setenv("WINDEX_WORKER_SLOTS", "7")
    monkeypatch.setenv("WINDEX_WORKER_LANES", "io, net")
    monkeypatch.setenv("WINDEX_WORKER_SLICE_SECONDS", "45")
    cfg = config_from_env()
    assert (cfg.slots, cfg.lanes, cfg.slice_seconds) == (7, ("io", "net"), 45.0)


def test_a_bad_env_value_falls_back_instead_of_crashing_the_pool(monkeypatch):
    monkeypatch.setenv("WINDEX_WORKER_SLOTS", "four")
    assert config_from_env().slots == PoolConfig().slots


def test_worker_ids_identify_a_specific_process():
    assert worker_id("pool", 2, 4242) == "pool/2/4242"
