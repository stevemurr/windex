"""Slice-execution tests: yielding, resuming, cancelling, failing.

The central assertion here is the one the whole design rests on: **a task run in
many slices produces exactly what the same task run to completion produces.** If
that is not true, slicing is not free and the fairness story is a liability
rather than a feature.

The runner used throughout drains a real unit table (`task_units`) exactly the
way `crawl/run.py`'s `claim_batch` does — no second lock, because a leased task
has one claimant — so the resume path being tested is the real one.
"""

import threading
import uuid

import pytest

from windex.worker import claim as C
from windex.worker import dag
from windex.worker.config import PoolConfig
from windex.worker.execute import run_slice
from windex.worker.protocol import PermanentTaskError, SliceResult

pytestmark = pytest.mark.usefixtures("pg")


# --- helpers ----------------------------------------------------------------

def cfg(**kw) -> PoolConfig:
    # A fast heartbeat: the flags a slice reacts to (cancel, pause, preemption)
    # are refreshed by the background thread, so the tests would otherwise be
    # waiting ten seconds to observe a pause.
    base = {"slice_seconds": 60.0, "heartbeat_seconds": 0.05,
            "state_dir": None, "max_tasks_per_slot": 5}
    base.update(kw)
    if base["state_dir"] is None:
        base.pop("state_dir")
    return PoolConfig(**base)


def submit(conn, *, source="s", nodes, recipe="r", priority=50):
    run_id = dag.submit_run(conn, recipe=recipe, source=source, spec={},
                            tasks=nodes, priority=priority,
                            dedupe_key=f"{recipe}-{uuid.uuid4()}")
    assert run_id is not None
    return run_id


def node(name="a", **kw):
    return {"node": name, "module": "test.units", "lane": "io", "kind": "fetch", **kw}


def claim(conn, worker="w1"):
    return C.claim_task(conn, worker=worker, lanes=["io", "net", "gpu"], caps={},
                        satisfied=(), default_cap=9)


def seed_units(conn, run_id, task_id, n):
    with conn.cursor() as cur:
        for i in range(n):
            cur.execute("INSERT INTO task_units (run_id, task_id, unit_key) "
                        "VALUES (%s, %s, %s)", (run_id, task_id, f"u{i}"))
    conn.commit()


def unit_states(conn, task_id):
    with conn.cursor() as cur:
        cur.execute("SELECT state, count(*) FROM task_units WHERE task_id = %s "
                    "GROUP BY state", (task_id,))
        return dict(cur.fetchall())


def task_row(conn, task_id):
    with conn.cursor() as cur:
        cur.execute("SELECT state, attempts, units_done, units_failed, cursor, stats, "
                    "error, lease_worker FROM run_tasks WHERE id = %s", (task_id,))
        cols = ("state", "attempts", "units_done", "units_failed", "cursor", "stats",
                "error", "lease_worker")
        return dict(zip(cols, cur.fetchone(), strict=True))


class UnitRunner:
    """Drains `task_units` in batches, committing each batch, yielding on request.

    This is the shape every real module will have: claim a batch, do the work,
    commit, report, check should_yield. `batch` is small so the tests can force a
    yield at a known boundary.
    """

    def __init__(self, batch=2, fail_keys=(), boom=None, per_unit_stat="done"):
        self.batch = batch
        self.fail_keys = set(fail_keys)
        self.boom = boom
        self.per_unit_stat = per_unit_stat
        self.slices = 0

    def __call__(self, ctx) -> SliceResult:
        self.slices += 1
        done = failed = 0
        seen = list(ctx.cursor.get("seen", []))
        while True:
            with ctx.conn.cursor() as cur:
                cur.execute("SELECT unit_key FROM task_units WHERE task_id = %s "
                            "AND state = 'pending' ORDER BY seq LIMIT %s",
                            (ctx.task_id, self.batch))
                batch = [r[0] for r in cur.fetchall()]
            if not batch:
                return SliceResult(units_done=done, units_failed=failed, exhausted=True,
                                   cursor={"seen": seen}, stats={"slices": self.slices})
            if self.boom is not None and self.boom <= done:
                raise self.boom_error()
            with ctx.conn.cursor() as cur:
                for key in batch:
                    bad = key in self.fail_keys
                    cur.execute("UPDATE task_units SET state = %s, seq = nextval('task_unit_seq'), "
                                "finished_at = now() WHERE task_id = %s AND unit_key = %s",
                                ("failed" if bad else "done", ctx.task_id, key))
                    seen.append(key)
                    if bad:
                        failed += 1
                    else:
                        done += 1
            ctx.conn.commit()
            ctx.heartbeat(done, failed, {"last": batch[-1]})
            if ctx.should_yield():
                return SliceResult(units_done=done, units_failed=failed,
                                   cursor={"seen": seen}, stats={"slices": self.slices})

    def boom_error(self):
        return RuntimeError(self.boom) if isinstance(self.boom, str) else RuntimeError("boom")


def run_one(pg, pg_dsn, runner, config=None, *, worker="w1", drain=None, task=None):
    """Claim and run exactly one slice, on two connections like the slot does."""
    import psycopg

    task = task or claim(pg, worker)
    assert task is not None
    work = psycopg.connect(pg_dsn)
    try:
        return task, run_slice(pg, work, task, runner, config or cfg(), drain=drain)
    finally:
        work.close()


# --- the equivalence property ----------------------------------------------

def test_run_to_completion_drains_every_unit(pg, pg_dsn):
    run_id = submit(pg, nodes=[node()])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 6)
    C.release(pg, task, C.Release(outcome="yielded"))     # give it back, then slice it
    runner = UnitRunner(batch=6)
    task, outcome = run_one(pg, pg_dsn, runner)
    assert outcome.outcome == "succeeded"
    assert unit_states(pg, task.id) == {"done": 6}
    assert task_row(pg, task.id)["units_done"] == 6


def test_yield_then_resume_matches_run_to_completion(pg, pg_dsn):
    """Six units, two per batch, a slice that ends after every batch.

    The result — units drained, counters, terminal state — must be identical to
    the single-slice run above. This is the property that makes yielding free.
    """
    run_id = submit(pg, nodes=[node()])
    first = claim(pg)
    seed_units(pg, run_id, first.id, 6)
    C.release(pg, first, C.Release(outcome="yielded"))

    runner = UnitRunner(batch=2)
    slice_cfg = cfg(slice_units=2)          # yield after every batch
    outcomes = []
    for _ in range(6):
        task, outcome = run_one(pg, pg_dsn, runner, slice_cfg)
        outcomes.append(outcome.outcome)
        if outcome.outcome == "succeeded":
            break
        assert task_row(pg, task.id)["state"] == "ready", "a yield must re-queue"
        assert task_row(pg, task.id)["lease_worker"] is None

    assert outcomes == ["yielded", "yielded", "yielded", "succeeded"]
    assert unit_states(pg, first.id) == {"done": 6}
    row = task_row(pg, first.id)
    assert row["units_done"] == 6 and row["state"] == "succeeded"
    # Counters accumulate across slices rather than restarting each one.
    assert row["attempts"] == 0


def test_the_cursor_survives_a_yield(pg, pg_dsn):
    run_id = submit(pg, nodes=[node()])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 4)
    C.release(pg, task, C.Release(outcome="yielded"))
    runner = UnitRunner(batch=2)
    task, _ = run_one(pg, pg_dsn, runner, cfg(slice_units=2))
    assert task_row(pg, task.id)["cursor"] == {"seen": ["u0", "u1"]}
    # The next slice is handed the persisted cursor, not an empty one.
    resumed = claim(pg, "w2")
    assert resumed.cursor == {"seen": ["u0", "u1"]}


def test_exhausted_task_succeeds_and_releases_the_lane(pg, pg_dsn):
    run_id = submit(pg, nodes=[node("a"), node("b", depends_on=["a"])])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 2)
    C.release(pg, task, C.Release(outcome="yielded"))
    task, outcome = run_one(pg, pg_dsn, UnitRunner(batch=2))
    assert outcome.outcome == "succeeded"
    # ...and the dependent node becomes claimable. The slot does this advance
    # inline after a terminal slice (see test_worker_pool); run_slice itself
    # deliberately only owns the task, so the DAG step is explicit here.
    dag.advance(pg, run_id)
    assert claim(pg, "w2").node == "b"


def test_failed_units_are_counted_without_failing_the_task(pg, pg_dsn):
    """One bad page must not sink a run — the crawl driver's rule, generalized."""
    run_id = submit(pg, nodes=[node()])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 4)
    C.release(pg, task, C.Release(outcome="yielded"))
    task, outcome = run_one(pg, pg_dsn, UnitRunner(batch=4, fail_keys={"u1", "u2"}))
    assert outcome.outcome == "succeeded"
    row = task_row(pg, task.id)
    assert (row["units_done"], row["units_failed"]) == (2, 2)


# --- stopping conditions ----------------------------------------------------

def test_deadline_ends_the_slice(pg, pg_dsn):
    run_id = submit(pg, nodes=[node()])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 8)
    C.release(pg, task, C.Release(outcome="yielded"))
    task, outcome = run_one(pg, pg_dsn, UnitRunner(batch=1), cfg(slice_seconds=0.0))
    assert outcome.outcome == "yielded" and outcome.reason == "slice_deadline"
    assert unit_states(pg, task.id)["done"] == 1     # one batch, then out


def test_a_pause_yields_the_slot_instead_of_holding_it(pg, pg_dsn):
    """Paused work must not sit on a slot: that is the difference between
    'pause frees the GPU' and 'pause stops progress but keeps the lane'."""
    run_id = submit(pg, source="wiki", nodes=[node()])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 100)
    # Paused AFTER the claim, which is the interesting case: the claim predicate
    # already refuses paused work, so the only way to be running inside a paused
    # scope is to have been running when the pause arrived.
    C.set_pause(pg, "source:wiki", reason="test")
    # The pause is observed by the heartbeat thread, so the slice ends on its own.
    task, outcome = run_one(pg, pg_dsn, UnitRunner(batch=1), cfg(slice_seconds=30),
                            task=task)
    assert outcome.outcome == "yielded"
    assert outcome.reason == "paused:source:wiki"
    assert task_row(pg, task.id)["state"] == "ready"
    # And it is not claimable again until the pause lifts.
    assert claim(pg, "w2") is None


def test_a_yield_request_ends_the_slice(pg, pg_dsn):
    run_id = submit(pg, nodes=[node()])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 100)
    C.release(pg, task, C.Release(outcome="yielded"))

    def preempt_after_first_batch(ctx):
        with ctx.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM task_units WHERE task_id = %s", (ctx.task_id,))
        C.request_yield(pg, task_id=ctx.task_id, reason="test")
        deadline = 0
        while not ctx.should_yield() and deadline < 200:
            deadline += 1
            import time
            time.sleep(0.01)
        return SliceResult(units_done=1, cursor={"n": 1})

    task, outcome = run_one(pg, pg_dsn, preempt_after_first_batch, cfg())
    assert outcome.outcome == "yielded" and outcome.reason == "preempted"


def test_cancellation_stops_the_task_cleanly(pg, pg_dsn):
    run_id = submit(pg, nodes=[node()])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 100)
    # Cancelled while running — a cancelled run's queued tasks are simply never
    # claimed, so mid-flight is the only case that needs a protocol.
    dag.request_cancel(pg, run_id, by="test")
    task, outcome = run_one(pg, pg_dsn, UnitRunner(batch=1), cfg(slice_seconds=30),
                            task=task)
    assert outcome.outcome == "cancelled"
    assert task_row(pg, task.id)["state"] == "cancelled"
    dag.apply_cancellations(pg)
    with pg.cursor() as cur:
        cur.execute("SELECT state FROM runs WHERE id = %s", (run_id,))
        assert cur.fetchone()[0] == "cancelled"
    # Committed units stay committed: a cancel is a stop, not a rollback.
    assert unit_states(pg, task.id)["done"] >= 1


def test_slot_drain_ends_the_slice(pg, pg_dsn):
    """SIGTERM to a slot (deploy, memory high-water) yields rather than kills."""
    run_id = submit(pg, nodes=[node()])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 20)
    C.release(pg, task, C.Release(outcome="yielded"))
    drain = threading.Event()
    drain.set()
    task, outcome = run_one(pg, pg_dsn, UnitRunner(batch=1), cfg(), drain=drain)
    assert outcome.outcome == "yielded" and outcome.reason == "slot_draining"
    assert task_row(pg, task.id)["state"] == "ready"


# --- failure paths ----------------------------------------------------------

def test_a_raising_runner_retries_then_fails(pg, pg_dsn):
    run_id = submit(pg, nodes=[node(max_attempts=2)])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 4)
    C.release(pg, task, C.Release(outcome="yielded"))

    def explode(ctx):
        raise RuntimeError("the gateway said no")

    task, outcome = run_one(pg, pg_dsn, explode)
    assert outcome.outcome == "failed"
    row = task_row(pg, task.id)
    assert row["state"] == "ready" and row["attempts"] == 1     # retryable
    assert "the gateway said no" in row["error"] or row["error"] is None

    task, outcome = run_one(pg, pg_dsn, explode, worker="w2")
    row = task_row(pg, task.id)
    assert row["state"] == "failed" and row["attempts"] == 2
    assert "the gateway said no" in row["error"]
    dag.advance(pg, run_id)
    with pg.cursor() as cur:
        cur.execute("SELECT state, error FROM runs WHERE id = %s", (run_id,))
        state, error = cur.fetchone()
    assert state == "failed" and "gateway" in error


def test_a_permanent_error_skips_the_retry_budget(pg, pg_dsn):
    submit(pg, nodes=[node(max_attempts=5)])
    task = claim(pg)
    C.release(pg, task, C.Release(outcome="yielded"))

    def explode(ctx):
        raise PermanentTaskError("config names a store this recipe does not own")

    task, outcome = run_one(pg, pg_dsn, explode)
    row = task_row(pg, task.id)
    assert row["state"] == "failed" and row["attempts"] == 1
    assert "does not own" in row["error"]


def test_a_runner_returning_the_wrong_type_fails_permanently(pg, pg_dsn):
    submit(pg, nodes=[node()])
    task = claim(pg)
    C.release(pg, task, C.Release(outcome="yielded"))
    task, outcome = run_one(pg, pg_dsn, lambda ctx: None)
    row = task_row(pg, task.id)
    assert row["state"] == "failed" and "not SliceResult" in row["error"]


def test_a_runner_that_leaves_a_broken_transaction_does_not_poison_the_slot(pg, pg_dsn):
    """The work connection is rolled back after every slice, so the next task on
    the same slot is not executed inside a failed transaction."""
    import psycopg

    run_id = submit(pg, nodes=[node()])
    task = claim(pg)
    seed_units(pg, run_id, task.id, 2)
    C.release(pg, task, C.Release(outcome="yielded"))

    def bad_sql(ctx):
        with ctx.conn.cursor() as cur:
            cur.execute("SELECT * FROM a_table_that_does_not_exist")
        return SliceResult()

    work = psycopg.connect(pg_dsn)
    try:
        task = claim(pg)
        outcome = run_slice(pg, work, task, bad_sql, cfg())
        assert outcome.outcome == "failed"
        # Same connection, next slice: it must work.
        task2 = claim(pg, "w2")
        assert task2 is not None
        outcome2 = run_slice(pg, work, task2, UnitRunner(batch=2), cfg())
        assert outcome2.outcome == "succeeded"
    finally:
        work.close()


def test_a_reclaimed_task_writes_nothing_when_the_slice_ends(pg, pg_dsn):
    """The reclaim race: the lease expired and someone else took the task while
    this slice was still running. The loser must write nothing at all."""
    import psycopg

    run_id = submit(pg, nodes=[node()])
    task = claim(pg, "pool/0/111")
    seed_units(pg, run_id, task.id, 4)

    def steal_then_finish(ctx):
        # Simulate the reaper + a second slot taking the task mid-slice.
        with psycopg.connect(pg_dsn) as other:
            with other.cursor() as cur:
                cur.execute("UPDATE run_tasks SET lease_expires_at = now() - "
                            "interval '1 minute' WHERE id = %s", (ctx.task_id,))
            other.commit()
            C.reclaim_expired(other)
            C.claim_task(other, worker="pool/1/222", lanes=["io"], caps={},
                         satisfied=(), default_cap=9)
        return SliceResult(units_done=99, exhausted=True)

    work = psycopg.connect(pg_dsn)
    try:
        outcome = run_slice(pg, work, task, steal_then_finish, cfg())
    finally:
        work.close()
    assert outcome.outcome == "lease_lost"
    row = task_row(pg, task.id)
    assert row["state"] == "running" and row["lease_worker"] == "pool/1/222"
    assert row["units_done"] == 0, "the loser overwrote the new holder's counters"


def test_units_total_is_recorded_when_discovery_learns_it(pg, pg_dsn):
    submit(pg, nodes=[node()])
    task = claim(pg)
    C.release(pg, task, C.Release(outcome="yielded"))
    task, _ = run_one(pg, pg_dsn,
                      lambda ctx: SliceResult(units_done=0, units_total=4321, cursor={"a": 1}))
    with pg.cursor() as cur:
        cur.execute("SELECT units_total FROM run_tasks WHERE id = %s", (task.id,))
        assert cur.fetchone()[0] == 4321
