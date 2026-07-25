"""Claim protocol tests: exclusivity, the scheduling predicate, leases, fairness.

These run against the real database (the `pg` fixture) because every property
under test is a property of a SQL statement under concurrency — pauses that must
be evaluated in the same statement as the claim, an anti-starvation guard that
two workers can race, a lease comparison that has to be a timestamp. A mock of
the database would test the mock.
"""

import threading
import uuid

import psycopg
import pytest

from windex.worker import claim as C
from windex.worker import dag
from windex.worker.protocol import LANES

ALL_PRECONDITIONS = ("storage:staging", "storage:downloads", "gateway", "gh_token")


def submit(conn, *, recipe="r", source="s", nodes, priority=50, dedupe=None):
    """Queue a run whose root tasks are immediately ready."""
    run_id = dag.submit_run(
        conn, recipe=recipe, source=source, spec={"nodes": [n["node"] for n in nodes]},
        tasks=nodes, priority=priority, dedupe_key=dedupe or f"{recipe}-{uuid.uuid4()}")
    assert run_id is not None
    return run_id


def node(name="a", *, lane="io", module="test.echo", **kw):
    return {"node": name, "module": module, "lane": lane, "kind": "transform", **kw}


def claim(conn, worker="w1", *, lanes=LANES, caps=None, satisfied=ALL_PRECONDITIONS,
          default_cap=99):
    return C.claim_task(conn, worker=worker, lanes=list(lanes), caps=caps or {},
                        satisfied=satisfied, default_cap=default_cap)


def states(conn, run_id):
    with conn.cursor() as cur:
        cur.execute("SELECT node, state FROM run_tasks WHERE run_id = %s", (run_id,))
        return dict(cur.fetchall())


def expire_lease(conn, task_id):
    """Backdate a lease instead of sleeping through one."""
    with conn.cursor() as cur:
        cur.execute("UPDATE run_tasks SET lease_expires_at = now() - interval '1 minute' "
                    "WHERE id = %s", (task_id,))
    conn.commit()


# --- the basic contract -----------------------------------------------------

def test_claim_leases_a_ready_task(pg):
    run_id = submit(pg, nodes=[node("a")])
    task = claim(pg)
    assert task is not None and task.node == "a" and task.run_id == run_id
    with pg.cursor() as cur:
        cur.execute("SELECT state, lease_worker, lease_expires_at > now(), attempts "
                    "FROM run_tasks WHERE id = %s", (task.id,))
        state, worker, leased, attempts = cur.fetchone()
    assert (state, worker, leased) == ("running", "w1", True)
    # Attempts counts FAILURES, not claims: a long task yields hundreds of times
    # and every yield is a re-claim.
    assert attempts == 0


def test_claim_promotes_the_run_to_running(pg):
    run_id = submit(pg, nodes=[node("a")])
    claim(pg)
    with pg.cursor() as cur:
        cur.execute("SELECT state, started_at IS NOT NULL FROM runs WHERE id = %s", (run_id,))
        assert cur.fetchone() == ("running", True)


def test_pending_tasks_are_not_claimable_until_dependencies_finish(pg):
    run_id = submit(pg, nodes=[node("a"), node("b", depends_on=["a"])])
    first = claim(pg)
    assert first.node == "a"
    assert claim(pg, "w2") is None          # b is still pending
    C.release(pg, first, C.Release(outcome="succeeded"))
    dag.advance(pg, run_id)
    assert claim(pg, "w2").node == "b"


def test_priority_beats_fair_share(pg):
    submit(pg, recipe="low", source="s1", nodes=[node("a")], priority=10)
    submit(pg, recipe="high", source="s2", nodes=[node("a")], priority=90)
    assert claim(pg).source == "s2"


def test_claim_skips_a_cancelled_run(pg):
    run_id = submit(pg, nodes=[node("a")])
    with pg.cursor() as cur:
        cur.execute("UPDATE runs SET cancel_requested = true WHERE id = %s", (run_id,))
    pg.commit()
    assert claim(pg) is None


# --- exclusivity ------------------------------------------------------------

def test_claim_is_exclusive_under_concurrency(pg, pg_dsn):
    """Eight threads, eight claimable tasks, no task claimed twice.

    Each task is in its own source AND its own lane so that neither the
    anti-starvation guard nor the lane cap is what produces the answer — the
    only thing under test here is that two workers cannot take the same row.
    """
    lanes = ["io", "net", "gpu", "cpu_heavy", "maint"]
    for i in range(8):
        submit(pg, recipe=f"r{i}", source=f"s{i}",
               nodes=[node("a", lane=lanes[i % len(lanes)])])

    claimed: list[int] = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker(n):
        with psycopg.connect(pg_dsn) as conn:
            barrier.wait()
            for _ in range(4):        # retry: a lane lock loss is a skip, not a failure
                task = claim(conn, f"w{n}", default_cap=99)
                if task is not None:
                    with lock:
                        claimed.append(task.id)
                    return

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(claimed) == len(set(claimed)), "a task was claimed twice"
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM run_tasks WHERE state = 'running'")
        assert cur.fetchone()[0] == len(claimed)


def test_anti_starvation_one_running_task_per_source_and_lane(pg):
    submit(pg, source="wiki", nodes=[node("a"), node("b"), node("c")])
    assert claim(pg, "w1") is not None
    assert claim(pg, "w2") is None, "a second task for the same (source, lane) ran"
    # A different lane for the same source is fine — the guard is per pair.
    submit(pg, recipe="r2", source="wiki", nodes=[node("a", lane="net")])
    assert claim(pg, "w3").lane == "net"


def test_anti_starvation_holds_under_concurrency(pg, pg_dsn):
    """The race the advisory lock exists for: two workers reading `count(*) = 0`
    for the same (source, lane) in overlapping transactions."""
    submit(pg, source="wiki", nodes=[node(f"n{i}") for i in range(6)])
    got: list = []
    lock = threading.Lock()
    barrier = threading.Barrier(6)

    def worker(n):
        with psycopg.connect(pg_dsn) as conn:
            barrier.wait()
            task = claim(conn, f"w{n}")
            if task is not None:
                with lock:
                    got.append(task.id)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert len(got) == 1


def test_lane_cap_is_fleet_wide(pg):
    for i in range(3):
        submit(pg, recipe=f"r{i}", source=f"s{i}", nodes=[node("a", lane="gpu")])
    caps = {"gpu": 2}
    assert claim(pg, "w1", caps=caps, default_cap=99) is not None
    assert claim(pg, "w2", caps=caps, default_cap=99) is not None
    assert claim(pg, "w3", caps=caps, default_cap=99) is None


def test_lane_cap_holds_under_concurrency(pg, pg_dsn):
    for i in range(6):
        submit(pg, recipe=f"r{i}", source=f"s{i}", nodes=[node("a", lane="cpu_heavy")])
    got: list = []
    lock = threading.Lock()
    barrier = threading.Barrier(6)

    def worker(n):
        with psycopg.connect(pg_dsn) as conn:
            barrier.wait()
            task = claim(conn, f"w{n}", caps={"cpu_heavy": 1}, default_cap=99)
            if task is not None:
                with lock:
                    got.append(task.id)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert len(got) == 1, "cpu_heavy cap of 1 was exceeded — the memory guard is not a guard"


def test_worker_only_claims_lanes_it_serves(pg):
    submit(pg, nodes=[node("a", lane="gpu")])
    assert claim(pg, lanes=["io", "net"]) is None
    assert claim(pg, lanes=["gpu"]) is not None


# --- pauses -----------------------------------------------------------------

@pytest.mark.parametrize("scope", ["global", "source:wiki", "lane:gpu", "recipe:nightly"])
def test_pause_scopes_block_the_claim(pg, scope):
    submit(pg, recipe="nightly", source="wiki", nodes=[node("a", lane="gpu")])
    C.set_pause(pg, scope, reason="test")
    assert claim(pg) is None, f"{scope} did not block"
    C.clear_pause(pg, scope)
    assert claim(pg) is not None


def test_an_unrelated_pause_does_not_block(pg):
    submit(pg, recipe="nightly", source="wiki", nodes=[node("a", lane="gpu")])
    C.set_pause(pg, "source:hn")
    C.set_pause(pg, "lane:io")
    assert claim(pg) is not None


def test_an_expired_pause_is_not_a_pause(pg):
    submit(pg, nodes=[node("a")])
    with pg.cursor() as cur:
        cur.execute("INSERT INTO pauses (scope, expires_at) VALUES ('global', "
                    "now() - interval '1 minute')")
    pg.commit()
    assert claim(pg) is not None


def test_pause_covering_reports_the_scope(pg):
    C.set_pause(pg, "lane:gpu", reason="freeing the GPU for queries")
    assert C.pause_covering(pg, "wiki", "gpu", "nightly") == "lane:gpu"
    assert C.pause_covering(pg, "wiki", "io", "nightly") == ""


# --- preconditions ----------------------------------------------------------

def test_declared_preconditions_must_be_satisfied(pg):
    submit(pg, nodes=[node("a", preconditions=["storage:staging"])])
    assert claim(pg, satisfied=()) is None
    assert claim(pg, satisfied=("gateway",)) is None
    assert claim(pg, satisfied=("storage:staging",)) is not None


def test_a_task_with_no_preconditions_always_passes(pg):
    submit(pg, nodes=[node("a")])
    assert claim(pg, satisfied=()) is not None


def test_an_unknown_precondition_parks_the_task(pg):
    """A typo must park the task visibly, not run it in exactly the situation
    the author was excluding."""
    submit(pg, nodes=[node("a", preconditions=["stroage:staging"])])
    assert claim(pg, satisfied=ALL_PRECONDITIONS) is None


# --- leases -----------------------------------------------------------------

def test_expired_lease_returns_the_task_to_ready(pg):
    submit(pg, nodes=[node("a")])
    task = claim(pg)
    expire_lease(pg, task.id)
    reclaimed = C.reclaim_expired(pg)
    assert [r["state"] for r in reclaimed] == ["ready"]
    with pg.cursor() as cur:
        cur.execute("SELECT state, attempts, lease_worker FROM run_tasks WHERE id = %s",
                    (task.id,))
        assert cur.fetchone() == ("ready", 1, None)
    assert claim(pg, "w2") is not None


def test_a_live_lease_is_not_reclaimed(pg):
    submit(pg, nodes=[node("a")])
    claim(pg)
    assert C.reclaim_expired(pg) == []


def test_max_attempts_turns_a_reclaim_into_a_failure(pg):
    run_id = submit(pg, nodes=[node("a", max_attempts=2)])
    for expected in ("ready", "failed"):
        task = claim(pg)
        assert task is not None
        expire_lease(pg, task.id)
        assert C.reclaim_expired(pg)[0]["state"] == expected
    with pg.cursor() as cur:
        cur.execute("SELECT state, error FROM run_tasks WHERE run_id = %s", (run_id,))
        state, error = cur.fetchone()
    assert state == "failed" and "lease expired" in error
    dag.advance(pg, run_id)
    with pg.cursor() as cur:
        cur.execute("SELECT state FROM runs WHERE id = %s", (run_id,))
        assert cur.fetchone()[0] == "failed"


def test_release_worker_reclaims_a_dead_slot_immediately(pg):
    submit(pg, nodes=[node("a")])
    claim(pg, "pool/0/1234")
    freed = C.release_worker(pg, "pool/0/1234")
    assert [f["state"] for f in freed] == ["ready"]
    assert claim(pg, "pool/0/9999") is not None


def test_heartbeat_extends_the_lease_and_reports_signals(pg):
    submit(pg, recipe="nightly", source="wiki", nodes=[node("a")])
    task = claim(pg)
    with pg.cursor() as cur:
        cur.execute("UPDATE run_tasks SET lease_expires_at = now() + interval '1 second' "
                    "WHERE id = %s", (task.id,))
    pg.commit()
    signals = C.heartbeat(pg, task.id, task.worker, units_done=3, units_failed=1,
                          stats={"pages": 3})
    assert signals.should_stop is False
    with pg.cursor() as cur:
        cur.execute("SELECT units_done, units_failed, stats, "
                    "lease_expires_at > now() + interval '30 seconds' "
                    "FROM run_tasks WHERE id = %s", (task.id,))
        done, failed, stats, extended = cur.fetchone()
    assert (done, failed, stats["pages"], extended) == (3, 1, 3, True)

    C.set_pause(pg, "source:wiki")
    assert C.heartbeat(pg, task.id, task.worker, units_done=3, units_failed=1).paused \
        == "source:wiki"


def test_heartbeat_after_a_reclaim_raises_lease_lost(pg):
    submit(pg, nodes=[node("a")])
    task = claim(pg)
    expire_lease(pg, task.id)
    C.reclaim_expired(pg)
    with pytest.raises(Exception) as exc:
        C.heartbeat(pg, task.id, task.worker, units_done=1, units_failed=0)
    assert "no longer leased" in str(exc.value)


def test_release_by_a_stale_holder_is_refused(pg):
    """The write guard that keeps two generations of one slot from interleaving
    counters on the same task."""
    submit(pg, nodes=[node("a")])
    task = claim(pg, "pool/0/111")
    expire_lease(pg, task.id)
    C.reclaim_expired(pg)
    claim(pg, "pool/0/222")               # a new holder
    with pytest.raises(Exception):
        C.release(pg, task, C.Release(outcome="succeeded", units_done=99))
    with pg.cursor() as cur:
        cur.execute("SELECT state, units_done FROM run_tasks WHERE id = %s", (task.id,))
        assert cur.fetchone() == ("running", 0)


# --- fair share -------------------------------------------------------------

def vtimes(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT source, vtime, in_flight FROM source_sched ORDER BY source")
        return {r[0]: (round(r[1], 3), r[2]) for r in cur.fetchall()}


def test_yield_charges_service_time_to_the_source(pg):
    submit(pg, source="wiki", nodes=[node("a")])
    task = claim(pg)
    C.release(pg, task, C.Release(outcome="yielded", elapsed=12.0))
    assert vtimes(pg)["wiki"] == (12.0, 0)


def test_weight_divides_the_charge(pg):
    submit(pg, source="wiki", nodes=[node("a")])
    with pg.cursor() as cur:
        cur.execute("INSERT INTO source_sched (source, weight) VALUES ('wiki', 4) "
                    "ON CONFLICT (source) DO UPDATE SET weight = 4")
    pg.commit()
    task = claim(pg)
    C.release(pg, task, C.Release(outcome="yielded", elapsed=8.0))
    assert vtimes(pg)["wiki"][0] == 2.0


def test_a_heavy_source_does_not_starve_a_light_one(pg):
    """The property the whole fair-share machinery exists for.

    `heavy` yields after long slices, `light` after short ones. Under the FIFO
    the crawl loop uses today, heavy's queue of tasks would run to exhaustion
    first. Under WFQ the two interleave, and heavy is charged for the time it
    actually consumed.
    """
    for i in range(6):
        submit(pg, recipe=f"h{i}", source="heavy", nodes=[node("a")])
        submit(pg, recipe=f"l{i}", source="light", nodes=[node("a")])

    order = []
    for _ in range(8):
        task = claim(pg)
        assert task is not None
        order.append(task.source)
        C.release(pg, task, C.Release(
            outcome="succeeded", elapsed=60.0 if task.source == "heavy" else 6.0))

    # Not strict alternation — heavy's 60 s slice buys it ten of light's — but
    # neither source may be shut out, which is exactly what FIFO does today.
    assert order.count("heavy") >= 1 and order.count("light") >= 1
    assert order[0] != order[-1] or len(set(order)) > 1
    assert vtimes(pg)["heavy"][0] > vtimes(pg)["light"][0]


def test_returning_idle_source_is_rebased_not_given_free_credit(pg):
    """The classic WFQ bug: idle for a week, accumulate infinite credit, then
    monopolize on return."""
    with pg.cursor() as cur:
        cur.execute("INSERT INTO source_sched (source, vtime, in_flight) "
                    "VALUES ('busy', 1000, 1), ('idle', 0, 0)")
    pg.commit()
    submit(pg, source="idle", nodes=[node("a")])
    task = claim(pg)
    assert task.source == "idle"
    # Rebased to the in-flight floor on claim, so its NEXT claim competes fairly
    # instead of running for a thousand seconds of credit first.
    assert vtimes(pg)["idle"][0] == 1000.0


def test_in_flight_is_reconciled_against_reality(pg):
    """in_flight is a counter in a table — the shape embed/budget.py rejects for
    the GPU budget. It is only safe because this heals it every tick."""
    with pg.cursor() as cur:
        cur.execute("INSERT INTO source_sched (source, in_flight) VALUES ('ghost', 7)")
    pg.commit()
    assert C.reconcile_in_flight(pg) == 1
    assert vtimes(pg)["ghost"][1] == 0


# --- preemption -------------------------------------------------------------

def test_higher_priority_arrival_asks_the_holder_to_yield(pg):
    submit(pg, recipe="backfill", source="s1", nodes=[node("a", lane="net")], priority=10)
    task = claim(pg)
    assert C.request_yield_for_priority(pg) == 0        # nothing is waiting yet
    submit(pg, recipe="manual", source="s2", nodes=[node("a", lane="net")], priority=100)
    assert C.request_yield_for_priority(pg) == 1
    assert C.heartbeat(pg, task.id, task.worker, units_done=0,
                       units_failed=0).yield_requested is True


def test_preemption_is_per_lane(pg):
    submit(pg, recipe="backfill", source="s1", nodes=[node("a", lane="net")], priority=10)
    claim(pg)
    submit(pg, recipe="manual", source="s2", nodes=[node("a", lane="gpu")], priority=100)
    assert C.request_yield_for_priority(pg) == 0


# --- events -----------------------------------------------------------------

def test_the_lifecycle_is_recorded_in_run_events(pg):
    run_id = submit(pg, nodes=[node("a")])
    task = claim(pg)
    C.release(pg, task, C.Release(outcome="succeeded", units_done=4, elapsed=1.5))
    dag.advance(pg, run_id)
    with pg.cursor() as cur:
        cur.execute("SELECT event FROM run_events WHERE run_id = %s ORDER BY seq", (run_id,))
        events = [r[0] for r in cur.fetchall()]
    assert events == ["run.queued", "task.leased", "task.succeeded", "run.succeeded"]
