"""The tick loop and its singleton lock.

Deliberately shallow — a never-exiting loop is not something to test by running
it. What is tested is the two decisions the loop makes that matter: it ticks when
it holds the lock, and it stands by (rather than exiting) when it does not.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg.types.json import Jsonb

from windex.scheduler import loop as sched_loop
from windex.scheduler.loop import run_loop, try_scheduler_lock

UTC = timezone.utc


def nodes(spec):
    return [{"node": "run", "kind": "discover", "module": "test.discover"}]


@pytest.fixture()
def armed(pg):
    """A recipe plus a trigger that is already overdue, so one tick fires it."""
    with pg.cursor() as cur:
        cur.execute("""INSERT INTO recipes (name, source, spec, spec_hash)
                       VALUES ('wiki', 'wiki', %s, 'sha1:wiki')""", (Jsonb({}),))
        cur.execute(
            """INSERT INTO triggers (name, recipe, type, cron, timezone, next_fire_at)
               VALUES ('nightly', 'wiki', 'cron', '0 3 * * *', 'UTC', %s)""",
            (datetime.now(UTC) - timedelta(seconds=5),))
    pg.commit()
    yield pg
    sched_loop._ANNOUNCED.clear()


def test_one_tick_fires_and_reports(armed, pg_dsn):
    seen = []
    run_loop(pg_dsn, compile_tasks=nodes, once=True, on_result=seen.append)
    assert len(seen) == 1 and len(seen[0].fired) == 1
    assert "nightly" in seen[0].summary()

    with armed.cursor() as cur:
        cur.execute("SELECT count(*) FROM runs")
        assert cur.fetchone()[0] == 1
    armed.rollback()


def test_a_standby_instance_does_not_tick_and_does_not_exit(armed, pg_dsn):
    """Exiting on lock contention would make a rolling restart leave NO scheduler
    running for as long as the new container takes to come up — the shape of the
    2026-07-17 stall, where components exiting by design turned a transient
    failure into ~36 hours of nothing."""
    holder = psycopg.connect(pg_dsn)
    try:
        assert try_scheduler_lock(holder) is True

        seen = []
        run_loop(pg_dsn, compile_tasks=nodes, once=True, on_result=seen.append)

        assert seen == []                      # stood by
        with armed.cursor() as cur:
            cur.execute("SELECT count(*) FROM runs")
            assert cur.fetchone()[0] == 0      # did NOT tick
        armed.rollback()
    finally:
        holder.close()

    # The holder is gone; the standby takes over on its next pass.
    seen = []
    run_loop(pg_dsn, compile_tasks=nodes, once=True, on_result=seen.append)
    assert len(seen) == 1 and len(seen[0].fired) == 1


def test_a_raising_report_callback_does_not_stop_the_loop(armed, pg_dsn):
    def boom(result):
        raise RuntimeError("the console went away")

    run_loop(pg_dsn, compile_tasks=nodes, once=True, on_result=boom)
    with armed.cursor() as cur:
        cur.execute("SELECT count(*) FROM runs")
        assert cur.fetchone()[0] == 1          # the fire still committed
    armed.rollback()


def test_a_compiler_that_raises_does_not_stop_the_loop(armed, pg_dsn):
    """The tick isolates it to one trigger; the loop keeps going. A scheduler that
    dies on a bad recipe takes every other recipe down with it."""
    def boom(spec):
        raise RuntimeError("compiler blew up")

    seen = []
    run_loop(pg_dsn, compile_tasks=boom, once=True, on_result=seen.append)
    assert len(seen) == 1 and seen[0].failed and not seen[0].fired
