"""The tick loop, and the advisory lock that makes exactly one of them authoritative.

Two properties, both learned the hard way elsewhere in this codebase.

**Exactly one ticker.** The lock is
`pg_try_advisory_lock(hashtext('windex.scheduler'))`, taken at session scope on a
dedicated connection. It replaces nothing — today's `windex scheduler` has no
mutual exclusion at all, and `jobs._spawn_lock`'s flock (the nearest equivalent
anywhere in windex) is a *file* lock, which cannot see across container
boundaries and so does not work in the deployment this actually runs in. A
Postgres advisory lock is held by a session and released automatically when that
session ends — including when the process is SIGKILLed, when the container is
recreated, and when the host↔container TCP link drops. There is nothing to clean
up and no stale-lock timeout to tune.

**Losing the lock is standby, not exit.** A second instance that cannot take the
lock keeps ticking its own loop, retrying the acquisition each time, and takes
over within one interval of the holder disappearing. Exiting instead would make
a rolling restart leave *no* scheduler running for as long as the new container
takes to come up — the shape of the 2026-07-17 incident, where the embed loops
exited by design during a 25-minute gateway outage and a transient failure became
a ~36-hour stall because nothing brought them back.

The loop never exits on error, for the same reason.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Callable

import psycopg

from windex import db
from windex.scheduler.fire import CompileTasks, TickResult, tick
from windex.scheduler.triggers import DEFAULT_MISFIRE_GRACE

log = logging.getLogger("windex.scheduler")

# The lock name. `hashtext` is computed by Postgres rather than in Python so the
# value cannot drift between a Python implementation and a psql session an
# operator uses to check `pg_locks`.
SCHEDULER_LOCK_NAME = "windex.scheduler"
SCHEDULER_LOCK_KEY = f"hashtext('{SCHEDULER_LOCK_NAME}')"

# 10 s. Fine enough that a `POST /admin/v1/triggers/{n}/fire` and a due timer feel
# equally prompt, coarse enough that the tick is invisible in `pg_stat_activity`:
# one indexed scan of `triggers` per tick, and `triggers_due_idx` makes it an
# index-only lookup returning nothing on the ~8,600 ticks a day when nothing is
# due. The old loop's 60 s was chosen when firing meant forking a process.
DEFAULT_INTERVAL = 10.0


def try_scheduler_lock(conn: psycopg.Connection) -> bool:
    """Try to become the authoritative ticker. Non-blocking; safe to re-call.

    Advisory locks are re-entrant per session: a second call from the session
    that already holds it returns true and increments a counter. That is
    deliberate here — the loop calls this every tick as its liveness check, and a
    version that leaked a lock count per tick would need a matching unlock, which
    is one more thing to get wrong on the error path. The count is irrelevant
    because the lock is released wholesale when the session ends, which is the
    only way this process ever gives it up.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT pg_try_advisory_lock({SCHEDULER_LOCK_KEY})")
        return bool(cur.fetchone()[0])


def run_loop(dsn: str, *, compile_tasks: CompileTasks,
             interval: float = DEFAULT_INTERVAL,
             grace_seconds: float = DEFAULT_MISFIRE_GRACE,
             once: bool = False,
             on_result: Callable[[TickResult], None] | None = None) -> None:
    """Never-exiting scheduler loop. `once=True` runs a single tick and returns.

    Uses one dedicated connection for both the lock and the work. Sharing is
    correct and it is also what makes the failure mode right: a connection that
    dies takes the lock with it *and* forces this loop through its reconnect
    path, so "I still hold the lock" and "I can still reach the database" can
    never disagree. Two connections could, and the disagreement would be a
    silent second ticker.

    `on_result` is the reporting seam (the CLI prints; a future metrics exporter
    would count). Kept out of the loop body so a raising callback cannot stop the
    scheduler.

    Closes its connection on every exit path, which is what *releases* the lock.
    That matters for `once=True` specifically: `--once` from cron, or an in-process
    caller, would otherwise leave the session — and therefore the lock — alive
    until garbage collection got round to it, and any scheduler starting in the
    meantime would silently stand by instead of ticking.
    """
    conn: psycopg.Connection | None = None
    holding = False

    try:
        while True:
            started = time.monotonic()
            try:
                if conn is None or conn.closed:
                    conn = db.connect(dsn)
                    holding = False   # a new session holds nothing
                if not holding:
                    holding = try_scheduler_lock(conn)
                    if not holding:
                        # Another instance is authoritative. Standing by, not
                        # exiting — see the module docstring.
                        log.debug("scheduler lock held elsewhere; standing by")
                        if once:
                            return
                        _sleep_remaining(started, interval)
                        continue
                    log.info("scheduler: holding %s, ticking every %.0fs",
                             SCHEDULER_LOCK_NAME, interval)

                result = tick(conn, compile_tasks=compile_tasks,
                              now=datetime.now(timezone.utc),
                              grace_seconds=grace_seconds, announced=_ANNOUNCED)
                if on_result is not None and result:
                    try:
                        on_result(result)
                    except Exception:  # noqa: BLE001 — reporting must not stop the loop
                        log.exception("scheduler: on_result callback failed")
            except psycopg.OperationalError as exc:
                # The connection died: the lock died with it. Drop both and rebuild
                # on the next pass rather than ticking against a corpse.
                log.warning("scheduler: lost the database connection (%s) — reconnecting",
                            exc)
                _close(conn)
                conn, holding = None, False
            except Exception:  # noqa: BLE001 — a blip must never kill the scheduler
                log.exception("scheduler: tick failed")

            if once:
                return
            _sleep_remaining(started, interval)
    finally:
        _close(conn)


# Carried across ticks so a paused scope announces itself once per pause episode
# instead of once per tick. Process-global because `run_loop` is by construction
# the only ticker in the process; see fire.tick for why the guard exists at all.
_ANNOUNCED: dict[str, tuple] = {}


def _sleep_remaining(started: float, interval: float) -> None:
    """Sleep out the rest of the interval, so a slow tick does not add to it.

    A fixed `sleep(interval)` after the work makes the real period
    `interval + tick_duration`, which drifts a nominally 10 s loop out to 12-15 s
    under load — right when firing promptly matters most.
    """
    time.sleep(max(0.0, interval - (time.monotonic() - started)))


def _close(conn: psycopg.Connection | None) -> None:
    try:
        if conn is not None:
            conn.close()
    except Exception:  # noqa: BLE001 — a dead connection may raise on close
        pass
