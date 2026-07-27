"""One slot: claim a task, run a slice, repeat, then exit so it can be recycled.

A slot is a whole process on purpose (plan §C.1). The three reasons, restated
because each one is a specific past failure and not a preference:

* **Memory.** CPython does not reliably return arena memory to the OS. A thread
  pool that once handled a 333 MB wiki shard carries that high-water mark until
  the process dies, so recycling *is* the memory ceiling — and only a process
  can be recycled.
* **Parallelism.** trafilatura/lxml/bz2/orjson are CPU-bound Python. Threads
  would serialize behind the GIL what today's per-source containers were
  accidentally parallelising, so the "consolidate the containers" change would
  have been a throughput regression dressed as a cleanup.
* **Killability.** A wedged task is one SIGKILL to one slot. A wedged
  C-extension call inside a thread pool cannot be interrupted at all.

The loop deliberately exits — on drain, on ``max_tasks_per_slot``, on a database
outage it cannot ride out. Exiting is not failure here; the supervisor forks a
replacement, and a fresh process is the cheapest possible free().
"""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from contextlib import suppress

import psycopg

from windex import db
from windex.worker import canonical_claim as C
from windex.worker import control as ctrlfile
from windex.pipeline import run_store
from windex.worker.config import PoolConfig
from windex.worker.execute import run_slice
from windex.worker.protocol import Resolve

log = logging.getLogger("windex.worker.slot")

# Exit codes the supervisor reads. Anything else means the slot died in a way it
# did not choose, which is logged and re-forked identically — the distinction is
# for humans reading journalctl, not for control flow.
EXIT_DONE = 0        # recycled cleanly (drain or max_tasks_per_slot)
EXIT_DB = 3          # could not keep a database connection


def worker_id(pool: str, index: int, pid: int | None = None) -> str:
    """The ``run_tasks.lease_worker`` string.

    Carries the pid so a lease is attributable to a *specific* process: two
    generations of slot 2 must not be able to write to each other's task, and
    the guard that prevents it is a string comparison in every control-plane
    UPDATE.
    """
    return f"{pool}/{index}/{pid if pid is not None else os.getpid()}"


def slot_main(dsn: str, resolve: Resolve, cfg: PoolConfig, index: int) -> int:
    """Entry point of a forked slot. Returns a process exit code."""
    drain = threading.Event()

    def _drain(signum: int, _frame: object) -> None:
        # Finish the slice, commit, release the lease, exit. The whole point of
        # slicing is that this costs nothing: a yield is the crash-resume path
        # minus the crash, so a deploy or a memory-driven recycle never loses
        # work and never has to wait for a task to complete.
        drain.set()

    # Installed as early as possible, because until it is the child still holds
    # the SUPERVISOR's inherited handler — and the supervisor's handler sets a
    # flag on an object the child does not care about, so a retire signal
    # arriving in that window is swallowed rather than fatal. The supervisor
    # re-sends on every tick for exactly this reason (supervisor._retire).
    signal.signal(signal.SIGTERM, _drain)
    signal.signal(signal.SIGINT, _drain)

    me = worker_id(cfg.name, index)
    log.info("slot %s starting (lanes=%s)", me, ",".join(cfg.lanes))
    slices = 0
    ctl = work = None
    try:
        ctl, work = db.connect(dsn), db.connect(dsn)
        while not drain.is_set() and slices < cfg.max_tasks_per_slot:
            published = ctrlfile.read(cfg.control_path,
                                      stale_seconds=cfg.control_stale_seconds)
            task = C.claim_task(
                ctl, worker=me, lanes=published.lanes(cfg.lanes),
                caps=cfg.lane_caps, satisfied=published.satisfied_for_claim(),
                default_cap=cfg.default_lane_cap,
            )
            if task is None:
                drain.wait(cfg.claim_idle_seconds)
                continue
            slices += 1
            active_path = cfg.active_slice_path(index)
            # Publish before resolving or invoking module code. Failure to
            # publish is fail-closed: the slot exits and the supervisor releases
            # the lease rather than running work it cannot bound.
            ctrlfile.write_active_slice(
                active_path,
                pid=os.getpid(),
                worker=me,
                task_id=task.id,
                generation=slices,
                started_at=time.monotonic(),
            )
            try:
                _run_one(ctl, work, task, resolve, cfg, drain)
            finally:
                ctrlfile.clear_active_slice(active_path)
    except psycopg.OperationalError as exc:
        # Postgres went away. Exiting is correct: the supervisor re-forks with
        # backoff, and a slot that sat retrying forever is how the embed loops
        # turned a 25-minute gateway blip into a 36-hour stall.
        log.warning("slot %s lost the database: %s", me, exc)
        return EXIT_DB
    finally:
        for conn in (ctl, work):
            if conn is not None:
                with suppress(Exception):
                    conn.close()
    log.info("slot %s exiting after %d slice(s)", me, slices)
    return EXIT_DONE


def _run_one(ctl: psycopg.Connection, work: psycopg.Connection, task: C.ClaimedTask,
             resolve: Resolve, cfg: PoolConfig, drain: threading.Event) -> None:
    """Resolve the module, run the slice, and advance the run if it ended."""
    started = time.monotonic()
    try:
        runner = resolve(task.module)
        if task.executor == "builtin":
            from windex.pipeline import registry

            actual = registry.implementation_digest(task.module)
            if actual != task.module_digest:
                raise RuntimeError(
                    "frozen Module digest does not match this worker deployment "
                    f"({task.module}: expected {task.module_digest}, got {actual})")
        elif task.executor == "platform":
            platform_digests = {
                "platform.index": "builtin:platform.index/1",
                "platform.reset": "builtin:platform.reset/1",
            }
            if platform_digests.get(task.module) != task.module_digest:
                raise RuntimeError(
                    f"unsupported platform Module lock for {task.module}")
    except Exception as exc:                    # noqa: BLE001
        # An unknown module will be unknown on the retry too, so this skips the
        # retry budget entirely: three identical failures buy nothing but a
        # delayed red state and three times the log noise.
        C.release(ctl, task, C.Release(
            outcome="failed", units_done=task.units_done, units_failed=task.units_failed,
            elapsed=time.monotonic() - started, permanent=True,
            error=f"cannot resolve module {task.module!r}: {exc}",
            reason="unresolved_module"))
        run_store.advance(ctl, task.run_id)
        return

    outcome = run_slice(ctl, work, task, runner, cfg, drain=drain)
    if outcome.outcome == "lease_lost":
        log.warning("slot lost the lease on task %s mid-slice (%s)", task.id, outcome.reason)
        return
    log.info("task %s %s (%s) done=%d failed=%d in %.1fs", task.id, outcome.outcome,
             outcome.reason, outcome.units_done, outcome.units_failed, outcome.elapsed)
    if outcome.state in C.TERMINAL:
        # Advance inline rather than waiting for the supervisor's sweep: the next
        # node in the DAG becomes claimable immediately, which for a three-node
        # Pipeline is the difference between finishing in a slice and finishing in
        # a slice plus three supervisor ticks.
        run_store.advance(ctl, task.run_id)


def slot_entry(dsn: str, resolve: Resolve, cfg: PoolConfig, index: int) -> None:
    """``multiprocessing`` target wrapper.

    Exists only because ``Process`` discards its target's return value — the
    exit code has to be raised, not returned, or the supervisor sees every death
    as a clean exit and loses the one signal that distinguishes "recycled" from
    "the database went away".
    """
    raise SystemExit(slot_main(dsn, resolve, cfg, index))
