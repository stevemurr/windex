"""The supervisor: one small process that owns the slots and the sweeps.

    windex-worker
    ├─ supervisor (this file — no pyarrow, no datatrove, no qdrant client)
    │    ├─ claim-side policy: preconditions, backpressure, lane availability
    │    ├─ reaper: lease expiry, RSS watch, slot recycling, cancellations
    │    └─ DAG advancement backstop
    └─ slots 1..K — forked children, one task at a time (slot.py)

It stays deliberately dependency-light: everything heavy is imported inside a
slot, so the process that must survive to reap a memory blow-up is not itself
holding 2 GB of arrow buffers. Six resident Python processes replace fourteen.

**The fork hazard, stated because it is invisible and fatal.** A psycopg
connection must never be open across ``fork()``: the child inherits the socket
file descriptor, and if the child's interpreter ever finalizes that object it
sends a terminate packet down a connection the *parent* is still using. The
supervisor therefore opens its connection inside each sweep and closes it before
any fork can happen — ``_ensure_slots`` runs only after ``_sweep`` has returned.
Children open their own connections after they start.

Everything the supervisor does is idempotent and level-triggered, so the
supervisor itself is restartable at any instant: it reads the world, decides,
acts, and keeps nothing that matters in memory. Kill -9 it and the only loss is
the RSS history.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import signal
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import psycopg

from windex import db
from windex.worker import canonical_claim as C
from windex.worker import control as ctrlfile
from windex.worker import memory
from windex.pipeline import run_store
from windex.worker.config import PoolConfig
from windex.worker.control import SlotStatus
from windex.worker.protocol import Resolve
from windex.worker.slot import slot_entry, worker_id

log = logging.getLogger("windex.worker.supervisor")


@dataclass(frozen=True)
class RunningTask:
    task_id: int
    worker: str
    yield_requested: bool = False
    cancelled: bool = False
    paused: bool = False


class Pool:
    """Supervisor for K slot subprocesses.

    ``resolve`` maps ``run_tasks.module`` to a runner and is inherited by the
    children through ``fork`` — which is why the fork start method is pinned
    below rather than left to the platform default. Under ``spawn`` the resolver
    would have to be picklable, and requiring that of a module registry would
    force the registry's shape from the far end of the system.
    """

    def __init__(self, dsn: str, resolve: Resolve, cfg: PoolConfig | None = None, *,
                 settings: Any = None,
                 precond: Callable[[], set[str]] | None = None) -> None:
        self.dsn = dsn
        self.resolve = resolve
        self.cfg = cfg or PoolConfig()
        self.settings = settings
        self.slots: dict[int, SlotStatus] = {}
        self._procs: dict[int, mp.process.BaseProcess] = {}
        self._generation = 0
        self._stopping = False
        self._ctx = mp.get_context("fork")
        # slot index -> when we first asked it to retire. Retirement is retried
        # every tick and escalates, see _retire.
        self._retiring: dict[int, float] = {}
        # index -> whether release_worker should charge an execution attempt.
        # A true slice overrun is a task failure; an operator cancel/pause is not.
        self._forced_exits: dict[int, bool] = {}
        self._prev_signals: dict[int, object] = {}
        if precond is not None:
            self._precond = precond
        elif settings is not None:
            from windex.worker.preconditions import Cache

            self._precond = Cache(settings, ttl=self.cfg.precondition_ttl_seconds).get
        else:
            # No way to verify anything ⇒ verify nothing. Tasks that declare no
            # preconditions still run, which keeps a misconfigured pool useful
            # instead of silently idle.
            log.warning("no settings and no precondition evaluator — tasks declaring "
                        "preconditions will never be claimed")
            self._precond = set

    # --- lifecycle ---------------------------------------------------------

    def run(self, *, until: Callable[[], bool] | None = None) -> None:
        """Supervise until signalled (or until ``until()`` is true, for tests)."""
        self._install_signals()
        log.info("worker pool '%s' starting: %d slots, lanes=%s",
                 self.cfg.name, self.cfg.slots, ",".join(self.cfg.lanes))
        try:
            while not self._stopping and not (until is not None and until()):
                started = time.monotonic()
                try:
                    self.tick()
                except psycopg.OperationalError as exc:
                    # A database blip must never take the supervisor down: it is
                    # the thing that would otherwise notice everything else. The
                    # slots exit on their own and are re-forked when it returns.
                    log.warning("supervisor tick failed (database): %s", exc)
                except Exception as exc:            # noqa: BLE001 — see above
                    log.exception("supervisor tick failed: %s", exc)
                time.sleep(max(0.0, self.cfg.tick_seconds - (time.monotonic() - started)))
        finally:
            self.shutdown()
            self._restore_signals()

    def _install_signals(self) -> None:
        """Catch SIGTERM/SIGINT, and put the previous handlers back afterwards.

        The restore matters more than it looks: a *forked child* inherits
        whatever disposition is installed here until it overwrites it, so a
        handler left behind after ``run()`` returns can silently swallow a
        SIGTERM sent to a slot in its first milliseconds — the slot then never
        retires, which for a memory-driven recycle means the pool keeps the
        high-water process it decided to replace. Leaving process-global state
        behind is also just how in-process users (tests, an embedded pool) get
        mysteriously affected by a pool that has already stopped.
        """
        def _stop(signum: int, _frame: object) -> None:
            log.info("supervisor received signal %s — draining", signum)
            self._stopping = True

        for sig in (signal.SIGTERM, signal.SIGINT):
            with suppress(ValueError):
                self._prev_signals[sig] = signal.signal(sig, _stop)

    def _restore_signals(self) -> None:
        for sig, handler in self._prev_signals.items():
            with suppress(ValueError, TypeError):
                signal.signal(sig, handler)     # type: ignore[arg-type]
        self._prev_signals.clear()

    def shutdown(self) -> None:
        """Drain: ask every slot to finish its slice, then insist.

        SIGTERM first because a slice boundary releases the lease cleanly and the
        task is instantly claimable elsewhere; SIGKILL only after the grace,
        after which the lease reclaim (which the next supervisor start performs)
        is the safety net.
        """
        deadline = time.monotonic() + self.cfg.stop_grace_seconds
        while True:
            alive = [i for i, p in self._procs.items() if p.is_alive()]
            if not alive or time.monotonic() >= deadline:
                break
            # Re-sent every pass, for the fork-window reason in _press_retirements.
            for index in alive:
                self._signal(self.slots[index].pid, signal.SIGTERM)
            time.sleep(0.1)
        for index, proc in list(self._procs.items()):
            if proc.is_alive():
                log.warning("slot %d did not stop within %.0fs — killing",
                            index, self.cfg.stop_grace_seconds)
                self._signal(self.slots[index].pid, signal.SIGKILL)
                proc.join(timeout=5)
        self._reap(release=True)

    # --- one supervision cycle --------------------------------------------

    def tick(self) -> dict:
        """One pass. Returns a small summary, which is what the tests assert on."""
        readings = memory.read_all([s.pid for s in self.slots.values()])
        for status in self.slots.values():
            status.rss = readings.get(status.pid)
        retire_pids, kill_pids = memory.classify(
            readings, high_water=self.cfg.rss_high_water_bytes,
            hard=self.cfg.rss_hard_bytes)
        total_rss = memory.total(readings)
        blocked = memory.backpressure_lanes(
            total_rss, self.cfg.mem_limit_bytes, self.cfg.backpressure_fraction,
            self.cfg.backpressure_lanes)

        satisfied = self._safe_precond()
        summary = self._sweep(retire_pids, satisfied)
        summary["rss_total"] = total_rss
        summary["blocked_lanes"] = blocked

        # Signals go out AFTER the sweep so the yield request is already durable
        # when the slot wakes up: SIGTERM alone would make the slot finish its
        # slice, but the yield row is what stops it claiming a new one anywhere.
        for pid in retire_pids:
            status = self._by_pid(pid)
            if status is not None:
                self._retire(status.index,
                             f"{readings[pid] / 1024 ** 3:.1f} GiB RSS "
                             f"(high water {self.cfg.rss_high_water_bytes / 1024 ** 3:.1f})")
        for pid in kill_pids:
            log.error("slot pid %d at %.1f GiB — hard ceiling, killing now",
                      pid, readings[pid] / 1024 ** 3)
            self._signal(pid, signal.SIGKILL)
        self._press_retirements()

        self._generation += 1
        ctrlfile.write(self.cfg.control_path, satisfied=satisfied,
                       blocked_lanes=blocked, generation=self._generation)

        # Reaping and forking come last, and only here, because forking with an
        # open database connection is the hazard described in the module
        # docstring. _sweep() closed its connection before returning.
        summary["reaped"] = self._reap()
        if not self._stopping:
            summary["forked"] = self._ensure_slots()
        return summary

    def _safe_precond(self) -> set[str]:
        try:
            return self._precond()
        except Exception as exc:            # noqa: BLE001
            log.warning("precondition evaluation failed: %s", exc)
            return set()

    def _sweep(self, retire_pids: list[int], satisfied: set[str]) -> dict:
        """All the database-side reaping, in one short-lived connection."""
        with db.connect(self.dsn) as conn:
            reclaimed = C.reclaim_expired(conn)
            blocked = C.reconcile_blocked(conn, satisfied)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM runs WHERE cancel_requested "
                    "AND state IN ('queued','running','blocked')")
                cancelled_ids = [row[0] for row in cur.fetchall()]
            conn.commit()
            cancelled = 0
            for run_id in cancelled_ids:
                cancelled += int(run_store.cancel(conn, run_id, by="supervisor"))
            preempted = C.request_yield_for_priority(conn)
            for pid in retire_pids:
                status = self._by_pid(pid)
                if status is not None:
                    C.request_yield(conn, worker=status.worker, reason="memory high-water")
            self._hung_watch(conn)
            C.reconcile_in_flight(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM runs WHERE state IN ('queued','running','blocked') "
                    "ORDER BY id LIMIT 500")
                live_ids = [row[0] for row in cur.fetchall()]
            conn.commit()
            for run_id in live_ids:
                run_store.advance(conn, run_id)
            runs = len(live_ids)
        return {
            "reclaimed": len(reclaimed), "cancelled": cancelled,
            "preempted": preempted, "runs_advanced": runs,
            **blocked,
        }

    def _hung_watch(self, conn: psycopg.Connection) -> None:
        """Enforce slice and control deadlines outside the slot process.

        Module code may be stuck in Python, native code, or a C extension that
        holds the GIL. The supervisor therefore cannot rely on should_yield(),
        the heartbeat thread, or a timer inside the slot. Each slot atomically
        publishes its current slice boundary and this process enforces it.
        """
        with conn.cursor() as cur:
            cur.execute(
                """SELECT t.id, t.lease_worker, t.yield_requested,
                          r.cancel_requested,
                          coalesce(c.paused, false),
                          t.module
                     FROM run_tasks t
                     JOIN runs r ON r.id = t.run_id
                     LEFT JOIN source_control c ON c.source_id = t.source_id
                    WHERE t.state = 'running'
                      AND t.lease_worker IS NOT NULL"""
            )
            rows = cur.fetchall()
        conn.commit()
        pending = {
            worker: RunningTask(
                task_id=task_id,
                worker=worker,
                yield_requested=bool(yield_requested),
                cancelled=bool(cancelled),
                paused=bool(paused) and module != "platform.reset",
            )
            for task_id, worker, yield_requested, cancelled, paused, module in rows
        }
        self._enforce_hung_policy(pending)

    def _enforce_hung_policy(
        self,
        pending: dict[str, RunningTask],
        *,
        now: float | None = None,
    ) -> None:
        """Apply the process-level deadline policy.

        Kept separate from the query so focused tests can exercise real slice
        transitions and signals without a database.
        """
        observed_at = time.monotonic() if now is None else now
        for status in self.slots.values():
            task = pending.get(status.worker)
            active = ctrlfile.read_active_slice(
                self.cfg.active_slice_path(status.index))
            if (
                task is None
                or active is None
                or active.pid != status.pid
                or active.worker != status.worker
                or active.task_id != task.task_id
            ):
                status.yield_since = None
                status.task_id = None
                status.slice_generation = None
                continue

            # A cooperative yield followed by another claim may use the same
            # task id and worker. Generation is the value that makes those two
            # healthy slices distinct.
            if (
                status.task_id != task.task_id
                or status.slice_generation != active.generation
            ):
                status.task_id = task.task_id
                status.slice_generation = active.generation
                status.yield_since = None

            grace = self.cfg.hung_grace_seconds
            control_reason = (
                "cancel" if task.cancelled
                else "pause" if task.paused
                else "yield" if task.yield_requested
                else ""
            )
            if control_reason:
                if status.yield_since is None:
                    status.yield_since = observed_at
                control_wait = observed_at - status.yield_since
            else:
                status.yield_since = None
                control_wait = -1.0

            slice_overrun = (
                observed_at - active.started_at - self.cfg.slice_seconds)
            control_expired = bool(control_reason) and control_wait >= grace
            slice_expired = slice_overrun >= grace
            if not control_expired and not slice_expired:
                continue

            # Cancellation and pause are operator lifecycle actions, not module
            # failures. A killed task is released without spending an attempt.
            # Priority/memory yield requests and deadline overruns still spend an
            # attempt so a broken module cannot spin forever across fresh slots.
            lifecycle_control = task.cancelled or task.paused
            reason = (
                f"ignored {control_reason} for {control_wait:.0f}s"
                if control_expired
                else f"exceeded slice by {slice_overrun:.0f}s"
            )
            self._force_exit(
                status.index,
                reason=reason,
                penalize=not lifecycle_control,
            )

    def _force_exit(self, index: int, *, reason: str, penalize: bool) -> None:
        if index in self._forced_exits:
            return
        status = self.slots.get(index)
        if status is None:
            return
        self._forced_exits[index] = penalize
        log.error(
            "slot %s is non-cooperative (%s) — killing%s",
            status.worker,
            reason,
            " and charging an attempt" if penalize else "",
        )
        self._signal(status.pid, signal.SIGKILL)

    # --- slots -------------------------------------------------------------

    def _retire(self, index: int, reason: str) -> None:
        """Ask a slot to finish its slice and exit. Idempotent."""
        status = self.slots.get(index)
        if status is None or index in self._retiring:
            return
        log.warning("retiring slot %s: %s", status.worker, reason)
        self._retiring[index] = time.monotonic()
        self._signal(status.pid, signal.SIGTERM)

    def _press_retirements(self) -> None:
        """Keep asking, then insist.

        SIGTERM is re-sent every tick rather than once, because a slot that has
        only just been forked may not have installed its handler yet — until it
        does, it still carries the supervisor's inherited disposition and the
        signal is swallowed. Sending once would leave that slot running forever
        with the RSS high-water mark we retired it for, which is the precise
        failure the memory ceiling exists to prevent. Re-sending is free.
        """
        for index, since in list(self._retiring.items()):
            status = self.slots.get(index)
            proc = self._procs.get(index)
            if status is None or proc is None or not proc.is_alive():
                self._retiring.pop(index, None)
                continue
            waited = time.monotonic() - since
            if waited > self.cfg.stop_grace_seconds:
                log.error("slot %s ignored %.0fs of retire requests — killing",
                          status.worker, waited)
                self._signal(status.pid, signal.SIGKILL)
            else:
                self._signal(status.pid, signal.SIGTERM)

    def _by_pid(self, pid: int) -> SlotStatus | None:
        for status in self.slots.values():
            if status.pid == pid:
                return status
        return None

    def _signal(self, pid: int, sig: int) -> None:
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass                     # already gone; _reap will notice
        except PermissionError:      # pragma: no cover
            log.error("not permitted to signal slot pid %d", pid)

    def _reap(self, release: bool = True) -> int:
        """Collect exited slots and hand their tasks straight back.

        Waiting for the lease to expire would work — that is what it is for —
        but it would idle the task for up to ``lease_seconds`` (300 s, and a
        polite fetch may declare 600) for a death the supervisor watched happen.
        Releasing eagerly is the difference between a recycle costing a fork and
        a recycle costing five minutes of a lane.
        """
        dead = [i for i, p in self._procs.items() if not p.is_alive()]
        if not dead:
            return 0
        workers: list[tuple[str, bool]] = []
        for index in dead:
            retiring = index in self._retiring
            self._retiring.pop(index, None)
            forced_penalty = self._forced_exits.pop(index, None)
            proc = self._procs.pop(index)
            status = self.slots.pop(index, None)
            code = proc.exitcode
            if code not in (0, None):
                log.warning("slot %d (pid %s) exited with %s", index, proc.pid, code)
            proc.join(timeout=1)
            if status is not None:
                expected = self._stopping or retiring or code in (0, None)
                penalize = forced_penalty if forced_penalty is not None else not expected
                workers.append((status.worker, penalize))
                ctrlfile.clear_active_slice(self.cfg.active_slice_path(index))
        if release and workers:
            with db.connect(self.dsn) as conn:
                for worker, penalize in workers:
                    freed = C.release_worker(
                        conn, worker, penalize=penalize)
                    if freed:
                        level = log.warning if penalize else log.info
                        level(
                            "released %d task(s) held by %s slot %s",
                            len(freed),
                            "failed" if penalize else "drained",
                            worker,
                        )
        return len(dead)

    def _ensure_slots(self) -> int:
        forked = 0
        for index in range(self.cfg.slots):
            if index in self._procs:
                continue
            proc = self._ctx.Process(
                target=slot_entry, args=(self.dsn, self.resolve, self.cfg, index),
                name=f"windex-slot-{index}", daemon=False)
            proc.start()
            assert proc.pid is not None
            self._procs[index] = proc
            self.slots[index] = SlotStatus(index=index, pid=proc.pid,
                                           worker=worker_id(self.cfg.name, index, proc.pid))
            forked += 1
        return forked


def run_pool(dsn: str, resolve: Resolve, cfg: PoolConfig | None = None, *,
             settings: Any = None, until: Callable[[], bool] | None = None) -> None:
    """Convenience entry point used by ``windex worker``."""
    Pool(dsn, resolve, cfg, settings=settings).run(until=until)
