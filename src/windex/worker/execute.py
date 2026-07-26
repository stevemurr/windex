"""Running one slice of one leased task.

The loop the plan describes —

    units = claim_units(task, batch)
    if none                 -> succeed, release lease
    process(units); commit
    heartbeat(lease, units_done, stats)
    if cancel_requested     -> cancelled
    if paused(scope)        -> yield
    if yield_requested      -> yield
    if elapsed > slice_seconds or units > slice_units -> yield

— is split across two owners. The *runner* (a Pipeline Module) owns everything on
the left of the arrow: claiming units, processing them, committing. The *pool*
owns everything on the right: the lease, the flags, the clock, and turning the
runner's return value into a state transition. That split is what keeps the two
phases independently testable, and it means a module author cannot accidentally
get the lease protocol wrong because they never touch it.

TWO CONNECTIONS, NOT ONE. The control plane (heartbeat, lease renewal, flag
polling, the final release) runs on a connection the runner never sees. With one
shared connection a heartbeat issued mid-slice would join the runner's open
transaction — and its commit would durably commit whatever half-finished work
the runner had staged, which is precisely the ordering violation
``embed/pipeline.py``'s docstring spends a paragraph forbidding. It would also
mean a runner's rollback silently discards a lease renewal. Two connections make
both impossible rather than merely unlikely.

A BACKGROUND HEARTBEAT, NOT A POLLED ONE. The lease must be renewed on wall
time, but a runner is a single call that may legitimately spend minutes inside
one C extension (datatrove, lxml, a 36 s Qdrant upsert). A daemon thread renews
independently, which has a second and larger benefit: ``should_yield()`` becomes
an in-memory flag read instead of a query, so a runner can call it in its inner
loop — and a yield check nobody can afford to call is a yield check that does
not exist.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import psycopg

from windex.worker import canonical_claim as C
from windex.worker.config import PoolConfig
from windex.worker.protocol import (
    LeaseLost,
    PermanentTaskError,
    Runner,
    SliceResult,
    TaskContext,
)

log = logging.getLogger("windex.worker.execute")


@dataclass
class SliceOutcome:
    state: str                    # ready | succeeded | failed | cancelled
    outcome: str                  # yielded | succeeded | failed | cancelled
    reason: str = ""
    units_done: int = 0
    units_failed: int = 0
    elapsed: float = 0.0
    error: str | None = None
    stats: dict = field(default_factory=dict)


class SliceControl:
    """Lease renewal, flag caching and the slice deadline for one leased task.

    Owns the control connection for the duration of the slice. Every access to
    that connection goes through ``_lock`` because the heartbeat thread and the
    main thread both use it — psycopg connections are not thread-safe for
    concurrent use, and the failure is a corrupted protocol stream rather than a
    clean error.
    """

    def __init__(self, ctl: psycopg.Connection, task: C.ClaimedTask,
                 cfg: PoolConfig, *, drain: threading.Event | None = None) -> None:
        self.ctl = ctl
        self.task = task
        self.cfg = cfg
        self.started = time.monotonic()
        self.deadline = self.started + cfg.slice_seconds
        self._lock = threading.Lock()
        self._stop = threading.Event()
        # Externally-set "this process is going away": SIGTERM from the
        # supervisor (slot recycling, deploy, memory high-water). Faster than the
        # DB round trip and works even when Postgres is unreachable.
        self.drain = drain or threading.Event()
        self.signals = C.Signals()
        self.lease_lost = False
        self.units_done = 0
        self.units_failed = 0
        self.stats: dict[str, Any] = {}
        self._thread: threading.Thread | None = None

    # --- lifecycle ---------------------------------------------------------

    def __enter__(self) -> SliceControl:
        self._thread = threading.Thread(
            target=self._beat_loop, name=f"hb-{self.task.id}", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> bool:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.cfg.heartbeat_seconds + 5)
        return False

    def _beat_loop(self) -> None:
        while not self._stop.wait(self.cfg.heartbeat_seconds):
            try:
                self._beat()
                self._warn_if_overdue()
            except LeaseLost:
                # Somebody else owns the task now. Stop beating and let
                # should_yield() bring the runner home; writing anything more
                # about this task would corrupt the new holder's counters.
                self.lease_lost = True
                return
            except Exception as exc:      # noqa: BLE001 — a DB blip must not kill the slice
                log.warning("heartbeat failed for task %s: %s", self.task.id, exc)

    def _beat(self) -> None:
        with self._lock:
            self.signals = C.heartbeat(
                self.ctl, self.task.id, self.task.worker,
                units_done=self.task.units_done + self.units_done,
                units_failed=self.task.units_failed + self.units_failed,
                stats=self.stats,
            )

    def _warn_if_overdue(self) -> None:
        """Complain loudly about a runner that is ignoring ``should_yield()``.

        The lease keeps being renewed anyway, and that is the deliberate choice:
        a slow runner is *alive*, and letting its lease lapse would hand the same
        task to a second worker while the first is still writing — the one thing
        leases exist to prevent. So overdue is treated as a visible module bug
        (it monopolizes a lane) rather than silently repaired. The forcible
        remedy is the supervisor's, which kills the slot first and only then lets
        the lease expire, in that order.
        """
        over = time.monotonic() - self.deadline
        if over > self.cfg.slice_seconds:
            log.warning(
                "task %s (%s) is %.0fs past its %.0fs slice and still holding lane %s — "
                "the module is not calling should_yield()",
                self.task.id, self.task.module, over, self.cfg.slice_seconds,
                self.task.lane)

    # --- the runner-facing API --------------------------------------------

    def should_yield(self) -> bool:
        """True when the slice must end. Cheap: no I/O, no lock."""
        if self.drain.is_set() or self.lease_lost:
            return True
        if self.signals.should_stop:
            return True
        if time.monotonic() >= self.deadline:
            return True
        if self.cfg.slice_units and self.units_done >= self.cfg.slice_units:
            return True
        return False

    def heartbeat(self, units_done: int, units_failed: int = 0,
                  stats: Any = None) -> None:
        """Runner-called progress report: counters in, nothing blocking out.

        Deliberately does no I/O. The lease is the background thread's job, and
        making this synchronous would put a database round trip inside whatever
        inner loop the module chose to call it from.
        """
        self.units_done = units_done
        self.units_failed = units_failed
        if stats:
            self.stats.update(dict(stats))

    def why(self) -> str:
        """The reason the slice is ending — recorded on the yield event so the
        UI can distinguish "still working" from "paused" from "preempted"."""
        if self.lease_lost:
            return "lease_lost"
        if self.signals.cancelled:
            return "cancelled"
        if self.signals.paused:
            return f"paused:{self.signals.paused}"
        if self.signals.yield_requested:
            return "preempted"
        if self.drain.is_set():
            return "slot_draining"
        if time.monotonic() >= self.deadline:
            return "slice_deadline"
        if self.cfg.slice_units and self.units_done >= self.cfg.slice_units:
            return "slice_units"
        return ""


def run_slice(ctl: psycopg.Connection, work: psycopg.Connection,
              task: C.ClaimedTask, runner: Runner, cfg: PoolConfig, *,
              drain: threading.Event | None = None) -> SliceOutcome:
    """Execute one slice and record its end. Never raises for task-level failure.

    Task failure is *data*, not an exception: a run whose task died must end up
    red in the database, and an exception escaping here would instead leave the
    task leased until it expired — turning a two-second failure into a
    five-minute stall, repeated three times.
    """
    result: SliceResult | None = None
    error: str | None = None
    permanent = False

    with SliceControl(ctl, task, cfg, drain=drain) as ctrl:
        ctx = TaskContext(
            run_id=task.run_id, task_id=task.id,
            pipeline_name=task.pipeline_name,
            pipeline_version=task.pipeline_version,
            pipeline_hash=task.pipeline_hash,
            source_id=task.source_id,
            source_name=task.source_name,
            state_namespace=task.state_namespace,
            search_name=task.search_name,
            id_prefix=task.id_prefix,
            collection_key=task.collection_key,
            search_profile=task.search_profile,
            node=task.node, kind=task.kind,
            module=task.module, module_version=task.module_version,
            module_digest=task.module_digest, config=task.config,
            spec=task.spec, cursor=task.cursor,
            conn=work, should_yield=ctrl.should_yield, heartbeat=ctrl.heartbeat,
            effective_config=task.effective_config, inputs=task.inputs,
            mode=task.mode, attempt=task.attempts, worker=task.worker,
        )
        try:
            result = runner(ctx)
            if not isinstance(result, SliceResult):   # a module returning None
                raise PermanentTaskError(
                    f"{task.module} returned {type(result).__name__}, not SliceResult")
        except PermanentTaskError as exc:
            error, permanent = f"{type(exc).__name__}: {exc}", True
        except Exception as exc:                      # noqa: BLE001 — see docstring
            error = f"{type(exc).__name__}: {exc}"
            log.exception("task %s (%s) raised", task.id, task.module)
        finally:
            # The runner may have left a transaction open — on the failure path
            # almost certainly has. Rolling back here keeps the connection
            # reusable for the next slice; leaving it in a failed transaction
            # would break every subsequent task this slot claims.
            try:
                work.rollback()
            except Exception:                         # noqa: BLE001
                log.warning("work connection unusable after task %s", task.id)

        reason = ctrl.why()
        done = ctrl.units_done if result is None else result.units_done
        failed = ctrl.units_failed if result is None else result.units_failed
        stats = dict(ctrl.stats)
        if result is not None:
            stats.update(result.stats)
        elapsed = time.monotonic() - ctrl.started
        lease_lost = ctrl.lease_lost
        cancelled = ctrl.signals.cancelled

    if lease_lost:
        # Nothing to write: the row is somebody else's now. The slot logs and
        # moves on — this is the designed outcome of a reclaim race, not an error.
        return SliceOutcome(state="", outcome="lease_lost", reason=reason,
                            units_done=done, units_failed=failed, elapsed=elapsed)

    if error is not None:
        outcome = "failed"
    elif cancelled:
        outcome = "cancelled"
    elif result is not None and result.exhausted:
        outcome = "succeeded"
    else:
        outcome = "yielded"

    rel = C.Release(
        outcome=outcome,
        units_done=task.units_done + done,
        units_failed=task.units_failed + failed,
        elapsed=elapsed,
        cursor=(result.cursor if result is not None and result.cursor else None),
        stats=stats,
        reason=reason or outcome,
        error=error,
        units_total=(result.units_total if result is not None else -1),
        permanent=permanent,
    )
    try:
        state = C.release(ctl, task, rel)
    except LeaseLost:
        return SliceOutcome(state="", outcome="lease_lost", reason="reclaimed",
                            units_done=done, units_failed=failed, elapsed=elapsed)
    return SliceOutcome(state=state, outcome=outcome, reason=reason or outcome,
                        units_done=done, units_failed=failed, elapsed=elapsed,
                        error=error, stats=stats)
