"""The contract between the worker pool and the code that does the actual work.

This module is deliberately the *only* thing a task module has to know about the
pool, and it imports nothing from windex. That is not tidiness — it is what lets
the pool and Pipeline compiler be built and tested
independently: the pool takes a ``resolve: Callable[[str], Runner]`` mapping
``run_tasks.module`` to something executable, tests pass fakes, and the real
registry drops in later with no edit here.

THE SLICE CONTRACT, which is the part that is easy to get wrong:

* A runner is called with a task that is *already leased*. It does not claim,
  lease, heartbeat-for-liveness or finish the task — the pool does all of that.
* A runner does **not** run the task to completion. It runs until either the work
  is exhausted (``SliceResult.exhausted``) or ``ctx.should_yield()`` returns True,
  whichever comes first, and then returns. Returning early is not a failure and
  costs nothing: the pool re-queues the task and a later slice resumes from
  committed state. Yielding is exactly the crash-resume path minus the crash.
* A runner MUST commit ``ctx.conn`` before it returns and before every
  ``ctx.heartbeat()``. Everything the pool writes about the task (lease, counters,
  cursor, terminal state) goes on a *different* connection, precisely so a
  heartbeat cannot accidentally commit a runner's half-finished transaction and a
  runner's rollback cannot lose the lease. The corollary is that the pool cannot
  rescue uncommitted work: whatever is not committed when the runner returns did
  not happen.
* Crash-safety is therefore the runner's unit table, not the pool's bookkeeping.
  A crash between the runner's last commit and the pool's slice-end write means
  the next slice sees a slightly stale ``cursor``/counters and re-does the last
  batch — which every windex ingest path already treats as a text_hash no-op.
  A runner that cannot tolerate that must write ``run_tasks.cursor`` itself,
  inside its own transaction, where it is atomic with the units it describes.
* ``should_yield()`` is cheap by construction (an in-memory flag maintained by a
  background heartbeat thread) so it is safe to call in a tight loop. Call it
  often; a runner that ignores it holds a lane until its lease expires and is the
  one failure mode the whole slicing design exists to prevent.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

# The lane vocabulary (run_tasks.lane). A lane is a *contended resource class*,
# not a priority: the pool caps how many tasks may run per lane fleet-wide.
# DataTrove has its own measured two-task budget; other memory-heavy work remains
# serialized so a Wiki shard and a custom module cannot peak together.
LANES: tuple[str, ...] = ("gpu", "net", "warc", "cpu_heavy", "io", "maint")


@dataclass(frozen=True)
class TaskContext:
    """Everything a runner is given. Frozen: a runner mutates the world through
    ``conn``, never through the context."""

    run_id: int
    task_id: int
    pipeline_name: str
    pipeline_version: int
    pipeline_hash: str
    source_id: int | None
    source_name: str
    state_namespace: str
    search_name: str
    id_prefix: str
    collection_key: str
    search_profile: str
    node: str
    kind: str
    module: str
    module_version: str
    module_digest: str
    config: dict          # frozen node config from run_tasks.config
    spec: dict            # frozen normalized Pipeline spec
    cursor: dict          # resume point INSIDE a unit, as of this slice's start
    conn: Any             # psycopg connection, owned by the runner (see docstring)
    should_yield: Callable[[], bool]
    heartbeat: Callable[[int, int, Mapping[str, Any]], None]  # done, failed, stats
    # Extras the pool knows and a runner occasionally needs, kept out of the
    # positional contract so adding one is not a breaking change.
    effective_config: dict = field(default_factory=dict)
    inputs: dict = field(default_factory=dict)
    mode: str = "run"                              # run | dry_run
    attempt: int = 0                               # run_tasks.attempts at claim
    worker: str = ""                               # lease_worker id, for logs
    source_generation: int = 0                     # frozen Source generation


@dataclass(frozen=True)
class SliceResult:
    """What one slice accomplished.

    ``exhausted`` is the only signal that ends the task: it means "I looked for
    more work and there is none", which the pool turns into ``succeeded``.
    Anything else — deadline, pause, preemption, memory pressure — is a yield,
    and the task goes back on the queue with its cursor.
    """

    units_done: int = 0
    units_failed: int = 0
    exhausted: bool = False
    cursor: dict = field(default_factory=dict)
    stats: dict = field(default_factory=dict)
    # Optional: set once discovery knows the denominator, so the progress bar can
    # be `counted` rather than `indeterminate`. -1 leaves run_tasks.units_total
    # alone (the "still unknown" default).
    units_total: int = -1


Runner = Callable[[TaskContext], SliceResult]
Resolve = Callable[[str], Runner]


class PermanentTaskError(Exception):
    """Raised by a runner (or by resolution) for a failure that a retry cannot fix.

    Retrying is the default because most task failures are transient — a 502, a
    dropped connection, an OOM-killed slot. But a task whose module does not
    exist, whose config is invalid, or whose destination was deleted will fail
    identically three times and the only thing the retries buy is three times the
    log noise and a delayed red state in the UI. This exception short-circuits to
    ``failed`` with ``attempts = max_attempts``, so the reason is visible
    immediately and the run stops pretending it might still succeed.
    """


class LeaseLost(Exception):
    """The task is no longer ours: the lease expired and was reclaimed, or an
    operator/cancel path moved the row.

    This is raised by the pool's own control-plane writes, never by a runner. It
    is not an error state for the task — some other slot may already be running
    it — so the slot must abandon *silently* and write nothing further about it.
    Writing anything after this point is how two workers corrupt one task's
    counters.
    """
