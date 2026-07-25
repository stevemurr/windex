"""The worker pool: one supervisor, K slot subprocesses, leased and sliced tasks.

    supervisor.py   forks and reaps slots, sweeps leases, publishes control state
    slot.py         one slot's loop: claim → slice → repeat → exit to be recycled
    execute.py      running one slice of one leased task
    canonical_claim.py  claim/lease/release SQL — pauses, lanes, fair share
    memory.py       RSS accounting, the layer that prevents a whole-box reset
    preconditions.py  fleet-level facts a task can require before it is claimable
    control.py      the supervisor → slot channel
    protocol.py     the runner contract (TaskContext / SliceResult) — zero imports
    config.py       every knob, with the WINDEX_WORKER_* name it will take

WHAT THIS REPLACES. Fourteen resident loop processes, each an independent FIFO
over one source, none of which could yield, be paused mid-flight, or be told to
make room for something more urgent. The pool is the same claim/heartbeat/reclaim
protocol the crawl driver already proved (``crawl/run.py``), generalized one
level up so that *every* source shares it — and so that the answer to "why is
nothing happening" is a row in ``run_tasks`` rather than a log file inside
whichever container wrote it.

The Pipeline runtime maps ``run_tasks.module`` to executable code. The pool takes
a ``resolve: Callable[[str], Runner]`` so its lease protocol stays independently
testable. ``default_resolve`` below is the seam.
"""

from __future__ import annotations

from windex.worker.config import PoolConfig, config_from_env
from windex.worker.protocol import (
    LANES,
    LeaseLost,
    PermanentTaskError,
    Resolve,
    Runner,
    SliceResult,
    TaskContext,
)
from windex.worker.supervisor import Pool, run_pool

__all__ = [
    "LANES", "LeaseLost", "PermanentTaskError", "Pool", "PoolConfig", "Resolve",
    "Runner", "SliceResult", "TaskContext", "config_from_env", "default_resolve",
    "run_pool",
]


def default_resolve(module: str) -> Runner:
    """Resolve one exact built-in or approved local Module implementation."""
    try:
        from windex import pipeline as _pipeline          # noqa: PLC0415
    except Exception as exc:                          # noqa: BLE001
        raise PermanentTaskError(
            f"no runner registry for Module {module!r}: the Pipeline runtime "
            f"is not available ({exc})") from None

    resolver = getattr(_pipeline, "resolve", None)
    if callable(resolver):
        try:
            return resolver(module)
        except LookupError:
            # Worker processes do not share the API's in-memory registry cache.
            # Refresh approved descriptors from canonical storage on a miss.
            from windex import db
            from windex.config import get_settings
            from windex.pipeline import registry

            with db.connect(get_settings().pg_dsn) as conn:
                registry.load_custom(conn)
            return resolver(module)
    runners = getattr(_pipeline, "RUNNERS", None)
    if isinstance(runners, dict) and module in runners:
        return runners[module]
    raise PermanentTaskError(f"module {module!r} is not registered")
