"""The worker pool: one supervisor, K slot subprocesses, leased and sliced tasks.

    supervisor.py   forks and reaps slots, sweeps leases, publishes control state
    slot.py         one slot's loop: claim → slice → repeat → exit to be recycled
    execute.py      running one slice of one leased task
    claim.py        the claim/lease/release SQL — pauses, lanes, fair share
    dag.py          which tasks become claimable, and when a run is over
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

The module registry that maps ``run_tasks.module`` to executable code belongs to
the recipe engine (Phase 6). The pool never imports it: it takes a
``resolve: Callable[[str], Runner]``, which is what lets the two halves be built,
tested and merged independently. ``default_resolve`` below is the seam.
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
    """Look a module up in the recipe registry, if this build has one.

    Imported lazily and by name so the pool has no build-time dependency on the
    recipe engine: a checkout with no ``windex.recipe`` still starts a pool,
    still claims tasks, and fails each one with a message that names the actual
    problem. The alternative — an ImportError at startup — would make the pool
    untestable until the far end shipped, and would report "windex-worker is
    down" for a missing registry.

    Two shapes are accepted because Phase 6 has not fixed one yet: a
    ``resolve(module) -> Runner`` callable, or a ``RUNNERS`` mapping. Whichever
    it turns out to be, nothing here changes.
    """
    try:
        from windex import recipe as _recipe          # noqa: PLC0415
        registry = _recipe.registry                   # type: ignore[attr-defined]
    except Exception as exc:                          # noqa: BLE001
        raise PermanentTaskError(
            f"no runner registry for module {module!r}: the recipe engine "
            f"(windex.recipe) is not available ({exc})") from None

    resolver = getattr(registry, "resolve", None)
    if callable(resolver):
        return resolver(module)
    runners = getattr(registry, "RUNNERS", None)
    if isinstance(runners, dict) and module in runners:
        return runners[module]
    raise PermanentTaskError(f"module {module!r} is not registered")
