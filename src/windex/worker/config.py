"""Pool knobs, in one dataclass.

These are deliberately NOT on ``windex.config.Settings`` yet — the worker pool is
being built alongside the Pipeline compiler and adding fields to the global Settings
mid-flight is how two branches collide in a file neither owns. Every knob here is
a plain default that the CLI can override, and each one carries the name it
should take when it is promoted (``WINDEX_WORKER_*``).

The defaults come from plan §C.1-C.3 and are chosen for the GB10 box: 40 GB
container cap, 20 cores, one GPU, and a history of whole-box resets under memory
pressure rather than graceful OOM kills.
"""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from windex.worker.protocol import LANES

GIB = 1024 ** 3

# Fleet-wide in-flight cap per lane. General cpu_heavy work stays serialized.
# WARC extraction has a separate measured budget: one process consumed about
# 1.6 GiB on the 20-core/40-GiB worker, so two are a conservative next step and
# do not admit a concurrent Wiki/custom-module peak. gpu = 2 sits above the
# flock budget in embed/budget.py rather than replacing it — 2 tasks x
# embed_concurrency 3 = 6 in flight *by construction*, so the flock becomes a
# correctness backstop that rarely blocks instead of the primary throttle.
DEFAULT_LANE_CAPS: dict[str, int] = {
    "gpu": 2,
    "net": 5,        # bounded download/pagination concurrency
    "warc": 2,
    "cpu_heavy": 1,
    "io": 4,
    "maint": 1,      # maintenance is never worth contending with real ingest
}


@dataclass(frozen=True)
class PoolConfig:
    """One immutable snapshot of the pool's configuration.

    Frozen because it is read by forked children: a mutable config would be
    silently per-process after the first fork, which is exactly the class of bug
    that makes "I changed the setting and nothing happened" reports.
    """

    # --- shape -------------------------------------------------------------
    # WINDEX_WORKER_SLOTS. Subprocesses, not threads (plan §C.1): CPython does
    # not return arena memory to the OS, so recycling a *process* is the only
    # mechanism that actually reclaims a 333 MB wiki shard's high-water mark.
    slots: int = 6
    name: str = "pool"                 # identifies this pool in lease_worker ids
    lanes: tuple[str, ...] = LANES     # lanes this pool is willing to serve
    lane_caps: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_LANE_CAPS))
    default_lane_cap: int = 2          # a lane nobody configured

    # --- slicing -----------------------------------------------------------
    # WINDEX_WORKER_SLICE_SECONDS. 120 s means a 20,000-page crawl (~11 h) yields
    # ~330 times, and between every pair every other source gets a fair shot.
    # Today that same crawl holds windex-loop-crawl exclusively for 11 hours.
    slice_seconds: float = 120.0
    slice_units: int = 0               # extra unit-count cap per slice; 0 = off
    # How often the background thread renews the lease and refreshes the
    # cancel/pause/yield flags. Must be comfortably under the shortest
    # lease_seconds any Pipeline sets (300 by default) or a slow slice reaps itself.
    heartbeat_seconds: float = 10.0
    claim_idle_seconds: float = 1.0    # slot poll interval when nothing is claimable

    # --- memory ceiling (plan §C.3) ----------------------------------------
    # The container cgroup limit is layer 1 and lives in compose.yaml; these are
    # layers 2 and 3, which are what turn "the box hard-resets" into "one slot is
    # recycled and its task resumes from the last committed unit".
    mem_limit_bytes: int = 40 * GIB    # must match compose's mem_limit
    rss_high_water_bytes: int = 6 * GIB    # request yield, retire the slot
    rss_hard_bytes: int = 10 * GIB         # SIGKILL; the lease reclaims the task
    # Stop claiming cpu_heavy above this fraction of the limit; keep claiming
    # io/net so the pool trickles instead of dying.
    backpressure_fraction: float = 0.70
    backpressure_lanes: tuple[str, ...] = ("warc", "cpu_heavy")
    max_tasks_per_slot: int = 20       # unconditional recycle (counts slices)

    # --- supervisor --------------------------------------------------------
    tick_seconds: float = 5.0          # reaper / RSS / control-file cadence
    precondition_ttl_seconds: float = 30.0
    # WINDEX_WORKER_HUNG_GRACE_SECONDS. A runner gets this much time beyond its
    # slice deadline, or after a cancel/pause/yield request, before the supervisor
    # kills its slot. This is deliberately finite by default: a module that never
    # calls should_yield() must not renew its lease and hold a lane forever.
    # Zero means no additional grace, not "disabled".
    hung_grace_seconds: float = 60.0
    # A slot refuses tasks with preconditions when the control file is older than
    # this. Fail-closed: a precondition nobody has checked recently is not
    # satisfied, and "claimed a load task onto a full disk" is worse than idling.
    control_stale_seconds: float = 60.0
    state_dir: Path = field(default_factory=lambda: Path.home() / ".windex" / "worker")
    # Grace between SIGTERM (finish the slice, then exit) and SIGKILL when the
    # pool is shutting down or retiring a slot. Longer than one slice would stall
    # a deploy; shorter than a commit risks killing mid-transaction, which is
    # safe but wastes the slice.
    stop_grace_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not math.isfinite(self.slice_seconds) or self.slice_seconds <= 0:
            raise ValueError("slice_seconds must be greater than zero")
        if not math.isfinite(self.heartbeat_seconds) or self.heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be greater than zero")
        if not math.isfinite(self.hung_grace_seconds) or self.hung_grace_seconds < 0:
            raise ValueError("hung_grace_seconds must be zero or greater")
        if not math.isfinite(self.stop_grace_seconds) or self.stop_grace_seconds < 0:
            raise ValueError("stop_grace_seconds must be zero or greater")

    @property
    def control_path(self) -> Path:
        return self.state_dir / "pool.json"

    def active_slice_path(self, index: int) -> Path:
        """Per-slot safety record consumed by the supervisor.

        Pool names are operator input, so make them path-safe rather than letting
        a name containing a slash escape ``state_dir``.
        """
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", self.name).strip("._")
        return self.state_dir / "slots" / f"{safe_name or 'pool'}-{index}.json"

    def with_overrides(self, **kw: object) -> PoolConfig:
        """A copy with non-None overrides applied — the CLI's "0 means default"
        idiom, kept out of the command body."""
        from dataclasses import replace

        return replace(self, **{k: v for k, v in kw.items() if v is not None})


def config_from_env(base: PoolConfig | None = None) -> PoolConfig:
    """Read the WINDEX_WORKER_* environment.

    A stopgap until these land on Settings: the container is configured through
    the environment, and a pool that could only be configured through CLI flags
    would need a compose edit for every knob. Names match what the Settings
    fields will be called, so promoting them later is a rename of nothing.
    """
    cfg = base or PoolConfig()
    over: dict[str, object] = {}

    def _num(env: str, cast):
        raw = os.environ.get(env)
        if raw:
            try:
                return cast(raw)
            except ValueError:
                return None
        return None

    for env, attr, cast in (
        ("WINDEX_WORKER_SLOTS", "slots", int),
        ("WINDEX_WORKER_SLICE_SECONDS", "slice_seconds", float),
        ("WINDEX_WORKER_SLICE_UNITS", "slice_units", int),
        ("WINDEX_WORKER_HEARTBEAT_SECONDS", "heartbeat_seconds", float),
        ("WINDEX_WORKER_MEM_LIMIT_BYTES", "mem_limit_bytes", int),
        ("WINDEX_WORKER_RSS_HIGH_WATER_BYTES", "rss_high_water_bytes", int),
        ("WINDEX_WORKER_RSS_HARD_BYTES", "rss_hard_bytes", int),
        ("WINDEX_WORKER_MAX_TASKS_PER_SLOT", "max_tasks_per_slot", int),
        ("WINDEX_WORKER_TICK_SECONDS", "tick_seconds", float),
        ("WINDEX_WORKER_HUNG_GRACE_SECONDS", "hung_grace_seconds", float),
        ("WINDEX_WORKER_STOP_GRACE_SECONDS", "stop_grace_seconds", float),
    ):
        val = _num(env, cast)
        if val is not None:
            over[attr] = val
    lanes = os.environ.get("WINDEX_WORKER_LANES", "").strip()
    if lanes:
        over["lanes"] = tuple(x.strip() for x in lanes.split(",") if x.strip())
    state = os.environ.get("WINDEX_WORKER_STATE_DIR", "").strip()
    if state:
        over["state_dir"] = Path(state)
    return cfg.with_overrides(**over)
