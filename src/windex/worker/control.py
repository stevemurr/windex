"""The supervisor → slot channel: one small JSON file, written atomically.

Two facts have to reach the slots between claims: which preconditions currently
hold (evaluated centrally, see ``preconditions.py``) and which lanes are under
memory backpressure. A file is used rather than a pipe or shared memory for one
reason that outweighs elegance — **a recycled slot is a new process**, and a
file is the only channel that survives the fork/exit cycle without the
supervisor having to re-hand it to every child. It also makes the pool's current
decision inspectable with ``cat`` while you are staring at a stalled queue,
which a pipe does not.

Written with the write-temp-then-rename idiom so a slot never reads a half-file:
``rename`` within a directory is atomic on every filesystem windex runs on, and
a torn read here would look exactly like "no preconditions are satisfied", i.e.
a mysterious stall.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("windex.worker.control")


@dataclass(frozen=True)
class Control:
    satisfied: frozenset[str] = frozenset()
    blocked_lanes: tuple[str, ...] = ()
    generation: int = 0
    updated_at: float = 0.0
    stale: bool = False

    def lanes(self, wanted: tuple[str, ...]) -> list[str]:
        return [lane for lane in wanted if lane not in self.blocked_lanes]

    def satisfied_for_claim(self) -> frozenset[str]:
        """What the slot may tell the claim query it has verified.

        Stale ⇒ nothing. The claim's containment test then matches only tasks
        that declare no preconditions at all, which is the fail-closed reading:
        a precondition nobody has checked in the last minute has not been
        checked. The alternative (assume the last known-good set) is how a pool
        keeps happily claiming ``load`` tasks for ten minutes after the disk
        filled up.
        """
        return frozenset() if self.stale else self.satisfied


def write(path: Path, *, satisfied: frozenset[str] | set[str],
          blocked_lanes: tuple[str, ...] | list[str], generation: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "satisfied": sorted(satisfied),
        "blocked_lanes": list(blocked_lanes),
        "generation": generation,
        "updated_at": time.time(),
    }
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload))
    tmp.replace(path)


def read(path: Path, *, stale_seconds: float) -> Control:
    """Read the published control state, or a fail-closed default.

    Wall-clock rather than monotonic time: the two processes only share the
    former, and the comparison is against a 60-second staleness window where a
    clock step is both unlikely and harmless.
    """
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError):
        return Control(stale=True)
    updated = float(raw.get("updated_at", 0.0))
    stale = (time.time() - updated) > stale_seconds
    if stale:
        log.warning("worker control file is %.0fs old — claiming only tasks with "
                    "no preconditions", time.time() - updated)
    return Control(
        satisfied=frozenset(raw.get("satisfied", ())),
        blocked_lanes=tuple(raw.get("blocked_lanes", ())),
        generation=int(raw.get("generation", 0)),
        updated_at=updated,
        stale=stale,
    )


@dataclass
class SlotStatus:
    """What the supervisor knows about one live slot. In-memory only."""

    index: int
    pid: int
    worker: str
    started_at: float = field(default_factory=time.time)
    rss: int | None = None
    task_id: int | None = None
    yield_since: float | None = None   # when we first saw an unheeded yield request
    slice_generation: int | None = None


@dataclass(frozen=True)
class ActiveSlice:
    """Exact identity and start time of the call currently running in a slot."""

    pid: int
    worker: str
    task_id: int
    generation: int
    started_at: float


def write_active_slice(
    path: Path,
    *,
    pid: int,
    worker: str,
    task_id: int,
    generation: int,
    started_at: float,
) -> None:
    """Publish a slice boundary atomically before invoking module code.

    This record lives outside the child process so the supervisor can enforce a
    deadline even when module code is stuck in a C extension and neither the
    runner nor the heartbeat thread can acquire the GIL.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pid": pid,
        "worker": worker,
        "task_id": task_id,
        "generation": generation,
        "started_at": started_at,
    }
    tmp = path.with_suffix(f".{pid}.tmp")
    tmp.write_text(json.dumps(payload))
    tmp.replace(path)


def read_active_slice(path: Path) -> ActiveSlice | None:
    try:
        raw = json.loads(path.read_text())
        active = ActiveSlice(
            pid=int(raw["pid"]),
            worker=str(raw["worker"]),
            task_id=int(raw["task_id"]),
            generation=int(raw["generation"]),
            started_at=float(raw["started_at"]),
        )
    except (KeyError, OSError, TypeError, ValueError):
        return None
    if active.pid <= 0 or active.task_id <= 0 or active.generation <= 0:
        return None
    if not math.isfinite(active.started_at) or active.started_at <= 0 or not active.worker:
        return None
    return active


def clear_active_slice(path: Path) -> None:
    """Remove the safety record after the task lease has been released."""
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        # The pid/worker/generation checks make a stale record harmless. Task
        # release and slot recycling must not fail merely because cleanup did.
        log.warning("could not clear active-slice record %s: %s", path, exc)
