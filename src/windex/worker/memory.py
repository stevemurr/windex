"""RSS accounting for the slot processes — layer 2 of the memory ceiling.

Context, because this is not generic hygiene: this box has hard-reset under
chronic memory pressure with no vmcore and no OOM kill to point at. The chain
this file belongs to converts that into a survivable event:

  1. the container cgroup limit (compose ``mem_limit``) means the kernel kills a
     process instead of the box wedging;
  2. **slot recycling — this file** — means we usually get there first, retiring
     a slot at ``rss_high_water`` before the cgroup has to act;
  3. backpressure means we stop *starting* memory-hungry work while the total is
     already high, so the pool trickles instead of dying.

Slot recycling is the only mechanism that actually returns memory, because
CPython does not reliably hand arena memory back to the OS: a pool that once
processed a 333 MB wiki shard keeps that high-water mark for the life of the
process. Killing the process is the free().

VmRSS from ``/proc/<pid>/status`` is used rather than psutil (no new dependency)
and rather than ``getrusage`` (which reports the *maximum* RSS, not the current
one, and so never falls after a peak — useless for deciding when to recycle).
On any platform without procfs every reading is None and the RSS layers disable
themselves cleanly; the dev machine is macOS and production is Linux, and a
guard that raised there would be a guard nobody could run tests through.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from pathlib import Path

log = logging.getLogger("windex.worker.memory")

_PROC = Path("/proc")


def rss_bytes(pid: int) -> int | None:
    """Current resident set size of ``pid``, or None if it cannot be read.

    None means "unknown", never "zero": a dead process, a container without
    procfs and a permissions error must not read as 0 bytes, because 0 would
    make every ceiling look comfortably satisfied.
    """
    try:
        with (_PROC / str(pid) / "status").open() as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    # "VmRSS:\t  123456 kB"
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def read_all(pids: Iterable[int]) -> dict[int, int]:
    """RSS for each pid that could be read. Unreadable pids are omitted rather
    than zero-filled, for the reason in ``rss_bytes``."""
    out: dict[int, int] = {}
    for pid in pids:
        val = rss_bytes(pid)
        if val is not None:
            out[pid] = val
    return out


def total(readings: Mapping[int, int]) -> int:
    """Sum of what we could measure.

    Deliberately an *under*-estimate when a reading is missing. The alternative —
    assuming a missing slot is at the limit — would make one unreadable pid
    freeze the whole pool, and a pool that stops claiming for an unexplainable
    reason is worse than one that briefly under-counts, because the cgroup limit
    still backstops the real ceiling.
    """
    return sum(readings.values())


def classify(readings: Mapping[int, int], *, high_water: int, hard: int
             ) -> tuple[list[int], list[int]]:
    """Split slot pids into (retire, kill).

    ``retire`` gets a polite yield request plus SIGTERM: it finishes its slice,
    commits, and the supervisor forks a fresh slot. ``kill`` gets SIGKILL,
    because past the hard ceiling the next allocation may be the one that takes
    the box down, and a task killed mid-slice loses nothing that was committed.
    """
    retire, kill = [], []
    for pid, rss in readings.items():
        if rss >= hard:
            kill.append(pid)
        elif rss >= high_water:
            retire.append(pid)
    return retire, kill


def backpressure_lanes(total_rss: int, cfg_limit: int, fraction: float,
                       lanes: Iterable[str]) -> list[str]:
    """Lanes to stop claiming into while memory is already high.

    Only the expensive lanes are blocked (``cpu_heavy`` by default) — io and net
    keep claiming, so the pool keeps making progress on cheap work instead of
    going quiet exactly when an operator is watching it most closely.
    """
    if cfg_limit <= 0 or fraction <= 0:
        return []
    return list(lanes) if total_rss >= cfg_limit * fraction else []
