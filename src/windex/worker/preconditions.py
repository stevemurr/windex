"""What ``run_tasks.preconditions`` names, and how the pool decides it holds.

A precondition is a *fleet-level* fact that makes a whole class of task
pointless or dangerous right now: the staging filesystem is nearly full, the
embedding gateway is down, no GitHub token is configured. Modelling them as
claim-time predicates rather than as runtime errors is what turns three real
incidents into non-events:

* a gateway outage used to make every embed loop exit and nothing supervised
  them, so a 25-minute outage became a ~36 h indexing stall. Under the pool the
  gpu tasks simply are not claimable while ``gateway`` is unsatisfied, and they
  become claimable again by themselves.
* the staging volume has vanished twice. ``storage:staging`` also catches the
  strictly larger failure the mount check never could — the disk is *present*
  but nearly full — which since the move to local NVMe would take Postgres and
  Qdrant down with it, not merely stall ingest.
* a missing GH token turns a hydration task into a rate-limit crash loop that
  burns its retry budget and reports a red run for a configuration problem.

**Evaluated by the supervisor, never by a slot.** Some of these cost a network
round trip, and four slots polling a gateway every second is a self-inflicted
outage. The supervisor evaluates on a TTL and publishes the satisfied set; slots
read it. A slot that finds the published set stale refuses every task that
declares *any* precondition, which is fail-closed on purpose: an unverified
precondition is not a satisfied one.
"""

from __future__ import annotations

import logging
import shutil
import time
from collections.abc import Callable, Iterable
from pathlib import Path

log = logging.getLogger("windex.worker.preconditions")

# The vocabulary. Kept closed (plan §A.6: allowlist, not denylist) so a typo in a
# recipe is an unsatisfiable precondition that visibly parks the task, rather
# than a silently-ignored string that lets it run in exactly the situation the
# author was trying to exclude.
KNOWN: tuple[str, ...] = (
    "storage:staging",
    "storage:downloads",
    "gateway",
    "gh_token",
)

# `staging_mount` is what earlier drafts of the plan called it. Accepted as an
# alias so recipes written against that vocabulary keep working; it maps to the
# strictly stronger free-space check.
ALIASES = {"staging_mount": "storage:staging"}


def _storage_ok(path: Path, min_free: int) -> bool:
    """Free space above the reserve, with the directory actually present.

    A missing directory is a failed check rather than an exception: on a box
    where staging, pgdata and qdrant share one filesystem, "the path is gone" and
    "the path is full" have the same correct response — do not start work that
    writes there.
    """
    try:
        if not path.exists():
            return False
        if min_free <= 0:            # 0 disables the reserve, matching Settings
            return True
        return shutil.disk_usage(path).free >= min_free
    except OSError as exc:
        log.warning("storage check failed for %s: %s", path, exc)
        return False


def _gateway_ok(settings: object) -> bool:
    """Can we reach the embedding backend?

    Uses the Embedder interface's own ``ping`` (CLAUDE.md: everything flows
    through that interface) and treats any exception as down. Never raises: a
    precondition that can raise takes the supervisor loop down with it, and the
    supervisor is the thing that would otherwise notice.
    """
    try:
        from windex.embed import build_embedder

        embedder = build_embedder(settings)   # type: ignore[arg-type]
        try:
            return bool(embedder.ping())
        finally:
            embedder.close()
    except Exception as exc:                  # noqa: BLE001
        log.info("gateway precondition unsatisfied: %s", exc)
        return False


def evaluate(settings: object, *, names: Iterable[str] = KNOWN,
             extra: dict[str, Callable[[], bool]] | None = None) -> set[str]:
    """The set of preconditions that currently hold.

    ``settings`` is a ``windex.config.Settings``; typed loosely so this module
    stays importable (and testable) with a stub that carries only the three
    attributes it reads.
    """
    checks: dict[str, Callable[[], bool]] = {
        "storage:staging": lambda: _storage_ok(
            Path(settings.staging_dir),            # type: ignore[attr-defined]
            int(getattr(settings, "storage_min_free_bytes", 0))),
        "storage:downloads": lambda: _storage_ok(
            Path(settings.downloads_dir),          # type: ignore[attr-defined]
            int(getattr(settings, "storage_min_free_bytes", 0))),
        "gateway": lambda: _gateway_ok(settings),
        "gh_token": lambda: bool(getattr(settings, "github_token_list", [])),
    }
    checks.update(extra or {})
    out: set[str] = set()
    for name in names:
        check = checks.get(name)
        if check is None:
            continue
        try:
            if check():
                out.add(name)
        except Exception as exc:                   # noqa: BLE001 — see _gateway_ok
            log.warning("precondition %s raised: %s", name, exc)
    # Publish the aliases alongside their targets so a recipe using either
    # spelling matches; the claim compares raw strings in SQL.
    for alias, target in ALIASES.items():
        if target in out:
            out.add(alias)
    return out


class Cache:
    """TTL cache around ``evaluate``.

    The gateway check is a network round trip and the storage checks are
    ``statvfs``; the supervisor tick is 5 s and neither needs to be that fresh.
    A slower TTL also damps flapping: a gateway that is up-down-up across three
    ticks should not requeue the fleet three times.
    """

    def __init__(self, settings: object, ttl: float = 30.0,
                 evaluator: Callable[[], set[str]] | None = None) -> None:
        self.settings = settings
        self.ttl = ttl
        self._evaluator = evaluator or (lambda: evaluate(settings))
        self._value: set[str] = set()
        self._at = 0.0

    def get(self, *, force: bool = False) -> set[str]:
        now = time.monotonic()
        if force or not self._at or now - self._at >= self.ttl:
            self._value = self._evaluator()
            self._at = now
        return set(self._value)
