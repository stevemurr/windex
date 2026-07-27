"""Bounded, database-aware cleanup for Pipeline-owned transient storage.

Only paths whose ownership can be proved from the canonical Run layout are
considered. A file must belong to an old terminal Run, be older than the
file-age grace, and have no live database reference before it can be removed.
The declared ``run_artifacts`` store has its own expiry contract and remains
owned by ``source.scheduler.prune_expired_artifacts``.
"""

from __future__ import annotations

import logging
import os
import stat
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg

from windex.config import Settings
from windex.pipeline.events import append

log = logging.getLogger("windex.pipeline.maintenance")

_TERMINAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
_WIRE_ARTIFACT = "_WireArtifact"
_RAW_BLOB = "RawBlob"


@dataclass(frozen=True)
class _Candidate:
    path: Path
    root: Path
    kind: str
    run_id: int
    task_id: int | None
    size: int
    mtime: float
    device: int
    inode: int


@dataclass
class _Snapshot:
    active_runs: set[int] = field(default_factory=set)
    eligible_terminal_runs: set[int] = field(default_factory=set)
    disposable_download_tasks: set[tuple[int, int]] = field(default_factory=set)
    references: set[Path] = field(default_factory=set)


@dataclass
class StorageGCResult:
    """One maintenance pass, suitable for logs, events, and Prometheus."""

    scanned_files: int = 0
    deleted_files: int = 0
    deleted_bytes: int = 0
    deleted: dict[str, dict[str, int]] = field(default_factory=dict)
    preserved_active: int = 0
    preserved_referenced: int = 0
    preserved_retained: int = 0
    preserved_too_new: int = 0
    preserved_policy: int = 0
    preserved_unsafe: int = 0
    missing: int = 0
    file_cap_reached: bool = False
    byte_cap_reached: bool = False
    error_count: int = 0
    errors: list[str] = field(default_factory=list)

    def record_error(self, message: str) -> None:
        self.error_count += 1
        if len(self.errors) < 20:
            self.errors.append(message)

    def record_deleted(self, kind: str, size: int) -> None:
        self.deleted_files += 1
        self.deleted_bytes += size
        bucket = self.deleted.setdefault(kind, {"files": 0, "bytes": 0})
        bucket["files"] += 1
        bucket["bytes"] += size

    def as_event_data(self) -> dict[str, Any]:
        return {
            "scanned_files": self.scanned_files,
            "deleted_files": self.deleted_files,
            "deleted_bytes": self.deleted_bytes,
            "deleted": self.deleted,
            "preserved": {
                "active": self.preserved_active,
                "referenced": self.preserved_referenced,
                "retained": self.preserved_retained,
                "too_new": self.preserved_too_new,
                "policy": self.preserved_policy,
                "unsafe": self.preserved_unsafe,
            },
            "missing": self.missing,
            "file_cap_reached": self.file_cap_reached,
            "byte_cap_reached": self.byte_cap_reached,
            "error_count": self.error_count,
            "errors": self.errors,
        }


def _safe_reference(path: Path, root: Path) -> Path | None:
    """Normalize a DB-provided reference without accepting an escaped path."""
    try:
        resolved = path.resolve(strict=False)
    except OSError:
        return None
    if resolved == root or not resolved.is_relative_to(root):
        return None
    return resolved


def _payload_references(
    payload: Any,
    *,
    staging_root: Path,
    downloads_root: Path,
    artifacts_root: Path,
) -> Iterable[Path]:
    """Extract only file references from typed durable wire shapes."""
    if isinstance(payload, list):
        for item in payload:
            yield from _payload_references(
                item,
                staging_root=staging_root,
                downloads_root=downloads_root,
                artifacts_root=artifacts_root,
            )
        return
    if not isinstance(payload, dict):
        return

    kind = payload.get("type")
    if kind == _WIRE_ARTIFACT:
        raw = payload.get("path")
        if isinstance(raw, str):
            reference = _safe_reference(
                staging_root / "_pipeline_runs" / raw, staging_root)
            if reference is not None:
                yield reference
    elif kind == _RAW_BLOB:
        raw = payload.get("path")
        if isinstance(raw, str) and raw:
            path = Path(raw)
            for root in (staging_root, downloads_root, artifacts_root):
                reference = _safe_reference(path, root)
                if reference is not None:
                    yield reference
                    break

    # Captured output can wrap an artifact id, but its path comes from the
    # run_artifacts manifest below. Recurse to find typed values nested in a
    # captured WireBatch without treating arbitrary user field names as paths.
    for value in payload.values():
        if isinstance(value, (dict, list)):
            yield from _payload_references(
                value,
                staging_root=staging_root,
                downloads_root=downloads_root,
                artifacts_root=artifacts_root,
            )


def _db_snapshot(
    conn: psycopg.Connection,
    settings: Settings,
    *,
    terminal_cutoff: datetime,
) -> _Snapshot:
    staging_root = settings.staging_dir.resolve()
    downloads_root = settings.downloads_dir.resolve()
    artifacts_root = settings.artifacts_dir.resolve()
    snapshot = _Snapshot()

    with conn.cursor() as cur:
        cur.execute("SELECT id, state, finished_at FROM runs")
        for run_id, state, finished_at in cur.fetchall():
            if state not in _TERMINAL_STATES:
                snapshot.active_runs.add(int(run_id))
            elif finished_at is not None and finished_at <= terminal_cutoff:
                snapshot.eligible_terminal_runs.add(int(run_id))

        # Only http.download tasks explicitly marked disposable are eligible.
        # ``keep: true``, unknown task ids, and other module output are retained.
        cur.execute(
            """
            SELECT id, run_id
              FROM run_tasks
             WHERE module = 'http.download'
               AND NOT coalesce((config->>'keep')::boolean, false)
            """
        )
        snapshot.disposable_download_tasks.update(
            (int(run_id), int(task_id)) for task_id, run_id in cur.fetchall()
        )

        cur.execute(
            "SELECT DISTINCT text_ref FROM documents WHERE text_ref IS NOT NULL"
        )
        for (relative,) in cur.fetchall():
            if not isinstance(relative, str):
                continue
            reference = _safe_reference(staging_root / relative, staging_root)
            if reference is not None:
                snapshot.references.add(reference)

        # Task wire artifacts, RawBlob downloads, and coverage are live only
        # while their Run can still execute. Terminal rows deliberately cease
        # pinning transient files after the configured retention.
        cur.execute(
            """
            SELECT u.outputs, u.counts
              FROM task_units u
              JOIN runs r ON r.id = u.run_id
             WHERE r.state NOT IN ('succeeded', 'failed', 'cancelled')
            """
        )
        for outputs, counts in cur.fetchall():
            snapshot.references.update(_payload_references(
                outputs,
                staging_root=staging_root,
                downloads_root=downloads_root,
                artifacts_root=artifacts_root,
            ))
            if isinstance(counts, dict):
                coverage = counts.get("coverage_path")
                if isinstance(coverage, str):
                    reference = _safe_reference(
                        staging_root / coverage, staging_root)
                    if reference is not None:
                        snapshot.references.add(reference)

        # A generic terminal capture may itself contain a typed RawBlob. Active
        # Runs retain such captures; terminal disposable downloads remain owned
        # by the http.download ``keep`` policy above.
        cur.execute(
            """
            SELECT o.value
              FROM run_outputs o
              JOIN runs r ON r.id = o.run_id
             WHERE r.state NOT IN ('succeeded', 'failed', 'cancelled')
            """
        )
        for (value,) in cur.fetchall():
            snapshot.references.update(_payload_references(
                value,
                staging_root=staging_root,
                downloads_root=downloads_root,
                artifacts_root=artifacts_root,
            ))

        # This collector does not delete the artifacts tier. Recording its live
        # manifests here makes the reference snapshot complete and prevents a
        # future shared scanner from silently violating that separate contract.
        cur.execute("SELECT relative_path FROM run_artifacts")
        for (relative,) in cur.fetchall():
            if not isinstance(relative, str):
                continue
            reference = _safe_reference(artifacts_root / relative, artifacts_root)
            if reference is not None:
                snapshot.references.add(reference)
    conn.commit()
    return snapshot


def _integer(part: str) -> int | None:
    try:
        value = int(part)
    except ValueError:
        return None
    return value if value > 0 else None


def _wire_owner(parts: tuple[str, ...]) -> tuple[str, int, int | None] | None:
    if len(parts) >= 3 and parts[0] == "coverage":
        run_id = _integer(parts[1])
        task_id = _integer(parts[2])
        return (
            ("runtime_coverage", run_id, task_id)
            if run_id is not None else None
        )
    if len(parts) >= 2:
        run_id = _integer(parts[0])
        task_id = _integer(parts[1])
        return ("runtime_wire", run_id, task_id) if run_id is not None else None
    return None


def _extract_owner(parts: tuple[str, ...]) -> tuple[str, int, int | None] | None:
    offset = 1 if parts and parts[0] == "wiki" else 0
    if len(parts) <= offset:
        return None
    run_id = _integer(parts[offset])
    task_id = _integer(parts[offset + 1]) if len(parts) > offset + 1 else None
    return ("runtime_extract", run_id, task_id) if run_id is not None else None


def _ordinary_owner(
    kind: str,
) -> Callable[[tuple[str, ...]], tuple[str, int, int | None] | None]:
    def owner(parts: tuple[str, ...]) -> tuple[str, int, int | None] | None:
        if not parts:
            return None
        run_id = _integer(parts[0])
        task_id = _integer(parts[1]) if len(parts) > 1 else None
        return (kind, run_id, task_id) if run_id is not None else None

    return owner


def _walk_candidates(
    root: Path,
    owner: Callable[
        [tuple[str, ...]], tuple[str, int, int | None] | None
    ],
    result: StorageGCResult,
    *,
    suffix: str | None = None,
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    if not root.exists():
        return candidates
    if root.is_symlink() or not root.is_dir():
        result.preserved_unsafe += 1
        return candidates
    managed_root = root.resolve()
    try:
        iterator = os.walk(managed_root, followlinks=False)
        for directory, dirnames, filenames in iterator:
            directory_path = Path(directory)
            safe_dirs: list[str] = []
            for name in dirnames:
                child = directory_path / name
                try:
                    mode = child.lstat().st_mode
                except OSError as exc:
                    result.record_error(f"scan {child}: {exc}")
                    continue
                if stat.S_ISLNK(mode):
                    result.preserved_unsafe += 1
                elif stat.S_ISDIR(mode):
                    safe_dirs.append(name)
                else:
                    result.preserved_unsafe += 1
            dirnames[:] = safe_dirs
            for name in filenames:
                path = directory_path / name
                if suffix is not None and path.suffix != suffix:
                    continue
                result.scanned_files += 1
                try:
                    info = path.lstat()
                except FileNotFoundError:
                    result.missing += 1
                    continue
                except OSError as exc:
                    result.record_error(f"scan {path}: {exc}")
                    continue
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                    result.preserved_unsafe += 1
                    continue
                try:
                    relative = path.relative_to(managed_root)
                except ValueError:
                    result.preserved_unsafe += 1
                    continue
                ownership = owner(relative.parts)
                if ownership is None:
                    result.preserved_policy += 1
                    continue
                kind, run_id, task_id = ownership
                candidates.append(_Candidate(
                    path=path,
                    root=managed_root,
                    kind=kind,
                    run_id=run_id,
                    task_id=task_id,
                    size=int(info.st_size),
                    mtime=float(info.st_mtime),
                    device=int(info.st_dev),
                    inode=int(info.st_ino),
                ))
    except OSError as exc:
        result.record_error(f"walk {managed_root}: {exc}")
    return candidates


def _scan_candidates(settings: Settings, result: StorageGCResult) -> list[_Candidate]:
    staging_root = settings.staging_dir.resolve()
    downloads_root = settings.downloads_dir.resolve()
    candidates: list[_Candidate] = []
    candidates.extend(_walk_candidates(
        staging_root / "_pipeline_runs", _wire_owner, result))
    candidates.extend(_walk_candidates(
        staging_root / "_pipeline_extract", _extract_owner, result))
    candidates.extend(_walk_candidates(
        staging_root / "_pipeline_hf_fetch",
        _ordinary_owner("runtime_fetch"),
        result,
    ))
    candidates.extend(_walk_candidates(
        downloads_root / "_pipeline_runs",
        _ordinary_owner("download"),
        result,
    ))

    # Pipeline load batches have a deliberately narrow managed layout:
    # <search-name>/pipeline/<run-id>/<digest>.parquet. Other parquet trees are
    # owned by source-specific state machines and are never inferred as garbage.
    if staging_root.exists() and staging_root.is_dir():
        try:
            source_dirs = list(staging_root.iterdir())
        except OSError as exc:
            result.record_error(f"scan {staging_root}: {exc}")
            source_dirs = []
        for source_dir in source_dirs:
            if source_dir.is_symlink() or not source_dir.is_dir():
                continue
            pipeline_root = source_dir / "pipeline"
            candidates.extend(_walk_candidates(
                pipeline_root,
                _ordinary_owner("pipeline_parquet"),
                result,
                suffix=".parquet",
            ))
    return candidates


class _UnsafeCandidate(RuntimeError):
    pass


def _unlink_candidate(candidate: _Candidate, *, oldest_mtime: float) -> int | None:
    """Unlink one unchanged regular file, rechecking every safety invariant."""
    try:
        info = candidate.path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise _UnsafeCandidate("path is no longer a regular file")
    if (
        info.st_dev != candidate.device
        or info.st_ino != candidate.inode
        or info.st_size != candidate.size
    ):
        raise _UnsafeCandidate("path changed after the maintenance scan")
    if info.st_mtime > oldest_mtime:
        raise _UnsafeCandidate("path became too new after the maintenance scan")
    try:
        resolved = candidate.path.resolve(strict=True)
    except OSError as exc:
        raise _UnsafeCandidate(f"path cannot be resolved: {exc}") from exc
    if resolved == candidate.root or not resolved.is_relative_to(candidate.root):
        raise _UnsafeCandidate("path escaped its managed root")
    candidate.path.unlink()
    return int(info.st_size)


def _prune_empty_parents(path: Path, stop: Path) -> None:
    parent = path.parent
    while parent != stop and parent.is_relative_to(stop):
        if parent.is_symlink():
            return
        try:
            parent.rmdir()
        except OSError:
            return
        parent = parent.parent


def _record_result(conn: psycopg.Connection, result: StorageGCResult) -> None:
    with conn.cursor() as cur:
        append(
            cur,
            component="maintenance",
            event="storage.gc.completed",
            level="warn" if result.error_count else "info",
            message=(
                f"Pipeline storage GC removed {result.deleted_files} files "
                f"({result.deleted_bytes} bytes)"
            ),
            data=result.as_event_data(),
        )
    conn.commit()


def prune_pipeline_storage(
    conn: psycopg.Connection,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> StorageGCResult:
    """Remove bounded old Pipeline storage without interrupting scheduling.

    Database discovery, scanning, individual unlinks, and event persistence are
    all failure-isolated. The caller always receives a result and can continue
    arming/firing Source triggers in the same scheduler iteration.
    """
    result = StorageGCResult()
    instant = now or datetime.now(UTC)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=UTC)
    terminal_cutoff = instant - timedelta(
        seconds=settings.pipeline_gc_terminal_retention_seconds)
    oldest_mtime = instant.timestamp() - settings.pipeline_gc_min_file_age_seconds

    try:
        snapshot = _db_snapshot(
            conn,
            settings,
            terminal_cutoff=terminal_cutoff,
        )
    except Exception as exc:  # noqa: BLE001 - maintenance must fail open
        conn.rollback()
        result.record_error(
            f"database snapshot: {type(exc).__name__}: {exc}")
        log.exception("Pipeline storage GC could not build its DB reference set")
        try:
            _record_result(conn, result)
        except Exception:  # noqa: BLE001 - scheduling must still continue
            conn.rollback()
            log.exception("Pipeline storage GC could not record snapshot failure")
        return result

    candidates = _scan_candidates(settings, result)
    candidates.sort(key=lambda item: (item.mtime, str(item.path)))

    for candidate in candidates:
        if candidate.run_id in snapshot.active_runs:
            result.preserved_active += 1
            continue
        try:
            resolved = candidate.path.resolve(strict=False)
        except OSError:
            result.preserved_unsafe += 1
            continue
        if resolved in snapshot.references:
            result.preserved_referenced += 1
            continue
        if candidate.run_id not in snapshot.eligible_terminal_runs:
            result.preserved_retained += 1
            continue
        if candidate.kind == "download" and (
            candidate.task_id is None
            or (candidate.run_id, candidate.task_id)
            not in snapshot.disposable_download_tasks
        ):
            result.preserved_policy += 1
            continue
        if candidate.mtime > oldest_mtime:
            result.preserved_too_new += 1
            continue
        if result.deleted_files >= settings.pipeline_gc_max_files_per_tick:
            result.file_cap_reached = True
            break
        if (
            result.deleted_bytes + candidate.size
            > settings.pipeline_gc_max_bytes_per_tick
        ):
            result.byte_cap_reached = True
            continue
        try:
            removed_size = _unlink_candidate(
                candidate, oldest_mtime=oldest_mtime)
        except _UnsafeCandidate as exc:
            result.preserved_unsafe += 1
            result.record_error(f"unsafe {candidate.path}: {exc}")
            continue
        except OSError as exc:
            result.record_error(f"delete {candidate.path}: {exc}")
            continue
        if removed_size is None:
            result.missing += 1
            continue
        result.record_deleted(candidate.kind, removed_size)
        _prune_empty_parents(candidate.path, candidate.root)

    try:
        _record_result(conn, result)
    except Exception:  # noqa: BLE001 - cleanup success must not stop scheduling
        conn.rollback()
        log.exception("Pipeline storage GC could not persist its summary event")
    level = logging.WARNING if result.error_count else logging.INFO
    log.log(
        level,
        "Pipeline storage GC scanned=%d deleted=%d bytes=%d errors=%d "
        "file_cap=%s byte_cap=%s",
        result.scanned_files,
        result.deleted_files,
        result.deleted_bytes,
        result.error_count,
        result.file_cap_reached,
        result.byte_cap_reached,
    )
    return result


__all__ = ["StorageGCResult", "prune_pipeline_storage"]
