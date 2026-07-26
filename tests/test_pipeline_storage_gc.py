from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from psycopg.types.json import Jsonb

from windex.api.prom import WindexCollector
from windex.pipeline import maintenance
from windex.pipeline.run_store import submit_source


def _run(pg, settings, state: str, now: datetime) -> tuple[int, int]:
    run_id = submit_source(
        pg, "arxiv", settings=settings, dedupe=False)
    assert run_id is not None
    with pg.cursor() as cur:
        cur.execute(
            """UPDATE runs
                  SET state = %s,
                      finished_at = CASE WHEN %s IN (
                          'succeeded', 'failed', 'cancelled'
                      ) THEN %s ELSE NULL END,
                      updated_at = %s
                WHERE id = %s""",
            (state, state, now - timedelta(days=3), now, run_id),
        )
        cur.execute(
            "SELECT id FROM run_tasks WHERE run_id = %s ORDER BY id LIMIT 1",
            (run_id,),
        )
        task_id = int(cur.fetchone()[0])
        if state in {"succeeded", "failed", "cancelled"}:
            cur.execute(
                """UPDATE run_tasks
                      SET state = %s, finished_at = %s
                    WHERE run_id = %s""",
                (state, now - timedelta(days=3), run_id),
            )
    pg.commit()
    return int(run_id), task_id


def _file(path: Path, data: bytes, when: datetime) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    timestamp = when.timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


def _disposable_download(pg, run_id: int, task_id: int, *, keep: bool = False) -> None:
    with pg.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks
                  SET module = 'http.download', config = %s
                WHERE id = %s AND run_id = %s""",
            (Jsonb({"keep": keep}), task_id, run_id),
        )
    pg.commit()


def _sample(family, **labels) -> float | None:
    for item in family.samples:
        if item.name == family.name and item.labels == labels:
            return float(item.value)
    return None


def test_gc_removes_every_terminal_runtime_class_and_failed_wiki_download(
    pg, settings,
):
    now = datetime.now(UTC)
    old = now - timedelta(days=2)
    run_id, task_id = _run(pg, settings, "failed", now)
    _disposable_download(pg, run_id, task_id)
    staging = settings.staging_dir
    downloads = settings.downloads_dir

    paths = {
        "runtime_wire": _file(
            staging / "_pipeline_runs" / str(run_id) / str(task_id) / "wire.json.gz",
            b"wire",
            old,
        ),
        "runtime_coverage": _file(
            staging / "_pipeline_runs" / "coverage" / str(run_id)
            / str(task_id) / "coverage.txt.gz",
            b"coverage",
            old,
        ),
        "runtime_extract": _file(
            staging / "_pipeline_extract" / "wiki" / str(run_id)
            / str(task_id) / "blocks.json",
            b"blocks",
            old,
        ),
        "runtime_fetch": _file(
            staging / "_pipeline_hf_fetch" / str(run_id)
            / str(task_id) / "root" / "plan.json",
            b"plan",
            old,
        ),
        "pipeline_parquet": _file(
            staging / "arxiv" / "pipeline" / str(run_id) / "batch.parquet",
            b"parquet",
            old,
        ),
        "download": _file(
            downloads / "_pipeline_runs" / str(run_id)
            / str(task_id) / "enwiki-cirrus.json.gz.bz2",
            b"failed wiki shard",
            old,
        ),
    }
    declared = _file(
        settings.artifacts_dir / "runs" / str(run_id) / "declared.json.gz",
        b"declared",
        old,
    )
    expected_bytes = sum(path.stat().st_size for path in paths.values())

    result = maintenance.prune_pipeline_storage(
        pg, settings, now=now)

    assert result.deleted_files == len(paths)
    assert result.deleted_bytes == expected_bytes
    assert set(result.deleted) == set(paths)
    assert all(not path.exists() for path in paths.values())
    assert declared.read_bytes() == b"declared"
    with pg.cursor() as cur:
        cur.execute(
            """SELECT data FROM operational_events
                WHERE event = 'storage.gc.completed'
                ORDER BY seq DESC LIMIT 1"""
        )
        event = cur.fetchone()[0]
    assert event["deleted_files"] == len(paths)
    assert event["error_count"] == 0
    families = {
        family.name: family
        for family in WindexCollector._read_database(pg)
    }
    assert _sample(
        families["windex_storage_gc_deleted_files"],
        kind="download",
    ) == 1.0
    assert _sample(families["windex_storage_gc_errors"]) == 0.0


def test_gc_preserves_active_db_references_keep_policy_and_declared_artifacts(
    pg, settings,
):
    now = datetime.now(UTC)
    old = now - timedelta(days=2)
    terminal_run, terminal_task = _run(pg, settings, "succeeded", now)
    active_run, active_task = _run(pg, settings, "running", now)
    keep_run, keep_task = _run(pg, settings, "cancelled", now)
    _disposable_download(pg, terminal_run, terminal_task)
    _disposable_download(pg, keep_run, keep_task, keep=True)
    staging = settings.staging_dir
    downloads = settings.downloads_dir

    active_wire = _file(
        staging / "_pipeline_runs" / str(active_run)
        / str(active_task) / "active.json.gz",
        b"active",
        old,
    )
    referenced_wire = _file(
        staging / "_pipeline_runs" / str(terminal_run)
        / str(terminal_task) / "referenced.json.gz",
        b"referenced",
        old,
    )
    referenced_coverage = _file(
        staging / "_pipeline_runs" / "coverage" / str(terminal_run)
        / str(terminal_task) / "referenced.txt.gz",
        b"coverage",
        old,
    )
    referenced_download = _file(
        downloads / "_pipeline_runs" / str(terminal_run)
        / str(terminal_task) / "referenced.bin",
        b"download",
        old,
    )
    kept_download = _file(
        downloads / "_pipeline_runs" / str(keep_run)
        / str(keep_task) / "keep.bin",
        b"keep",
        old,
    )
    parquet_relative = (
        f"arxiv/pipeline/{terminal_run}/referenced.parquet")
    referenced_parquet = _file(
        staging / parquet_relative, b"parquet", old)

    with pg.cursor() as cur:
        cur.execute(
            """SELECT source_id, source_name FROM runs WHERE id = %s""",
            (terminal_run,),
        )
        source_id, source_name = cur.fetchone()
        cur.execute(
            """INSERT INTO documents
                   (id, source_id, owner_run_id, source, url, status, text_ref)
               VALUES (%s, %s, %s, %s, 'https://example.test', 'staged', %s)""",
            (
                f"{source_name}:gc-reference",
                source_id,
                terminal_run,
                source_name,
                parquet_relative,
            ),
        )
        cur.execute(
            """INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, counts)
               VALUES (%s, %s, 'live-ref', 'done', %s, %s)""",
            (
                active_run,
                active_task,
                Jsonb([{
                    "type": "_WireArtifact",
                    "path": (
                        f"{terminal_run}/{terminal_task}/referenced.json.gz"
                    ),
                }]),
                Jsonb({
                    "coverage_path": (
                        f"_pipeline_runs/coverage/{terminal_run}/"
                        f"{terminal_task}/referenced.txt.gz"
                    ),
                }),
            ),
        )
        cur.execute(
            """INSERT INTO run_outputs
                   (run_id, boundary, value_type, value, size_bytes, checksum)
               VALUES (%s, 'capture', 'WireBatch', %s, 1, 'sha256:test')""",
            (
                active_run,
                Jsonb([{
                    "type": "RawBlob",
                    "path": str(referenced_download),
                }]),
            ),
        )
    pg.commit()

    result = maintenance.prune_pipeline_storage(
        pg, settings, now=now)

    assert result.deleted_files == 0
    assert result.preserved_active >= 1
    assert result.preserved_referenced >= 4
    assert result.preserved_policy >= 1
    assert all(path.exists() for path in (
        active_wire,
        referenced_wire,
        referenced_coverage,
        referenced_download,
        kept_download,
        referenced_parquet,
    ))


def test_gc_enforces_file_byte_and_age_caps(pg, settings):
    now = datetime.now(UTC)
    old = now - timedelta(days=2)
    run_id, _task_id = _run(pg, settings, "succeeded", now)
    root = settings.staging_dir / "arxiv" / "pipeline" / str(run_id)
    oldest = _file(root / "a.parquet", b"aaaa", old - timedelta(hours=2))
    second = _file(root / "b.parquet", b"bbbb", old - timedelta(hours=1))
    too_new = _file(root / "c.parquet", b"cccc", now)
    settings.pipeline_gc_max_files_per_tick = 1

    first = maintenance.prune_pipeline_storage(pg, settings, now=now)

    assert first.deleted_files == 1
    assert first.file_cap_reached is True
    assert not oldest.exists()
    assert second.exists()
    assert too_new.exists()

    settings.pipeline_gc_max_files_per_tick = 10
    settings.pipeline_gc_max_bytes_per_tick = 3
    second_pass = maintenance.prune_pipeline_storage(pg, settings, now=now)

    assert second_pass.deleted_files == 0
    assert second_pass.byte_cap_reached is True
    assert second.exists()
    assert too_new.exists()


def test_gc_never_follows_symlinks_or_deletes_outside_managed_roots(
    pg, settings, tmp_path,
):
    now = datetime.now(UTC)
    old = now - timedelta(days=2)
    run_id, task_id = _run(pg, settings, "failed", now)
    root = (
        settings.staging_dir / "_pipeline_runs" / str(run_id) / str(task_id))
    root.mkdir(parents=True)
    outside_file = _file(tmp_path / "outside.bin", b"outside", old)
    outside_dir = tmp_path / "outside-dir"
    outside_dir.mkdir()
    _file(outside_dir / "nested.bin", b"nested", old)
    (root / "file-link").symlink_to(outside_file)
    (root / "dir-link").symlink_to(outside_dir, target_is_directory=True)
    assert maintenance._safe_reference(
        settings.staging_dir / ".." / ".." / "outside.bin",
        settings.staging_dir.resolve(),
    ) is None

    result = maintenance.prune_pipeline_storage(
        pg, settings, now=now)

    assert result.deleted_files == 0
    assert result.preserved_unsafe >= 2
    assert outside_file.read_bytes() == b"outside"
    assert (outside_dir / "nested.bin").read_bytes() == b"nested"
    assert (root / "file-link").is_symlink()
    assert (root / "dir-link").is_symlink()


def test_gc_isolates_file_failures_and_retries_on_the_next_pass(
    pg, settings, monkeypatch,
):
    now = datetime.now(UTC)
    old = now - timedelta(days=2)
    run_id, _task_id = _run(pg, settings, "succeeded", now)
    root = settings.staging_dir / "arxiv" / "pipeline" / str(run_id)
    bad = _file(root / "a-bad.parquet", b"bad", old)
    good = _file(root / "b-good.parquet", b"good", old)
    vanished = _file(root / "c-vanished.parquet", b"vanished", old)
    original = maintenance._unlink_candidate
    failed_once = False

    def flaky(candidate, *, oldest_mtime):
        nonlocal failed_once
        if candidate.path == bad and not failed_once:
            failed_once = True
            raise PermissionError("fixture denial")
        if candidate.path == vanished and candidate.path.exists():
            candidate.path.unlink()
        return original(candidate, oldest_mtime=oldest_mtime)

    monkeypatch.setattr(maintenance, "_unlink_candidate", flaky)
    first = maintenance.prune_pipeline_storage(pg, settings, now=now)

    assert first.deleted_files == 1
    assert first.error_count == 1
    assert first.missing == 1
    assert bad.exists()
    assert not good.exists()
    assert not vanished.exists()

    second = maintenance.prune_pipeline_storage(pg, settings, now=now)

    assert second.deleted_files == 1
    assert second.error_count == 0
    assert not bad.exists()


def test_gc_database_failure_is_reported_without_raising(
    pg, settings, monkeypatch,
):
    def broken_snapshot(*_args, **_kwargs):
        raise RuntimeError("fixture database failure")

    monkeypatch.setattr(maintenance, "_db_snapshot", broken_snapshot)

    result = maintenance.prune_pipeline_storage(pg, settings)

    assert result.deleted_files == 0
    assert result.error_count == 1
    assert "database snapshot" in result.errors[0]
