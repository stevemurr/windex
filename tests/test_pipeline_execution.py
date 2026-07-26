from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
import uuid

import psycopg
import pytest
from psycopg.types.json import Jsonb

from windex.api.canonical import (
    DeploymentReport,
    PipelineModel,
    RunModel,
    SourceModel,
    TaskPreviewResponse,
)
from windex.db.canonical import init_canonical_db
from windex.modules.discover import time_calendar, time_windows
from windex.modules.collect import _write
from windex.pipeline.events import append, list_events
from windex.pipeline.indexing import _record_ownership
from windex.pipeline import wire
from windex.pipeline.ports import ExtractedDoc, PartitionRecord, PartitionRef
from windex.pipeline.run_store import (
    artifact,
    get_run,
    outputs,
    submit_pipeline,
    submit_source,
)
from windex.pipeline.store import create_pipeline, get_pipeline, task_preview
from windex.source.scheduler import tick
from windex.source.store import (
    create_source,
    create_trigger,
    get_source,
    reset,
    reset_preview,
    validate_candidate,
)
from windex.worker import canonical_claim


@pytest.fixture
def canonical_conn():
    admin_dsn = "postgresql://windex:windex@127.0.0.1:5432/windex"
    try:
        admin = psycopg.connect(admin_dsn, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("postgres not running")
    name = "windex_execution_" + uuid.uuid4().hex[:10]
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}"')
    conn = psycopg.connect(admin_dsn.rsplit("/", 1)[0] + "/" + name)
    try:
        init_canonical_db(conn, bootstrap_id="execution-test")
        yield conn
    finally:
        conn.close()
        with admin.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s", (name,))
            cur.execute(f'DROP DATABASE "{name}"')
        admin.close()


def _push_source(conn, name: str):
    return create_source(conn, {
        "name": name,
        "title": name,
        "origin": {"ingress": "push"},
        "pipeline_name": "custom",
        "pipeline_version": 1,
        "search_name": f"{name}-search",
        "id_prefix": f"{name}:",
        "collection_key": f"{name}-collection",
        "search_profile": "generic",
        "include_in_all": False,
        "state_namespace": f"{name}-state",
        "enabled": True,
        "values": {},
    })


def _documents():
    return {
        "documents": {
            "mode": "delta",
            "documents": [{
                "id": "one",
                "url": "https://example.test/one",
                "title": "One",
                "text": "body",
                "fields": {},
                "deleted": False,
            }],
        },
    }


def test_core_store_payloads_match_typed_api_contracts(canonical_conn):
    source = _push_source(canonical_conn, "typed_contract")
    PipelineModel.model_validate(get_pipeline(canonical_conn, "custom"))
    SourceModel.model_validate(source)
    DeploymentReport.model_validate(validate_candidate(
        canonical_conn, {
            **source,
            "name": "typed_contract_candidate",
            "search_name": "typed-contract-candidate",
            "id_prefix": "typed-contract-candidate:",
            "collection_key": "typed-contract-candidate",
            "state_namespace": "typed-contract-candidate",
        },
    ))
    TaskPreviewResponse.model_validate(
    task_preview(canonical_conn, "custom", 1))
    run_id = submit_source(
        canonical_conn, "typed_contract", inputs=_documents(), dedupe=False)
    run = get_run(canonical_conn, run_id)
    RunModel.model_validate(run)
    index_task = next(
        task for task in run["tasks"] if task["module"] == "platform.index")
    assert index_task["preconditions"] == ["gateway"]


def test_index_ownership_matches_canonical_generation_schema(canonical_conn):
    source = _push_source(canonical_conn, "ownership")
    run_id = submit_source(
        canonical_conn, "ownership", inputs=_documents(), dedupe=False)
    task = canonical_claim.claim_task(
        canonical_conn,
        worker="pytest/ownership",
        lanes=["io"],
        caps={"io": 1},
        satisfied=[],
        default_cap=1,
    )
    assert task is not None
    assert task.run_id == run_id
    assert task.source_generation == source["generation"]

    from windex.worker.protocol import TaskContext

    context = TaskContext(
        run_id=run_id,
        task_id=task.id,
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
        node=task.node,
        kind=task.kind,
        module=task.module,
        module_version=task.module_version,
        module_digest=task.module_digest,
        config=task.config,
        spec=task.spec,
        cursor=task.cursor,
        conn=canonical_conn,
        should_yield=lambda: False,
        heartbeat=lambda _done, _failed, _stats: None,
        source_generation=task.source_generation,
    )
    _record_ownership(
        context,
        collection="ownership-collection__model",
        alias="ownership-collection_current",
        model="model",
    )
    canonical_conn.commit()
    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT generation, resource_type, resource_name,
                      metadata->>'source_id'
                 FROM storage_ownership ORDER BY resource_type""")
        rows = cur.fetchall()
    assert rows == [
        (
            source["generation"],
            "qdrant_alias",
            "ownership-collection_current",
            str(source["id"]),
        ),
        (
            source["generation"],
            "qdrant_collection",
            "ownership-collection__model",
            str(source["id"]),
        ),
    ]


def test_warc_lane_allows_two_tasks_for_one_source(canonical_conn):
    run_id = submit_source(
        canonical_conn, "ccnews", flow="ingest", dedupe=False)
    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks
                  SET state = 'ready'
                WHERE run_id = %s AND node IN ('extract_a', 'extract_b')""",
            (run_id,),
        )
    canonical_conn.commit()

    claimed = [
        canonical_claim.claim_task(
            canonical_conn,
            worker=f"pytest/warc/{index}",
            lanes=["warc"],
            caps={"warc": 2},
            satisfied=["storage:downloads", "storage:staging"],
            default_cap=1,
        )
        for index in range(2)
    ]

    assert {task.node for task in claimed if task is not None} == {
        "extract_a",
        "extract_b",
    }


def test_expected_worker_exit_requeues_without_spending_attempt(canonical_conn):
    run_id = submit_source(canonical_conn, "arxiv", dedupe=False)
    task = canonical_claim.claim_task(
        canonical_conn,
        worker="pytest/drained",
        lanes=["io"],
        caps={"io": 1},
        satisfied=[],
        default_cap=1,
    )
    assert task is not None
    assert task.run_id == run_id

    assert canonical_claim.release_worker(
        canonical_conn, task.worker) == [task.id]
    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT state, attempts, lease_worker, lease_expires_at
                 FROM run_tasks WHERE id = %s""",
            (task.id,),
        )
        assert cur.fetchone() == ("ready", 0, None, None)

    claimed_again = canonical_claim.claim_task(
        canonical_conn,
        worker="pytest/crashed",
        lanes=["io"],
        caps={"io": 1},
        satisfied=[],
        default_cap=1,
    )
    assert claimed_again is not None
    assert claimed_again.id == task.id
    assert canonical_claim.release_worker(
        canonical_conn, claimed_again.worker, penalize=True) == [task.id]
    with canonical_conn.cursor() as cur:
        cur.execute(
            "SELECT state, attempts, error FROM run_tasks WHERE id = %s",
            (task.id,),
        )
        assert cur.fetchone() == (
            "ready",
            1,
            "worker exited unexpectedly",
        )


def test_expired_lease_uses_separate_recovery_budget(canonical_conn):
    run_id = submit_source(canonical_conn, "arxiv", dedupe=False)
    task = canonical_claim.claim_task(
        canonical_conn,
        worker="pytest/lost",
        lanes=["io"],
        caps={"io": 1},
        satisfied=[],
        default_cap=1,
    )
    assert task is not None
    assert task.run_id == run_id
    with canonical_conn.cursor() as cur:
        cur.execute(
            "UPDATE run_tasks SET lease_expires_at = now() - interval '1 second' "
            "WHERE id = %s",
            (task.id,),
        )
    canonical_conn.commit()

    assert canonical_claim.reclaim_expired(canonical_conn) == [{
        "id": task.id,
        "run_id": run_id,
        "source_id": task.source_id,
        "node": task.node,
        "state": "ready",
    }]
    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT state, attempts, stats->>'lease_recoveries', error
                 FROM run_tasks WHERE id = %s""",
            (task.id,),
        )
        assert cur.fetchone() == ("ready", 0, "1", None)

        cur.execute(
            """UPDATE run_tasks
                  SET state = 'running', lease_worker = 'pytest/lost-again',
                      lease_expires_at = now() - interval '1 second',
                      stats = jsonb_set(
                          stats, '{lease_recoveries}', to_jsonb(%s::integer))
                WHERE id = %s""",
            (canonical_claim.MAX_LEASE_RECOVERIES - 1, task.id),
        )
    canonical_conn.commit()

    recovered = canonical_claim.reclaim_expired(canonical_conn)
    assert recovered[0]["state"] == "failed"
    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT state, attempts, stats->>'lease_recoveries', error
                 FROM run_tasks WHERE id = %s""",
            (task.id,),
        )
        assert cur.fetchone() == (
            "failed",
            0,
            str(canonical_claim.MAX_LEASE_RECOVERIES),
            "lease recovery limit exceeded",
        )


def test_discovery_order_accepts_canonical_text_partition_keys(canonical_conn):
    run_id = submit_source(canonical_conn, "arxiv", dedupe=False)
    task = canonical_claim.claim_task(
        canonical_conn,
        worker="pytest/windows",
        lanes=["io"],
        caps={"io": 1},
        satisfied=[],
        default_cap=1,
    )
    assert task is not None
    assert task.run_id == run_id
    assert task.module == "time.windows"

    def heartbeat(_done, _failed, _stats):
        return None

    context = SimpleNamespace(
        config=task.config,
        search_name=task.search_name,
        state_namespace=task.state_namespace,
        conn=canonical_conn,
        task_id=task.id,
        run_id=task.run_id,
        should_yield=lambda: False,
        heartbeat=heartbeat,
    )
    windows = time_windows(context)
    assert windows.units_done == 100
    context.config = {
        "unit": "day",
        "trailing_days": 2,
        "into": "calendar",
        "format": "%Y-%m-%d",
    }
    calendar = time_calendar(context)
    assert calendar.units_done == 2

    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT data_type FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'source_units' AND column_name = 'ord'""")
        assert cur.fetchone()[0] == "text"
        cur.execute(
            """SELECT count(*), count(source_id), bool_and(ord IS NOT NULL)
                 FROM source_units WHERE state_namespace = 'arxiv'""")
        total, bound, ordered = cur.fetchone()
    assert total > 100
    assert bound == total
    assert ordered is True


def test_store_upsert_defaults_missing_stage_and_preserves_existing(canonical_conn):
    context = SimpleNamespace(
        state_namespace="docs",
        search_name="docs",
        run_id=999,
        conn=canonical_conn,
    )
    record = PartitionRecord(
        store="docset",
        key="markdown",
        upstream={"mtime": 1},
        payload={"slug": "markdown"},
    )

    _write(context, record, "merge")
    canonical_conn.commit()
    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE source_units SET stage = 'ready'
                WHERE state_namespace = 'docs'
                  AND store = 'docset' AND unit_key = 'markdown'""")
    canonical_conn.commit()

    _write(context, record, "merge")
    canonical_conn.commit()
    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT stage FROM source_units
                WHERE state_namespace = 'docs'
                  AND store = 'docset' AND unit_key = 'markdown'""")
        assert cur.fetchone()[0] == "ready"


def _generic_output_pipeline():
    return {
        "schema": "windex.pipeline/1",
        "parameters": [],
        "state": {},
        "flows": {
            "receive": {
                "inputs": [{
                    "id": "documents",
                    "type": "DocumentBatch",
                    "max_items": 10,
                    "max_bytes": 2 * 1024 * 1024,
                }],
                "outputs": [{
                    "id": "documents_out",
                    "type": "ExtractedDoc",
                    "max_bytes": 2 * 1024 * 1024,
                }],
                "nodes": {
                    "receive": {
                        "kind": "receive",
                        "uses": "push.docs",
                        "with": {"max_docs": 10},
                    },
                },
                "edges": [
                    {
                        "from": {"input": "documents"},
                        "to": {"node": "receive"},
                    },
                    {
                        "from": {"node": "receive"},
                        "to": {"output": "documents_out"},
                    },
                ],
            },
        },
        "refresh": [],
    }


def test_sources_sharing_revision_freeze_isolated_bindings(canonical_conn):
    first = _push_source(canonical_conn, "alpha")
    second = _push_source(canonical_conn, "beta")
    assert first["pipeline_revision_id"] == second["pipeline_revision_id"]

    run_a = submit_source(
        canonical_conn, "alpha", inputs=_documents(), dedupe=False)
    run_b = submit_source(
        canonical_conn, "beta", inputs=_documents(), dedupe=False)
    frozen_a = get_run(canonical_conn, run_a)
    frozen_b = get_run(canonical_conn, run_b)
    assert frozen_a["source_snapshot"]["state_namespace"] == "alpha-state"
    assert frozen_b["source_snapshot"]["state_namespace"] == "beta-state"
    assert frozen_a["source_snapshot"]["collection_key"] != \
        frozen_b["source_snapshot"]["collection_key"]
    assert frozen_a["tasks"][-1]["config"]["collection_key"] == "alpha-collection"
    assert frozen_b["tasks"][-1]["config"]["collection_key"] == "beta-collection"


def test_push_idempotency_and_source_scoped_dedupe(canonical_conn):
    _push_source(canonical_conn, "push_idem")
    first = submit_source(
        canonical_conn, "push_idem", inputs=_documents(),
        idempotency_key="request-0001", dedupe=False)
    duplicate = submit_source(
        canonical_conn, "push_idem", inputs=_documents(),
        idempotency_key="request-0001", dedupe=False)
    assert first is not None
    assert duplicate == first


def test_claim_contains_frozen_pipeline_and_source_context(canonical_conn):
    _push_source(canonical_conn, "claim")
    run_id = submit_source(
        canonical_conn, "claim", inputs=_documents(), dedupe=False)
    task = canonical_claim.claim_task(
        canonical_conn, worker="pytest/1", lanes=["io"], caps={"io": 4},
        satisfied=[], default_cap=4)
    assert task is not None
    assert task.run_id == run_id
    assert task.pipeline_name == "custom"
    assert task.source_name == "claim"
    assert task.state_namespace == "claim-state"
    assert task.search_name == "claim-search"
    assert task.inputs == _documents()
    assert task.module_version and task.module_digest
    state = canonical_claim.release(
        canonical_conn, task,
        canonical_claim.Release(outcome="succeeded", units_done=1))
    assert state == "succeeded"


def test_unsatisfied_preconditions_are_observable_and_recover(canonical_conn):
    _push_source(canonical_conn, "blocked")
    run_id = submit_source(
        canonical_conn, "blocked", inputs=_documents(), dedupe=False)
    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks SET preconditions = ARRAY['fixture_ready']
                WHERE run_id = %s AND state = 'ready'""",
            (run_id,),
        )
    canonical_conn.commit()

    changed = canonical_claim.reconcile_blocked(canonical_conn, [])
    assert changed == {
        "tasks_blocked": 1,
        "tasks_unblocked": 0,
        "runs_blocked": 1,
        "runs_unblocked": 0,
    }
    assert get_run(canonical_conn, run_id)["state"] == "blocked"
    events = list_events(canonical_conn, after=0, run_id=run_id)
    assert "task.blocked" in {event["event"] for event in events}
    assert "run.blocked" in {event["event"] for event in events}

    recovered = canonical_claim.reconcile_blocked(
        canonical_conn, ["fixture_ready"])
    assert recovered["tasks_unblocked"] == 1
    assert recovered["runs_unblocked"] == 1
    assert get_run(canonical_conn, run_id)["state"] == "queued"


def test_generic_boundary_output_uses_typed_durable_artifact(
    canonical_conn, tmp_path, monkeypatch,
):
    monkeypatch.setenv("WINDEX_DATA_ROOT", str(tmp_path / "data"))
    create_pipeline(
        canonical_conn, name="generic_output", spec=_generic_output_pipeline())
    run_id = submit_pipeline(
        canonical_conn, "generic_output", version=1, flow="receive",
        inputs={"documents": {"documents": []}}, parameters={})
    task = canonical_claim.claim_task(
        canonical_conn, worker="pytest/output", lanes=["io"],
        caps={"io": 1}, satisfied=[], default_cap=1)
    assert task is not None and task.run_id == run_id
    encoded = wire.encode(ExtractedDoc(
        ref=PartitionRef(store="", key="one"),
        suffix="one",
        url="https://example.test/one",
        text="x" * (1024 * 1024 + 32_000),
        epoch=run_id,
    ))
    with canonical_conn.cursor() as cur:
        cur.execute(
            """INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, finished_at)
               VALUES (%s, %s, 'one', 'done', %s, now())""",
            (run_id, task.id, Jsonb([encoded])),
        )
    canonical_conn.commit()
    assert canonical_claim.release(
        canonical_conn, task,
        canonical_claim.Release(outcome="succeeded", units_done=1),
    ) == "succeeded"

    result = outputs(canonical_conn, run_id)
    assert len(result) == 2  # frozen input plus terminal output
    terminal = next(item for item in result if item["boundary"] == "documents_out")
    assert terminal["type"] == "ExtractedDoc"
    assert terminal["value"]["inline"] is False
    metadata = artifact(
        canonical_conn, run_id, terminal["value"]["artifact_id"])
    assert metadata is not None
    path = (
        tmp_path / "data" / "generations" / "current" / "artifacts"
        / metadata["relative_path"]
    )
    assert path.is_file()
    assert path.stat().st_size == metadata["size_bytes"]


def test_generic_dry_run_is_frozen_and_reaches_worker_context(canonical_conn):
    create_pipeline(
        canonical_conn,
        name="generic_dry_run",
        spec=_generic_output_pipeline(),
    )
    run_id = submit_pipeline(
        canonical_conn,
        "generic_dry_run",
        version=1,
        flow="receive",
        inputs={"documents": {"documents": []}},
        parameters={},
        dry_run=True,
    )
    run = get_run(canonical_conn, run_id)
    RunModel.model_validate(run)
    assert run["mode"] == "dry_run"
    assert run["pipeline_version"] == 1
    assert run["source_id"] is None

    task = canonical_claim.claim_task(
        canonical_conn,
        worker="pytest/dry-run",
        lanes=["io"],
        caps={"io": 1},
        satisfied=[],
        default_cap=1,
    )
    assert task is not None
    assert task.run_id == run_id
    assert task.mode == "dry_run"


def test_scheduler_fire_and_watermark_are_atomic(canonical_conn):
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 3600},
        "enabled": True,
        "next_fire_at": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
    })
    instant = datetime.now(UTC)
    result = tick(canonical_conn, now=instant)
    assert len(result.fired) == 1
    assert result.fired[0]["trigger_id"] == trigger["id"]
    with canonical_conn.cursor() as cur:
        cur.execute(
            "SELECT last_run_id, next_fire_at FROM source_triggers WHERE id = %s",
            (trigger["id"],))
        run_id, next_fire = cur.fetchone()
    assert run_id == result.fired[0]["run_id"]
    assert next_fire > instant


def test_reset_is_a_paused_asynchronous_platform_run(canonical_conn):
    preview = reset_preview(canonical_conn, "hn")
    queued = reset(canonical_conn, "hn", preview["confirmation_token"])
    assert queued["state"] == "queued"
    source = get_source(canonical_conn, "hn")
    assert source["paused"] is True
    run = get_run(canonical_conn, queued["run_id"])
    assert run["mode"] == "reset"
    assert [task["module"] for task in run["tasks"]] == ["platform.reset"]

    # The reset task is the one task allowed to drain while the Source pause
    # prevents every ordinary task from claiming more work.
    task = canonical_claim.claim_task(
        canonical_conn, worker="pytest/reset", lanes=["maint"],
        caps={"maint": 1}, satisfied=[], default_cap=1)
    assert task is not None
    assert task.module == "platform.reset"


def test_operational_events_redact_before_storage(canonical_conn):
    with canonical_conn.cursor() as cur:
        append(
            cur, component="pytest", event="redaction.test",
            message="Bearer super-secret-token",
            source_name="hn",
            data={
                "api_key": "value-never-store",
                "nested": {"text": "token=another-secret"},
            },
        )
    canonical_conn.commit()
    events = list_events(
        canonical_conn, component="pytest", source="hn", text="redaction")
    assert len(events) == 1
    serialized = str(events[0])
    assert "super-secret-token" not in serialized
    assert "value-never-store" not in serialized
    assert "another-secret" not in serialized
    assert "[REDACTED]" in serialized
