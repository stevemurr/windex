from __future__ import annotations

from datetime import UTC, datetime, timedelta
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
from windex.pipeline.events import append, list_events
from windex.pipeline import wire
from windex.pipeline.ports import ExtractedDoc, PartitionRef
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
    RunModel.model_validate(get_run(canonical_conn, run_id))


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
