from __future__ import annotations

import copy
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient

from windex.db.canonical import init_canonical_db
from windex.module_sandbox.app import app
from windex.modules import admin
from windex.pipeline import registry
from windex.pipeline.run_store import RunConflictError, rerun, submit_pipeline
from windex.pipeline.store import create_pipeline, get_pipeline
from windex.source.store import create_source, get_source, list_sources


def test_sandbox_executes_bounded_jsonl_transform():
    client = TestClient(app)
    response = client.post("/v1/execute", json={
        "runtime": "python",
        "source": (
            "def transform(record, config):\n"
            "    record['title'] = record.get('title', '') + config.get('suffix', '')\n"
            "    return record\n"
        ),
        "records": [{"type": "ExtractedDoc", "title": "Hello"}],
        "config": {"suffix": "!"},
        "limits": {"wall_seconds": 2, "output_bytes": 100_000},
    })
    assert response.status_code == 200
    assert response.json()["outputs"][0]["title"] == "Hello!"


@pytest.mark.parametrize("source", [
    "def transform(record, config):\n import os\n return record\n",
    "def transform(record, config):\n return ().__class__.__base__\n",
    "def transform(record, config):\n return open('/etc/passwd').read()\n",
])
def test_hostile_source_is_rejected_before_execution(source):
    response = TestClient(app).post("/v1/execute", json={
        "runtime": "python",
        "source": source,
        "records": [{}],
        "config": {},
        "limits": {},
    })
    assert response.status_code == 422


def test_runaway_source_hits_wall_limit():
    response = TestClient(app).post("/v1/execute", json={
        "runtime": "python",
        "source": (
            "def transform(record, config):\n"
            "    while True:\n"
            "        pass\n"
        ),
        "records": [{}],
        "config": {},
        "limits": {"wall_seconds": 0.1},
    })
    assert response.status_code == 200
    assert response.json()["ok"] is False
    assert response.json()["error"] == "wall_time_limit_exceeded"


@pytest.fixture
def canonical_conn():
    admin_dsn = "postgresql://windex:windex@127.0.0.1:5432/windex"
    try:
        root = psycopg.connect(admin_dsn, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("postgres not running")
    name = "windex_modules_" + uuid.uuid4().hex[:10]
    with root.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}"')
    conn = psycopg.connect(admin_dsn.rsplit("/", 1)[0] + "/" + name)
    try:
        init_canonical_db(conn, bootstrap_id="module-test")
        yield conn
    finally:
        conn.close()
        with root.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s", (name,))
            cur.execute(f'DROP DATABASE "{name}"')
        root.close()


def test_approved_version_is_immutable_and_revocation_removes_it(canonical_conn):
    created = admin.create_version(
        canonical_conn,
        name="local.upper",
        title="Uppercase",
        description="fixture",
        runtime="python",
        kind="transform",
        port_spec={"input": "ExtractedDoc", "output": "ExtractedDoc"},
        parameter_schema=[],
        source=(
            "def transform(record, config):\n"
            "    record['title'] = record.get('title', '').upper()\n"
            "    return record\n"
        ),
    )
    assert created["approval_state"] == "validated"
    admin.mark_tested(
        canonical_conn, "local.upper", created["version"], {"ok": True})
    available = admin.approve(
        canonical_conn, "local.upper", created["version"],
        approved_by="pytest")
    assert available["approval_state"] == "available"
    assert registry.get("local.upper") is not None
    assert registry.implementation_digest("local.upper") == \
        created["source_digest"]

    with pytest.raises(psycopg.Error):
        with canonical_conn.cursor() as cur:
            cur.execute(
                """UPDATE module_versions SET source = 'changed'
                    WHERE id = %s""",
                (created["id"],))
        canonical_conn.commit()
    canonical_conn.rollback()

    revoked = admin.revoke(
        canonical_conn, "local.upper", created["version"], by="pytest")
    assert revoked["approval_state"] == "revoked"
    assert registry.get("local.upper") is None


def test_revoked_sandbox_module_marks_source_not_ready(canonical_conn):
    created = admin.create_version(
        canonical_conn,
        name="local.source_ready",
        title="Source readiness fixture",
        description="fixture",
        runtime="python",
        kind="transform",
        port_spec={"input": "ExtractedDoc", "output": "ExtractedDoc"},
        parameter_schema=[],
        source=(
            "def transform(record, config):\n"
            "    return record\n"
        ),
    )
    admin.mark_tested(
        canonical_conn,
        "local.source_ready",
        created["version"],
        {"ok": True},
    )
    admin.approve(
        canonical_conn,
        "local.source_ready",
        created["version"],
        approved_by="pytest",
    )

    memory = get_pipeline(canonical_conn, "memory")
    assert memory
    spec = copy.deepcopy(memory["spec"])
    flow = spec["flows"]["receive"]
    flow["nodes"]["custom"] = {
        "kind": "transform",
        "uses": "local.source_ready",
        "with": {},
    }
    flow["edges"] = [
        {
            "from": {"input": "documents"},
            "to": {"node": "push"},
        },
        {
            "from": {"node": "push"},
            "to": {"node": "custom"},
        },
        {
            "from": {"node": "custom"},
            "to": {"node": "stage"},
        },
    ]
    create_pipeline(
        canonical_conn,
        name="custom_ready",
        title="Custom readiness",
        spec=spec,
    )
    created_source = create_source(canonical_conn, {
        "name": "custom_ready",
        "pipeline_name": "custom_ready",
        "pipeline_version": 1,
        "origin": {"ingress": "push"},
        "search_name": "custom_ready",
        "id_prefix": "custom_ready:",
        "collection_key": "custom_ready",
        "search_profile": "memory",
        "state_namespace": "custom_ready",
        "values": {},
    })
    assert created_source["ready"] is True

    admin.revoke(
        canonical_conn,
        "local.source_ready",
        created["version"],
        by="pytest",
    )

    assert get_source(canonical_conn, "custom_ready")["ready"] is False
    assert next(
        item for item in list_sources(canonical_conn)
        if item["name"] == "custom_ready"
    )["ready"] is False


def test_revocation_blocks_exact_historic_rerun(canonical_conn):
    source = (
        "def transform(record, config):\n"
        "    record['title'] = record.get('title', '').upper()\n"
        "    return record\n"
    )
    version = admin.create_version(
        canonical_conn, name="local.historic", runtime="python",
        kind="transform",
        port_spec={"input": "ExtractedDoc", "output": "ExtractedDoc"},
        parameter_schema=[], source=source)
    admin.mark_tested(
        canonical_conn, "local.historic", version["version"], {"ok": True})
    admin.approve(
        canonical_conn, "local.historic", version["version"],
        approved_by="pytest")
    create_pipeline(canonical_conn, name="historic_custom", spec={
        "schema": "windex.pipeline/1",
        "parameters": [],
        "state": {},
        "flows": {
            "run": {
                "inputs": [{"id": "documents", "type": "DocumentBatch"}],
                "outputs": [{"id": "result", "type": "ExtractedDoc"}],
                "nodes": {
                    "receive": {
                        "kind": "receive", "uses": "push.docs", "with": {}},
                    "custom": {
                        "kind": "transform", "uses": "local.historic", "with": {}},
                },
                "edges": [
                    {
                        "from": {"input": "documents"},
                        "to": {"node": "receive"},
                    },
                    {
                        "from": {"node": "receive"},
                        "to": {"node": "custom"},
                    },
                    {
                        "from": {"node": "custom"},
                        "to": {"output": "result"},
                    },
                ],
            },
        },
        "refresh": [],
    })
    run_id = submit_pipeline(
        canonical_conn, "historic_custom", version=1, flow="run",
        inputs={"documents": {"documents": []}}, parameters={})
    admin.revoke(
        canonical_conn, "local.historic", version["version"], by="pytest")
    with pytest.raises(RunConflictError, match="module_revoked"):
        rerun(canonical_conn, run_id)
