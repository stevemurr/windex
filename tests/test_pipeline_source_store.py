from __future__ import annotations

import copy
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from windex.config import Settings, effective_settings, invalidate_overrides
from windex.api.canonical import UpgradePreviewResponse
from windex.db.canonical import init_canonical_db
from windex.pipeline.run_store import get_run, submit_source
from windex.pipeline.store import (
    PipelinePreconditionRequiredError,
    StalePipelineError,
    create_pipeline,
    get_layout,
    get_pipeline,
    list_revisions,
    list_pipelines,
    publish_revision,
    put_layout,
)
from windex.source.store import (
    SourceConflictError,
    StaleSourceError,
    create_trigger,
    get_source,
    get_operator_settings,
    delete_operator_setting,
    list_triggers,
    patch_settings,
    patch_operator_settings,
    settings_projection,
    module_statuses,
    update_trigger,
    upgrade,
    upgrade_preview,
)


@pytest.fixture
def canonical_conn():
    settings = Settings()
    admin = psycopg.connect(settings.pg_dsn, autocommit=True)
    name = "windex_pipeline_source_" + uuid.uuid4().hex[:10]
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}"')
    admin.close()
    parts = settings.pg_dsn.rsplit("/", 1)
    conn = psycopg.connect(parts[0] + "/" + name)
    try:
        init_canonical_db(conn, bootstrap_id="pipeline-source-test")
        yield conn
    finally:
        conn.close()
        admin = psycopg.connect(settings.pg_dsn, autocommit=True)
        with admin.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s", (name,))
            cur.execute(f'DROP DATABASE "{name}"')
        admin.close()


def test_bootstrap_and_layout_are_deterministic(canonical_conn):
    assert len(list_pipelines(canonical_conn)) == 11
    pipeline = get_pipeline(canonical_conn, "hn")
    assert pipeline and pipeline["version"] == 1
    layout = get_layout(canonical_conn, "hn", 1, "harvest")
    assert layout
    moved = {**layout["layout"], "annotations": [{"text": "note"}]}
    saved = put_layout(
        canonical_conn, "hn", 1, "harvest", moved, if_match=layout["etag"])
    assert saved["etag"] != layout["etag"]
    assert get_pipeline(canonical_conn, "hn")["spec_hash"] == pipeline["spec_hash"]


def test_noop_publish_and_stale_settings(canonical_conn):
    pipeline = get_pipeline(canonical_conn, "hn")
    revision = publish_revision(
        canonical_conn, "hn", pipeline["spec"],
        expected_version=1, expected_hash=pipeline["spec_hash"])
    assert revision["version"] == 1

    source = get_source(canonical_conn, "hn")
    assert source and source["pipeline_name"] == "hn"
    settings = settings_projection(canonical_conn, "hn")
    updated = patch_settings(
        canonical_conn, "hn", {"incremental_days": 3}, if_match=settings["etag"])
    assert updated["etag"] != settings["etag"]
    with pytest.raises(StaleSourceError):
        patch_settings(
            canonical_conn, "hn", {"incremental_days": 4}, if_match=settings["etag"])


def test_initial_pipeline_publication_needs_no_parent_guard(canonical_conn):
    template = get_pipeline(canonical_conn, "hn")
    created = create_pipeline(
        canonical_conn,
        name="initial-" + uuid.uuid4().hex[:8],
        spec=template["spec"],
        title="Initial publication",
    )

    assert created["version"] == 1
    assert created["head_revision_id"] is not None


def test_existing_pipeline_publication_requires_parent_guard(canonical_conn):
    pipeline = get_pipeline(canonical_conn, "hn")

    with pytest.raises(
        PipelinePreconditionRequiredError,
        match="requires parent_version, parent_hash, or If-Match",
    ):
        publish_revision(canonical_conn, "hn", pipeline["spec"])


def test_two_writers_based_on_same_pipeline_head_reject_stale_writer(
    canonical_conn,
):
    pipeline = get_pipeline(canonical_conn, "hn")
    environment = Settings()
    dsn = (
        environment.pg_dsn.rsplit("/", 1)[0]
        + "/"
        + canonical_conn.info.dbname
    )
    gate = threading.Barrier(2)

    def candidate(suffix: str) -> dict:
        spec = copy.deepcopy(pipeline["spec"])
        spec["parameters"].append({
            "key": f"writer_{suffix}",
            "kind": "str",
            "default": suffix,
        })
        return spec

    def publish(suffix: str) -> dict | Exception:
        conn = psycopg.connect(dsn)
        try:
            gate.wait(timeout=10)
            return publish_revision(
                conn,
                "hn",
                candidate(suffix),
                expected_version=pipeline["version"],
                expected_hash=pipeline["spec_hash"],
            )
        except Exception as exc:
            return exc
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(publish, ("alpha", "beta")))

    published = [item for item in outcomes if isinstance(item, dict)]
    rejected = [item for item in outcomes if isinstance(item, Exception)]
    assert len(published) == 1
    assert published[0]["version"] == 2
    assert len(rejected) == 1
    assert isinstance(rejected[0], StalePipelineError)
    assert get_pipeline(canonical_conn, "hn")["version"] == 2
    assert [r["version"] for r in list_revisions(canonical_conn, "hn")] == [2, 1]


def test_source_run_freezes_binding_and_index_continuation(canonical_conn):
    run_id = submit_source(canonical_conn, "hn", dedupe=False)
    run = get_run(canonical_conn, run_id, include_spec=True)
    assert run["source_snapshot"]["search_name"] == "hn"
    assert run["pipeline_name"] == "hn"
    assert run["tasks"][-1]["node"] == "__index__"
    assert run["tasks"][-1]["depends_on"]


def test_source_status_surfaces_unavailable_module_lock(
    canonical_conn, monkeypatch,
):
    from windex.pipeline import registry

    healthy = next(
        item for item in module_statuses(canonical_conn)
        if item["source"] == "memory")
    assert healthy["available"] is True
    assert healthy["upgrade_required"] is False

    implementation_digest = registry.implementation_digest
    monkeypatch.setattr(
        registry,
        "implementation_digest",
        lambda name: (
            "sha256:changed"
            if name == "push.docs"
            else implementation_digest(name)
        ),
    )

    degraded = next(
        item for item in module_statuses(canonical_conn)
        if item["source"] == "memory")
    assert degraded["available"] is False
    assert degraded["upgrade_required"] is True
    assert degraded["unavailable_modules"] == ["push.docs"]


def test_multiflow_source_run_selects_only_its_flow_locks(canonical_conn):
    source = get_source(canonical_conn, "docs")
    assert source and source["values"]["slugs"] is None

    run_id = submit_source(canonical_conn, "docs", flow="sync", dedupe=False)
    run = get_run(canonical_conn, run_id, include_spec=True)
    assert all(task["node"] != "__index__" for task in run["tasks"])
    task_modules = {
        task["module"]
        for task in run["tasks"]
    }

    assert task_modules == {
        "static.once", "http.download", "list.json_manifest", "store.upsert",
    }
    assert set(run["module_locks"]) == task_modules


def test_source_upgrade_accepts_edited_candidate_and_is_atomic(canonical_conn):
    pipeline = get_pipeline(canonical_conn, "hn")
    source = get_source(canonical_conn, "hn")
    assert pipeline and source

    spec = copy.deepcopy(pipeline["spec"])
    spec["parameters"].append({
        "key": "install_profile",
        "kind": "str",
        "required": True,
        "stage": "install",
    })
    revision = publish_revision(
        canonical_conn,
        "hn",
        spec,
        expected_version=pipeline["version"],
        expected_hash=pipeline["spec_hash"],
    )
    assert revision["version"] == 2

    incomplete = upgrade_preview(canonical_conn, "hn", 2)
    UpgradePreviewResponse.model_validate(incomplete)
    assert incomplete["valid"] is False
    assert incomplete["confirmation_token"] is None
    assert incomplete["missing"] == ["install_profile"]

    edited_values = {
        **source["values"],
        "install_profile": "standard",
    }
    preview = upgrade_preview(
        canonical_conn,
        "hn",
        2,
        values=edited_values,
    )
    UpgradePreviewResponse.model_validate(preview)
    assert preview["valid"] is True
    assert preview["candidate"]["install_profile"] == "standard"
    assert preview["install_stage_changed"] == ["install_profile"]
    assert preview["confirmation_token"]

    with pytest.raises(SourceConflictError):
        upgrade(
            canonical_conn,
            "hn",
            2,
            {**edited_values, "install_profile": "tampered"},
            preview["confirmation_token"],
        )
    unchanged = get_source(canonical_conn, "hn")
    assert unchanged["pipeline_version"] == 1
    assert "install_profile" not in unchanged["values"]

    upgraded = upgrade(
        canonical_conn,
        "hn",
        2,
        edited_values,
        preview["confirmation_token"],
    )
    assert upgraded["pipeline_version"] == 2
    assert upgraded["values"]["install_profile"] == "standard"


def _publish_hn_upgrade_target(
    conn,
    *,
    target_flow: str = "harvest",
):
    pipeline = get_pipeline(conn, "hn")
    assert pipeline
    spec = copy.deepcopy(pipeline["spec"])
    if target_flow != "harvest":
        flow = spec["flows"].pop("harvest")
        spec["flows"][target_flow] = flow
        spec["refresh"] = [
            target_flow if name == "harvest" else name
            for name in spec["refresh"]
        ]
    else:
        spec["parameters"].append({
            "key": "upgrade_marker",
            "kind": "str",
            "default": "compatible",
        })
    return publish_revision(
        conn,
        "hn",
        spec,
        expected_version=pipeline["version"],
        expected_hash=pipeline["spec_hash"],
    )


def _interval_trigger(conn, *, enabled: bool = True):
    return create_trigger(conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 60},
        "enabled": enabled,
    })


def test_upgrade_preview_accepts_trigger_compatible_target(canonical_conn):
    trigger = _interval_trigger(canonical_conn)
    revision = _publish_hn_upgrade_target(canonical_conn)

    preview = upgrade_preview(canonical_conn, "hn", revision["version"])

    UpgradePreviewResponse.model_validate(preview)
    assert preview["valid"] is True
    assert preview["issues"] == []
    assert preview["state_impact"]["trigger_bindings_checked"] == 1
    assert (
        preview["state_impact"]["trigger_bindings_policy"]
        == "all_enabled_and_disabled"
    )
    assert preview["state_impact"]["trigger_bindings_hash"].startswith("sha256:")
    assert list_triggers(canonical_conn, "hn")[0]["id"] == trigger["id"]


def test_upgrade_preview_rejects_removed_trigger_flow(canonical_conn):
    trigger = _interval_trigger(canonical_conn)
    revision = _publish_hn_upgrade_target(
        canonical_conn,
        target_flow="replacement",
    )

    preview = upgrade_preview(canonical_conn, "hn", revision["version"])

    UpgradePreviewResponse.model_validate(preview)
    assert preview["valid"] is False
    assert preview["confirmation_token"] is None
    assert preview["issues"] == [{
        "path": f"triggers.{trigger['id']}.flow_name",
        "code": "trigger_flow_missing",
        "severity": "error",
        "message": (
            f"Enabled trigger {trigger['id']} references Flow 'harvest', "
            f"which target revision {revision['version']} does not define. "
            "Rebind or delete the trigger before upgrading."
        ),
    }]
    with pytest.raises(SourceConflictError) as raised:
        upgrade(
            canonical_conn,
            "hn",
            revision["version"],
            preview["candidate"],
            "not-a-valid-token",
        )
    assert raised.value.args[0]["issues"] == preview["issues"]
    assert get_source(canonical_conn, "hn")["pipeline_version"] == 1


def test_upgrade_preview_also_rejects_disabled_trigger_with_removed_flow(
    canonical_conn,
):
    trigger = _interval_trigger(canonical_conn, enabled=False)
    revision = _publish_hn_upgrade_target(
        canonical_conn,
        target_flow="replacement",
    )

    preview = upgrade_preview(canonical_conn, "hn", revision["version"])

    assert preview["valid"] is False
    assert preview["confirmation_token"] is None
    issue = preview["issues"][0]
    assert issue["path"] == f"triggers.{trigger['id']}.flow_name"
    assert issue["code"] == "trigger_flow_missing"
    assert issue["message"].startswith(
        f"Disabled trigger {trigger['id']} references Flow 'harvest'")
    assert "can be re-enabled" in issue["message"]


def test_trigger_edit_after_upgrade_preview_invalidates_confirmation(
    canonical_conn,
):
    trigger = _interval_trigger(canonical_conn)
    revision = _publish_hn_upgrade_target(canonical_conn)
    preview = upgrade_preview(canonical_conn, "hn", revision["version"])
    assert preview["valid"] is True
    token = preview["confirmation_token"]
    assert token

    environment = Settings()
    dsn = (
        environment.pg_dsn.rsplit("/", 1)[0]
        + "/"
        + canonical_conn.info.dbname
    )
    other = psycopg.connect(dsn)
    try:
        update_trigger(
            other,
            "hn",
            trigger["id"],
            {"trigger_spec": {"seconds": 120}},
        )
    finally:
        other.close()

    with pytest.raises(
        SourceConflictError,
        match="upgrade preview is stale",
    ):
        upgrade(
            canonical_conn,
            "hn",
            revision["version"],
            preview["candidate"],
            token,
        )
    assert get_source(canonical_conn, "hn")["pipeline_version"] == 1
    assert list_triggers(canonical_conn, "hn")[0]["trigger_spec"] == {
        "seconds": 120,
    }


def test_source_upgrade_preserves_compatible_trigger(canonical_conn):
    trigger = _interval_trigger(canonical_conn)
    revision = _publish_hn_upgrade_target(canonical_conn)
    preview = upgrade_preview(canonical_conn, "hn", revision["version"])
    token = preview["confirmation_token"]
    assert token

    upgraded = upgrade(
        canonical_conn,
        "hn",
        revision["version"],
        preview["candidate"],
        token,
    )

    assert upgraded["pipeline_version"] == revision["version"]
    assert list_triggers(canonical_conn, "hn") == [{
        **trigger,
        "trigger_spec": {"seconds": 60},
    }]


def test_operator_settings_use_canonical_store_and_runtime_cache(canonical_conn):
    environment = Settings()
    dsn = (
        environment.pg_dsn.rsplit("/", 1)[0]
        + "/"
        + canonical_conn.info.dbname
    )
    current = get_operator_settings(canonical_conn)
    assert current["scope"] == "_global"
    assert current["values"] == {}

    updated = patch_operator_settings(
        canonical_conn,
        {"embed_concurrency": 12},
        if_match=current["etag"],
    )
    assert updated["values"]["embed_concurrency"] == 12
    assert effective_settings(dsn=dsn).embed_concurrency == 12

    with pytest.raises(ValueError, match="not editable"):
        patch_operator_settings(
            canonical_conn,
            {"pg_dsn": "postgresql://attacker/other"},
            if_match=updated["etag"],
        )

    reverted = delete_operator_setting(
        canonical_conn,
        "embed_concurrency",
        if_match=updated["etag"],
    )
    assert reverted["values"] == {}
    assert effective_settings(
        dsn=dsn).embed_concurrency == environment.embed_concurrency
    invalidate_overrides(clear=True)
