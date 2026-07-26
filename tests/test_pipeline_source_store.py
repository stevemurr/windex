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
from windex.pipeline.bootstrap import seed_canonical
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
    get_source,
    get_operator_settings,
    delete_operator_setting,
    patch_settings,
    patch_operator_settings,
    settings_projection,
    module_statuses,
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
    with canonical_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM operational_events "
            "WHERE pipeline_name = 'hn'")
        events_before = cur.fetchone()[0]
    revision = publish_revision(
        canonical_conn, "hn", pipeline["spec"],
        expected_version=1, expected_hash=pipeline["spec_hash"])
    assert revision.action == "noop"
    assert revision.revision["version"] == 1
    unchanged = get_pipeline(canonical_conn, "hn")
    assert unchanged["head_revision_id"] == pipeline["head_revision_id"]
    assert unchanged["updated_at"] == pipeline["updated_at"]
    with canonical_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM operational_events "
            "WHERE pipeline_name = 'hn'")
        assert cur.fetchone()[0] == events_before

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

    published = [
        item for item in outcomes
        if not isinstance(item, Exception)
    ]
    rejected = [item for item in outcomes if isinstance(item, Exception)]
    assert len(published) == 1
    assert published[0].action == "created"
    assert published[0].revision["version"] == 2
    assert len(rejected) == 1
    assert isinstance(rejected[0], StalePipelineError)
    assert get_pipeline(canonical_conn, "hn")["version"] == 2
    assert [r["version"] for r in list_revisions(canonical_conn, "hn")] == [2, 1]


def test_existing_semantic_revision_becomes_head_without_rewriting_history_or_sources(
    canonical_conn,
):
    pipeline_v1 = get_pipeline(canonical_conn, "hn")
    source_v1 = get_source(canonical_conn, "hn")
    assert pipeline_v1 and source_v1

    layout_v1 = get_layout(canonical_conn, "hn", 1, "harvest")
    assert layout_v1
    annotated_layout = {
        **layout_v1["layout"],
        "annotations": [{"text": "version-one-layout"}],
    }
    saved_layout = put_layout(
        canonical_conn,
        "hn",
        1,
        "harvest",
        annotated_layout,
        if_match=layout_v1["etag"],
    )

    spec_v2 = copy.deepcopy(pipeline_v1["spec"])
    spec_v2["parameters"].append({
        "key": "revision_two_marker",
        "kind": "str",
        "default": "two",
    })
    published_v2 = publish_revision(
        canonical_conn,
        "hn",
        spec_v2,
        expected_version=1,
        expected_hash=pipeline_v1["spec_hash"],
    )
    assert published_v2.action == "created"
    revision_v2 = published_v2.revision
    assert revision_v2["version"] == 2

    preview = upgrade_preview(
        canonical_conn,
        "hn",
        2,
        values=source_v1["values"],
    )
    assert preview["valid"] is True
    assert preview["confirmation_token"]
    upgraded = upgrade(
        canonical_conn,
        "hn",
        2,
        source_v1["values"],
        preview["confirmation_token"],
    )
    assert upgraded["pipeline_version"] == 2

    rollback = publish_revision(
        canonical_conn,
        "hn",
        pipeline_v1["spec"],
        expected_version=2,
        expected_hash=revision_v2["spec_hash"],
    )

    assert rollback.action == "rollback"
    assert rollback.revision["id"] == pipeline_v1["head_revision_id"]
    assert rollback.revision["version"] == 1
    head = get_pipeline(canonical_conn, "hn")
    assert head["head_revision_id"] == pipeline_v1["head_revision_id"]
    assert head["version"] == 1
    assert head["spec_hash"] == pipeline_v1["spec_hash"]
    assert [item["version"] for item in list_revisions(canonical_conn, "hn")] == [2, 1]
    # Source deployments are immutable revision pins, independent of Pipeline head.
    assert get_source(canonical_conn, "hn")["pipeline_revision_id"] == revision_v2["id"]
    assert get_source(canonical_conn, "hn")["pipeline_version"] == 2
    assert get_layout(canonical_conn, "hn", 1, "harvest") == {
        "flow": "harvest",
        "layout": annotated_layout,
        "etag": saved_layout["etag"],
        "updated_at": saved_layout["updated_at"],
    }

    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT event, pipeline_version, data
                 FROM operational_events
                WHERE pipeline_name = 'hn'
                ORDER BY seq""",
        )
        events = cur.fetchall()
    assert [item[0] for item in events] == [
        "pipeline.revision_published",
        "pipeline.head_rolled_back",
    ]
    assert events[-1][1] == 1
    assert events[-1][2]["from_version"] == 2
    assert events[-1][2]["to_version"] == 1

    spec_v3 = copy.deepcopy(pipeline_v1["spec"])
    spec_v3["parameters"].append({
        "key": "revision_three_marker",
        "kind": "str",
        "default": "three",
    })
    published_v3 = publish_revision(
        canonical_conn,
        "hn",
        spec_v3,
        expected_version=1,
        expected_hash=pipeline_v1["spec_hash"],
    )
    assert published_v3.action == "created"
    assert published_v3.revision["version"] == 3
    assert (
        published_v3.revision["parent_revision_id"]
        == pipeline_v1["head_revision_id"]
    )
    assert [item["version"] for item in list_revisions(canonical_conn, "hn")] == [3, 2, 1]
    assert (
        get_layout(canonical_conn, "hn", 3, "harvest")["layout"]["nodes"]
        == annotated_layout["nodes"]
    )
    assert get_source(canonical_conn, "hn")["pipeline_revision_id"] == revision_v2["id"]


def test_concurrent_semantic_rollbacks_honor_the_original_head_guard(
    canonical_conn,
):
    pipeline_v1 = get_pipeline(canonical_conn, "hn")
    assert pipeline_v1
    spec_v2 = copy.deepcopy(pipeline_v1["spec"])
    spec_v2["parameters"].append({
        "key": "rollback_race_marker",
        "kind": "str",
        "default": "two",
    })
    revision_v2 = publish_revision(
        canonical_conn,
        "hn",
        spec_v2,
        expected_version=1,
        expected_hash=pipeline_v1["spec_hash"],
    ).revision

    environment = Settings()
    dsn = (
        environment.pg_dsn.rsplit("/", 1)[0]
        + "/"
        + canonical_conn.info.dbname
    )
    gate = threading.Barrier(2)

    def rollback() -> object:
        conn = psycopg.connect(dsn)
        try:
            gate.wait(timeout=10)
            return publish_revision(
                conn,
                "hn",
                pipeline_v1["spec"],
                expected_version=2,
                expected_hash=revision_v2["spec_hash"],
            )
        except Exception as exc:
            return exc
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _index: rollback(), range(2)))

    accepted = [
        item for item in outcomes
        if not isinstance(item, Exception)
    ]
    rejected = [item for item in outcomes if isinstance(item, Exception)]
    assert len(accepted) == 1
    assert accepted[0].action == "rollback"
    assert accepted[0].revision["version"] == 1
    assert len(rejected) == 1
    assert isinstance(rejected[0], StalePipelineError)
    assert get_pipeline(canonical_conn, "hn")["version"] == 1
    assert [item["version"] for item in list_revisions(canonical_conn, "hn")] == [2, 1]


def test_bootstrap_can_reactivate_an_older_semantic_revision_without_moving_sources(
    canonical_conn,
    monkeypatch,
):
    from windex.pipeline import registry

    initial = get_pipeline(canonical_conn, "hn")
    source = get_source(canonical_conn, "hn")
    assert initial and source
    implementation_digest = registry.implementation_digest
    monkeypatch.setattr(
        registry,
        "implementation_digest",
        lambda name: (
            "sha256:changed-hn-extractor"
            if name == "algolia.hn_stories"
            else implementation_digest(name)
        ),
    )

    changed = seed_canonical(canonical_conn)
    changed_head = get_pipeline(canonical_conn, "hn")
    assert changed_head["version"] == 2
    assert changed_head["spec_hash"] != initial["spec_hash"]
    assert {"pipeline": "hn", "action": "revised"} in changed["actions"]
    assert get_source(canonical_conn, "hn")["pipeline_revision_id"] == (
        source["pipeline_revision_id"]
    )

    monkeypatch.setattr(registry, "implementation_digest", implementation_digest)
    restored = seed_canonical(canonical_conn)

    assert {"pipeline": "hn", "action": "revised"} in restored["actions"]
    assert get_pipeline(canonical_conn, "hn")["head_revision_id"] == (
        initial["head_revision_id"]
    )
    assert get_pipeline(canonical_conn, "hn")["version"] == 1
    assert [item["version"] for item in list_revisions(canonical_conn, "hn")] == [2, 1]
    assert get_source(canonical_conn, "hn")["pipeline_revision_id"] == (
        source["pipeline_revision_id"]
    )


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
    assert revision.action == "created"
    assert revision.revision["version"] == 2

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
