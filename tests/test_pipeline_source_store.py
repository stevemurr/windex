from __future__ import annotations

import copy
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext

import psycopg
import pytest
from fastapi.testclient import TestClient

from windex.api.app import admin
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
    create_source,
    create_trigger,
    delete_setting,
    get_source,
    get_operator_settings,
    delete_operator_setting,
    list_sources,
    list_triggers,
    patch_settings,
    patch_operator_settings,
    settings_projection,
    status,
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


def _create_settings_source(canonical_conn) -> str:
    suffix = uuid.uuid4().hex[:10]
    name = f"settings_{suffix}"
    template = get_pipeline(canonical_conn, "hn")
    spec = copy.deepcopy(template["spec"])
    spec["parameters"].extend([
        {
            "key": "optional_label",
            "kind": "str",
        },
        {
            "key": "resettable_limit",
            "kind": "int",
            "lo": 1,
            "hi": 10,
            "default": 4,
        },
        {
            "key": "preserved_label",
            "kind": "str",
        },
        {
            "key": "private_label",
            "kind": "str",
            "secret": True,
        },
    ])
    create_pipeline(
        canonical_conn,
        name=name,
        spec=spec,
        title="Source settings deletion fixture",
    )
    create_source(canonical_conn, {
        "name": name,
        "pipeline_name": name,
        "pipeline_version": 1,
        "search_name": name,
        "id_prefix": f"{name}:",
        "collection_key": name,
        "search_profile": "hn",
        "state_namespace": name,
        "values": {
            "optional_label": "  remove me  ",
            "resettable_limit": 9,
            "preserved_label": "  keep me  ",
            "private_label": "  hidden value  ",
        },
    })
    return name


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


def test_delete_setting_exactly_removes_or_resets_without_losing_values(
    canonical_conn,
):
    name = _create_settings_source(canonical_conn)
    original = settings_projection(canonical_conn, name)
    assert original["values"]["optional_label"] == "remove me"
    assert original["values"]["preserved_label"] == "keep me"
    assert "private_label" not in original["values"]

    removed = delete_setting(
        canonical_conn,
        name,
        "optional_label",
        if_match=original["etag"],
    )

    assert removed["etag"] != original["etag"]
    assert "optional_label" not in removed["values"]
    optional = next(
        field for field in removed["fields"]
        if field["key"] == "optional_label"
    )
    assert optional["origin"] == "unset"
    assert optional["value"] is None
    stored = get_source(canonical_conn, name)
    assert "optional_label" not in stored["values"]
    assert stored["values"]["preserved_label"] == "keep me"
    assert stored["values"]["private_label"] == "hidden value"

    reset = delete_setting(
        canonical_conn,
        name,
        "resettable_limit",
        if_match=removed["etag"],
    )

    assert reset["values"]["resettable_limit"] == 4
    assert reset["values"]["preserved_label"] == "keep me"
    assert "optional_label" not in reset["values"]
    stored = get_source(canonical_conn, name)
    assert "optional_label" not in stored["values"]
    assert stored["values"]["private_label"] == "hidden value"
    private = next(
        field for field in reset["fields"]
        if field["key"] == "private_label"
    )
    assert private["value"] is None
    assert private["secret_set"] is True

    with pytest.raises(StaleSourceError):
        delete_setting(
            canonical_conn,
            name,
            "preserved_label",
            if_match=original["etag"],
        )
    assert get_source(canonical_conn, name)["values"]["preserved_label"] == "keep me"

    with pytest.raises(ValueError, match="unknown Pipeline parameter"):
        delete_setting(
            canonical_conn,
            name,
            "not_declared",
            if_match=reset["etag"],
        )


def test_source_setting_delete_api_uses_etag_and_exact_replacement(
    canonical_conn,
    monkeypatch,
):
    from windex.api import app as app_module
    from windex.api import canonical

    name = _create_settings_source(canonical_conn)
    dsn = (
        Settings().pg_dsn.rsplit("/", 1)[0]
        + "/"
        + canonical_conn.info.dbname
    )
    settings = Settings(
        _env_file=None,
        pg_dsn=dsn,
        write_token="",
        serve_host="127.0.0.1",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(canonical, "get_settings", lambda: settings)
    monkeypatch.setattr(
        canonical.db,
        "pooled",
        lambda _dsn: nullcontext(canonical_conn),
    )
    client = TestClient(admin)

    original = client.get(f"/v1/sources/{name}/settings").json()
    response = client.delete(
        f"/v1/sources/{name}/settings/optional_label",
        headers={"If-Match": f'"{original["etag"]}"'},
    )
    assert response.status_code == 200
    removed = response.json()
    assert "optional_label" not in removed["values"]
    assert removed["values"]["preserved_label"] == "keep me"

    stale = client.delete(
        f"/v1/sources/{name}/settings/preserved_label",
        headers={"If-Match": f'"{original["etag"]}"'},
    )
    assert stale.status_code == 412
    assert stale.json()["detail"] == "Source settings ETag is stale"

    response = client.delete(
        f"/v1/sources/{name}/settings/resettable_limit",
        headers={"If-Match": f'"{removed["etag"]}"'},
    )
    assert response.status_code == 200
    reset = response.json()
    assert reset["values"]["resettable_limit"] == 4
    assert reset["values"]["preserved_label"] == "keep me"
    assert "optional_label" not in reset["values"]

    unknown = client.delete(
        f"/v1/sources/{name}/settings/not_declared",
        headers={"If-Match": f'"{reset["etag"]}"'},
    )
    assert unknown.status_code == 422
    assert unknown.json()["detail"] == (
        "unknown Pipeline parameter 'not_declared'"
    )
    stored = get_source(canonical_conn, name)
    assert stored["values"]["preserved_label"] == "keep me"
    assert stored["values"]["private_label"] == "hidden value"


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


def test_source_status_selects_newest_active_run_across_flows(
    canonical_conn,
):
    running = submit_source(
        canonical_conn, "docs", flow="sync", dedupe=False)
    blocked = submit_source(
        canonical_conn, "docs", flow="ingest", dedupe=False)
    queued = submit_source(
        canonical_conn, "docs", flow="sync", dedupe=False)
    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE runs
                  SET state = CASE id
                        WHEN %s THEN 'running'
                        WHEN %s THEN 'blocked'
                        ELSE 'queued'
                      END,
                      cancel_requested = (id = %s),
                      started_at = CASE WHEN id = %s THEN now() ELSE NULL END,
                      updated_at = now()
                WHERE id = ANY(%s)""",
            (running, blocked, blocked, running, [running, blocked, queued]),
        )
    canonical_conn.commit()

    projection = status(canonical_conn, "docs")
    assert projection["latest_run"]["id"] == queued
    assert projection["current_run"]["id"] == queued
    assert projection["current_run"]["state"] == "queued"
    assert projection["current_run"]["cancel_requested"] is False

    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE runs
                  SET state = 'succeeded', started_at = now(),
                      finished_at = now(), updated_at = now()
                WHERE id = %s""",
            (queued,),
        )
    canonical_conn.commit()
    projection = status(canonical_conn, "docs")
    assert projection["latest_run"]["id"] == queued
    assert projection["latest_run"]["state"] == "succeeded"
    assert projection["current_run"]["id"] == blocked
    assert projection["current_run"]["state"] == "blocked"
    assert projection["current_run"]["cancel_requested"] is True
    assert projection["last_success"] == projection["latest_run"]["finished_at"]

    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE runs
                  SET state = 'cancelled', finished_at = now(), updated_at = now()
                WHERE id = %s""",
            (blocked,),
        )
    canonical_conn.commit()
    projection = status(canonical_conn, "docs")
    assert projection["current_run"]["id"] == running
    assert projection["current_run"]["state"] == "running"


def test_source_status_api_keeps_older_active_run_visible(
    canonical_conn,
    monkeypatch,
):
    from windex.api import app as app_module
    from windex.api import canonical

    running = submit_source(
        canonical_conn, "docs", flow="sync", dedupe=False)
    failed = submit_source(
        canonical_conn, "docs", flow="ingest", dedupe=False)
    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE runs
                  SET state = 'running', started_at = now(), updated_at = now()
                WHERE id = %s""",
            (running,),
        )
        cur.execute(
            """UPDATE runs
                  SET state = 'failed', started_at = now(),
                      finished_at = now(), error = 'ingest failed',
                      updated_at = now()
                WHERE id = %s""",
            (failed,),
        )
    canonical_conn.commit()

    dsn = (
        Settings().pg_dsn.rsplit("/", 1)[0]
        + "/"
        + canonical_conn.info.dbname
    )
    settings = Settings(
        _env_file=None,
        pg_dsn=dsn,
        write_token="",
        serve_host="127.0.0.1",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(canonical, "get_settings", lambda: settings)
    monkeypatch.setattr(
        canonical.db,
        "pooled",
        lambda _dsn: nullcontext(canonical_conn),
    )
    client = TestClient(admin)

    response = client.get("/v1/sources/docs/status")
    assert response.status_code == 200
    projection = response.json()
    assert projection["latest_run"]["id"] == failed
    assert projection["latest_run"]["state"] == "failed"
    assert projection["current_run"]["id"] == running
    assert projection["current_run"]["state"] == "running"
    assert projection["recent_error"] == "ingest failed"
    assert projection["last_failure"] == projection["latest_run"]["finished_at"]

    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE runs
                  SET state = 'cancelled', cancel_requested = true,
                      finished_at = now(), updated_at = now()
                WHERE id = %s""",
            (running,),
        )
    canonical_conn.commit()

    response = client.get("/v1/sources/docs/status")
    assert response.status_code == 200
    projection = response.json()
    assert projection["latest_run"]["id"] == failed
    assert projection["current_run"] is None
    assert projection["recent_error"] == "ingest failed"


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


def test_source_ready_tracks_builtin_digest_relock(
    canonical_conn, monkeypatch,
):
    from windex.pipeline import registry

    pipeline = get_pipeline(canonical_conn, "memory")
    source = get_source(canonical_conn, "memory")
    assert pipeline and source
    assert source["ready"] is True
    assert next(
        item for item in list_sources(canonical_conn)
        if item["name"] == "memory"
    )["ready"] is True

    implementation_digest = registry.implementation_digest
    monkeypatch.setattr(
        registry,
        "implementation_digest",
        lambda name: (
            "sha256:replacement-push-docs"
            if name == "push.docs"
            else implementation_digest(name)
        ),
    )

    assert get_source(canonical_conn, "memory")["ready"] is False
    assert next(
        item for item in list_sources(canonical_conn)
        if item["name"] == "memory"
    )["ready"] is False
    assert next(
        item for item in module_statuses(canonical_conn)
        if item["source"] == "memory"
    )["available"] is False

    publication = publish_revision(
        canonical_conn,
        "memory",
        pipeline["spec"],
        expected_version=pipeline["version"],
        expected_hash=pipeline["spec_hash"],
    )
    assert publication.action == "created"
    assert publication.revision["version"] == 2
    preview = upgrade_preview(canonical_conn, "memory", 2)
    upgraded = upgrade(
        canonical_conn,
        "memory",
        2,
        preview["candidate"],
        preview["confirmation_token"],
    )

    assert upgraded["ready"] is True
    assert get_source(canonical_conn, "memory")["ready"] is True
    assert next(
        item for item in module_statuses(canonical_conn)
        if item["source"] == "memory"
    )["available"] is True


def test_source_api_ready_matches_get_list_and_archived_projection(
    canonical_conn, monkeypatch,
):
    from windex.api import app as app_module
    from windex.api import canonical
    from windex.pipeline import registry
    from windex.source.store import archive

    implementation_digest = registry.implementation_digest
    monkeypatch.setattr(
        registry,
        "implementation_digest",
        lambda name: (
            "sha256:replacement-push-docs"
            if name == "push.docs"
            else implementation_digest(name)
        ),
    )
    dsn = (
        Settings().pg_dsn.rsplit("/", 1)[0]
        + "/"
        + canonical_conn.info.dbname
    )
    settings = Settings(
        _env_file=None,
        pg_dsn=dsn,
        write_token="",
        serve_host="127.0.0.1",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(canonical, "get_settings", lambda: settings)
    monkeypatch.setattr(
        canonical.db,
        "pooled",
        lambda _dsn: nullcontext(canonical_conn),
    )
    client = TestClient(admin)

    detail = client.get("/v1/sources/memory")
    listing = client.get("/v1/sources")
    assert detail.status_code == 200
    assert listing.status_code == 200
    assert detail.json()["ready"] is False
    assert next(
        item for item in listing.json()["sources"]
        if item["name"] == "memory"
    )["ready"] is False

    assert archive(canonical_conn, "memory") is True
    active = client.get("/v1/sources").json()["sources"]
    archived = client.get(
        "/v1/sources", params={"include_archived": True},
    ).json()["sources"]
    assert all(item["name"] != "memory" for item in active)
    assert next(
        item for item in archived if item["name"] == "memory"
    )["ready"] is False


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
    ).revision


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
