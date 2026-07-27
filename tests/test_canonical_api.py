from contextlib import nullcontext

import pytest
from fastapi import HTTPException, Response
from fastapi.testclient import TestClient
from pydantic import ValidationError

from windex.api.app import admin, app
from windex.api.canonical import (
    IngestDocument,
    IngestRequest,
    ModuleHealthResponse,
    PipelineRevisionCreate,
    RegistryResponse,
    SourcePatch,
    module_health,
    pipeline_revision_publish,
    source_ingest,
)
from windex.pipeline import registry
from windex.pipeline.contracts import CONTRACT_EPOCH


def _push_source(*, max_documents: int = 10_000) -> dict:
    return {
        "origin": {"ingress": "push"},
        "search_name": "fixture",
        "spec": {
            "flows": {
                "receive": {
                    "nodes": {
                        "push": {
                            "uses": "push.docs",
                            "with": {
                                "mode": "delta",
                                "max_docs": max_documents,
                            },
                        },
                    },
                },
            },
        },
    }


@pytest.fixture
def ingest_api(monkeypatch):
    from windex.api import app as app_module
    from windex.api import canonical
    from windex.config import Settings

    settings = Settings(
        _env_file=None,
        write_token="",
        serve_host="127.0.0.1",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(canonical, "get_settings", lambda: settings)
    monkeypatch.setattr(
        canonical.db, "pooled", lambda _dsn: nullcontext(object()))
    monkeypatch.setattr(
        canonical.run_store, "submit_source",
        lambda *_args, **_kwargs: 42,
    )
    return TestClient(app), canonical


def _ingest(client: TestClient, **changes):
    body = {
        "mode": "delta",
        "documents": [{
            "id": "fixture/1",
            "url": "https://example.test/1",
            "text": "hello",
        }],
    }
    body.update(changes)
    return client.post(
        "/v1/sources/fixture/ingest",
        headers={"Idempotency-Key": "batch-0001"},
        json=body,
    )


def test_openapi_is_one_pipeline_source_contract_epoch():
    admin_schema = admin.openapi()
    public_schema = app.openapi()
    admin_paths = set(admin_schema["paths"])
    assert "/v1/pipelines" in admin_paths
    assert "/v1/sources" in admin_paths
    assert "/v1/runs" in admin_paths
    assert "/v1/overview" in admin_paths
    assert "/v1/module-health" in admin_paths
    assert "/v1/sources/{name}/module-status" in admin_paths
    assert "/v1/events/stream" in admin_paths
    assert "/v1/log-events/stream" in admin_paths
    assert "/v1/sources/{name}/ingest" in public_schema["paths"]
    encoded = str(admin_schema).lower()
    assert "recipe" not in encoded
    assert "marketplace" not in encoded

    schemas = admin_schema["components"]["schemas"]
    assert "metadata" in schemas["SourceCreate"]["properties"]
    assert "metadata" in schemas["SourcePatch"]["properties"]
    assert "metadata" in schemas["SourceModel"]["required"]
    assert "module_status" in schemas["SourceStatusResponse"]["properties"]
    assert schemas["PipelineRunCreate"]["properties"]["dry_run"] == {
        "default": False,
        "title": "Dry Run",
        "type": "boolean",
    }
    assert "values" in schemas["SourceUpgradePreviewRequest"]["properties"]
    assert "values" in schemas["SourceUpgradeRequest"]["required"]
    assert "candidate" in schemas["UpgradePreviewResponse"]["required"]
    assert "issues" in schemas["UpgradePreviewResponse"]["required"]
    publication_responses = admin_schema["paths"][
        "/v1/pipelines/{name}/revisions"
    ]["post"]["responses"]
    assert {"200", "201"} <= set(publication_responses)


def test_source_metadata_is_bounded_at_the_api_boundary():
    accepted = SourcePatch(metadata={"talkie": {"recipe": {"tool": "http.get"}}})
    assert accepted.metadata == {
        "talkie": {"recipe": {"tool": "http.get"}},
    }

    with pytest.raises(ValidationError, match="65536 encoded bytes"):
        SourcePatch(metadata={
            "talkie": {"recipe": {"program": "x" * (64 * 1024)}},
        })


def test_upgrade_preview_exposes_structured_trigger_flow_issue(monkeypatch):
    from windex.api import app as app_module
    from windex.api import canonical
    from windex.config import Settings

    settings = Settings(
        _env_file=None,
        write_token="",
        serve_host="127.0.0.1",
    )
    issue = {
        "path": "triggers.17.flow_name",
        "code": "trigger_flow_missing",
        "severity": "error",
        "message": (
            "Enabled trigger 17 references Flow 'harvest', which target "
            "revision 2 does not define. Rebind or delete the trigger before "
            "upgrading."
        ),
    }
    preview = {
        "source_id": 7,
        "from_version": 1,
        "target_version": 2,
        "target_hash": "sha256:target",
        "expected_etag": "sha256:config",
        "candidate_hash": "sha256:candidate",
        "candidate": {},
        "retained": {},
        "defaulted": {},
        "removed": [],
        "clamped": {},
        "missing": [],
        "install_stage_changed": [],
        "state_impact": {
            "stores_preserved": [],
            "requires_confirmation": False,
            "trigger_bindings_checked": 1,
            "trigger_bindings_policy": "all_enabled_and_disabled",
            "trigger_bindings_hash": "sha256:triggers",
        },
        "issues": [issue],
        "confirmation_token": None,
        "valid": False,
    }
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(canonical, "get_settings", lambda: settings)
    monkeypatch.setattr(
        canonical.db,
        "pooled",
        lambda _dsn: nullcontext(object()),
    )
    monkeypatch.setattr(
        canonical.source_store,
        "upgrade_preview",
        lambda *_args, **_kwargs: preview,
    )

    response = TestClient(admin).post(
        "/v1/sources/hn/upgrade/preview",
        json={"target_version": 2},
    )

    assert response.status_code == 200
    assert response.json()["valid"] is False
    assert response.json()["confirmation_token"] is None
    assert response.json()["issues"] == [issue]

    def reject_upgrade(*_args, **_kwargs):
        raise canonical.source_store.SourceConflictError({
            "message": "Source upgrade candidate is invalid",
            "issues": [issue],
        })

    monkeypatch.setattr(
        canonical.source_store,
        "upgrade",
        reject_upgrade,
    )
    response = TestClient(admin).post(
        "/v1/sources/hn/upgrade",
        json={
            "target_version": 2,
            "values": {},
            "confirmation_token": "stale",
        },
    )
    assert response.status_code == 409
    assert response.json()["detail"] == {
        "message": "Source upgrade candidate is invalid",
        "issues": [issue],
    }


def test_registry_response_is_fully_typed():
    response = RegistryResponse.model_validate(registry.describe())
    assert response.contract == "windex.registry/3"
    assert response.ports
    assert response.kinds
    assert response.modules
    assert all(module.implementation_digest for module in response.modules)
    assert all(module.fields is not None for module in response.modules)


def test_health_advertises_epoch_and_secure_module_channel(monkeypatch):
    from windex.api import app as app_module
    from windex.config import Settings

    settings = Settings(
        _env_file=None,
        write_token="",
        module_admin_token="separate-module-token",
        serve_host="127.0.0.1",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    client = TestClient(admin)
    response = client.get("/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["contract_epoch"] == CONTRACT_EPOCH
    assert body["supported_contract_epochs"] == [CONTRACT_EPOCH]
    assert body["capabilities"]["pipelines"] is True
    assert body["capabilities"]["sources"] is True
    assert body["capabilities"]["secure_module_upload"] is True


def test_module_health_reports_stranded_sources(monkeypatch):
    from windex.api import canonical

    monkeypatch.setattr(
        canonical.db, "pooled", lambda _dsn: nullcontext(object()))
    monkeypatch.setattr(
        canonical.source_store, "module_statuses",
        lambda _conn, enabled_only=False: [{
            "source": "memory",
            "pipeline_revision_id": 9,
            "pipeline_version": 1,
            "latest_pipeline_version": 2,
            "available": False,
            "upgrade_required": True,
            "unavailable_modules": ["push.docs"],
        }],
    )
    response = ModuleHealthResponse.model_validate(module_health())
    assert response.status == "degraded"
    assert response.stranded_sources == 1
    assert response.sources[0].source == "memory"
    assert response.sources[0].unavailable_modules == ["push.docs"]


def test_memory_identity_errors_are_rejected_before_queueing(monkeypatch):
    from windex.api import canonical

    source = {
        "origin": {"ingress": "push"},
        "search_name": "memory",
        "spec": {
            "flows": {
                "receive": {
                    "nodes": {
                        "push": {
                            "uses": "push.docs",
                            "with": {"mode": "full_set", "max_docs": 500},
                        },
                    },
                },
            },
        },
    }
    monkeypatch.setattr(
        canonical.db, "pooled", lambda _dsn: nullcontext(object()))
    monkeypatch.setattr(
        canonical.source_store, "get_source",
        lambda _conn, _name, include_spec=False: source,
    )
    monkeypatch.setattr(
        canonical.run_store, "submit_source",
        lambda *_args, **_kwargs: pytest.fail("invalid batch was queued"),
    )
    body = IngestRequest(
        mode="full",
        partition="conversation-a",
        documents=[IngestDocument(
            id="conversation-b/00000",
            url="llmchat://chat/conversation-b?chunk=0",
            text="wrong conversation",
            fields={
                "conversation_id": "conversation-b",
                "chunk_index": 0,
            },
        )],
    )
    with pytest.raises(HTTPException) as raised:
        source_ingest("memory", body, "batch-0001")
    assert raised.value.status_code == 422
    assert "does not match partition" in str(raised.value.detail)


def test_ingest_document_count_limit_remains_payload_too_large(
    ingest_api, monkeypatch,
):
    client, canonical = ingest_api
    monkeypatch.setattr(
        canonical.source_store, "get_source",
        lambda *_args, **_kwargs: _push_source(max_documents=1),
    )

    response = _ingest(client, documents=[
        {
            "id": "fixture/1",
            "url": "https://example.test/1",
            "text": "one",
        },
        {
            "id": "fixture/2",
            "url": "https://example.test/2",
            "text": "two",
        },
    ])

    assert response.status_code == 413
    assert response.json() == {
        "detail": "Source accepts at most 1 documents",
    }


def test_ingest_byte_limit_remains_payload_too_large(ingest_api, monkeypatch):
    client, canonical = ingest_api
    assert canonical.MAX_INGEST_TEXT_BYTES == 64 * 1024 * 1024
    monkeypatch.setattr(canonical, "MAX_INGEST_TEXT_BYTES", 3)

    response = _ingest(client, documents=[{
        "id": "fixture/1",
        "url": "https://example.test/1",
        "text": "four",
    }])

    assert response.status_code == 413
    assert response.json() == {"detail": "ingest payload exceeds 64 MiB"}


def test_ingest_request_validation_remains_unprocessable(ingest_api):
    client, _canonical = ingest_api

    response = _ingest(client, mode="replace")

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "mode"]


def test_ingest_missing_source_remains_not_found(ingest_api, monkeypatch):
    client, canonical = ingest_api
    monkeypatch.setattr(
        canonical.source_store, "get_source",
        lambda *_args, **_kwargs: None,
    )

    response = _ingest(client)

    assert response.status_code == 404
    assert response.json() == {"detail": "resource not found"}


def test_ingest_push_conflict_remains_conflict(ingest_api, monkeypatch):
    client, canonical = ingest_api
    source = _push_source()
    source["origin"] = {"ingress": "pull"}
    monkeypatch.setattr(
        canonical.source_store, "get_source",
        lambda *_args, **_kwargs: source,
    )

    response = _ingest(client)

    assert response.status_code == 409
    assert response.json() == {"detail": "Source is not push-rooted"}


def test_canonical_exception_mapping_preserves_response_metadata():
    from windex.api import canonical

    original = HTTPException(
        413,
        detail={"message": "bounded"},
        headers={"Retry-After": "9"},
    )
    with pytest.raises(HTTPException) as raised:
        canonical._raise(original)

    assert raised.value is original
    assert raised.value.detail == {"message": "bounded"}
    assert raised.value.headers == {"Retry-After": "9"}


def test_ingest_internal_failure_is_not_misreported_as_validation(
    ingest_api, monkeypatch,
):
    _client, canonical = ingest_api

    def fail(*_args, **_kwargs):
        raise RuntimeError("database transport failed")

    monkeypatch.setattr(canonical.source_store, "get_source", fail)
    response = _ingest(TestClient(app, raise_server_exceptions=False))

    assert response.status_code == 500
    assert response.text == "Internal Server Error"


def test_plaintext_module_authoring_is_refused(monkeypatch):
    from windex.api import app as app_module
    from windex.config import Settings

    settings = Settings(
        _env_file=None,
        write_token="admin-token",
        module_admin_token="module-token",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    client = TestClient(admin)
    response = client.get(
        "/v1/modules",
        headers={"Authorization": "Bearer module-token"})
    assert response.status_code == 426


def test_generic_run_requires_frozen_revision_precondition(monkeypatch):
    from windex.api import app as app_module
    from windex.config import Settings

    settings = Settings(_env_file=None, write_token="", serve_host="127.0.0.1")
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    response = TestClient(admin).post(
        "/v1/pipelines/example/runs",
        json={"flow": "run", "inputs": {}, "parameters": {}},
    )
    assert response.status_code == 428


def test_pipeline_revision_publish_requires_concurrency_guard(monkeypatch):
    from windex.api import app as app_module
    from windex.config import Settings

    settings = Settings(_env_file=None, write_token="", serve_host="127.0.0.1")
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    response = TestClient(admin).post(
        "/v1/pipelines/example/revisions",
        json={"spec": {}},
    )

    assert response.status_code == 428
    assert "parent_version, parent_hash, or If-Match" in response.json()["detail"]


def test_pipeline_revision_header_and_body_hash_must_match(monkeypatch):
    from windex.api import app as app_module
    from windex.config import Settings

    settings = Settings(_env_file=None, write_token="", serve_host="127.0.0.1")
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    response = TestClient(admin).post(
        "/v1/pipelines/example/revisions",
        headers={"If-Match": '"sha256:header"'},
        json={"spec": {}, "parent_hash": "sha256:body"},
    )

    assert response.status_code == 409
    assert "identify different Pipeline heads" in response.json()["detail"]


def test_pipeline_revision_stale_guard_maps_to_precondition_failed(monkeypatch):
    from windex.api import canonical

    received: dict = {}
    monkeypatch.setattr(
        canonical.db, "pooled", lambda _dsn: nullcontext(object()))
    monkeypatch.setattr(
        canonical.registry, "load_custom", lambda _conn: None)

    def stale(*_args, **kwargs):
        received.update(kwargs)
        raise canonical.pipeline_store.StalePipelineError("head moved")

    monkeypatch.setattr(canonical.pipeline_store, "publish_revision", stale)
    with pytest.raises(HTTPException) as raised:
        pipeline_revision_publish(
            "example",
            PipelineRevisionCreate(
                spec={}, parent_version=1, parent_hash="sha256:same"),
            Response(),
            '"sha256:same"',
        )

    assert raised.value.status_code == 412
    assert raised.value.detail == "head moved"
    assert received["expected_version"] == 1
    assert received["expected_hash"] == "sha256:same"


@pytest.mark.parametrize(
    ("action", "status_code"),
    (("created", 201), ("rollback", 200), ("noop", 200)),
)
def test_pipeline_revision_publish_status_describes_resource_creation(
    monkeypatch,
    action,
    status_code,
):
    from windex.api import canonical

    monkeypatch.setattr(
        canonical.db, "pooled", lambda _dsn: nullcontext(object()))
    monkeypatch.setattr(
        canonical.registry, "load_custom", lambda _conn: None)
    monkeypatch.setattr(
        canonical.pipeline_store,
        "publish_revision",
        lambda *_args, **_kwargs: canonical.pipeline_store.PublicationResult(
            revision={"version": 7},
            action=action,
        ),
    )

    response = Response()
    result = pipeline_revision_publish(
        "example",
        PipelineRevisionCreate(spec={}, parent_version=6),
        response,
        None,
    )

    assert response.status_code == status_code
    assert result == {"version": 7}


def test_initial_pipeline_publication_uses_create_route_without_guard(monkeypatch):
    from windex.api import app as app_module
    from windex.api import canonical
    from windex.config import Settings

    settings = Settings(_env_file=None, write_token="", serve_host="127.0.0.1")
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        canonical.db, "pooled", lambda _dsn: nullcontext(object()))
    monkeypatch.setattr(
        canonical.registry, "load_custom", lambda _conn: None)
    monkeypatch.setattr(
        canonical.pipeline_store,
        "create_pipeline",
        lambda _conn, **body: {
            "id": 99,
            "name": body["name"],
            "title": body["title"],
            "description": body["description"],
            "builtin": False,
            "archived_at": None,
            "created_at": "2026-07-26T00:00:00+00:00",
            "updated_at": "2026-07-26T00:00:00+00:00",
            "head_revision_id": 100,
            "version": 1,
            "spec_hash": "sha256:initial",
            "spec": body["spec"],
        },
    )

    response = TestClient(admin).post(
        "/v1/pipelines",
        json={"name": "new-pipeline", "spec": {}},
    )
    assert response.status_code == 201
    assert response.json()["version"] == 1
