from contextlib import nullcontext

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from windex.api.app import admin, app
from windex.api.canonical import (
    IngestDocument,
    IngestRequest,
    ModuleHealthResponse,
    RegistryResponse,
    module_health,
    source_ingest,
)
from windex.pipeline import registry
from windex.pipeline.contracts import CONTRACT_EPOCH


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
    assert schemas["PipelineRunCreate"]["properties"]["dry_run"] == {
        "default": False,
        "title": "Dry Run",
        "type": "boolean",
    }
    assert "values" in schemas["SourceUpgradePreviewRequest"]["properties"]
    assert "values" in schemas["SourceUpgradeRequest"]["required"]
    assert "candidate" in schemas["UpgradePreviewResponse"]["required"]
    assert "issues" in schemas["UpgradePreviewResponse"]["required"]


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
