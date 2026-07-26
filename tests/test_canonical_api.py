from fastapi.testclient import TestClient

from windex.api.app import admin, app
from windex.api.canonical import RegistryResponse
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
    assert "/v1/events/stream" in admin_paths
    assert "/v1/log-events/stream" in admin_paths
    assert "/v1/sources/{name}/ingest" in public_schema["paths"]
    encoded = str(admin_schema).lower()
    assert "recipe" not in encoded
    assert "marketplace" not in encoded


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
