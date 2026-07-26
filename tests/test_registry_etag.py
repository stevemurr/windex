from contextlib import nullcontext

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def registry_api(monkeypatch):
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
        canonical.registry, "load_custom", lambda _conn: None)
    document = {
        "contract": "windex.registry/3",
        "registry_contract": "windex.registry/3",
        "registry_version": 3,
        "registry_digest": "sha256:current",
        "ports": [],
        "port_types": {},
        "kinds": [],
        "modules": [],
        "always_before_load": [],
    }
    monkeypatch.setattr(
        canonical.registry, "describe", lambda: document)
    return TestClient(app_module.app), canonical, document


def test_registry_etag_match_returns_empty_304_from_mounted_admin(registry_api):
    client, _canonical, _document = registry_api
    first = client.get("/admin/v1/registry")

    assert first.status_code == 200
    assert first.headers["etag"].startswith('"sha256:')
    assert first.json()["registry_digest"] == first.headers["etag"].strip('"')

    cached = client.get(
        "/admin/v1/registry",
        headers={"If-None-Match": first.headers["etag"]},
    )

    assert cached.status_code == 304
    assert cached.content == b""
    assert cached.headers["etag"] == first.headers["etag"]
    assert "content-length" not in cached.headers


@pytest.mark.parametrize(
    "validator",
    [
        'W/"{digest}"',
        '"unrelated", W/"{digest}", "another"',
        '"legal,comma", W/"{digest}"',
        "*",
    ],
)
def test_registry_if_none_match_uses_get_weak_comparison(
    registry_api, validator,
):
    client, _canonical, _document = registry_api
    digest = client.get("/admin/v1/registry").json()["registry_digest"]

    response = client.get(
        "/admin/v1/registry",
        headers={"If-None-Match": validator.format(digest=digest)},
    )

    assert response.status_code == 304
    assert response.content == b""
    assert response.headers["etag"] == f'"{digest}"'


@pytest.mark.parametrize(
    "validator",
    [
        '"sha256:not-current"',
        '"sha256:not-current", W/"sha256:also-not-current"',
        "sha256:unquoted",
        'w/"sha256:lowercase-weak-prefix"',
        '"unterminated',
        '*,"sha256:list-wildcard-is-invalid"',
    ],
)
def test_registry_if_none_match_miss_keeps_normal_200(
    registry_api, validator,
):
    client, _canonical, _document = registry_api

    response = client.get(
        "/admin/v1/registry",
        headers={"If-None-Match": validator},
    )

    assert response.status_code == 200
    assert response.json()["registry_digest"] == response.headers["etag"].strip('"')


def test_registry_etag_changes_when_registry_digest_changes(
    registry_api, monkeypatch,
):
    client, canonical, original = registry_api
    digests = iter(("sha256:first", "sha256:second", "sha256:second"))

    def describe():
        return {**original, "registry_digest": next(digests)}

    monkeypatch.setattr(canonical.registry, "describe", describe)

    first = client.get("/admin/v1/registry")
    changed = client.get(
        "/admin/v1/registry",
        headers={"If-None-Match": first.headers["etag"]},
    )
    unchanged = client.get(
        "/admin/v1/registry",
        headers={"If-None-Match": changed.headers["etag"]},
    )

    assert first.status_code == 200
    assert first.headers["etag"] == '"sha256:first"'
    assert changed.status_code == 200
    assert changed.headers["etag"] == '"sha256:second"'
    assert changed.json()["registry_digest"] == "sha256:second"
    assert unchanged.status_code == 304
    assert unchanged.headers["etag"] == '"sha256:second"'
