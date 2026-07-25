"""The inert recipe marketplace and its lossless update rules."""

from pathlib import Path

from fastapi.testclient import TestClient

import windex.api.app as app_mod
from windex.api.app import app
from windex.recipe import marketplace, store


def test_bundled_catalog_lists_and_installs_with_config(pg, settings):
    entries = marketplace.list_entries(pg, settings)
    docs = next(entry for entry in entries if entry["id"] == "windex:web_docs")
    assert docs["installed"] is False
    assert docs["executable"] is False
    assert "http.get" in docs["unavailable_modules"]

    installed = marketplace.install(
        pg,
        settings,
        "windex:web_docs",
        name="team_docs",
        values={"seeds": ["https://example.com/docs/"]},
    )
    assert installed["name"] == "team_docs"
    assert installed["source"] == "team_docs"
    assert store.get_recipe_config(pg, "team_docs")["seeds"] == [
        "https://example.com/docs/"]

    listed = next(
        entry for entry in marketplace.list_entries(pg, settings)
        if entry["id"] == "windex:web_docs")
    assert listed["installed"] is True
    assert listed["installed_name"] == "team_docs"
    assert listed["locally_edited"] is False
    assert listed["update_available"] is False


def test_install_requires_recipe_config(pg, settings):
    try:
        marketplace.install(pg, settings, "windex:web_docs")
    except ValueError as exc:
        assert "config.seeds is required" in str(exc)
    else:
        raise AssertionError("required install config was ignored")


def test_catalog_update_refuses_to_overwrite_local_edits(
        pg, settings, tmp_path: Path):
    source = Path("src/windex/recipe/catalog/web_docs.yaml").read_text()
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    recipe_path = catalog / "web_docs.yaml"
    recipe_path.write_text(source)
    settings.recipe_catalog_dirs = str(catalog)
    entry_id = f"{catalog.name}:web_docs"

    marketplace.install(
        pg,
        settings,
        entry_id,
        name="private_docs",
        values={"seeds": ["https://example.com/docs/"]},
    )
    installed = store.get_recipe(pg, "private_docs")
    edited = dict(installed["spec"])
    edited["title"] = "My local title"
    store.update_recipe(pg, "private_docs", edited, settings)

    recipe_path.write_text(source.replace(
        "title: Documentation website",
        "title: Documentation website v2",
    ))
    entry = marketplace.get_entry(pg, settings, entry_id)
    assert entry["update_available"] is True
    assert entry["locally_edited"] is True
    try:
        marketplace.update(pg, settings, entry_id)
    except marketplace.CatalogConflict as exc:
        assert "local edits" in str(exc)
    else:
        raise AssertionError("catalog update overwrote a local edit")


def test_marketplace_admin_surface(pg, settings, monkeypatch):
    monkeypatch.setattr(settings, "write_token", "marketplace-test-token")
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    client = TestClient(app)
    auth = {"Authorization": "Bearer marketplace-test-token"}

    listed = client.get("/admin/v1/marketplace", headers=auth)
    assert listed.status_code == 200
    assert listed.json()["entries"][0]["id"] == "windex:web_docs"

    installed = client.post(
        "/admin/v1/marketplace/windex:web_docs/install",
        json={
            "name": "api_docs",
            "values": {"seeds": ["https://example.com/docs/"]},
        },
        headers=auth,
    )
    assert installed.status_code == 201, installed.text
    assert installed.json()["name"] == "api_docs"

    again = client.post(
        "/admin/v1/marketplace/windex:web_docs/install",
        json={
            "name": "api_docs",
            "values": {"seeds": ["https://example.com/docs/"]},
        },
        headers=auth,
    )
    assert again.status_code == 409
