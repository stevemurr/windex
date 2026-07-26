from __future__ import annotations

import json

import pytest

from windex.config import Settings
from windex.db.cutover import (
    Marker,
    UnsafeCutover,
    _resume_manifest,
    preflight,
    quarantine_previous,
)
from windex.pipeline.contracts import CONTRACT_EPOCH


def test_preflight_resolves_exact_scoped_targets(tmp_path):
    root = tmp_path / "windex-data"
    old = root / "generations" / "epoch1"
    old.mkdir(parents=True)
    current = root / "generations" / "current"
    current.symlink_to(old.name)
    settings = Settings(
        _env_file=None,
        data_root=root,
        pg_dsn="postgresql://windex:pw@postgres.internal:5432/windex_epoch2",
        qdrant_url="http://qdrant.internal:6333",
    )
    manifest = preflight(settings, bootstrap_id="epoch2-test")
    assert manifest["contract_epoch"] == CONTRACT_EPOCH
    assert manifest["postgres"] == {
        "host": "postgres.internal",
        "database": "windex_epoch2",
        "schema": "public",
    }
    assert manifest["filesystem"]["new_generation"].endswith(
        "/generations/epoch2-test")
    assert manifest["filesystem"]["old_generation"] == str(old.resolve())
    assert manifest["filesystem"]["legacy_entries"] == []
    assert manifest["filesystem"]["quarantine"].endswith(
        "/quarantine/epoch1-epoch2-test")
    assert manifest["confirmation"].startswith("RESET epoch2-test ")
    assert manifest["quarantine_confirmation"].startswith(
        "QUARANTINE epoch2-test ")
    resources = manifest["qdrant"]["resources"]
    assert resources
    assert all("*" not in item["name"] for item in resources)
    assert all(
        item["name"].endswith("_current")
        for item in resources if item["type"] == "qdrant_alias")


def test_dedicated_preflight_enumerates_every_qdrant_resource(
    tmp_path, monkeypatch,
):
    class Response:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            return None

        def json(self):
            return self.body

    def get(url, **_kwargs):
        if url.endswith("/collections"):
            return Response({"result": {"collections": [
                {"name": "news__model"}, {"name": "orphaned__model"},
            ]}})
        return Response({"result": {"aliases": [{
            "alias_name": "news_current",
            "collection_name": "news__model",
        }]}})

    monkeypatch.setattr("windex.db.cutover.httpx.get", get)
    root = tmp_path / "safe" / "data"
    (root / "downloads").mkdir(parents=True)
    (root / "staging").mkdir()
    settings = Settings(
        _env_file=None,
        data_root=root,
        pg_dsn="postgresql://windex:pw@db:5432/windex",
        qdrant_url="http://qdrant:6333",
    )
    manifest = preflight(
        settings, bootstrap_id="epoch2", dedicated_qdrant=True)
    assert manifest["qdrant"]["reset_scope"] == "dedicated"
    assert {
        (item["type"], item["name"])
        for item in manifest["qdrant"]["resources"]
    } == {
        ("qdrant_alias", "news_current"),
        ("qdrant_collection", "news__model"),
        ("qdrant_collection", "orphaned__model"),
    }
    assert manifest["filesystem"]["legacy_entries"] == [
        str((root / "downloads").resolve()),
        str((root / "staging").resolve()),
    ]


def test_phase_marker_preserves_pre_reset_manifest_for_resume(tmp_path):
    root = tmp_path / "safe" / "windex-data"
    settings = Settings(
        _env_file=None,
        data_root=root,
        pg_dsn="postgresql://windex:pw@db:5432/windex",
        qdrant_url="http://qdrant:6333",
    )
    manifest = preflight(settings, bootstrap_id="epoch2")
    marker = Marker.load(root.resolve(), manifest)
    marker.complete("preflight")
    marker.complete("postgres_reset")

    resumed = _resume_manifest(
        settings, bootstrap_id="epoch2", root=root.resolve())
    assert resumed == manifest
    document = json.loads(marker.path.read_text())
    document["manifest"]["postgres"]["database"] = "other"
    marker.path.write_text(json.dumps(document))
    with pytest.raises(UnsafeCutover, match="integrity"):
        _resume_manifest(settings, bootstrap_id="epoch2", root=root.resolve())


@pytest.mark.parametrize("root", ["/", "/tmp"])
def test_preflight_refuses_broad_filesystem_root(root):
    settings = Settings(
        _env_file=None,
        data_root=root,
        pg_dsn="postgresql://windex:pw@db:5432/windex",
        qdrant_url="http://qdrant:6333",
    )
    with pytest.raises(UnsafeCutover):
        preflight(settings, bootstrap_id="epoch2")


@pytest.mark.parametrize("value", ["", "bad/id", "bad*", "${BAD}"])
def test_preflight_refuses_ambiguous_bootstrap_id(tmp_path, value):
    settings = Settings(
        _env_file=None,
        data_root=tmp_path / "safe" / "data",
        pg_dsn="postgresql://windex:pw@db:5432/windex",
        qdrant_url="http://qdrant:6333",
    )
    with pytest.raises(UnsafeCutover):
        preflight(settings, bootstrap_id=value)


def test_quarantine_requires_verified_exact_manifest(tmp_path):
    root = tmp_path / "safe" / "data"
    old = root / "generations" / "old"
    new = root / "generations" / "new"
    old.mkdir(parents=True)
    new.mkdir(parents=True)
    current = root / "generations" / "current"
    current.symlink_to(new.name)
    quarantine = root / "quarantine" / "old-new"
    marker = root / "cutover" / "new.json"
    marker.parent.mkdir(parents=True)
    manifest = {
        "bootstrap_id": "new",
        "manifest_hash": "abc123",
        "filesystem": {
            "old_generation": str(old),
            "new_generation": str(new),
            "quarantine": str(quarantine),
        },
    }
    marker.write_text(json.dumps({
        "manifest_hash": "abc123",
        "bootstrap_id": "new",
        "manifest": manifest,
        "completed": ["verified"],
    }))
    settings = Settings(
        _env_file=None,
        data_root=root,
        pg_dsn="postgresql://windex:pw@db:5432/windex",
        qdrant_url="http://qdrant:6333",
    )
    with pytest.raises(UnsafeCutover):
        quarantine_previous(
            settings, bootstrap_id="new", confirmation="wrong")
    result = quarantine_previous(
        settings, bootstrap_id="new",
        confirmation="QUARANTINE new abc123")
    assert result["quarantined"] is True
    assert quarantine.is_dir()
    assert not old.exists()


def test_quarantine_moves_exact_flat_legacy_entries(tmp_path):
    root = tmp_path / "safe" / "data"
    downloads = root / "downloads"
    staging = root / "staging"
    downloads.mkdir(parents=True)
    staging.mkdir()
    new = root / "generations" / "new"
    new.mkdir(parents=True)
    (root / "generations" / "current").symlink_to(new.name)
    quarantine = root / "quarantine" / "new"
    marker = root / "cutover" / "new.json"
    marker.parent.mkdir(parents=True)
    manifest = {
        "bootstrap_id": "new",
        "manifest_hash": "abc123",
        "filesystem": {
            "old_generation": None,
            "legacy_entries": [str(downloads), str(staging)],
            "new_generation": str(new),
            "quarantine": str(quarantine),
        },
    }
    marker.write_text(json.dumps({
        "manifest_hash": "abc123",
        "bootstrap_id": "new",
        "manifest": manifest,
        "completed": ["verified"],
    }))
    settings = Settings(
        _env_file=None,
        data_root=root,
        pg_dsn="postgresql://windex:pw@db:5432/windex",
        qdrant_url="http://qdrant:6333",
    )
    result = quarantine_previous(
        settings, bootstrap_id="new",
        confirmation="QUARANTINE new abc123")
    assert result["quarantined"] is True
    assert not downloads.exists()
    assert not staging.exists()
    assert (quarantine / "downloads").is_dir()
    assert (quarantine / "staging").is_dir()
