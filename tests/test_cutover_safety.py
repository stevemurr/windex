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
