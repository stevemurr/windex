"""Fail-closed, resumable contract-epoch cutover.

Normal startup never calls this module.  The destructive entry point requires
the exact confirmation string emitted by ``preflight``.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import psycopg

from windex.config import Settings
from windex.db.canonical import init_canonical_db
from windex.pipeline.contracts import CONTRACT_EPOCH
from windex.pipeline.store import load_seed_matrix, seed_matrix_hash
from windex.index import qdrant as qidx

PHASES = (
    "preflight",
    "postgres_reset",
    "qdrant_reset",
    "filesystem_generation",
    "schema_bootstrap",
    "seed",
    "verified",
)
_LEGACY_DATA_ENTRIES = ("artifacts", "downloads", "staging")


class UnsafeCutover(RuntimeError):
    pass


def _safe_root(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if resolved in (Path("/"), Path.home().resolve()) or len(resolved.parts) < 3:
        raise UnsafeCutover(f"unsafe WINDEX_DATA_ROOT target: {resolved}")
    if any(part in {"*", "?", ".."} for part in path.parts):
        raise UnsafeCutover(f"unresolved or wildcard filesystem target: {path}")
    return resolved


def _database(settings: Settings) -> dict[str, str]:
    parsed = urlparse(settings.pg_dsn)
    database = parsed.path.removeprefix("/")
    if not parsed.hostname or not database or any(ch in database for ch in "*?"):
        raise UnsafeCutover("Postgres host/database must resolve to exact values")
    return {
        "host": parsed.hostname,
        "database": database,
        "schema": "public",
    }


def _qdrant_endpoint(settings: Settings) -> str:
    value = settings.qdrant_url.rstrip("/")
    parsed = urlparse(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or any(ch in value for ch in "*?${}")
    ):
        raise UnsafeCutover("Qdrant endpoint must resolve to one exact service URL")
    return value


def _owned_resources(
    conn: psycopg.Connection | None, settings: Settings,
) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    if conn is not None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT resource_type, resource_name, metadata
                         FROM storage_ownership ORDER BY resource_type, resource_name""")
                resources.extend({
                    "type": row[0], "name": row[1], "metadata": row[2],
                } for row in cur.fetchall())
        except psycopg.Error:
            conn.rollback()
    # Fresh installs may not have an ownership table yet.  Exact aliases from
    # the checked-in seed matrix are safe; never enumerate and delete an entire
    # Qdrant service.
    known = {(item["type"], item["name"]) for item in resources}
    for seed in load_seed_matrix(settings):
        source = seed.get("source")
        if source is None:
            continue
        key = ("qdrant_alias", qidx.alias_name(source["collection_key"]))
        if key not in known:
            resources.append({
                "type": key[0], "name": key[1],
                "metadata": {"seed": seed["name"]},
            })
    return resources


def _dedicated_qdrant_resources(settings: Settings) -> list[dict[str, Any]]:
    """Enumerate every exact resource in an explicitly dedicated Qdrant service."""
    endpoint = _qdrant_endpoint(settings)
    collections_response = httpx.get(
        f"{endpoint}/collections", timeout=15)
    collections_response.raise_for_status()
    aliases_response = httpx.get(f"{endpoint}/aliases", timeout=15)
    aliases_response.raise_for_status()
    aliases = aliases_response.json().get("result", {}).get("aliases", [])
    collections = collections_response.json().get(
        "result", {}).get("collections", [])
    resources = [{
        "type": "qdrant_alias",
        "name": str(item["alias_name"]),
        "metadata": {
            "collection": str(item["collection_name"]),
            "reset_scope": "dedicated",
        },
    } for item in aliases]
    resources.extend({
        "type": "qdrant_collection",
        "name": str(item["name"]),
        "metadata": {"reset_scope": "dedicated"},
    } for item in collections)
    for resource in resources:
        name = resource["name"]
        if not name or any(ch in name for ch in "*?/"):
            raise UnsafeCutover(f"unsafe Qdrant resource name: {name!r}")
    return sorted(resources, key=lambda item: (item["type"], item["name"]))


def _legacy_data_entries(root: Path, current: Path) -> list[str]:
    if current.is_symlink():
        return []
    entries: list[str] = []
    for name in _LEGACY_DATA_ENTRIES:
        candidate = root / name
        if not candidate.exists():
            continue
        if candidate.is_symlink() or not candidate.is_dir():
            raise UnsafeCutover(
                f"legacy data entry must be a real directory: {candidate}")
        resolved = candidate.resolve()
        if resolved.parent != root or resolved.name != name:
            raise UnsafeCutover(
                f"legacy data entry escaped WINDEX_DATA_ROOT: {candidate}")
        entries.append(str(resolved))
    return entries


def _manifest_digest(manifest: dict[str, Any]) -> str:
    base = {
        key: value for key, value in manifest.items()
        if key not in {
            "manifest_hash", "confirmation", "quarantine_confirmation",
        }
    }
    encoded = json.dumps(base, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def preflight(
    settings: Settings,
    *,
    bootstrap_id: str,
    conn: psycopg.Connection | None = None,
    dedicated_qdrant: bool = False,
) -> dict[str, Any]:
    if not bootstrap_id or any(ch in bootstrap_id for ch in "/*?${}"):
        raise UnsafeCutover("bootstrap ID must be an exact non-empty identifier")
    root = _safe_root(settings.data_root)
    resources = (
        _dedicated_qdrant_resources(settings)
        if dedicated_qdrant
        else _owned_resources(conn, settings)
    )
    target = root / "generations" / bootstrap_id
    if target == root or root not in target.parents:
        raise UnsafeCutover("generation target escaped WINDEX_DATA_ROOT")
    current = root / "generations" / "current"
    old = current.resolve() if current.is_symlink() else None
    legacy_entries = _legacy_data_entries(root, current)
    generations = (root / "generations").resolve()
    if old is not None and generations not in old.parents:
        raise UnsafeCutover("current generation resolves outside generations/")
    quarantine = (
        root / "quarantine" / f"{old.name}-{bootstrap_id}"
        if old is not None else root / "quarantine" / bootstrap_id
    )
    manifest = {
        "contract_epoch": CONTRACT_EPOCH,
        "bootstrap_id": bootstrap_id,
        "postgres": _database(settings),
        "qdrant": {
            "endpoint": _qdrant_endpoint(settings),
            "reset_scope": "dedicated" if dedicated_qdrant else "owned",
            "resources": resources,
        },
        "filesystem": {
            "data_root": str(root),
            "new_generation": str(target),
            "old_generation": str(old) if old else None,
            "legacy_entries": legacy_entries,
            "quarantine": str(quarantine),
        },
        "seed_hash": seed_matrix_hash(settings),
    }
    digest = _manifest_digest(manifest)
    manifest["manifest_hash"] = digest
    manifest["confirmation"] = f"RESET {bootstrap_id} {digest}"
    manifest["quarantine_confirmation"] = (
        f"QUARANTINE {bootstrap_id} {digest}")
    return manifest


def _resume_manifest(
    settings: Settings,
    *,
    bootstrap_id: str,
    root: Path,
    dedicated_qdrant: bool = False,
) -> dict[str, Any] | None:
    """Load the pre-reset target manifest after any partially completed phase."""
    path = root / "cutover" / f"{bootstrap_id}.json"
    if not path.exists():
        return None
    try:
        document = json.loads(path.read_text())
        manifest = document["manifest"]
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise UnsafeCutover("existing cutover marker is invalid") from exc
    if (
        not isinstance(manifest, dict)
        or manifest.get("bootstrap_id") != bootstrap_id
        or document.get("manifest_hash") != manifest.get("manifest_hash")
        or _manifest_digest(manifest) != manifest.get("manifest_hash")
    ):
        raise UnsafeCutover("existing cutover marker failed integrity validation")
    if (
        manifest.get("contract_epoch") != CONTRACT_EPOCH
        or manifest.get("seed_hash") != seed_matrix_hash(settings)
        or manifest.get("postgres") != _database(settings)
        or manifest.get("qdrant", {}).get("endpoint") != _qdrant_endpoint(settings)
        or manifest.get("qdrant", {}).get("reset_scope") != (
            "dedicated" if dedicated_qdrant else "owned")
        or manifest.get("filesystem", {}).get("data_root") != str(root)
    ):
        raise UnsafeCutover(
            "current image or resolved targets differ from the resumable manifest")
    return manifest


@dataclass
class Marker:
    path: Path
    document: dict[str, Any]

    @classmethod
    def load(cls, root: Path, manifest: dict[str, Any]) -> Marker:
        directory = root / "cutover"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{manifest['bootstrap_id']}.json"
        if path.exists():
            doc = json.loads(path.read_text())
            if doc.get("manifest_hash") != manifest["manifest_hash"]:
                raise UnsafeCutover("existing phase marker has a different manifest")
        else:
            doc = {
                "manifest_hash": manifest["manifest_hash"],
                "bootstrap_id": manifest["bootstrap_id"],
                "manifest": manifest,
                "completed": [],
            }
        return cls(path, doc)

    def complete(self, phase: str) -> None:
        if phase not in PHASES:
            raise ValueError(phase)
        completed = self.document["completed"]
        if phase not in completed:
            completed.append(phase)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.document, indent=2, sort_keys=True))
        os.replace(temporary, self.path)


def _reset_postgres(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE")
        cur.execute("CREATE SCHEMA public")
    conn.commit()


def _reset_qdrant(manifest: dict[str, Any]) -> None:
    endpoint = manifest["qdrant"]["endpoint"].rstrip("/")
    aliases_response = httpx.get(f"{endpoint}/aliases", timeout=15)
    aliases_response.raise_for_status()
    aliases = {
        str(item["alias_name"])
        for item in aliases_response.json().get("result", {}).get("aliases", [])
    }
    for resource in manifest["qdrant"]["resources"]:
        name = resource["name"]
        if not name or any(ch in name for ch in "*?/"):
            raise UnsafeCutover(f"unsafe Qdrant resource name: {name!r}")
        if resource["type"] == "qdrant_alias":
            if name in aliases:
                response = httpx.post(
                    f"{endpoint}/collections/aliases",
                    json={"actions": [{"delete_alias": {"alias_name": name}}]},
                    timeout=15,
                )
                response.raise_for_status()
        elif resource["type"] == "qdrant_collection":
            response = httpx.delete(f"{endpoint}/collections/{name}", timeout=30)
            if response.status_code not in (200, 404):
                response.raise_for_status()


def execute(
    settings: Settings,
    *,
    bootstrap_id: str,
    confirmation: str,
    reset_qdrant: bool = True,
    dedicated_qdrant: bool = False,
) -> dict[str, Any]:
    root = _safe_root(settings.data_root)
    manifest = _resume_manifest(
        settings, bootstrap_id=bootstrap_id, root=root,
        dedicated_qdrant=dedicated_qdrant)
    with psycopg.connect(settings.pg_dsn) as conn:
        if manifest is None:
            manifest = preflight(
                settings, bootstrap_id=bootstrap_id, conn=conn,
                dedicated_qdrant=dedicated_qdrant)
        if confirmation != manifest["confirmation"]:
            raise UnsafeCutover(
                "confirmation mismatch; rerun preflight and provide its exact value")
        marker = Marker.load(root, manifest)
        marker.complete("preflight")
        if "postgres_reset" not in marker.document["completed"]:
            _reset_postgres(conn)
            marker.complete("postgres_reset")
        if "qdrant_reset" not in marker.document["completed"]:
            if reset_qdrant:
                _reset_qdrant(manifest)
            marker.complete("qdrant_reset")
        target = Path(manifest["filesystem"]["new_generation"])
        if "filesystem_generation" not in marker.document["completed"]:
            target.mkdir(parents=True, exist_ok=True)
            for child in ("artifacts", "downloads", "staging"):
                target.joinpath(child).mkdir(exist_ok=True)
            marker.complete("filesystem_generation")
        if "schema_bootstrap" not in marker.document["completed"]:
            init_canonical_db(conn, bootstrap_id=bootstrap_id, seed=False)
            marker.complete("schema_bootstrap")
        if "seed" not in marker.document["completed"]:
            from windex.pipeline.bootstrap import seed_canonical

            seed_canonical(conn, settings)
            marker.complete("seed")
        metadata = init_canonical_db(conn, bootstrap_id=bootstrap_id, seed=True)
        if (
            metadata["contract_epoch"] != CONTRACT_EPOCH
            or metadata["seed_hash"] != manifest["seed_hash"]
        ):
            raise RuntimeError("post-cutover contract/seed verification failed")
        current = root / "generations" / "current"
        temporary = root / "generations" / f".current-{bootstrap_id}"
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
        temporary.symlink_to(target.name)
        os.replace(temporary, current)
        marker.complete("verified")
        return {**manifest, "completed": marker.document["completed"]}


def quarantine_previous(
    settings: Settings,
    *,
    bootstrap_id: str,
    confirmation: str,
) -> dict[str, Any]:
    """Move the reviewed prior generation after end-to-end search verification."""
    root = _safe_root(settings.data_root)
    marker_path = root / "cutover" / f"{bootstrap_id}.json"
    if not marker_path.is_file():
        raise UnsafeCutover("verified cutover marker not found")
    document = json.loads(marker_path.read_text())
    manifest = document.get("manifest")
    if not isinstance(manifest, dict) or "verified" not in document.get("completed", []):
        raise UnsafeCutover("cutover is not verified or lacks an exact manifest")
    expected = (
        f"QUARANTINE {bootstrap_id} {manifest['manifest_hash']}")
    if confirmation != expected:
        raise UnsafeCutover(
            f"confirmation mismatch; expected the reviewed value {expected!r}")
    filesystem = manifest["filesystem"]
    old_value = filesystem.get("old_generation")
    legacy_values = filesystem.get("legacy_entries") or []
    if old_value is None and not legacy_values:
        return {
            "quarantined": False,
            "reason": "no prior generation or legacy data",
        }
    generations = (root / "generations").resolve()
    target = Path(filesystem["quarantine"]).resolve()
    quarantine_root = (root / "quarantine").resolve()
    current = (root / "generations" / "current").resolve()
    if quarantine_root not in target.parents or target == quarantine_root:
        raise UnsafeCutover("prior generation quarantine target is unsafe")
    moved: list[dict[str, str]] = []
    if old_value is not None:
        old = Path(old_value).resolve()
        if (
            generations not in old.parents
            or old in (current, generations)
            or target.exists()
        ):
            raise UnsafeCutover("prior generation quarantine target is unsafe")
        if not old.exists():
            raise UnsafeCutover("prior generation is no longer present")
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(old, target)
        moved.append({"from": str(old), "to": str(target)})
    else:
        target.mkdir(parents=True, exist_ok=True)
        for value in legacy_values:
            source = Path(value)
            if (
                source.is_symlink()
                or source.parent.resolve() != root
                or source.name not in _LEGACY_DATA_ENTRIES
            ):
                raise UnsafeCutover(
                    f"legacy quarantine source is unsafe: {source}")
            destination = target / source.name
            if source.exists() and destination.exists():
                raise UnsafeCutover(
                    f"legacy quarantine destination already exists: {destination}")
            if source.exists():
                os.replace(source, destination)
                moved.append({"from": str(source), "to": str(destination)})
            elif not destination.exists():
                raise UnsafeCutover(
                    f"legacy quarantine source is missing: {source}")
    document["quarantined"] = moved
    temporary = marker_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True))
    os.replace(temporary, marker_path)
    return {"quarantined": True, "moved": moved}


__all__ = [
    "PHASES", "UnsafeCutover", "execute", "preflight",
    "quarantine_previous",
]
