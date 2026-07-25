"""An inert, filesystem-backed recipe marketplace.

Catalogs contain YAML documents, never Python. The bundled catalog ships with
windex; operators may add git-synced directories with
``WINDEX_RECIPE_CATALOG_DIRS``. The API never clones or fetches a caller-chosen
URL, so browsing cannot turn the LAN control plane into an SSRF proxy.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path

import psycopg
import yaml

from windex.config import Settings
from windex.recipe import compile as recipe_compile
from windex.recipe import parse as recipe_parse
from windex.recipe import store


class CatalogConflict(RuntimeError):
    """An install exists or has local changes that an update would overwrite."""


def _catalog_dirs(settings: Settings) -> list[tuple[str, Path]]:
    bundled = Path(str(files("windex.recipe").joinpath("catalog")))
    out = [("windex", bundled)]
    for raw in settings.recipe_catalog_dir_list():
        path = Path(raw).expanduser().resolve()
        out.append((path.name or "local", path))
    return out


def _load(settings: Settings) -> dict[str, dict]:
    entries: dict[str, dict] = {}
    for catalog, root in _catalog_dirs(settings):
        if not root.is_dir():
            continue
        for path in sorted(root.glob("*.yaml")):
            raw = path.read_bytes()
            body = yaml.safe_load(raw)
            if not isinstance(body, dict):
                raise ValueError(f"catalog recipe {path.name} must be an object")
            recipe = recipe_parse.parse(body, settings, builtin=False)
            document = recipe.to_dict()
            entry_id = f"{catalog}:{recipe.name}"
            if entry_id in entries:
                raise ValueError(f"duplicate marketplace entry: {entry_id}")
            tasks = recipe_compile.compile_tasks(document, settings=settings)
            unavailable = recipe_compile.unavailable_modules(tasks)
            entries[entry_id] = {
                "id": entry_id,
                "catalog": catalog,
                "name": recipe.name,
                "title": recipe.title,
                "description": recipe.description,
                "version": recipe.version,
                "config": [field.describe() for field in recipe.config],
                "document": document,
                "executable": not unavailable,
                "unavailable_modules": unavailable,
                "blob_sha256": hashlib.sha256(raw).hexdigest(),
                "path": path.name,
            }
    return entries


def _installed(conn: psycopg.Connection) -> dict[str, dict]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT name, spec_hash, base_spec, version, origin
                 FROM recipes WHERE origin IS NOT NULL""")
        rows = cur.fetchall()
    out = {}
    for name, digest, base_spec, version, origin in rows:
        if not isinstance(origin, dict) or not origin.get("entry_id"):
            continue
        out[origin["entry_id"]] = {
            "name": name,
            "version": version,
            "locally_edited": (
                base_spec is None or digest != store.spec_hash(base_spec)),
            "origin": origin,
        }
    return out


def list_entries(conn: psycopg.Connection, settings: Settings) -> list[dict]:
    installed = _installed(conn)
    out = []
    for entry in _load(settings).values():
        row = installed.get(entry["id"])
        current_blob = (row or {}).get("origin", {}).get("blob_sha256")
        out.append({
            **entry,
            "installed": row is not None,
            "installed_name": (row or {}).get("name"),
            "installed_version": (row or {}).get("version"),
            "locally_edited": (row or {}).get("locally_edited", False),
            "update_available": row is not None
            and current_blob != entry["blob_sha256"],
        })
    return out


def get_entry(conn: psycopg.Connection, settings: Settings,
              entry_id: str) -> dict | None:
    return next(
        (row for row in list_entries(conn, settings) if row["id"] == entry_id),
        None,
    )


def _identity(document: dict, name: str) -> dict:
    out = dict(document)
    out["name"] = name
    corpus = dict(out.get("corpus") or {})
    corpus.update({
        "source": name,
        "id_prefix": f"{name}:",
        "collection": name,
    })
    out["corpus"] = corpus
    return out


def _origin(entry: dict) -> dict:
    return {
        "catalog": entry["catalog"],
        "entry_id": entry["id"],
        "path": entry["path"],
        "blob_sha256": entry["blob_sha256"],
        "installed_at": datetime.now(UTC).isoformat(),
    }


def install(conn: psycopg.Connection, settings: Settings, entry_id: str, *,
            name: str | None = None, values: dict | None = None) -> dict:
    entries = _load(settings)
    entry = entries.get(entry_id)
    if entry is None:
        raise KeyError(entry_id)
    document = _identity(entry["document"], name) if name else entry["document"]
    try:
        return store.create_recipe(
            conn,
            document,
            settings,
            author="marketplace",
            note=f"Installed from {entry_id}",
            origin=_origin(entry),
            config=values or {},
        )
    except KeyError as exc:
        raise CatalogConflict(f"recipe already exists: {exc.args[0]}") from exc


def update(conn: psycopg.Connection, settings: Settings, entry_id: str) -> dict:
    entries = _load(settings)
    entry = entries.get(entry_id)
    if entry is None:
        raise KeyError(entry_id)
    installed = _installed(conn).get(entry_id)
    if installed is None:
        raise KeyError(f"{entry_id} is not installed")
    if installed["origin"].get("blob_sha256") == entry["blob_sha256"]:
        got = store.get_recipe(conn, installed["name"])
        if got is None:
            raise KeyError(installed["name"])
        return got
    if installed["locally_edited"]:
        raise CatalogConflict(
            f"{installed['name']} has local edits; update would overwrite them")
    document = _identity(entry["document"], installed["name"])
    try:
        got = store.update_from_catalog(
            conn,
            installed["name"],
            document,
            settings,
            origin=_origin(entry),
        )
    except RuntimeError as exc:
        raise CatalogConflict(str(exc)) from exc
    if got is None:
        raise KeyError(installed["name"])
    return got
