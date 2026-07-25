"""Loading built-in recipes, and reading/writing the `recipes` table.

The built-ins are YAML on disk rather than Python dicts on purpose: YAML is the
marketplace's distribution format, so shipping the built-ins in it means the loader
that reads a community recipe is the same one that reads `ccnews`. A format only
used by strangers is a format nobody tests.

They are seeded, never hardcoded. Once a built-in is a row in `recipes`, the whole
system stops distinguishing it from anything installed: the same list endpoint, the
same editor, the same run history. That collapse is the point of the project — and
it is available BEFORE any module has an implementation, because opening and
editing a recipe needs the parser and the registry, not the executor.
"""

from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from pathlib import Path

import psycopg
import yaml
from psycopg.types.json import Jsonb

from windex.config import Settings
from windex.recipe import parse as recipe_parse


def spec_hash(spec: dict) -> str:
    """sha1 over canonical JSON. Cheap change detection, and what lets a re-seed
    leave an untouched recipe's `updated_at` alone."""
    canonical = json.dumps(spec, sort_keys=True, separators=(",", ":"))
    return "sha1:" + hashlib.sha1(canonical.encode()).hexdigest()


def builtin_dir() -> Path:
    return Path(str(files("windex.recipe").joinpath("builtin")))


def load_builtins(settings: Settings) -> list[recipe_parse.Recipe]:
    """Parse every shipped recipe. Raises on the first bad one.

    Deliberately strict: a built-in that does not parse is a bug in this repo, not
    a user's problem to discover at run time. `tests/test_recipe_builtin.py` runs
    this, so the failure lands in CI.
    """
    out = []
    for path in sorted(builtin_dir().glob("*.yaml")):
        doc = yaml.safe_load(path.read_text())
        try:
            # builtin=True is passed by the CALLER, never read from the document —
            # it relaxes the reserved-name guard, and a recipe that could claim it
            # could claim `news` and write into the news corpus.
            out.append(recipe_parse.parse(doc, settings, builtin=True))
        except ValueError as exc:
            raise ValueError(f"built-in recipe {path.name} is invalid: {exc}") from exc
    return out


def seed_builtins(conn: psycopg.Connection, settings: Settings,
                  *, force: bool = False) -> list[dict]:
    """Insert or refresh the shipped recipes. Idempotent.

    A recipe the operator has EDITED is left alone unless `force`: `builtin` means
    "shipped and restorable", not "overwritten on every deploy". Losing a local
    change to `ccnews` on an unrelated `init-db` would make editing a built-in feel
    unsafe, which would defeat the point of them being editable at all.
    """
    out = []
    for recipe in load_builtins(settings):
        spec = recipe.to_dict()
        digest = spec_hash(spec)
        with conn.cursor() as cur:
            cur.execute("SELECT spec_hash, version, builtin FROM recipes WHERE name = %s",
                        (recipe.name,))
            row = cur.fetchone()
            if row is not None:
                existing_hash, _version, is_builtin = row
                if existing_hash == digest:
                    out.append({"name": recipe.name, "action": "unchanged"})
                    continue
                if not force and not is_builtin:
                    out.append({"name": recipe.name, "action": "kept (locally edited)"})
                    continue
            cur.execute(
                """INSERT INTO recipes (name, source, kind, spec, spec_hash, base_spec,
                                        version, builtin, title, description)
                   VALUES (%s, %s, 'ingest', %s, %s, %s, %s, true, %s, %s)
                   ON CONFLICT (name) DO UPDATE SET
                       source = EXCLUDED.source, spec = EXCLUDED.spec,
                       spec_hash = EXCLUDED.spec_hash, base_spec = EXCLUDED.base_spec,
                       version = EXCLUDED.version, title = EXCLUDED.title,
                       description = EXCLUDED.description, updated_at = now()""",
                (recipe.name, recipe.corpus.source, Jsonb(spec), digest, Jsonb(spec),
                 recipe.version, recipe.title, recipe.description))
            out.append({"name": recipe.name,
                        "action": "updated" if row else "created"})
    conn.commit()
    return out


# --- reading ----------------------------------------------------------------

def _row(r) -> dict:
    (name, source, kind, spec, digest, version, enabled, builtin, title,
     description, created, updated) = r
    return {
        "name": name, "source": source, "kind": kind,
        "spec": spec, "spec_hash": digest, "version": version,
        "enabled": enabled, "builtin": builtin,
        "title": title, "description": description,
        "created_at": created.isoformat() if created else None,
        "updated_at": updated.isoformat() if updated else None,
        # The vocabulary consolidation: every source-shaped response carries all
        # the names a client might need, so nothing hardcodes {ccnews -> news}.
        "search_name": source, "corpus_name": source, "loop_name": name,
    }


_COLS = ("name, source, kind, spec, spec_hash, version, enabled, builtin, "
         "title, description, created_at, updated_at")


def list_recipes(conn: psycopg.Connection, *, include_spec: bool = False) -> list[dict]:
    """Every registered recipe. `include_spec` off by default — the list view wants
    names and status, and eleven full DAGs is a lot of bytes to send for a sidebar."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS} FROM recipes ORDER BY builtin DESC, name")
        rows = [_row(r) for r in cur.fetchall()]
    if not include_spec:
        for r in rows:
            r["spec"] = None
            r["node_count"] = None
    else:
        for r in rows:
            r["node_count"] = sum(len(f.get("nodes", {}))
                                  for f in (r["spec"] or {}).get("flows", {}).values())
    return rows


def get_recipe(conn: psycopg.Connection, name: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLS} FROM recipes WHERE name = %s", (name,))
        row = cur.fetchone()
    if row is None:
        return None
    out = _row(row)
    spec = out["spec"] or {}
    out["flows"] = {
        fname: {"nodes": list(f.get("nodes", {})), "edges": f.get("edges", [])}
        for fname, f in spec.get("flows", {}).items()
    }
    return out
