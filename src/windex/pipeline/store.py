"""Postgres persistence for immutable Pipeline lineages."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal, Mapping

import psycopg
import yaml
from psycopg.types.json import Jsonb

from windex.config import Settings
from windex.pipeline import compile as pipeline_compile
from windex.pipeline import registry
from windex.pipeline.events import append
from windex.pipeline.hashing import layout_etag, module_locks, semantic_hash
from windex.pipeline.spec import Pipeline, parse
from windex.pipeline.validation import source_capability


class StalePipelineError(RuntimeError):
    pass


class PipelinePreconditionRequiredError(RuntimeError):
    """A mutable Pipeline write omitted its expected-head guard."""


class PipelinePreconditionConflictError(RuntimeError):
    """Two representations of the expected Pipeline head disagree."""


@dataclass(frozen=True)
class PublicationResult:
    """The immutable revision now at the Pipeline head and how it got there."""

    revision: dict[str, Any]
    action: Literal["created", "rollback", "noop"]


def seed_dir() -> Path:
    return Path(str(files("windex.pipeline").joinpath("seeds")))


def load_seed_matrix(settings: Settings | None = None) -> list[dict[str, Any]]:
    root = seed_dir()
    manifest = yaml.safe_load(root.joinpath("manifest.yaml").read_text())
    result: list[dict[str, Any]] = []
    for name, metadata in manifest["pipelines"].items():
        raw = yaml.safe_load(root.joinpath(f"{name}.yaml").read_text())
        parsed = parse(raw, settings)
        normalized = parsed.to_dict()
        result.append({
            "name": name,
            "title": metadata.get("title", ""),
            "description": metadata.get("description", ""),
            "builtin": bool(metadata.get("builtin", True)),
            "spec": normalized,
            "spec_hash": semantic_hash(parsed, settings),
            "source": metadata.get("source"),
        })
    return result


def seed_matrix_hash(settings: Settings | None = None) -> str:
    value = json.dumps(
        load_seed_matrix(settings), sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


_REVISION_COLS = (
    "r.id, r.pipeline_id, p.name, r.version, r.parent_revision_id, r.spec, "
    "r.spec_hash, r.registry_version, r.registry_digest, r.module_locks, "
    "r.author, r.note, r.created_at"
)


def _revision(row: tuple[Any, ...]) -> dict[str, Any]:
    keys = (
        "id", "pipeline_id", "pipeline_name", "version", "parent_revision_id",
        "spec", "spec_hash", "registry_version", "registry_digest",
        "module_locks", "author", "note", "created_at",
    )
    out = dict(zip(keys, row))
    if out["created_at"] is not None:
        out["created_at"] = out["created_at"].isoformat()
    out["capability"] = source_capability(parse(out["spec"]))
    return out


def _revision_by_id(
    cur: psycopg.Cursor,
    revision_id: int,
) -> dict[str, Any]:
    cur.execute(
        f"""SELECT {_REVISION_COLS}
              FROM pipelines p
              JOIN pipeline_revisions r ON r.pipeline_id = p.id
             WHERE r.id = %s""",
        (revision_id,),
    )
    row = cur.fetchone()
    if row is None:  # pragma: no cover - protected by caller's row lock/FK
        raise RuntimeError(f"Pipeline revision {revision_id} disappeared")
    return _revision(row)


def get_revision(
    conn: psycopg.Connection,
    name: str,
    version: int | None = None,
) -> dict[str, Any] | None:
    registry.load_custom(conn)
    clause = "r.version = %s" if version is not None else "r.id = p.head_revision_id"
    args: tuple[Any, ...] = (name, version) if version is not None else (name,)
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT {_REVISION_COLS}
                  FROM pipelines p
                  JOIN pipeline_revisions r ON r.pipeline_id = p.id
                 WHERE p.name = %s AND {clause}""",
            args,
        )
        row = cur.fetchone()
    return _revision(row) if row else None


def list_revisions(conn: psycopg.Connection, name: str) -> list[dict[str, Any]]:
    registry.load_custom(conn)
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT {_REVISION_COLS}
                  FROM pipelines p
                  JOIN pipeline_revisions r ON r.pipeline_id = p.id
                 WHERE p.name = %s ORDER BY r.version DESC""",
            (name,),
        )
        return [_revision(row) for row in cur.fetchall()]


def _pipeline(row: tuple[Any, ...], *, include_spec: bool = False) -> dict[str, Any]:
    keys = (
        "id", "name", "title", "description", "builtin", "archived_at",
        "created_at", "updated_at", "head_revision_id", "version", "spec_hash",
        "spec",
    )
    out = dict(zip(keys, row))
    for key in ("archived_at", "created_at", "updated_at"):
        if out[key] is not None:
            out[key] = out[key].isoformat()
    if not include_spec:
        out.pop("spec")
    return out


_PIPELINE_SELECT = """
SELECT p.id, p.name, p.title, p.description, p.builtin, p.archived_at,
       p.created_at, p.updated_at, p.head_revision_id, r.version, r.spec_hash, r.spec
  FROM pipelines p
  LEFT JOIN pipeline_revisions r ON r.id = p.head_revision_id
"""


def list_pipelines(
    conn: psycopg.Connection, *, include_archived: bool = False,
) -> list[dict[str, Any]]:
    where = "" if include_archived else " WHERE p.archived_at IS NULL"
    with conn.cursor() as cur:
        cur.execute(_PIPELINE_SELECT + where + " ORDER BY p.builtin DESC, p.name")
        return [_pipeline(row) for row in cur.fetchall()]


def get_pipeline(
    conn: psycopg.Connection, name: str, *, include_spec: bool = True,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(_PIPELINE_SELECT + " WHERE p.name = %s", (name,))
        row = cur.fetchone()
    return _pipeline(row, include_spec=include_spec) if row else None


def _default_layout(pipeline: Pipeline) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for flow in pipeline.flows:
        result[flow.name] = {
            "nodes": {
                node.id: {"x": 220 * (index % 4), "y": 140 * (index // 4)}
                for index, node in enumerate(flow.nodes)
            },
            "groups": [],
            "annotations": [],
        }
    return result


def create_pipeline(
    conn: psycopg.Connection,
    *,
    name: str,
    spec: Mapping[str, Any],
    title: str = "",
    description: str = "",
    builtin: bool = False,
    author: str = "",
    note: str = "",
) -> dict[str, Any]:
    parsed = parse(dict(spec))
    normalized = parsed.to_dict()
    digest = semantic_hash(parsed)
    locks = module_locks(parsed)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO pipelines (name, title, description, builtin)
                   VALUES (%s, %s, %s, %s) RETURNING id""",
                (name, title, description, builtin),
            )
            pipeline_id = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO pipeline_revisions
                       (pipeline_id, version, spec, spec_hash, registry_version,
                        registry_digest, module_locks, author, note)
                   VALUES (%s, 1, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (
                    pipeline_id, Jsonb(normalized), digest, registry.REGISTRY_VERSION,
                    registry.registry_digest(), Jsonb(locks), author, note,
                ),
            )
            revision_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE pipelines SET head_revision_id = %s WHERE id = %s",
                (revision_id, pipeline_id),
            )
            for flow, layout in _default_layout(parsed).items():
                cur.execute(
                    """INSERT INTO pipeline_layouts
                           (pipeline_revision_id, flow_name, layout, layout_etag)
                       VALUES (%s, %s, %s, %s)""",
                    (revision_id, flow, Jsonb(layout), layout_etag(layout)),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return get_pipeline(conn, name)  # type: ignore[return-value]


def publish_revision(
    conn: psycopg.Connection,
    name: str,
    spec: Mapping[str, Any],
    *,
    expected_version: int | None = None,
    expected_hash: str | None = None,
    author: str = "",
    note: str = "",
) -> PublicationResult:
    """Publish a new semantic revision or atomically reactivate an existing one.

    The expected-head guard is checked while holding the Pipeline row lock. An
    existing hash is therefore either an idempotent no-op at the current head or
    a semantic rollback that moves only the Pipeline head; immutable revisions,
    their layouts, and Source revision pins remain untouched.
    """
    if expected_version is None and expected_hash is None:
        raise PipelinePreconditionRequiredError(
            "publishing an existing Pipeline requires parent_version, "
            "parent_hash, or If-Match")
    parsed = parse(dict(spec))
    normalized = parsed.to_dict()
    digest = semantic_hash(parsed)
    locks = module_locks(parsed)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT p.id, p.head_revision_id
                     FROM pipelines p
                    WHERE p.name = %s AND p.archived_at IS NULL
                    FOR UPDATE""",
                (name,),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(name)
            pipeline_id, parent_id = row
            cur.execute(
                """SELECT version, spec_hash FROM pipeline_revisions
                    WHERE id = %s""",
                (parent_id,),
            )
            head = cur.fetchone()
            if head is None:
                raise RuntimeError(f"Pipeline {name!r} has no current head")
            current_version, current_hash = head
            if expected_version is not None and expected_version != current_version:
                raise StalePipelineError(
                    f"head is version {current_version}, expected {expected_version}")
            if expected_hash is not None and expected_hash != current_hash:
                raise StalePipelineError(
                    f"head hash is {current_hash}, expected {expected_hash}")
            cur.execute(
                """SELECT id, version FROM pipeline_revisions
                    WHERE pipeline_id = %s AND spec_hash = %s""",
                (pipeline_id, digest),
            )
            existing = cur.fetchone()
            if existing is not None:
                revision_id, version = existing
                action: Literal["rollback", "noop"] = (
                    "noop" if revision_id == parent_id else "rollback"
                )
                if action == "rollback":
                    cur.execute(
                        """UPDATE pipelines
                              SET head_revision_id = %s, updated_at = now()
                            WHERE id = %s""",
                        (revision_id, pipeline_id),
                    )
                    append(
                        cur,
                        component="pipeline",
                        event="pipeline.head_rolled_back",
                        pipeline_name=name,
                        pipeline_version=version,
                        message=(
                            f"Pipeline head moved from version {current_version} "
                            f"to existing version {version}"
                        ),
                        data={
                            "from_revision_id": parent_id,
                            "from_version": current_version,
                            "from_hash": current_hash,
                            "to_revision_id": revision_id,
                            "to_version": version,
                            "to_hash": digest,
                        },
                    )
                revision = _revision_by_id(cur, revision_id)
                conn.commit()
                return PublicationResult(revision=revision, action=action)
            cur.execute(
                """SELECT coalesce(max(version), 0)
                     FROM pipeline_revisions
                    WHERE pipeline_id = %s""",
                (pipeline_id,),
            )
            # Version numbers identify immutable history, not distance from the
            # mutable head. After a rollback the next publication must therefore
            # advance past every historic version rather than reuse head + 1.
            version = cur.fetchone()[0] + 1
            cur.execute(
                """INSERT INTO pipeline_revisions
                       (pipeline_id, version, parent_revision_id, spec, spec_hash,
                        registry_version, registry_digest, module_locks, author, note)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (
                    pipeline_id, version, parent_id, Jsonb(normalized), digest,
                    registry.REGISTRY_VERSION, registry.registry_digest(),
                    Jsonb(locks), author, note,
                ),
            )
            revision_id = cur.fetchone()[0]
            old_layouts: dict[str, dict[str, Any]] = {}
            cur.execute(
                "SELECT flow_name, layout FROM pipeline_layouts "
                "WHERE pipeline_revision_id = %s",
                (parent_id,),
            )
            old_layouts.update(cur.fetchall())
            defaults = _default_layout(parsed)
            for flow, default in defaults.items():
                inherited = old_layouts.get(flow, {})
                old_nodes = inherited.get("nodes", {})
                default["nodes"].update({
                    node_id: value for node_id, value in old_nodes.items()
                    if node_id in default["nodes"]
                })
                cur.execute(
                    """INSERT INTO pipeline_layouts
                           (pipeline_revision_id, flow_name, layout, layout_etag)
                       VALUES (%s, %s, %s, %s)""",
                    (revision_id, flow, Jsonb(default), layout_etag(default)),
                )
            cur.execute(
                """UPDATE pipelines SET head_revision_id = %s, updated_at = now()
                    WHERE id = %s""",
                (revision_id, pipeline_id),
            )
            append(
                cur,
                component="pipeline",
                event="pipeline.revision_published",
                pipeline_name=name,
                pipeline_version=version,
                message=f"Published Pipeline revision {version}",
                data={
                    "revision_id": revision_id,
                    "version": version,
                    "spec_hash": digest,
                    "parent_revision_id": parent_id,
                    "parent_version": current_version,
                    "parent_hash": current_hash,
                },
            )
            revision = _revision_by_id(cur, revision_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return PublicationResult(revision=revision, action="created")


def get_layout(
    conn: psycopg.Connection, name: str, version: int, flow: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT l.layout, l.layout_etag, l.updated_at
                 FROM pipeline_layouts l
                 JOIN pipeline_revisions r ON r.id = l.pipeline_revision_id
                 JOIN pipelines p ON p.id = r.pipeline_id
                WHERE p.name = %s AND r.version = %s AND l.flow_name = %s""",
            (name, version, flow),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {"flow": flow, "layout": row[0], "etag": row[1],
            "updated_at": row[2].isoformat()}


def put_layout(
    conn: psycopg.Connection,
    name: str,
    version: int,
    flow: str,
    layout: Mapping[str, Any],
    *,
    if_match: str,
) -> dict[str, Any]:
    digest = layout_etag(dict(layout))
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE pipeline_layouts l
                  SET layout = %s, layout_etag = %s, updated_at = now()
                 FROM pipeline_revisions r, pipelines p
                WHERE l.pipeline_revision_id = r.id AND r.pipeline_id = p.id
                  AND p.name = %s AND r.version = %s AND l.flow_name = %s
                  AND l.layout_etag = %s
                RETURNING l.updated_at""",
            (Jsonb(dict(layout)), digest, name, version, flow, if_match),
        )
        row = cur.fetchone()
    if row is None:
        conn.rollback()
        raise StalePipelineError("layout ETag is stale or layout does not exist")
    conn.commit()
    return {"flow": flow, "layout": dict(layout), "etag": digest,
            "updated_at": row[0].isoformat()}


def archive(conn: psycopg.Connection, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pipelines SET archived_at = now(), updated_at = now() "
            "WHERE name = %s AND archived_at IS NULL RETURNING id",
            (name,),
        )
        changed = cur.fetchone() is not None
    conn.commit()
    return changed


def task_preview(
    conn: psycopg.Connection, name: str, version: int, *, flow: str | None = None,
) -> dict[str, Any]:
    revision = get_revision(conn, name, version)
    if revision is None:
        raise KeyError((name, version))
    flows = revision["spec"]["flows"]
    selected = flow or next(iter(revision["spec"].get("refresh") or flows))
    candidate = flows.get(selected)
    if candidate is None:
        raise ValueError(f"Pipeline has no Flow {selected!r}")
    compiled = pipeline_compile.compile_pipeline(
        revision["spec"], flow=flow, values={},
        inputs={
            boundary["id"]: []
            for boundary in candidate.get("inputs", [])
        },
        source_bound=True,
    )
    unavailable = pipeline_compile.unavailable_modules(compiled["tasks"])
    return {
        "pipeline": name,
        "version": version,
        "spec_hash": revision["spec_hash"],
        "flow": compiled["flow"],
        "tasks": compiled["tasks"],
        "modules_available": not unavailable,
        "unavailable_modules": unavailable,
        "capability": revision["capability"],
    }


__all__ = [
    "PublicationResult",
    "StalePipelineError",
    "archive",
    "create_pipeline",
    "get_layout",
    "get_pipeline",
    "get_revision",
    "list_pipelines",
    "list_revisions",
    "load_seed_matrix",
    "publish_revision",
    "put_layout",
    "seed_matrix_hash",
    "task_preview",
]
