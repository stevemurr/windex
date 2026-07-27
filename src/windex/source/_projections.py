"""Database projections for canonical Sources."""

from __future__ import annotations

from typing import Any

import psycopg


_SOURCE_SELECT = """
SELECT s.id, s.name, s.title, s.description, s.origin, s.metadata,
       s.pipeline_revision_id, p.name, r.version, r.spec_hash,
       s.search_contract_version, s.search_name, s.id_prefix, s.collection_key,
       s.search_profile, s.include_in_all, s.state_namespace, s.enabled,
       s.generation, s.archived_at, s.created_at, s.updated_at, c.values,
       c.values_hash, ctl.paused, ctl.pause_reason, ctl.paused_at, r.spec,
       r.module_locks
  FROM sources s
  JOIN pipeline_revisions r ON r.id = s.pipeline_revision_id
  JOIN pipelines p ON p.id = r.pipeline_id
  JOIN source_config c ON c.source_id = s.id
  JOIN source_control ctl ON ctl.source_id = s.id
"""


def _source(
    row: tuple[Any, ...],
    *,
    include_spec: bool = False,
    ready: bool,
) -> dict[str, Any]:
    keys = (
        "id", "name", "title", "description", "origin", "metadata",
        "pipeline_revision_id", "pipeline_name", "pipeline_version", "pipeline_hash",
        "search_contract_version", "search_name", "id_prefix", "collection_key",
        "search_profile", "include_in_all", "state_namespace", "enabled",
        "generation", "archived_at", "created_at", "updated_at", "values",
        "values_hash", "paused", "pause_reason", "paused_at", "spec",
        "_module_locks",
    )
    out = dict(zip(keys, row))
    for key in ("archived_at", "created_at", "updated_at", "paused_at"):
        if out[key] is not None:
            out[key] = out[key].isoformat()
    out["etag"] = out["values_hash"]
    out["ready"] = ready
    out.pop("_module_locks")
    if not include_spec:
        out.pop("spec")
    return out


def _source_projections(
    conn: psycopg.Connection,
    rows: list[tuple[Any, ...]],
    *,
    include_spec: bool = False,
) -> list[dict[str, Any]]:
    """Project Sources with the runtime availability of their frozen revision."""
    from windex.pipeline import registry

    lock_sets = [row[-1] or {} for row in rows]
    unavailable = registry.unavailable_modules_many(conn, lock_sets)
    return [
        _source(row, include_spec=include_spec, ready=not missing)
        for row, missing in zip(rows, unavailable, strict=True)
    ]


def get_source(
    conn: psycopg.Connection, name: str, *, include_spec: bool = False,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(_SOURCE_SELECT + " WHERE s.name = %s", (name,))
        row = cur.fetchone()
    if row is None:
        return None
    return _source_projections(
        conn, [row], include_spec=include_spec)[0]


def list_sources(
    conn: psycopg.Connection, *, include_archived: bool = False,
) -> list[dict[str, Any]]:
    where = "" if include_archived else " WHERE s.archived_at IS NULL"
    with conn.cursor() as cur:
        cur.execute(_SOURCE_SELECT + where + " ORDER BY s.name")
        rows = cur.fetchall()
    return _source_projections(conn, rows)


def lock_source(
    conn: psycopg.Connection,
    name: str,
    *,
    include_spec: bool = False,
) -> dict[str, Any] | None:
    """Lock a Source while changing configuration bound to its revision."""

    with conn.cursor() as cur:
        cur.execute(
            _SOURCE_SELECT + " WHERE s.name = %s FOR UPDATE OF s",
            (name,),
        )
        row = cur.fetchone()
    return (
        _source_projections(conn, [row], include_spec=include_spec)[0]
        if row is not None
        else None
    )
