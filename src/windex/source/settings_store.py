"""Typed configuration persistence for Source deployments."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.config import Settings
from windex.pipeline.compile import resolve_parameters
from windex.pipeline.spec import Pipeline, parse
from windex.source._projections import get_source
from windex.source._shared import StaleSourceError, values_hash


def settings_projection(
    conn: psycopg.Connection, name: str, *, settings: Settings | None = None,
) -> dict[str, Any]:
    source = get_source(conn, name, include_spec=True)
    if source is None:
        raise KeyError(name)
    pipeline = parse(source["spec"], settings)
    configured = source["values"]
    fields = []
    for declaration in pipeline.parameters:
        value = configured.get(declaration.key)
        origin = "source" if declaration.key in configured else (
            "default" if declaration.default is not None else "unset")
        fields.append({
            **declaration.to_spec(),
            "value": None if declaration.secret else value,
            "origin": origin,
            "secret_set": bool(value) if declaration.secret else False,
            "clamped": False,
        })
    return {
        "source": name,
        "pipeline": source["pipeline_name"],
        "pipeline_version": source["pipeline_version"],
        "etag": source["values_hash"],
        "values": {
            key: value for key, value in configured.items()
            if not next(
                (p.secret for p in pipeline.parameters if p.key == key), False)
        },
        "fields": fields,
    }


def _normalize_configured_parameters(
    pipeline: Pipeline,
    settings: Settings,
    values: Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve configured values without materializing an unset optional key."""
    normalized = resolve_parameters(pipeline, settings, values)
    for declaration in pipeline.parameters:
        if (
            declaration.key not in values
            and declaration.default is None
            and not declaration.required
        ):
            normalized.pop(declaration.key, None)
    return normalized


def patch_settings(
    conn: psycopg.Connection,
    name: str,
    changes: Mapping[str, Any],
    *,
    if_match: str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    source = get_source(conn, name, include_spec=True)
    if source is None:
        raise KeyError(name)
    if source["values_hash"] != if_match:
        raise StaleSourceError("Source settings ETag is stale")
    pipeline = parse(source["spec"], settings)
    candidate = {**source["values"], **dict(changes)}
    normalized = _normalize_configured_parameters(
        pipeline, settings or Settings(), candidate)
    return _replace_settings(
        conn,
        name,
        source,
        normalized,
        if_match=if_match,
        settings=settings,
    )


def _replace_settings(
    conn: psycopg.Connection,
    name: str,
    source: Mapping[str, Any],
    values: Mapping[str, Any],
    *,
    if_match: str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Store one exact, normalized Source configuration behind its ETag."""
    if source["values_hash"] != if_match:
        raise StaleSourceError("Source settings ETag is stale")
    digest = values_hash(values)
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE source_config
                  SET values = %s, values_hash = %s, updated_at = now()
                WHERE source_id = %s AND values_hash = %s RETURNING source_id""",
            (Jsonb(dict(values)), digest, source["id"], if_match),
        )
        if cur.fetchone() is None:
            conn.rollback()
            raise StaleSourceError("Source settings ETag is stale")
    conn.commit()
    return settings_projection(conn, name, settings=settings)


def delete_setting(
    conn: psycopg.Connection,
    name: str,
    key: str,
    *,
    if_match: str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    source = get_source(conn, name, include_spec=True)
    if source is None:
        raise KeyError(name)
    values = dict(source["values"])
    pipeline = parse(source["spec"], settings)
    declaration = next((p for p in pipeline.parameters if p.key == key), None)
    if declaration is None:
        raise ValueError(f"unknown Pipeline parameter {key!r}")
    values.pop(key, None)
    if declaration.default is not None:
        values[key] = declaration.default
    normalized = _normalize_configured_parameters(
        pipeline, settings or Settings(), values)
    return _replace_settings(
        conn,
        name,
        source,
        normalized,
        if_match=if_match,
        settings=settings,
    )
