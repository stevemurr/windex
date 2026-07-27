"""Persistence for deployment-wide operator settings and secret references."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.config import invalidate_overrides
from windex.source._shared import StaleSourceError, values_hash


def get_operator_settings(conn: psycopg.Connection) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT values, values_hash, updated_at FROM operator_settings "
            "WHERE scope = '_global'")
        row = cur.fetchone()
    if row is None:
        return {"scope": "_global", "values": {}, "etag": values_hash({})}
    return {
        "scope": "_global", "values": row[0], "etag": row[1],
        "updated_at": row[2].isoformat(),
    }


def patch_operator_settings(
    conn: psycopg.Connection,
    changes: Mapping[str, Any],
    *,
    if_match: str,
) -> dict[str, Any]:
    from windex import settings_schema

    current = get_operator_settings(conn)
    if current["etag"] != if_match:
        raise StaleSourceError("operator settings ETag is stale")
    normalized = settings_schema.coerce_all(
        settings_schema.GLOBAL, dict(changes))
    candidate = {**current["values"], **normalized}
    digest = values_hash(candidate)
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE operator_settings
                  SET values = %s, values_hash = %s, updated_at = now()
                WHERE scope = '_global' AND values_hash = %s RETURNING scope""",
            (Jsonb(candidate), digest, if_match),
        )
        if cur.fetchone() is None:
            conn.rollback()
            raise StaleSourceError("operator settings ETag is stale")
    conn.commit()
    invalidate_overrides()
    return get_operator_settings(conn)


def delete_operator_setting(
    conn: psycopg.Connection,
    key: str,
    *,
    if_match: str,
) -> dict[str, Any]:
    from windex import settings_schema

    allowed = {
        declaration.key
        for declaration in settings_schema.fields_for(settings_schema.GLOBAL)
    }
    if key not in allowed:
        raise ValueError(f"{key!r} is not an editable operator setting")
    current = get_operator_settings(conn)
    candidate = dict(current["values"])
    candidate.pop(key, None)
    # PATCH merges, so update directly for a deletion.
    if current["etag"] != if_match:
        raise StaleSourceError("operator settings ETag is stale")
    digest = values_hash(candidate)
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE operator_settings
                  SET values = %s, values_hash = %s, updated_at = now()
                WHERE scope = '_global' AND values_hash = %s RETURNING scope""",
            (Jsonb(candidate), digest, if_match),
        )
        if cur.fetchone() is None:
            conn.rollback()
            raise StaleSourceError("operator settings ETag is stale")
    conn.commit()
    invalidate_overrides()
    return get_operator_settings(conn)


def list_secrets(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, provider, configured, metadata, updated_at "
            "FROM secret_references ORDER BY name")
        return [{
            "name": row[0], "provider": row[1], "configured": row[2],
            "metadata": row[3], "updated_at": row[4].isoformat(),
        } for row in cur.fetchall()]
