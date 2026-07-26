"""Deterministic generation-2 schema bootstrap."""

from __future__ import annotations

from typing import Any

import psycopg

from windex.config import Settings
from windex.pipeline.store import (
    create_pipeline,
    get_pipeline,
    load_seed_matrix,
    publish_revision,
    seed_matrix_hash,
)
from windex.source.store import create_source, get_source


def seed_canonical(
    conn: psycopg.Connection, settings: Settings | None = None,
) -> dict[str, Any]:
    """Seed built-ins without moving existing Source pins.

    A changed built-in creates a new immutable revision.  An existing Source is
    intentionally left on its current revision until an explicit upgrade.
    """
    active = settings or Settings()
    actions: list[dict[str, str]] = []
    for item in load_seed_matrix(active):
        existing = get_pipeline(conn, item["name"])
        if existing is None:
            created = create_pipeline(
                conn,
                name=item["name"],
                title=item["title"],
                description=item["description"],
                builtin=True,
                spec=item["spec"],
                author="bootstrap",
                note="built-in initial revision",
            )
            action = "created"
            version = created["version"]
        elif existing["spec_hash"] == item["spec_hash"]:
            action = "unchanged"
            version = existing["version"]
        else:
            revision = publish_revision(
                conn,
                item["name"],
                item["spec"],
                expected_version=existing["version"],
                expected_hash=existing["spec_hash"],
                author="bootstrap",
                note="built-in definition update",
            )
            action = "revised"
            version = revision.revision["version"]
        actions.append({"pipeline": item["name"], "action": action})

        binding = item.get("source")
        if binding is None or get_source(conn, binding["name"]) is not None:
            continue
        create_source(conn, {
            **binding,
            "pipeline_name": item["name"],
            "pipeline_version": version,
            "title": item["title"],
            "description": item["description"],
            "origin": {"builtin": item["name"], "ingress": binding["ingress"]},
            "values": binding.get("values") or {},
        }, settings=active)
        actions.append({"source": binding["name"], "action": "created"})

    digest = seed_matrix_hash(active)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE windex_meta SET seed_hash = %s, updated_at = now() "
            "WHERE singleton",
            (digest,),
        )
        # Secret presence is metadata only; the value always remains in the
        # process secret provider (Settings/environment), never Postgres.
        cur.execute(
            """INSERT INTO secret_references
                   (name, provider, configured, metadata)
               VALUES ('github_tokens', 'environment', %s, %s)
               ON CONFLICT (name) DO UPDATE SET
                   configured = EXCLUDED.configured,
                   metadata = EXCLUDED.metadata,
                   updated_at = now()""",
            (bool(active.github_tokens), '{"setting":"WINDEX_GITHUB_TOKENS"}'),
        )
        cur.execute(
            """INSERT INTO operator_settings (scope, values, values_hash)
               VALUES ('_global', '{}', %s)
               ON CONFLICT (scope) DO NOTHING""",
            ("sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",),
        )
    conn.commit()
    return {"seed_hash": digest, "actions": actions}


__all__ = ["seed_canonical"]
