"""Persistence and lifecycle operations for Source deployments."""

from __future__ import annotations

import hashlib
import secrets
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.config import Settings
from windex.pipeline.compile import resolve_parameters
from windex.pipeline.contracts import SEARCH_SOURCE_CONTRACT
from windex.pipeline.spec import parse
from windex.pipeline.store import get_revision
from windex.pipeline.validation import validate_deployment
from windex.source._projections import get_source, list_sources, lock_source
from windex.source._shared import (
    SourceConflictError,
    StaleSourceError,
    values_hash,
)
from windex.source.operator_store import (
    delete_operator_setting,
    get_operator_settings,
    list_secrets,
    patch_operator_settings,
)
from windex.source.settings_store import (
    delete_setting,
    patch_settings,
    settings_projection,
)
from windex.source.trigger_store import (
    create_trigger,
    delete_trigger,
    list_triggers,
    update_trigger,
)
from windex.source.trigger_validation import (
    TriggerValidationError,
)


def _conflicts(conn: psycopg.Connection, *, exclude: str | None = None) -> dict[str, set[str]]:
    where = " WHERE name <> %s" if exclude else ""
    args = (exclude,) if exclude else ()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT search_name, id_prefix, collection_key, state_namespace "
            "FROM sources" + where,
            args,
        )
        rows = cur.fetchall()
    return {
        "search_name": {row[0] for row in rows},
        "id_prefix": {row[1] for row in rows},
        "collection_key": {row[2] for row in rows},
        "state_namespace": {row[3] for row in rows},
    }


def validate_candidate(
    conn: psycopg.Connection,
    body: Mapping[str, Any],
    *,
    settings: Settings | None = None,
    exclude: str | None = None,
) -> dict[str, Any]:
    pipeline_name = str(body.get("pipeline_name") or "")
    version = body.get("pipeline_version")
    revision = get_revision(
        conn, pipeline_name, int(version) if version is not None else None)
    if revision is None:
        return {
            "contract": SEARCH_SOURCE_CONTRACT,
            "valid": False,
            "issues": [{
                "path": "pipeline_version",
                "code": "pipeline_revision_not_found",
                "severity": "error",
                "message": "selected Pipeline revision does not exist",
            }],
        }
    candidate = dict(body)
    candidate["values"] = dict(body.get("values") or {})
    result = validate_deployment(
        parse(revision["spec"]),
        candidate,
        settings=settings,
        identity_conflicts=_conflicts(conn, exclude=exclude),
    )
    result["pipeline_hash"] = revision["spec_hash"]
    return result


def create_source(
    conn: psycopg.Connection,
    body: Mapping[str, Any],
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    validation = validate_candidate(conn, body, settings=settings)
    if not validation["valid"]:
        raise SourceConflictError(validation)
    revision = get_revision(
        conn, str(body["pipeline_name"]),
        int(body["pipeline_version"]) if body.get("pipeline_version") is not None else None,
    )
    assert revision is not None
    parsed = parse(revision["spec"], settings)
    normalized = resolve_parameters(
        parsed, settings or Settings(), dict(body.get("values") or {}))
    digest = values_hash(normalized)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sources
                       (name, title, description, origin, metadata,
                        pipeline_revision_id, search_contract_version,
                        search_name, id_prefix, collection_key, search_profile,
                        include_in_all, state_namespace, enabled)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (
                    body["name"], body.get("title", ""), body.get("description", ""),
                    Jsonb(dict(body.get("origin") or {})),
                    Jsonb(dict(body.get("metadata") or {})), revision["id"],
                    SEARCH_SOURCE_CONTRACT, body["search_name"],
                    body["id_prefix"], body["collection_key"], body["search_profile"],
                    bool(body.get("include_in_all", True)), body["state_namespace"],
                    bool(body.get("enabled", True)),
                ),
            )
            source_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO source_config (source_id, values, values_hash) "
                "VALUES (%s, %s, %s)",
                (source_id, Jsonb(normalized), digest),
            )
            cur.execute(
                "INSERT INTO source_control (source_id) VALUES (%s)", (source_id,))
            cur.execute(
                "INSERT INTO source_sched (source_id) VALUES (%s)", (source_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return get_source(conn, str(body["name"]))  # type: ignore[return-value]


def patch_source(
    conn: psycopg.Connection, name: str, changes: Mapping[str, Any],
) -> dict[str, Any]:
    forbidden = {
        "name", "search_name", "id_prefix", "collection_key", "search_profile",
        "state_namespace", "pipeline_name", "pipeline_version",
    } & set(changes)
    if forbidden:
        raise SourceConflictError(
            f"immutable Source fields: {', '.join(sorted(forbidden))}")
    allowed = {
        "title", "description", "origin", "metadata", "enabled",
        "include_in_all",
    }
    unknown = set(changes) - allowed
    if unknown:
        raise ValueError(f"unknown Source fields: {', '.join(sorted(unknown))}")
    assignments: list[str] = []
    args: list[Any] = []
    for key, value in changes.items():
        assignments.append(f"{key} = %s")
        args.append(
            Jsonb(dict(value))
            if key in {"origin", "metadata"}
            else value
        )
    if not assignments:
        current = get_source(conn, name)
        if current is None:
            raise KeyError(name)
        return current
    args.append(name)
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE sources SET {', '.join(assignments)}, updated_at = now() "
            "WHERE name = %s RETURNING id",
            args,
        )
        if cur.fetchone() is None:
            conn.rollback()
            raise KeyError(name)
    conn.commit()
    return get_source(conn, name)  # type: ignore[return-value]


def set_paused(
    conn: psycopg.Connection, name: str, paused: bool, reason: str = "",
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE source_control c
                  SET paused = %s, pause_reason = %s,
                      paused_at = CASE WHEN %s THEN now() ELSE NULL END,
                      updated_at = now()
                 FROM sources s
                WHERE c.source_id = s.id AND s.name = %s
                RETURNING c.source_id""",
            (paused, reason if paused else "", paused, name),
        )
        if cur.fetchone() is None:
            conn.rollback()
            raise KeyError(name)
    conn.commit()
    return get_source(conn, name)  # type: ignore[return-value]


def archive(conn: psycopg.Connection, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE sources
                  SET archived_at = now(), enabled = false, updated_at = now()
                WHERE name = %s AND archived_at IS NULL RETURNING id""",
            (name,),
        )
        changed = cur.fetchone() is not None
    conn.commit()
    return changed


def _confirmation(
    conn: psycopg.Connection, operation: str, subject: str, payload: Mapping[str, Any],
) -> str:
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    payload_hash = values_hash(payload)
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO operation_confirmations
                   (token_hash, operation, subject, payload_hash, expires_at)
               VALUES (%s, %s, %s, %s, %s)""",
            (
                token_hash, operation, subject, payload_hash,
                datetime.now(UTC) + timedelta(minutes=10),
            ),
        )
    conn.commit()
    return token


def reset_preview(conn: psycopg.Connection, name: str) -> dict[str, Any]:
    source = get_source(conn, name)
    if source is None:
        raise KeyError(name)
    payload = _reset_counts(conn, source)
    return {**payload, "confirmation_token": _confirmation(conn, "reset", name, payload)}


def _reset_counts(
    conn: psycopg.Connection, source: Mapping[str, Any],
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM documents WHERE source_id = %s", (source["id"],))
        documents = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM source_units WHERE source_id = %s", (source["id"],))
        units = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM run_tasks WHERE source_id = %s "
            "AND state IN ('pending','ready','running','blocked')",
            (source["id"],),
        )
        work = cur.fetchone()[0]
    return {
        "generation": source["generation"],
        "documents": documents,
        "state_units": units,
        "outstanding_tasks": work,
    }


def reset(conn: psycopg.Connection, name: str, token: str) -> dict[str, Any]:
    source = get_source(conn, name, include_spec=True)
    if source is None:
        raise KeyError(name)
    payload = _reset_counts(conn, source)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE operation_confirmations
                      SET consumed_at = now()
                    WHERE token_hash = %s AND operation = 'reset' AND subject = %s
                      AND consumed_at IS NULL AND expires_at > now()
                    RETURNING payload_hash""",
                (token_hash, name),
            )
            confirmation = cur.fetchone()
            if confirmation is None:
                raise SourceConflictError("invalid or expired reset confirmation")
            if confirmation[0] != values_hash(payload):
                raise SourceConflictError(
                    "reset preview is stale; request a new exact-count preview")
            cur.execute(
                """UPDATE source_control
                      SET paused = true, pause_reason = 'corpus reset pending',
                          paused_at = coalesce(paused_at, now()), updated_at = now()
                    WHERE source_id = %s""",
                (source["id"],),
            )
            cur.execute(
                """UPDATE runs SET cancel_requested = true, updated_at = now()
                    WHERE source_id = %s
                      AND state IN ('queued','running','blocked')""",
                (source["id"],),
            )
            cur.execute(
                """UPDATE run_tasks SET yield_requested = true
                    WHERE source_id = %s AND state = 'running'""",
                (source["id"],),
            )
        from windex.pipeline.events import append
        from windex.pipeline.run_store import submit_reset

        run_id = submit_reset(
            conn, source, was_paused=bool(source["paused"]),
            pause_reason=str(source["pause_reason"] or ""), commit=False)
        with conn.cursor() as cur:
            append(
                cur, component="source", event="source.reset_queued",
                source_name=name, pipeline_name=source["pipeline_name"],
                pipeline_version=source["pipeline_version"], run_id=run_id,
                data={**payload, "planned_generation": source["generation"] + 1},
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "source": name,
        "run_id": run_id,
        "generation": source["generation"],
        "planned_generation": source["generation"] + 1,
        "state": "queued",
    }


def _upgrade_trigger_bindings(
    conn: psycopg.Connection,
    source_id: int,
) -> list[dict[str, Any]]:
    """Return the stable trigger configuration covered by an upgrade token.

    Scheduling timestamps are intentionally excluded: the scheduler may update
    them while an operator reviews a preview, but those updates do not change
    whether a trigger can run against the target Pipeline revision.
    """

    with conn.cursor() as cur:
        cur.execute(
            """SELECT id, flow_name, trigger_type, trigger_spec, enabled
                 FROM source_triggers
                WHERE source_id = %s
                ORDER BY id""",
            (source_id,),
        )
        return [{
            "id": row[0],
            "flow_name": row[1],
            "trigger_type": row[2],
            "trigger_spec": row[3],
            "enabled": row[4],
        } for row in cur.fetchall()]


def _upgrade_plan(
    conn: psycopg.Connection,
    name: str,
    target_version: int,
    *,
    values: Mapping[str, Any] | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    source = get_source(conn, name, include_spec=True)
    if source is None:
        raise KeyError(name)
    target = get_revision(conn, source["pipeline_name"], target_version)
    if target is None:
        raise KeyError((source["pipeline_name"], target_version))
    pipeline = parse(target["spec"], settings)
    target_flows = {flow.name for flow in pipeline.flows}
    trigger_bindings = _upgrade_trigger_bindings(conn, source["id"])
    trigger_issues = []
    # Disabled bindings are constraints too: accepting a latent invalid
    # binding would merely defer the failure until an operator re-enables it.
    for trigger in trigger_bindings:
        if trigger["flow_name"] in target_flows:
            continue
        state = "enabled" if trigger["enabled"] else "disabled"
        disabled_note = (
            ""
            if trigger["enabled"]
            else (
                " Disabled triggers are checked because they can be "
                "re-enabled."
            )
        )
        trigger_issues.append({
            "path": f"triggers.{trigger['id']}.flow_name",
            "code": "trigger_flow_missing",
            "severity": "error",
            "message": (
                f"{state.capitalize()} trigger {trigger['id']} references "
                f"Flow {trigger['flow_name']!r}, which target revision "
                f"{target_version} does not define.{disabled_note} Rebind or "
                "delete the trigger before upgrading."
            ),
        })
    active_settings = settings or Settings()
    old_values = dict(source["values"])
    declarations = {item.key: item for item in pipeline.parameters}
    retained: dict[str, Any] = {}
    defaulted: dict[str, Any] = {}
    migration_clamped: dict[str, dict[str, Any]] = {}
    for key, declaration in declarations.items():
        if key in old_values and old_values[key] is not None:
            try:
                value = declaration.coerce(old_values[key], active_settings)
            except ValueError:
                continue
            retained[key] = value
            if value != old_values[key]:
                migration_clamped[key] = {
                    "from": old_values[key],
                    "to": value,
                }
        elif declaration.default is not None:
            defaulted[key] = declaration.default
    removed = sorted(set(old_values) - set(declarations))
    requested = dict(values) if values is not None else {**retained, **defaulted}
    validation = validate_candidate(
        conn,
        {
            **source,
            "pipeline_name": source["pipeline_name"],
            "pipeline_version": target_version,
            "values": requested,
        },
        settings=active_settings,
        exclude=name,
    )
    issues = [*validation["issues"], *trigger_issues]
    candidate: dict[str, Any]
    normalization_error: ValueError | None = None
    try:
        candidate = resolve_parameters(pipeline, active_settings, requested)
    except ValueError as exc:
        candidate = requested
        normalization_error = exc
    valid = bool(validation["valid"])
    if normalization_error is not None and valid:
        # Deployment validation and normalization must agree before a token can
        # authorize the atomic pointer/config write.
        valid = False
        issues.append({
            "path": "values",
            "code": "invalid_candidate",
            "severity": "error",
            "message": str(normalization_error),
        })
    valid = valid and not trigger_issues
    clamped = dict(migration_clamped if values is None else {})
    clamped.update({
        key: {"from": requested[key], "to": candidate[key]}
        for key in requested.keys() & candidate.keys()
        if requested[key] != candidate[key]
    })
    missing = sorted({
        issue["path"].removeprefix("values.")
        for issue in issues
        if issue["code"] == "required"
        and issue["path"].startswith("values.")
    })
    install_changed = sorted(
        key for key in candidate
        if key in declarations
        and declarations[key].stage == "install"
        and candidate.get(key) != old_values.get(key)
    )
    payload = {
        "source_id": source["id"],
        "from_version": source["pipeline_version"],
        "target_version": target_version,
        "target_hash": target["spec_hash"],
        "expected_etag": source["values_hash"],
        "candidate_hash": values_hash(candidate),
    }
    return {
        **payload,
        "candidate": candidate,
        "retained": retained,
        "defaulted": defaulted,
        "removed": removed,
        "clamped": clamped,
        "missing": missing,
        "install_stage_changed": install_changed,
        "state_impact": {
            "stores_preserved": sorted(pipeline.state),
            "requires_confirmation": bool(install_changed),
            "trigger_bindings_checked": len(trigger_bindings),
            "trigger_bindings_policy": "all_enabled_and_disabled",
            "trigger_bindings_hash": values_hash({
                "triggers": trigger_bindings,
            }),
        },
        "issues": issues,
        "valid": valid,
    }


def upgrade_preview(
    conn: psycopg.Connection,
    name: str,
    target_version: int,
    *,
    values: Mapping[str, Any] | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    plan = _upgrade_plan(
        conn,
        name,
        target_version,
        values=values,
        settings=settings,
    )
    token = (
        _confirmation(conn, "upgrade", name, plan)
        if plan["valid"] else None
    )
    return {**plan, "confirmation_token": token}


def upgrade(
    conn: psycopg.Connection,
    name: str,
    target_version: int,
    values: Mapping[str, Any],
    token: str,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    try:
        # Serialize revision changes with all trigger mutations.  The preview
        # hash detects edits that committed before this lock; operations that
        # start afterward block and then validate against the new revision.
        source = lock_source(conn, name, include_spec=True)
        if source is None:
            raise KeyError(name)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id FROM source_triggers
                    WHERE source_id = %s ORDER BY id
                    FOR UPDATE""",
                (source["id"],),
            )
            cur.fetchall()
        target = get_revision(conn, source["pipeline_name"], target_version)
        if target is None:
            raise KeyError((source["pipeline_name"], target_version))
        plan = _upgrade_plan(
            conn,
            name,
            target_version,
            values=values,
            settings=settings,
        )
        if not plan["valid"]:
            raise SourceConflictError({
                "message": "Source upgrade candidate is invalid",
                "issues": plan["issues"],
            })
        candidate = plan["candidate"]
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE operation_confirmations
                      SET consumed_at = now()
                    WHERE token_hash = %s AND operation = 'upgrade'
                      AND subject = %s AND payload_hash = %s
                      AND consumed_at IS NULL AND expires_at > now()
                    RETURNING token_hash""",
                (token_hash, name, values_hash(plan)),
            )
            if cur.fetchone() is None:
                raise SourceConflictError(
                    "upgrade preview is stale or confirmation expired")
            cur.execute(
                """UPDATE source_config
                      SET values = %s, values_hash = %s, updated_at = now()
                    WHERE source_id = %s AND values_hash = %s
                    RETURNING source_id""",
                (
                    Jsonb(candidate), plan["candidate_hash"], source["id"],
                    plan["expected_etag"],
                ),
            )
            if cur.fetchone() is None:
                raise StaleSourceError("Source configuration changed after preview")
            cur.execute(
                """UPDATE sources SET pipeline_revision_id = %s, updated_at = now()
                    WHERE id = %s AND pipeline_revision_id = %s RETURNING id""",
                (target["id"], source["id"], source["pipeline_revision_id"]),
            )
            if cur.fetchone() is None:
                raise StaleSourceError("Source revision changed after preview")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return get_source(conn, name)  # type: ignore[return-value]


def status(conn: psycopg.Connection, name: str) -> dict[str, Any]:
    from windex.pipeline.overview import run_progress

    source = get_source(conn, name)
    if source is None:
        raise KeyError(name)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT status, count(*), max(updated_at)
                 FROM documents WHERE source_id = %s GROUP BY status""",
            (source["id"],),
        )
        documents = {
            row[0]: {"count": row[1], "as_of": row[2].isoformat() if row[2] else None}
            for row in cur.fetchall()
        }
        cur.execute(
            """WITH latest AS (
                   SELECT id, state, cancel_requested, queued_at, started_at,
                          finished_at, progress, error
                     FROM runs
                    WHERE source_id = %s
                    ORDER BY id DESC
                    LIMIT 1
               ),
               current AS (
                   SELECT id, state, cancel_requested, queued_at, started_at,
                          finished_at, progress, error
                     FROM runs
                    WHERE source_id = %s
                      AND state IN ('queued', 'running', 'blocked')
                    ORDER BY id DESC
                    LIMIT 1
               )
               SELECT latest.*, 'latest' AS selection FROM latest
               UNION ALL
               SELECT current.*, 'current' AS selection FROM current""",
            (source["id"], source["id"]),
        )
        selected = {row[-1]: row[:-1] for row in cur.fetchall()}
        cur.execute(
            """SELECT max(finished_at) FILTER (WHERE state = 'succeeded'),
                      max(finished_at) FILTER (WHERE state = 'failed')
                 FROM runs WHERE source_id = %s""",
            (source["id"],),
        )
        success, failure = cur.fetchone()
    progress = run_progress(
        conn,
        list({row[0] for row in selected.values()}),
    )

    def run_projection(row: tuple[Any, ...] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "id": row[0],
            "state": row[1],
            "cancel_requested": row[2],
            "queued_at": row[3].isoformat() if row[3] else None,
            "started_at": row[4].isoformat() if row[4] else None,
            "finished_at": row[5].isoformat() if row[5] else None,
            "progress": progress.get(row[0], row[6]),
            "error": row[7],
        }

    latest = selected.get("latest")
    latest_run = run_projection(latest)
    module_status = next(
        iter(module_statuses(conn, name=name))
    )
    return {
        "source": name,
        "enabled": source["enabled"],
        "paused": source["paused"],
        "latest_run": latest_run,
        "current_run": run_projection(selected.get("current")),
        "documents": {
            "staged": documents.get("staged", {"count": 0, "as_of": None}),
            "embedding": documents.get("embedding", {"count": 0, "as_of": None}),
            "searchable": documents.get("searchable", {"count": 0, "as_of": None}),
            "failed": documents.get("failed", {"count": 0, "as_of": None}),
        },
        "last_success": success.isoformat() if success else None,
        "last_failure": failure.isoformat() if failure else None,
        "recent_error": latest[7] if latest and latest[1] == "failed" else None,
        "module_status": module_status,
    }


def module_statuses(
    conn: psycopg.Connection,
    *,
    enabled_only: bool = False,
    name: str | None = None,
) -> list[dict[str, Any]]:
    """Describe whether each Source's frozen revision is runnable here."""
    from windex.pipeline import registry

    clauses: list[str] = []
    args: list[Any] = []
    if enabled_only:
        clauses.append("s.enabled AND s.archived_at IS NULL")
    if name is not None:
        clauses.append("s.name = %s")
        args.append(name)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT s.name, r.id, r.version, r.module_locks,
                   (SELECT max(head.version)
                      FROM pipeline_revisions head
                     WHERE head.pipeline_id = r.pipeline_id)
              FROM sources s
              JOIN pipeline_revisions r ON r.id = s.pipeline_revision_id
              {where}
             ORDER BY s.name
            """,
            args,
        )
        rows = cur.fetchall()
    unavailable_sets = registry.unavailable_modules_many(
        conn, [row[3] or {} for row in rows])
    result = []
    for row, unavailable in zip(rows, unavailable_sets, strict=True):
        source, revision_id, version, _locks, latest_version = row
        result.append({
            "source": source,
            "pipeline_revision_id": revision_id,
            "pipeline_version": version,
            "latest_pipeline_version": latest_version,
            "available": not unavailable,
            "upgrade_required": bool(unavailable),
            "unavailable_modules": unavailable,
        })
    return result


__all__ = [
    "SourceConflictError",
    "StaleSourceError",
    "TriggerValidationError",
    "archive",
    "create_source",
    "create_trigger",
    "delete_setting",
    "delete_trigger",
    "get_source",
    "list_sources",
    "list_triggers",
    "patch_settings",
    "patch_source",
    "reset",
    "reset_preview",
    "set_paused",
    "settings_projection",
    "status",
    "module_statuses",
    "update_trigger",
    "upgrade",
    "upgrade_preview",
    "validate_candidate",
    "values_hash",
    "get_operator_settings",
    "patch_operator_settings",
    "delete_operator_setting",
    "list_secrets",
]
