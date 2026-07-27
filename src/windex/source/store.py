"""Persistence and lifecycle operations for Source deployments."""

from __future__ import annotations

import hashlib
import json
import secrets
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.config import Settings, invalidate_overrides
from windex.pipeline.compile import resolve_parameters
from windex.pipeline.contracts import SEARCH_SOURCE_CONTRACT
from windex.pipeline.spec import Pipeline, parse
from windex.pipeline.store import get_revision
from windex.pipeline.validation import validate_deployment
from windex.source.trigger_validation import (
    TriggerValidationError,
    scheduled_next_fire,
    validate_trigger,
)


class StaleSourceError(RuntimeError):
    pass


class SourceConflictError(RuntimeError):
    pass


def values_hash(values: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        dict(values), sort_keys=True, separators=(",", ":"), default=str)
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


_SOURCE_SELECT = """
SELECT s.id, s.name, s.title, s.description, s.origin, s.pipeline_revision_id,
       p.name, r.version, r.spec_hash, s.search_contract_version, s.search_name,
       s.id_prefix, s.collection_key, s.search_profile, s.include_in_all,
       s.state_namespace, s.enabled, s.generation, s.archived_at, s.created_at,
       s.updated_at, c.values, c.values_hash, ctl.paused, ctl.pause_reason,
       ctl.paused_at, r.spec
  FROM sources s
  JOIN pipeline_revisions r ON r.id = s.pipeline_revision_id
  JOIN pipelines p ON p.id = r.pipeline_id
  JOIN source_config c ON c.source_id = s.id
  JOIN source_control ctl ON ctl.source_id = s.id
"""


def _source(row: tuple[Any, ...], *, include_spec: bool = False) -> dict[str, Any]:
    keys = (
        "id", "name", "title", "description", "origin", "pipeline_revision_id",
        "pipeline_name", "pipeline_version", "pipeline_hash",
        "search_contract_version", "search_name", "id_prefix", "collection_key",
        "search_profile", "include_in_all", "state_namespace", "enabled",
        "generation", "archived_at", "created_at", "updated_at", "values",
        "values_hash", "paused", "pause_reason", "paused_at", "spec",
    )
    out = dict(zip(keys, row))
    for key in ("archived_at", "created_at", "updated_at", "paused_at"):
        if out[key] is not None:
            out[key] = out[key].isoformat()
    out["etag"] = out["values_hash"]
    out["ready"] = True
    if not include_spec:
        out.pop("spec")
    return out


def get_source(
    conn: psycopg.Connection, name: str, *, include_spec: bool = False,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(_SOURCE_SELECT + " WHERE s.name = %s", (name,))
        row = cur.fetchone()
    return _source(row, include_spec=include_spec) if row else None


def list_sources(
    conn: psycopg.Connection, *, include_archived: bool = False,
) -> list[dict[str, Any]]:
    where = "" if include_archived else " WHERE s.archived_at IS NULL"
    with conn.cursor() as cur:
        cur.execute(_SOURCE_SELECT + where + " ORDER BY s.name")
        return [_source(row) for row in cur.fetchall()]


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
                       (name, title, description, origin, pipeline_revision_id,
                        search_contract_version, search_name, id_prefix,
                        collection_key, search_profile, include_in_all,
                        state_namespace, enabled)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (
                    body["name"], body.get("title", ""), body.get("description", ""),
                    Jsonb(dict(body.get("origin") or {})), revision["id"],
                    SEARCH_SOURCE_CONTRACT, body["search_name"], body["id_prefix"],
                    body["collection_key"], body["search_profile"],
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
    allowed = {"title", "description", "origin", "enabled", "include_in_all"}
    unknown = set(changes) - allowed
    if unknown:
        raise ValueError(f"unknown Source fields: {', '.join(sorted(unknown))}")
    assignments: list[str] = []
    args: list[Any] = []
    for key, value in changes.items():
        assignments.append(f"{key} = %s")
        args.append(Jsonb(dict(value)) if key == "origin" else value)
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


def list_triggers(conn: psycopg.Connection, name: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT t.id, t.flow_name, t.trigger_type, t.trigger_spec, t.enabled,
                      t.next_fire_at, t.last_fired_at, t.last_run_id
                 FROM source_triggers t JOIN sources s ON s.id = t.source_id
                WHERE s.name = %s ORDER BY t.id""",
            (name,),
        )
        return [{
            "id": row[0], "flow_name": row[1], "trigger_type": row[2],
            "trigger_spec": row[3], "enabled": row[4],
            "next_fire_at": row[5].isoformat() if row[5] else None,
            "last_fired_at": row[6].isoformat() if row[6] else None,
            "last_run_id": row[7],
        } for row in cur.fetchall()]


def _lock_source(
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
        _source(row, include_spec=include_spec)
        if row is not None
        else None
    )


def _trigger_transaction_time(conn: psycopg.Connection) -> datetime:
    """Use Postgres' transaction clock for an atomic re-arm decision."""

    with conn.cursor() as cur:
        cur.execute("SELECT now()")
        return cur.fetchone()[0]


def _scheduled_deadline(
    *,
    trigger_type: str,
    trigger_spec: Mapping[str, Any],
    enabled: bool,
    explicit: Any,
    reset: bool,
    conn: psycopg.Connection,
) -> Any:
    """Resolve the stored deadline after a trigger create or patch.

    Disabled, event, and manual triggers are always unarmed.  A non-null
    explicit deadline wins for an enabled schedule.  Otherwise ``reset``
    requests a fresh deadline from the transaction clock.
    """

    if not enabled or trigger_type not in {"cron", "interval"}:
        return None
    if explicit is not None:
        return explicit
    if reset:
        return scheduled_next_fire(
            trigger_type,
            trigger_spec,
            _trigger_transaction_time(conn),
        )
    return None


def create_trigger(
    conn: psycopg.Connection, name: str, body: Mapping[str, Any],
) -> dict[str, Any]:
    enabled = body.get("enabled", True)
    if not isinstance(enabled, bool):
        raise TriggerValidationError(
            ("enabled",), "trigger enabled must be a boolean")
    validate_trigger(
        body.get("trigger_type"),
        body.get("trigger_spec", {}),
        next_fire_at=body.get("next_fire_at"),
    )
    source = _lock_source(conn, name, include_spec=True)
    if source is None:
        conn.rollback()
        raise KeyError(name)
    flows = set(source["spec"]["flows"])
    if body.get("flow_name") not in flows:
        conn.rollback()
        raise ValueError("trigger Flow does not exist in the active revision")
    deadline = _scheduled_deadline(
        trigger_type=body["trigger_type"],
        trigger_spec=body.get("trigger_spec") or {},
        enabled=enabled,
        explicit=body.get("next_fire_at"),
        reset=True,
        conn=conn,
    )
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO source_triggers
                   (source_id, flow_name, trigger_type, trigger_spec, enabled,
                    next_fire_at)
               VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
            (
                source["id"], body["flow_name"], body["trigger_type"],
                Jsonb(dict(body.get("trigger_spec") or {})),
                enabled, deadline,
            ),
        )
        trigger_id = cur.fetchone()[0]
    conn.commit()
    return next(item for item in list_triggers(conn, name) if item["id"] == trigger_id)


def update_trigger(
    conn: psycopg.Connection,
    name: str,
    trigger_id: int,
    changes: Mapping[str, Any],
) -> dict[str, Any]:
    fields = (
        "flow_name", "trigger_type", "trigger_spec", "enabled", "next_fire_at")
    allowed = set(fields)
    unknown = set(changes) - allowed
    if unknown:
        raise ValueError(f"unknown trigger fields: {', '.join(sorted(unknown))}")
    source = _lock_source(conn, name, include_spec=True)
    if source is None:
        conn.rollback()
        raise KeyError(name)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT t.id, t.flow_name, t.trigger_type, t.trigger_spec,
                      t.enabled, t.next_fire_at, t.last_fired_at, t.last_run_id
                 FROM source_triggers t
                WHERE t.id = %s AND t.source_id = %s
                  FOR UPDATE OF t""",
            (trigger_id, source["id"]),
        )
        row = cur.fetchone()
    if row is None:
        conn.rollback()
        raise KeyError(trigger_id)
    current = {
        "id": row[0],
        "flow_name": row[1],
        "trigger_type": row[2],
        "trigger_spec": row[3],
        "enabled": row[4],
        "next_fire_at": row[5].isoformat() if row[5] else None,
        "last_fired_at": row[6].isoformat() if row[6] else None,
        "last_run_id": row[7],
    }
    requested = dict(changes)
    candidate = {**current, **requested}
    if candidate["flow_name"] not in set(source["spec"]["flows"]):
        raise ValueError("trigger Flow does not exist in the active revision")
    if not isinstance(candidate["enabled"], bool):
        raise TriggerValidationError(
            ("enabled",), "trigger enabled must be a boolean")

    cadence_changed = (
        candidate["trigger_type"] != current["trigger_type"]
        or candidate["trigger_spec"] != current["trigger_spec"]
    )
    enabled_changed = candidate["enabled"] != current["enabled"]
    deadline_explicit = "next_fire_at" in requested
    explicit_deadline = requested.get("next_fire_at")

    # A stale deadline from the prior trigger kind must not make an otherwise
    # valid switch to event/manual fail validation.  A caller-supplied non-null
    # deadline is still rejected for those trigger kinds.
    if candidate["trigger_type"] not in {"cron", "interval"}:
        deadline_for_validation = (
            explicit_deadline
            if deadline_explicit and explicit_deadline is not None
            else None
        )
    else:
        deadline_for_validation = (
            explicit_deadline
            if deadline_explicit
            else candidate["next_fire_at"]
        )
    validate_trigger(
        candidate["trigger_type"],
        candidate["trigger_spec"],
        next_fire_at=deadline_for_validation,
    )

    reset_deadline = (
        cadence_changed
        or (enabled_changed and candidate["enabled"])
        or (deadline_explicit and explicit_deadline is None)
    )
    if (
        candidate["enabled"]
        and candidate["trigger_type"] in {"cron", "interval"}
        and not reset_deadline
        and not deadline_explicit
    ):
        deadline = current["next_fire_at"]
    else:
        deadline = _scheduled_deadline(
            trigger_type=candidate["trigger_type"],
            trigger_spec=candidate["trigger_spec"],
            enabled=candidate["enabled"],
            explicit=explicit_deadline if deadline_explicit else None,
            reset=reset_deadline,
            conn=conn,
        )
    candidate["next_fire_at"] = deadline

    effective = {
        key: candidate[key]
        for key in fields
        if candidate[key] != current[key]
    }
    if not effective:
        conn.commit()
        return current

    assignments, args = [], []
    for key, value in effective.items():
        assignments.append(f"{key} = %s")
        args.append(Jsonb(dict(value)) if key == "trigger_spec" else value)
    args.extend([trigger_id, source["id"]])
    with conn.cursor() as cur:
        cur.execute(
            f"""UPDATE source_triggers
                   SET {', '.join(assignments)}, updated_at = now()
                 WHERE id = %s AND source_id = %s
                 RETURNING id""",
            args,
        )
        if cur.fetchone() is None:
            conn.rollback()
            raise KeyError(trigger_id)
    conn.commit()
    return next(item for item in list_triggers(conn, name) if item["id"] == trigger_id)


def delete_trigger(conn: psycopg.Connection, name: str, trigger_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """DELETE FROM source_triggers t USING sources s
                WHERE t.source_id = s.id AND t.id = %s AND s.name = %s
                RETURNING t.id""",
            (trigger_id, name),
        )
        deleted = cur.fetchone() is not None
    conn.commit()
    return deleted


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
        source = _lock_source(conn, name, include_spec=True)
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
            """SELECT id, state, queued_at, started_at, finished_at, progress, error
                 FROM runs WHERE source_id = %s ORDER BY id DESC LIMIT 1""",
            (source["id"],),
        )
        latest = cur.fetchone()
        cur.execute(
            """SELECT max(finished_at) FILTER (WHERE state = 'succeeded'),
                      max(finished_at) FILTER (WHERE state = 'failed')
                 FROM runs WHERE source_id = %s""",
            (source["id"],),
        )
        success, failure = cur.fetchone()
    latest_run = None
    if latest:
        latest_run = {
            "id": latest[0], "state": latest[1],
            "queued_at": latest[2].isoformat() if latest[2] else None,
            "started_at": latest[3].isoformat() if latest[3] else None,
            "finished_at": latest[4].isoformat() if latest[4] else None,
            "progress": run_progress(conn, [latest[0]]).get(latest[0], latest[5]),
            "error": latest[6],
        }
    return {
        "source": name,
        "enabled": source["enabled"],
        "paused": source["paused"],
        "latest_run": latest_run,
        "current_run": (
            latest_run if latest_run and latest_run["state"] in
            ("queued", "running", "blocked") else None
        ),
        "documents": {
            "staged": documents.get("staged", {"count": 0, "as_of": None}),
            "embedding": documents.get("embedding", {"count": 0, "as_of": None}),
            "searchable": documents.get("searchable", {"count": 0, "as_of": None}),
            "failed": documents.get("failed", {"count": 0, "as_of": None}),
        },
        "last_success": success.isoformat() if success else None,
        "last_failure": failure.isoformat() if failure else None,
        "recent_error": latest[6] if latest and latest[1] == "failed" else None,
    }


def module_statuses(
    conn: psycopg.Connection,
    *,
    enabled_only: bool = False,
) -> list[dict[str, Any]]:
    """Describe whether each Source's frozen revision is runnable here."""
    from windex.pipeline import registry

    where = (
        "WHERE s.enabled AND s.archived_at IS NULL"
        if enabled_only else
        ""
    )
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
            """
        )
        rows = cur.fetchall()
    result = []
    for source, revision_id, version, locks, latest_version in rows:
        unavailable = registry.unavailable_modules(conn, locks or {})
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
