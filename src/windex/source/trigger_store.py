"""Persistence for Source trigger bindings."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.pipeline.events import lock_journal
from windex.source._projections import lock_source
from windex.source.trigger_validation import (
    TriggerValidationError,
    scheduled_next_fire,
    validate_trigger,
)


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

    Disabled, event, and manual triggers are always unarmed. A non-null
    explicit deadline wins for an enabled schedule. Otherwise ``reset``
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


def _event_journal_tail(cur: psycopg.Cursor) -> int:
    """Return the highest committed operational event visible to this change."""

    lock_journal(cur, exclusive=True)
    cur.execute("SELECT coalesce(max(seq), 0) FROM operational_events")
    return int(cur.fetchone()[0])


def _rebase_event_cursor(cur: psycopg.Cursor, trigger_id: int) -> None:
    """Start an event trigger after the journal state visible right now."""

    tail = _event_journal_tail(cur)
    cur.execute(
        """INSERT INTO source_event_trigger_cursors
                   (trigger_id, after_seq, last_checked_at, updated_at)
           VALUES (%s, %s, now(), now())
           ON CONFLICT (trigger_id) DO UPDATE
                   SET after_seq = excluded.after_seq,
                       last_checked_at = excluded.last_checked_at,
                       updated_at = excluded.updated_at""",
        (trigger_id, tail),
    )


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
    source = lock_source(conn, name, include_spec=True)
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
        if body["trigger_type"] == "event":
            _rebase_event_cursor(cur, trigger_id)
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
    source = lock_source(conn, name, include_spec=True)
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
    # valid switch to event/manual fail validation. A caller-supplied non-null
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
        if candidate["trigger_type"] == "event":
            # Any effective edit changes the meaning or availability of the
            # binding. Rebase rather than applying a new binding to events
            # observed under the old one (including time spent disabled).
            _rebase_event_cursor(cur, trigger_id)
        elif current["trigger_type"] == "event":
            cur.execute(
                "DELETE FROM source_event_trigger_cursors WHERE trigger_id = %s",
                (trigger_id,),
            )
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
