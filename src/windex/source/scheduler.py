"""Atomic Source trigger dispatcher."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

import psycopg

from windex.config import Settings
from windex.pipeline.events import append, lock_journal
from windex.pipeline.run_store import RunConflictError, submit_source
from windex.source.trigger_validation import (
    TriggerValidationError,
    scheduled_next_fire,
    validate_trigger,
)

_EVENT_SCAN_LIMIT = 200


@dataclass
class TickResult:
    fired: list[dict] = field(default_factory=list)
    coalesced: list[dict] = field(default_factory=list)
    skipped: list[dict] = field(default_factory=list)
    failed: list[dict] = field(default_factory=list)


def next_fire(
    trigger_type: str, spec: dict, after: datetime,
) -> datetime | None:
    return scheduled_next_fire(trigger_type, spec, after)


def _quarantine_invalid(
    cur: psycopg.Cursor,
    *,
    trigger_id: int,
    source_name: str,
    trigger_type: str,
    error: TriggerValidationError,
) -> None:
    """Disable one legacy-invalid row and leave an operator-visible event."""

    cur.execute(
        """UPDATE source_triggers
              SET enabled = false, next_fire_at = NULL, updated_at = now()
            WHERE id = %s""",
        (trigger_id,),
    )
    append(
        cur,
        component="scheduler",
        event="trigger.invalid",
        level="error",
        source_name=source_name,
        message=f"Disabled invalid {trigger_type} trigger: {error}",
        data={
            "trigger_id": trigger_id,
            "trigger_type": trigger_type,
            "error": str(error),
            "action": "disabled",
        },
    )


def tick(
    conn: psycopg.Connection,
    *,
    now: datetime | None = None,
    limit: int = 200,
    event_scan_limit: int = _EVENT_SCAN_LIMIT,
) -> TickResult:
    instant = now or datetime.now(UTC)
    result = TickResult()
    with conn.cursor() as scan:
        scan.execute(
            """SELECT id FROM source_triggers
                WHERE enabled AND trigger_type IN ('cron','interval')
                  AND next_fire_at IS NOT NULL AND next_fire_at <= %s
                ORDER BY next_fire_at, id LIMIT %s""",
            (instant, limit),
        )
        trigger_ids = [row[0] for row in scan.fetchall()]
    conn.commit()
    for trigger_id in trigger_ids:
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT t.id, s.name, t.flow_name, t.trigger_type,
                                  t.trigger_spec, t.next_fire_at, ctl.paused,
                                  s.enabled, s.archived_at
                             FROM source_triggers t
                             JOIN sources s ON s.id = t.source_id
                             JOIN source_control ctl ON ctl.source_id = s.id
                            WHERE t.id = %s AND t.enabled
                            FOR UPDATE OF t""",
                        (trigger_id,),
                    )
                    row = cur.fetchone()
                    if row is None or row[5] is None or row[5] > instant:
                        continue
                    (
                        _id, source_name, flow, kind, spec, planned, paused,
                        enabled, archived,
                    ) = row
                    try:
                        following = next_fire(kind, spec, instant)
                    except TriggerValidationError as exc:
                        _quarantine_invalid(
                            cur,
                            trigger_id=trigger_id,
                            source_name=source_name,
                            trigger_type=kind,
                            error=exc,
                        )
                        result.failed.append({
                            "trigger_id": trigger_id,
                            "source": source_name,
                            "error": str(exc),
                            "disabled": True,
                        })
                        continue
                    if paused or not enabled or archived:
                        cur.execute(
                            "UPDATE source_triggers SET next_fire_at = %s, "
                            "updated_at = now() WHERE id = %s",
                            (following, trigger_id),
                        )
                        result.skipped.append({
                            "trigger_id": trigger_id, "source": source_name,
                            "reason": "source unavailable",
                        })
                        append(
                            cur, component="scheduler", event="trigger.skipped",
                            source_name=source_name, message="Source unavailable",
                            data={"trigger_id": trigger_id})
                        continue
                    run_id = submit_source(
                        conn, source_name, flow=flow, trigger_type="schedule",
                        trigger_by=f"trigger:{trigger_id}", commit=False)
                    cur.execute(
                        """UPDATE source_triggers
                              SET last_fired_at = %s, last_run_id = coalesce(%s, last_run_id),
                                  next_fire_at = %s, updated_at = now()
                            WHERE id = %s""",
                        (instant, run_id, following, trigger_id),
                    )
                    target = result.fired if run_id is not None else result.coalesced
                    target.append({
                        "trigger_id": trigger_id, "source": source_name,
                        "run_id": run_id, "planned_for": planned.isoformat(),
                    })
        except (ValueError, RunConflictError) as exc:
            conn.rollback()
            result.failed.append({
                "trigger_id": trigger_id, "error": str(exc)})
    _tick_events(
        conn,
        instant=instant,
        trigger_limit=min(max(limit, 1), 1000),
        scan_limit=min(max(event_scan_limit, 1), 1000),
        result=result,
    )
    return result


def _ensure_event_cursors(conn: psycopg.Connection) -> None:
    """Rebase legacy event triggers that predate cursor persistence."""

    with conn.cursor() as cur:
        # Trigger mutations use trigger-row -> journal lock order.  Claim every
        # missing legacy row in stable order before taking the journal barrier
        # so an edit cannot deadlock with cursor initialization.
        cur.execute(
            """SELECT t.id
                 FROM source_triggers t
                 LEFT JOIN source_event_trigger_cursors c
                   ON c.trigger_id = t.id
                WHERE t.trigger_type = 'event' AND c.trigger_id IS NULL
                ORDER BY t.id
                FOR UPDATE OF t""",
        )
        missing = [int(row[0]) for row in cur.fetchall()]
        if not missing:
            conn.commit()
            return
        # A missing legacy cursor must use the same stable-tail barrier as a
        # newly created trigger.  Otherwise an event writer that already owns
        # a lower sequence could commit behind the rebased cursor.
        lock_journal(cur, exclusive=True)
        cur.execute(
            """INSERT INTO source_event_trigger_cursors
                       (trigger_id, after_seq, last_checked_at, updated_at)
               SELECT t.id, coalesce(tail.seq, 0), now(), now()
                 FROM source_triggers t
                CROSS JOIN (
                    SELECT max(seq) AS seq FROM operational_events
                ) tail
                WHERE t.id = ANY(%s) AND t.trigger_type = 'event'
               ON CONFLICT (trigger_id) DO NOTHING""",
            (missing,),
        )
    conn.commit()


def _event_trigger_ids(
    conn: psycopg.Connection,
    *,
    limit: int,
) -> list[int]:
    """Select a fair, bounded set of event triggers that have journal work."""

    _ensure_event_cursors(conn)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT c.trigger_id
                 FROM source_event_trigger_cursors c
                 JOIN source_triggers t ON t.id = c.trigger_id
                CROSS JOIN (
                    SELECT coalesce(max(seq), 0) AS seq
                      FROM operational_events
                ) tail
                WHERE t.enabled AND t.trigger_type = 'event'
                  AND c.after_seq < tail.seq
                ORDER BY c.last_checked_at, c.trigger_id
                LIMIT %s""",
            (limit,),
        )
        trigger_ids = [int(row[0]) for row in cur.fetchall()]
    conn.commit()
    return trigger_ids


def _advance_event_cursor(
    cur: psycopg.Cursor,
    *,
    trigger_id: int,
    after_seq: int,
) -> None:
    cur.execute(
        """UPDATE source_event_trigger_cursors
              SET after_seq = %s, last_checked_at = now(), updated_at = now()
            WHERE trigger_id = %s""",
        (after_seq, trigger_id),
    )


def _event_unavailable_reason(
    *,
    paused: bool,
    source_enabled: bool,
    archived: datetime | None,
) -> str | None:
    if archived is not None:
        return "source archived"
    if not source_enabled:
        return "source disabled"
    if paused:
        return "source paused"
    return None


def _append_event_dispatch(
    cur: psycopg.Cursor,
    *,
    outcome: str,
    trigger_id: int,
    source_name: str,
    event_seq: int,
    event_name: str,
    event_source: str | None,
    run_id: int | None = None,
    reason: str | None = None,
    level: str = "info",
) -> None:
    data = {
        "trigger_id": trigger_id,
        "event_seq": event_seq,
        "event": event_name,
        "event_source": event_source,
    }
    if reason is not None:
        data["reason"] = reason
    append(
        cur,
        component="scheduler",
        event=f"trigger.event_{outcome}",
        level=level,
        source_name=source_name,
        run_id=run_id,
        message=reason or f"Event trigger {outcome}",
        data=data,
    )


def _tick_event_trigger(
    conn: psycopg.Connection,
    *,
    trigger_id: int,
    instant: datetime,
    scan_limit: int,
    result: TickResult,
) -> None:
    context: dict[str, object] = {"trigger_id": trigger_id}
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT t.id, s.name, t.flow_name, t.trigger_spec,
                              ctl.paused, s.enabled, s.archived_at, c.after_seq
                         FROM source_triggers t
                         JOIN sources s ON s.id = t.source_id
                         JOIN source_control ctl ON ctl.source_id = s.id
                         JOIN source_event_trigger_cursors c
                           ON c.trigger_id = t.id
                        WHERE t.id = %s AND t.enabled
                          AND t.trigger_type = 'event'
                        FOR UPDATE OF t, c""",
                    (trigger_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return
                (
                    _id,
                    source_name,
                    flow,
                    spec,
                    paused,
                    source_enabled,
                    archived,
                    after_seq,
                ) = row
                context["source"] = source_name
                try:
                    validate_trigger("event", spec)
                except TriggerValidationError as exc:
                    _quarantine_invalid(
                        cur,
                        trigger_id=trigger_id,
                        source_name=source_name,
                        trigger_type="event",
                        error=exc,
                    )
                    result.failed.append({
                        "trigger_id": trigger_id,
                        "source": source_name,
                        "error": str(exc),
                        "disabled": True,
                    })
                    return
                # Trigger mutations take this row lock before rebasing under
                # the journal lock.  Preserve that order here to avoid a
                # trigger-edit/dispatch deadlock.
                #
                # The exclusive journal lock waits for event writers that may
                # already own a lower sequence, then prevents new writers until
                # cursor advancement and Run submission commit together.
                lock_journal(cur, exclusive=True)
                cur.execute(
                    """SELECT e.seq, e.component, e.event, e.source_name,
                              e.run_id,
                              coalesce(r.trigger_type = 'event', false)
                         FROM operational_events e
                         LEFT JOIN runs r ON r.id = e.run_id
                        WHERE e.seq > %s
                        ORDER BY e.seq
                        LIMIT %s""",
                    (after_seq, scan_limit),
                )
                events = cur.fetchall()
                if not events:
                    # The journal can contain sequence gaps after rolled-back
                    # inserts.  A checked timestamp still rotates this trigger
                    # fairly behind peers.
                    cur.execute(
                        """UPDATE source_event_trigger_cursors
                              SET last_checked_at = now(), updated_at = now()
                            WHERE trigger_id = %s""",
                        (trigger_id,),
                    )
                    return

                selected = None
                for event_row in events:
                    (
                        seq,
                        component,
                        event_name,
                        event_source,
                        _event_run_id,
                        event_triggered,
                    ) = event_row
                    # Scheduler events are dispatch bookkeeping, never inputs.
                    # This makes observable dispatch outcomes unable to sustain
                    # a trigger loop even if an operator names one explicitly.
                    if component == "scheduler":
                        continue
                    if event_name != spec["event"]:
                        continue
                    expected_source = spec.get("source")
                    if expected_source is not None and event_source != expected_source:
                        continue
                    selected = (
                        int(seq),
                        str(event_name),
                        event_source,
                        bool(event_triggered),
                    )
                    break

                if selected is None:
                    _advance_event_cursor(
                        cur,
                        trigger_id=trigger_id,
                        after_seq=int(events[-1][0]),
                    )
                    return

                event_seq, event_name, event_source, event_triggered = selected
                context.update({
                    "event_seq": event_seq,
                    "event": event_name,
                    "event_source": event_source,
                })
                if event_triggered:
                    reason = "event-triggered run causality is limited to one hop"
                    _advance_event_cursor(
                        cur, trigger_id=trigger_id, after_seq=event_seq)
                    item = {
                        "trigger_id": trigger_id,
                        "source": source_name,
                        "event_seq": event_seq,
                        "reason": "loop suppressed",
                    }
                    result.skipped.append(item)
                    _append_event_dispatch(
                        cur,
                        outcome="skipped",
                        trigger_id=trigger_id,
                        source_name=source_name,
                        event_seq=event_seq,
                        event_name=event_name,
                        event_source=event_source,
                        reason=reason,
                    )
                    return

                unavailable = _event_unavailable_reason(
                    paused=bool(paused),
                    source_enabled=bool(source_enabled),
                    archived=archived,
                )
                if unavailable is not None:
                    _advance_event_cursor(
                        cur, trigger_id=trigger_id, after_seq=event_seq)
                    result.skipped.append({
                        "trigger_id": trigger_id,
                        "source": source_name,
                        "event_seq": event_seq,
                        "reason": unavailable,
                    })
                    _append_event_dispatch(
                        cur,
                        outcome="skipped",
                        trigger_id=trigger_id,
                        source_name=source_name,
                        event_seq=event_seq,
                        event_name=event_name,
                        event_source=event_source,
                        reason=unavailable,
                    )
                    return

                run_id = submit_source(
                    conn,
                    source_name,
                    flow=flow,
                    trigger_type="event",
                    trigger_by=f"event-trigger:{trigger_id}:event:{event_seq}",
                    idempotency_key=f"event-trigger:{trigger_id}:{event_seq}",
                    commit=False,
                )
                _advance_event_cursor(
                    cur, trigger_id=trigger_id, after_seq=event_seq)
                cur.execute(
                    """UPDATE source_triggers
                          SET last_fired_at = %s,
                              last_run_id = coalesce(%s, last_run_id),
                              updated_at = now()
                        WHERE id = %s""",
                    (instant, run_id, trigger_id),
                )
                if run_id is None:
                    result.coalesced.append({
                        "trigger_id": trigger_id,
                        "source": source_name,
                        "run_id": None,
                        "event_seq": event_seq,
                    })
                    _append_event_dispatch(
                        cur,
                        outcome="coalesced",
                        trigger_id=trigger_id,
                        source_name=source_name,
                        event_seq=event_seq,
                        event_name=event_name,
                        event_source=event_source,
                        reason="an active Source Flow already covers this event",
                    )
                else:
                    result.fired.append({
                        "trigger_id": trigger_id,
                        "source": source_name,
                        "run_id": run_id,
                        "event_seq": event_seq,
                    })
                    _append_event_dispatch(
                        cur,
                        outcome="fired",
                        trigger_id=trigger_id,
                        source_name=source_name,
                        event_seq=event_seq,
                        event_name=event_name,
                        event_source=event_source,
                        run_id=run_id,
                    )
    except Exception as exc:  # noqa: BLE001 - isolate one durable trigger
        conn.rollback()
        failure = {**context, "error": str(exc)}
        result.failed.append(failure)
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    _append_event_dispatch(
                        cur,
                        outcome="error",
                        trigger_id=trigger_id,
                        source_name=str(context.get("source") or ""),
                        event_seq=int(context.get("event_seq") or 0),
                        event_name=str(context.get("event") or ""),
                        event_source=(
                            str(context["event_source"])
                            if context.get("event_source") is not None
                            else None
                        ),
                        reason=str(exc),
                        level="error",
                    )
        except psycopg.Error:
            conn.rollback()


def _tick_events(
    conn: psycopg.Connection,
    *,
    instant: datetime,
    trigger_limit: int,
    scan_limit: int,
    result: TickResult,
) -> None:
    for trigger_id in _event_trigger_ids(conn, limit=trigger_limit):
        _tick_event_trigger(
            conn,
            trigger_id=trigger_id,
            instant=instant,
            scan_limit=scan_limit,
            result=result,
        )


def arm_unplanned(
    conn: psycopg.Connection, *, now: datetime | None = None,
) -> int:
    instant = now or datetime.now(UTC)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT t.id, s.name, t.trigger_type, t.trigger_spec
                 FROM source_triggers t
                 JOIN sources s ON s.id = t.source_id
                WHERE t.enabled AND t.trigger_type IN ('cron','interval')
                  AND t.next_fire_at IS NULL
                ORDER BY t.id FOR UPDATE OF t SKIP LOCKED""")
        rows = cur.fetchall()
        armed = 0
        for trigger_id, source_name, kind, spec in rows:
            try:
                following = next_fire(kind, spec, instant)
            except TriggerValidationError as exc:
                _quarantine_invalid(
                    cur,
                    trigger_id=trigger_id,
                    source_name=source_name,
                    trigger_type=kind,
                    error=exc,
                )
                continue
            cur.execute(
                "UPDATE source_triggers SET next_fire_at = %s, updated_at = now() "
                "WHERE id = %s",
                (following, trigger_id),
            )
            armed += 1
    conn.commit()
    return armed


def maintain_partitions(
    conn: psycopg.Connection, *, keep_months: int = 6,
) -> list[dict[str, str]]:
    """Keep event/unit partitions ahead and enforce bounded journal retention."""
    if not 1 <= keep_months <= 120:
        raise ValueError("keep_months must be between 1 and 120")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT action, part FROM windex_roll_canonical_partitions(%s, %s)",
            (3, keep_months),
        )
        actions = [
            {"action": row[0], "partition": row[1]}
            for row in cur.fetchall()
        ]
    conn.commit()
    return actions


def prune_expired_artifacts(
    conn: psycopg.Connection, settings: Settings,
) -> int:
    """Delete expired artifact files and their metadata without escaping the root."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT id, relative_path FROM run_artifacts
                WHERE expires_at IS NOT NULL AND expires_at <= now()
                ORDER BY expires_at FOR UPDATE""")
        rows = cur.fetchall()
        root = settings.artifacts_dir.resolve()
        removable: list[str] = []
        for artifact_id, relative_path in rows:
            path = (root / relative_path).resolve()
            if path == root or root not in path.parents:
                continue
            try:
                path.unlink(missing_ok=True)
            except OSError:
                continue
            removable.append(artifact_id)
        if removable:
            cur.execute(
                "DELETE FROM run_artifacts WHERE id = ANY(%s)", (removable,))
    conn.commit()
    return len(removable)


__all__ = [
    "TickResult", "arm_unplanned", "maintain_partitions", "next_fire",
    "prune_expired_artifacts", "tick",
]
