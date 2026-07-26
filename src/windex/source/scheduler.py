"""Atomic Source trigger dispatcher."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg
from croniter import croniter

from windex.config import Settings
from windex.pipeline.events import append
from windex.pipeline.run_store import RunConflictError, submit_source


@dataclass
class TickResult:
    fired: list[dict] = field(default_factory=list)
    coalesced: list[dict] = field(default_factory=list)
    skipped: list[dict] = field(default_factory=list)
    failed: list[dict] = field(default_factory=list)


def next_fire(
    trigger_type: str, spec: dict, after: datetime,
) -> datetime | None:
    if trigger_type == "interval":
        seconds = int(spec.get("seconds") or spec.get("interval_seconds") or 0)
        if seconds < 1:
            raise ValueError("interval trigger requires positive seconds")
        return after + timedelta(seconds=seconds)
    if trigger_type == "cron":
        expression = str(spec.get("cron") or "")
        if len(expression.split()) != 5:
            raise ValueError("cron trigger requires a five-field expression")
        timezone = ZoneInfo(str(spec.get("timezone") or "UTC"))
        local = after.astimezone(timezone)
        return croniter(expression, local).get_next(datetime).astimezone(UTC)
    return None


def tick(
    conn: psycopg.Connection,
    *,
    now: datetime | None = None,
    limit: int = 200,
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
                    following = next_fire(kind, spec, instant)
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
    return result


def arm_unplanned(
    conn: psycopg.Connection, *, now: datetime | None = None,
) -> int:
    instant = now or datetime.now(UTC)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT id, trigger_type, trigger_spec FROM source_triggers
                WHERE enabled AND trigger_type IN ('cron','interval')
                  AND next_fire_at IS NULL FOR UPDATE SKIP LOCKED""")
        rows = cur.fetchall()
        for trigger_id, kind, spec in rows:
            cur.execute(
                "UPDATE source_triggers SET next_fire_at = %s, updated_at = now() "
                "WHERE id = %s",
                (next_fire(kind, spec, instant), trigger_id),
            )
    conn.commit()
    return len(rows)


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
