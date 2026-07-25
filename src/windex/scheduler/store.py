"""Reads and writes for the `triggers` table — the plain functions the
`/admin/v1/triggers*` routes and the native client sit on.

No HTTP here, deliberately: `service.py`'s seam is already right (zero routes
live there, ~50 pure functions do), and the point of that shape is that a second
transport — the Swift admin client, a CLI subcommand, an MCP tool — is a new
caller, not a new implementation. So these take a connection and return dicts.

Everything that writes goes through `triggers.validate` first. `triggers` carries
no CHECK constraints (schema.sql keeps it permissive so a future trigger type is
an INSERT rather than a migration), which means validation is not belt-and-braces
here — it is the only thing standing between a typo'd cron in a text field and a
row the tick has to cope with at 03:00.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.scheduler import pauses as pz
from windex.scheduler import triggers as tg
from windex.scheduler.fire import unit
from windex.scheduler.triggers import CRON, EVENT, INTERVAL, Trigger

# Fields a caller may set. `last_fired_at`, `next_fire_at` and `last_run_id` are
# scheduler-owned and deliberately absent: letting a client write `next_fire_at`
# directly is how a UI ends up able to schedule a fire in the past, and the tick
# would treat it as downtime.
EDITABLE = ("recipe", "type", "cron", "interval_seconds", "timezone", "event",
            "params", "priority", "jitter_seconds", "catch_up", "enabled")


def _as_dict(trig: Trigger) -> dict[str, Any]:
    return {
        "name": trig.name, "recipe": trig.recipe, "type": trig.type,
        "cron": trig.cron, "interval_seconds": trig.interval_seconds,
        "timezone": trig.timezone, "event": trig.event, "params": trig.params,
        "priority": trig.priority, "jitter_seconds": trig.jitter_seconds,
        "catch_up": trig.catch_up, "enabled": trig.enabled,
        "last_fired_at": trig.last_fired_at, "next_fire_at": trig.next_fire_at,
        "last_run_id": trig.last_run_id,
        "cadence": cadence(trig),
    }


def cadence(trig: Trigger) -> str:
    """A one-line human cadence, the successor to `service._cadence`.

    Always names the zone for a cron trigger, even when it is UTC. That is the
    whole remedy for the standing lie: the old console rendered `daily · 03:00`
    with no zone at all, so an operator read it as local time and was wrong for
    seven months of the year with nothing on screen to correct them.
    """
    if trig.type == CRON:
        return f"cron {trig.cron} ({trig.timezone})"
    if trig.type == INTERVAL:
        secs = trig.interval_seconds or 0
        if secs % 3600 == 0:
            return f"every {secs // 3600}h"
        if secs % 60 == 0:
            return f"every {secs // 60}m"
        return f"every {secs}s"
    if trig.type == EVENT:
        return f"on {trig.event}"
    return "manual only"


def list_triggers(conn: psycopg.Connection, *,
                  now: datetime | None = None) -> list[dict[str, Any]]:
    """Every trigger, with its cadence and its current pause state.

    The pause state is joined in here rather than left to the UI because "why did
    nothing run" is the question this table exists to answer, and answering it
    from two endpoints the client has to correlate is how the console ended up
    showing an unexplained gap in the first place.
    """
    now = now or datetime.now(timezone.utc)
    with unit(conn), conn.cursor() as cur:
        cur.execute(f"SELECT {Trigger.COLUMNS} FROM triggers ORDER BY name")
        trigs = [Trigger.from_row(r) for r in cur.fetchall()]
        cur.execute(
            """SELECT t.name, r.source FROM triggers t
                 LEFT JOIN recipes r ON r.name = t.recipe""")
        sources = dict(cur.fetchall())

        out = []
        for trig in trigs:
            row = _as_dict(trig)
            source = sources.get(trig.name) or trig.recipe
            pause = pz.active_pause(cur, pz.scopes_for(trig.recipe, source), now)
            row["source"] = source
            row["paused"] = pause is not None
            row["pause_scope"] = pause.scope if pause else None
            row["pause_reason"] = pause.reason if pause else None
            try:
                tg.validate(trig)
                row["valid"], row["error"] = True, None
            except ValueError as exc:
                # Surfaced rather than raised: one unevaluatable row must not make
                # the whole list endpoint 500, and the UI needs to be able to show
                # *which* row is broken so somebody can fix it.
                row["valid"], row["error"] = False, str(exc)
            out.append(row)
    return out


def get_trigger(conn: psycopg.Connection, name: str) -> dict[str, Any]:
    """One trigger. Raises KeyError (→ 404) if it does not exist."""
    with unit(conn), conn.cursor() as cur:
        cur.execute(f"SELECT {Trigger.COLUMNS} FROM triggers WHERE name = %s", (name,))
        row = cur.fetchone()
    if row is None:
        raise KeyError(name)
    return _as_dict(Trigger.from_row(row))


def upsert_trigger(conn: psycopg.Connection, entry: dict[str, Any], *,
                   now: datetime | None = None) -> dict[str, Any]:
    """Create or update a trigger. Partial edits preserve unspecified fields.

    Re-plans `next_fire_at` from `now` on every write, and that is the correct
    behaviour rather than an optimisation: changing a cron expression and leaving
    the old planned instant in place means the *next* fire still happens on the
    old schedule, which looks exactly like the edit was ignored. Raises ValueError
    (→ 422) for an invalid result.
    """
    now = now or datetime.now(timezone.utc)
    name = entry.get("name")
    if not name:
        raise ValueError("name is required")

    unknown = set(entry) - {"name"} - set(EDITABLE)
    if unknown:
        raise ValueError(f"unknown field(s): {', '.join(sorted(unknown))}. "
                         f"last_fired_at / next_fire_at / last_run_id are "
                         f"scheduler-owned and cannot be set directly")

    with unit(conn), conn.cursor() as cur:
        cur.execute(f"SELECT {Trigger.COLUMNS} FROM triggers WHERE name = %s FOR UPDATE",
                    (name,))
        row = cur.fetchone()
        if row is None:
            if not entry.get("recipe"):
                raise ValueError("recipe is required to create a trigger")
            base = {"name": name}
        else:
            existing = Trigger.from_row(row)
            base = {"name": name, **{k: getattr(existing, k) for k in EDITABLE}}
        base.update({k: v for k, v in entry.items() if k in EDITABLE})
        base["enabled"] = _coerce_bool(base.get("enabled", True))
        base["catch_up"] = _coerce_bool(base.get("catch_up", False))

        trig = Trigger(**base)
        tg.validate(trig)
        next_fire = tg.plan_next_fire(trig, now)

        cur.execute(
            """INSERT INTO triggers (name, recipe, type, cron, interval_seconds, timezone,
                                     event, params, priority, jitter_seconds, catch_up,
                                     enabled, next_fire_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (name) DO UPDATE SET
                   recipe = EXCLUDED.recipe, type = EXCLUDED.type, cron = EXCLUDED.cron,
                   interval_seconds = EXCLUDED.interval_seconds,
                   timezone = EXCLUDED.timezone, event = EXCLUDED.event,
                   params = EXCLUDED.params, priority = EXCLUDED.priority,
                   jitter_seconds = EXCLUDED.jitter_seconds, catch_up = EXCLUDED.catch_up,
                   enabled = EXCLUDED.enabled, next_fire_at = EXCLUDED.next_fire_at,
                   updated_at = now()""",
            (trig.name, trig.recipe, trig.type, trig.cron, trig.interval_seconds,
             trig.timezone, trig.event, Jsonb(trig.params), trig.priority,
             trig.jitter_seconds, trig.catch_up, trig.enabled, next_fire))

    out = _as_dict(trig)
    out["next_fire_at"] = next_fire
    return out


def delete_trigger(conn: psycopg.Connection, name: str) -> dict[str, Any]:
    """Delete a trigger. Raises KeyError (→ 404) if absent.

    Runs it has already created are untouched — `runs.recipe` is not a foreign key
    for exactly this reason. Deleting the schedule must not delete the history of
    what it did.
    """
    with unit(conn), conn.cursor() as cur:
        cur.execute("DELETE FROM triggers WHERE name = %s", (name,))
        deleted = cur.rowcount
    if not deleted:
        raise KeyError(name)
    return {"deleted": name}


def convert_timezone(conn: psycopg.Connection, name: str, tz: str, *,
                     now: datetime | None = None) -> dict[str, Any]:
    """Move a trigger to a different IANA zone, keeping its wall-clock time.

    This is the explicit action the migration's UTC pinning is designed to lead
    to: the app can offer "these run at 03:00 UTC — convert to Europe/London?" and
    call this. Separate from `upsert_trigger` so the audit trail distinguishes "an
    operator deliberately moved this by an hour" from "an operator edited a cron
    expression", which after a fix to a decade-old timezone lie is worth being able
    to tell apart.
    """
    now = now or datetime.now(timezone.utc)
    return upsert_trigger(conn, {"name": name, "timezone": tz}, now=now)


def trigger_events(conn: psycopg.Connection, name: str | None = None,
                   limit: int = 100) -> list[dict[str, Any]]:
    """Recent trigger-level events — the feed that answers "why did nothing run".

    Filters on `data->>'trigger'` rather than on `run_id`, because the interesting
    events (`trigger.skipped`, `trigger.missed`, `trigger.failed`) have no run:
    not creating one is the whole thing being reported. `run_events` has no index
    on `data`, so this is bounded by `event = ANY(...)` and `LIMIT` — fine for a UI
    panel, not for a scan.
    """
    kinds = ["trigger.skipped", "trigger.deferred", "trigger.missed",
             "trigger.failed", "trigger.coalesced"]
    sql = ["SELECT seq, ts, level, event, message, data, run_id FROM run_events",
           "WHERE event = ANY(%s)"]
    args: list[Any] = [kinds]
    if name:
        sql.append("AND data->>'trigger' = %s")
        args.append(name)
    sql.append("ORDER BY seq DESC LIMIT %s")
    args.append(limit)

    with unit(conn), conn.cursor() as cur:
        cur.execute(" ".join(sql), args)
        rows = cur.fetchall()
    return [{"seq": s, "ts": ts, "level": lv, "event": e, "message": m,
             "data": d, "run_id": rid} for s, ts, lv, e, m, d, rid in rows]


def _coerce_bool(value: Any) -> bool:
    """Coerce a JSON-body boolean, lifted verbatim in spirit from
    `service._coerce_bool`.

    The reason it exists there applies identically here: the route accepts an
    untyped body, so a client can send the string `"false"`, and `bool("false")`
    is True — silently ENABLING a trigger meant to be off. Accept real bools/ints
    and the literal string forms; reject the rest with a ValueError (→ 422) rather
    than guessing.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "yes", "on"):
            return True
        if low in ("false", "0", "no", "off", ""):
            return False
    raise ValueError(f"expected a boolean, got {value!r}")
