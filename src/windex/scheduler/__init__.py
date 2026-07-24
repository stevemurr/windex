"""The trigger scheduler: rows in, rows out — no processes spawned.

This replaces the `schedule` table plus `service.run_due`/`dispatch_entry` and the
`windex scheduler` timer loop. The shape of one tick:

    triggers(due, enabled) ─ FOR UPDATE SKIP LOCKED ─┐
                                                     ├─ paused?  ─ skip / defer
                                                     └─ fire ─┬─ INSERT runs
                                                              ├─ INSERT run_tasks
                                                              ├─ advance next_fire_at
                                                              └─ stamp last_fired_at
                                                              (ONE transaction)

Four defects in the thing it replaces, each of which cost real time:

**The UTC/local lie.** `schedule.hour`'s column comment says "0-23 (local time)"
but `service._is_due` compares against a naive `datetime.now()`, which inside a
container with no `TZ` set is UTC. The comment has been wrong for the life of the
table. Fixing it by *reading* the comment would silently shift every nightly job
by the box's UTC offset, so `migrate_schedule` pins **every** migrated trigger to
`timezone='UTC'` — behaviour preserved exactly — and converting to a real zone
becomes an explicit, visible edit. New triggers get a real IANA zone, and
`next_fire_at` is computed *in* that zone and stored **absolute**, so no stored
timestamp is ever ambiguous across a DST boundary.

**The re-fire gap.** `dispatch_entry` spawns the job and `_mark_ran` stamps
`last_run` in a *separate*, non-transactional statement. A crash (or a Postgres
blip — see `db.Reconnecting`, which exists because those happen here) between the
two leaves the entry looking un-run, so the next tick fires it again: a duplicate
ingest at 03:00 with nobody watching. Here the run row, its tasks, the watermark
advance and the fired stamp are one `COMMIT`. There is no gap to crash into.

**Pause doesn't stop the scheduler.** `run_due` consults `ingest_enabled` for
`kind='ingest'` rows only — a paused *command* entry fires anyway, and nothing
records that a pause suppressed anything, so the console shows a silent gap and
the operator cannot tell "paused" from "broken". Here every fire consults
`pauses`, and a suppressed fire writes a `trigger.skipped` event carrying the
scope and the reason, so the UI can say *why* nothing ran.

**No catch-up policy at all.** `_is_due` matches on the exact (hour, minute), so
downtime spanning 03:00 means the nightly job simply never runs and nothing says
so. Here a missed window is an explicit choice per trigger: `catch_up=false`
re-arms to the next occurrence (recording the miss), `catch_up=true` fires
**once** — never once per missed window, which is the "pause for a week, unpause,
84 runs stampede" hazard.

Modules:

  * ``triggers`` — pure evaluation: validation, cron/interval expansion in an IANA
    zone, jitter. No IO, so DST is testable without a database.
  * ``pauses``   — which scopes suppress a recipe, and whether one is live.
  * ``events``   — the bounded event vocabulary (a validation boundary, not a
    convenience: an unvalidated event name makes any string a trigger key).
  * ``fire``     — the one-transaction fire, the tick, and event dispatch.
  * ``migrate``  — `schedule` rows → `triggers` rows, UTC-preserving.
  * ``loop``     — the singleton tick loop behind `pg_try_advisory_lock`.
  * ``store``    — CRUD and the "why did nothing run" feed, as plain functions for
    the `/admin/v1/triggers*` routes to sit on.

**Interface seam.** Fanning out `run_tasks` needs a recipe's node list, which the
recipe engine (`windex.recipe.*`, built in parallel) compiles. Nothing here
imports it: every entry point takes a ``compile_tasks(spec) -> list[node dict]``
callable. Tests pass a fake; the real compiler is wired at the call site.
"""

from __future__ import annotations

from windex.scheduler.events import (
    EVENT_KINDS,
    parse_event,
    run_succeeded_event,
    validate_event,
)
from windex.scheduler.fire import (
    Fired,
    TickResult,
    emit_event,
    fire_trigger,
    tick,
)
from windex.scheduler.loop import SCHEDULER_LOCK_KEY, run_loop, try_scheduler_lock
from windex.scheduler.migrate import migrate_schedule
from windex.scheduler.pauses import Pause, active_pause, scopes_for
from windex.scheduler.store import (
    cadence,
    convert_timezone,
    delete_trigger,
    get_trigger,
    list_triggers,
    trigger_events,
    upsert_trigger,
)
from windex.scheduler.triggers import (
    CRON,
    EVENT,
    INTERVAL,
    MANUAL,
    Trigger,
    next_occurrence,
    plan_next_fire,
    validate,
)

__all__ = [
    "CRON",
    "EVENT",
    "EVENT_KINDS",
    "INTERVAL",
    "MANUAL",
    "SCHEDULER_LOCK_KEY",
    "Fired",
    "Pause",
    "TickResult",
    "Trigger",
    "active_pause",
    "cadence",
    "convert_timezone",
    "delete_trigger",
    "emit_event",
    "fire_trigger",
    "get_trigger",
    "list_triggers",
    "migrate_schedule",
    "next_occurrence",
    "parse_event",
    "plan_next_fire",
    "run_loop",
    "run_succeeded_event",
    "scopes_for",
    "tick",
    "trigger_events",
    "try_scheduler_lock",
    "upsert_trigger",
    "validate",
    "validate_event",
]
