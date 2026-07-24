"""`schedule` rows → `triggers` rows.

Ten rows on the real box, so this is not about throughput. It is about the one
decision that is easy to get wrong and impossible to notice afterwards.

**Every migrated trigger gets `timezone='UTC'`.** The `schedule.hour` column
comment says "0-23 (local time)" and has said so since the table was created. It
is wrong. `service._is_due` compares the entry's hour and minute against a naive
`datetime.now()`, and the process that evaluates it runs in a container with no
`TZ` set, where naive local time *is* UTC. So a row reading `hour=3` has always
meant 03:00 UTC, whatever the comment claims and whatever the operator believed
when they typed it.

There are two ways to "fix" that, and only one of them is honest:

* Read the comment, migrate to the box's local zone. Every nightly job silently
  shifts by the UTC offset. On a UK box in summer the 03:00 ingests become
  02:00 UTC; nothing errors, nothing logs, and the only symptom is that the
  overnight window moved for reasons nobody can reconstruct months later.
* Read the *code*, migrate to UTC. Behaviour is bit-for-bit preserved, the lie
  is over, and converting to `Europe/London` becomes a deliberate edit the
  operator makes with the new times on screen in front of them.

This does the second. It is the whole reason the migration is a named function
with tests instead of three lines of SQL.

**Jitter is deliberately zero.** `db._seed_schedule` already staggers the seeded
ingests 15 minutes apart (03:00, 03:15, 03:30…) by baking the offset into
`minute`. That stagger survives the migration inside the cron expression, so
adding `jitter_seconds` on top would re-spread rows that are already spread and
change fire times for no reason. Jitter is for *new* triggers, where it replaces
the hand-rolled arithmetic rather than compounding it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

import psycopg

from windex.scheduler import triggers as tg
from windex.scheduler.fire import unit
from windex.scheduler.triggers import CRON, Trigger

# schedule.weekday is 0=Sun … 6=Sat (see schema.sql), and crontab's day-of-week
# field is 0=Sun … 6=Sat too, so the value carries across unchanged. Written down
# rather than assumed: this is the one field where an off-by-one would move a
# weekly job to the wrong day and look like a scheduling flake for months.
_DOW_IS_IDENTICAL = True


def schedule_to_cron(hour: int, minute: int, weekday: int | None) -> str:
    """The 5-field crontab expression equivalent to one `schedule` row."""
    dow = "*" if weekday is None else str(weekday)
    return f"{minute} {hour} * * {dow}"


def default_recipe_for(kind: str, target: str) -> str:
    """Which recipe a legacy entry becomes.

    Identity on `target` in both directions, and that is not a placeholder: C.5's
    premise is that *everything* is a recipe, so `kind='ingest', target='hf'`
    becomes the `hf` recipe and `kind='command', target='maintain'` becomes the
    `maintain` recipe. The `kind` column disappears with the table — the
    distinction it encoded (`windex refresh --source X` vs a mapped CLI command)
    was an artifact of dispatching *processes*, and a scheduler that writes rows
    has no use for it.
    """
    return target


def migrate_schedule(conn: psycopg.Connection, *,
                     recipe_for: Callable[[str, str], str] | None = None,
                     now: datetime | None = None,
                     overwrite: bool = False) -> list[dict]:
    """Create a `triggers` row for every `schedule` row. Returns what it did.

    Idempotent, and re-runnable at any point during the cutover: the insert is
    `ON CONFLICT (name) DO NOTHING` unless `overwrite=True`, so running it twice
    — or running it after an operator has already hand-tuned a migrated trigger —
    never clobbers the newer value. That matters because the cutover is not
    atomic: the old `windex scheduler` keeps running against `schedule` until the
    new loop takes over, and this may well be run more than once in between.

    Both tables stay populated afterwards, by design. Nothing here deletes a
    `schedule` row; rollback during the cutover has to be "stop the new loop",
    not "restore from a backup".
    """
    now = now or datetime.now(timezone.utc)
    recipe_for = recipe_for or default_recipe_for
    out: list[dict] = []

    with unit(conn), conn.cursor() as cur:
        cur.execute(
            """SELECT name, kind, target, hour, minute, weekday, enabled, last_run
                 FROM schedule ORDER BY hour, minute, name""")
        rows = cur.fetchall()

        for name, kind, target, hour, minute, weekday, enabled, last_run in rows:
            recipe = recipe_for(kind, target)
            cron = schedule_to_cron(hour, minute, weekday)
            trig = Trigger(
                name=name, recipe=recipe, type=CRON, cron=cron,
                # THE line. See the module docstring — do not "fix" this to a
                # local zone without also telling every operator their nightly
                # jobs moved.
                timezone="UTC",
                jitter_seconds=0,          # the stagger is already in `minute`
                catch_up=False,            # matches _is_due: a missed window is gone
                enabled=enabled,
                last_fired_at=last_run,
            )
            tg.validate(trig)
            next_fire = tg.plan_next_fire(trig, now)

            conflict = ("""ON CONFLICT (name) DO UPDATE SET
                               recipe = EXCLUDED.recipe, type = EXCLUDED.type,
                               cron = EXCLUDED.cron, timezone = EXCLUDED.timezone,
                               enabled = EXCLUDED.enabled,
                               last_fired_at = EXCLUDED.last_fired_at,
                               next_fire_at = EXCLUDED.next_fire_at,
                               updated_at = now()"""
                        if overwrite else "ON CONFLICT (name) DO NOTHING")
            cur.execute(
                f"""INSERT INTO triggers (name, recipe, type, cron, timezone,
                                          jitter_seconds, catch_up, enabled,
                                          last_fired_at, next_fire_at)
                    VALUES (%s, %s, 'cron', %s, 'UTC', 0, false, %s, %s, %s)
                    {conflict}
                    RETURNING name""",
                (name, recipe, cron, enabled, last_run, next_fire),
            )
            out.append({
                "name": name, "recipe": recipe, "cron": cron, "timezone": "UTC",
                "enabled": enabled, "next_fire_at": next_fire,
                "created": cur.fetchone() is not None,
                "from": {"kind": kind, "target": target, "hour": hour,
                         "minute": minute, "weekday": weekday},
            })
    return out
