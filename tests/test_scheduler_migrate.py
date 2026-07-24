"""`schedule` → `triggers`.

The assertion that carries the whole file: **every migrated trigger is UTC.**
`schedule.hour`'s column comment claims local time; `service._is_due` compares it
against a naive `datetime.now()`, which in a container with no `TZ` is UTC. Reading
the comment instead of the code would shift every nightly job by the box's offset
with nothing logging it.

`schedule` is deliberately absent from conftest's TRUNCATE list, so these run
against the rows `db._seed_schedule` really writes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from windex.scheduler import triggers as tg
from windex.scheduler.migrate import migrate_schedule, schedule_to_cron

UTC = timezone.utc
NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def sched_rows(pg):
    with pg.cursor() as cur:
        cur.execute("SELECT name, kind, target, hour, minute, weekday, enabled, last_run "
                    "FROM schedule ORDER BY name")
        rows = cur.fetchall()
    pg.rollback()
    return rows


def trig_rows(pg):
    with pg.cursor() as cur:
        cur.execute("SELECT name, recipe, type, cron, timezone, jitter_seconds, catch_up, "
                    "enabled, last_fired_at, next_fire_at FROM triggers ORDER BY name")
        rows = cur.fetchall()
    pg.rollback()
    return {r[0]: r for r in rows}


@pytest.fixture()
def pg_sched(pg):
    """The `pg` fixture truncates `triggers` but not `schedule`, which is what we
    want here: real seeded rows in, a blank target table."""
    return pg


@pytest.fixture()
def add_schedule(pg):
    """Insert extra `schedule` rows, and remove them again afterwards.

    `schedule` is session-lived (conftest truncates `triggers` but not `schedule`,
    deliberately — the seeded rows are the realistic input). A test that leaves
    rows behind changes what every later test in the session sees, which is how a
    suite starts passing or failing on ordering. Cleaning up keeps the seeded set
    the only shared state.
    """
    added: list[str] = []

    def _add(name, kind, target, hour, minute, weekday=None, enabled=True,
             last_run=None):
        with pg.cursor() as cur:
            cur.execute(
                """INSERT INTO schedule (name, kind, target, hour, minute, weekday,
                                         enabled, last_run)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (name) DO UPDATE SET hour = EXCLUDED.hour,
                       minute = EXCLUDED.minute, weekday = EXCLUDED.weekday,
                       enabled = EXCLUDED.enabled, last_run = EXCLUDED.last_run""",
                (name, kind, target, hour, minute, weekday, enabled, last_run))
        pg.commit()
        added.append(name)

    yield _add

    if added:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM schedule WHERE name = ANY(%s)", (added,))
        pg.commit()


# --- the cron mapping --------------------------------------------------------

@pytest.mark.parametrize("hour, minute, weekday, expected", [
    (3, 0, None, "0 3 * * *"),
    (3, 15, None, "15 3 * * *"),
    (5, 45, None, "45 5 * * *"),
    (2, 15, 0, "15 2 * * 0"),      # Sunday: schedule 0=Sun, crontab 0=Sun — identical
    (6, 30, 6, "30 6 * * 6"),      # Saturday
    (0, 0, 3, "0 0 * * 3"),        # Wednesday
])
def test_schedule_to_cron(hour, minute, weekday, expected):
    assert schedule_to_cron(hour, minute, weekday) == expected


def test_the_weekday_convention_survives_the_mapping(pg_sched, add_schedule):
    """schedule.weekday is 0=Sun; crontab's dow field is 0=Sun. An off-by-one here
    would move a weekly job to the wrong day and read as a scheduling flake for
    months."""
    add_schedule("weekly-eval", "command", "eval", 6, 30, weekday=0)  # Sunday
    migrate_schedule(pg_sched, now=NOW)

    row = trig_rows(pg_sched)["weekly-eval"]
    assert row[3] == "30 6 * * 0"
    # 2026-07-26 is a Sunday.
    assert row[9] == datetime(2026, 7, 26, 6, 30, tzinfo=UTC)
    assert row[9].weekday() == 6      # Python's Sunday


# --- UTC preservation: the point of the exercise -----------------------------

def test_every_migrated_trigger_is_utc(pg_sched):
    rows = migrate_schedule(pg_sched, now=NOW)
    assert rows, "expected db._seed_schedule to have populated `schedule`"
    assert all(r["timezone"] == "UTC" for r in rows)
    assert {t[4] for t in trig_rows(pg_sched).values()} == {"UTC"}


def test_the_fire_time_is_exactly_what_is_due_today_meant(pg_sched, add_schedule):
    """The behaviour-preservation check, stated as an absolute instant: an entry
    reading hour=3, minute=15 has ALWAYS meant 03:15 UTC, whatever the column
    comment says. After migrating it still does."""
    add_schedule("ingest-probe", "ingest", "wiki", 3, 15)
    migrate_schedule(pg_sched, now=NOW)

    got = trig_rows(pg_sched)["ingest-probe"][9]
    assert got == datetime(2026, 7, 25, 3, 15, tzinfo=UTC)
    # And explicitly NOT the local-time reading the comment invites. On a UK box
    # in July that would be 02:15 UTC — an hour earlier, silently.
    assert got != datetime(2026, 7, 25, 3, 15,
                           tzinfo=ZoneInfo("Europe/London")).astimezone(UTC)


def test_every_row_keeps_its_exact_hour_and_minute(pg_sched):
    """Migration adds no jitter and shifts no times. `_seed_schedule` already
    staggers the seeded ingests 15 minutes apart by baking the offset into
    `minute`, and that stagger rides along inside the cron expression — jittering
    on top would re-spread rows that are already spread, for no reason.

    Checked against the source rows rather than against an expected list, so the
    invariant holds whatever is in `schedule` on the day.
    """
    want = {name: (hour, minute, weekday)
            for name, _k, _t, hour, minute, weekday, _e, _lr in sched_rows(pg_sched)}
    migrate_schedule(pg_sched, now=NOW)

    rows = trig_rows(pg_sched)
    assert set(rows) == set(want)
    for name, (hour, minute, weekday) in want.items():
        minute_f, hour_f, dom, mon, dow = rows[name][3].split()
        assert (int(minute_f), int(hour_f)) == (minute, hour)
        assert (dom, mon) == ("*", "*")
        assert dow == ("*" if weekday is None else str(weekday))
        assert rows[name][5] == 0          # jitter_seconds
        assert rows[name][6] is False      # catch_up: matches _is_due's behaviour


# --- preserved fields --------------------------------------------------------

def test_disabled_entries_stay_disabled(pg_sched, add_schedule):
    add_schedule("ingest-off", "ingest", "hn", 4, 0, enabled=False)
    migrate_schedule(pg_sched, now=NOW)
    assert trig_rows(pg_sched)["ingest-off"][7] is False


def test_last_run_becomes_last_fired_at(pg_sched, add_schedule):
    """Carried over so the console's "last run" column does not go blank at
    cutover — a UI that forgets what happened yesterday looks like an outage."""
    when = datetime(2026, 7, 23, 3, 15, tzinfo=UTC)
    add_schedule("ingest-hist", "ingest", "docs", 3, 15, last_run=when)
    migrate_schedule(pg_sched, now=NOW)
    assert trig_rows(pg_sched)["ingest-hist"][8] == when


def test_both_entry_kinds_become_recipes(pg_sched, add_schedule):
    """C.5's premise: `kind` disappears because everything is a recipe. The column
    only ever encoded how to dispatch a *process*."""
    add_schedule("ingest-wiki2", "ingest", "wiki", 3, 30)
    add_schedule("maintain2", "command", "maintain", 5, 45)
    migrate_schedule(pg_sched, now=NOW)
    rows = trig_rows(pg_sched)
    assert rows["ingest-wiki2"][1] == "wiki"
    assert rows["maintain2"][1] == "maintain"
    assert {rows["ingest-wiki2"][2], rows["maintain2"][2]} == {"cron"}


def test_a_recipe_name_override_is_respected(pg_sched, add_schedule):
    """The seam for a cutover where a legacy target and its recipe disagree."""
    add_schedule("ingest-gh2", "ingest", "gh", 4, 30)
    migrate_schedule(pg_sched, now=NOW,
                     recipe_for=lambda kind, target: f"{target}_repos"
                     if target == "gh" else target)
    assert trig_rows(pg_sched)["ingest-gh2"][1] == "gh_repos"


# --- idempotence -------------------------------------------------------------

def test_re_running_never_clobbers_a_hand_tuned_trigger(pg_sched, add_schedule):
    """The cutover is not atomic — the old loop keeps running against `schedule`
    until the new one takes over, and this may be run more than once in between.
    An operator who has already converted a trigger to a real zone must not have
    it reset to UTC by a second pass."""
    add_schedule("ingest-tuned", "ingest", "hf", 3, 45)
    migrate_schedule(pg_sched, now=NOW)

    with pg_sched.cursor() as cur:
        cur.execute("UPDATE triggers SET timezone = 'Europe/London', cron = '0 4 * * *' "
                    "WHERE name = 'ingest-tuned'")
    pg_sched.commit()

    second = migrate_schedule(pg_sched, now=NOW)
    assert not any(r["created"] for r in second if r["name"] == "ingest-tuned")
    row = trig_rows(pg_sched)["ingest-tuned"]
    assert row[4] == "Europe/London" and row[3] == "0 4 * * *"


def test_overwrite_is_opt_in(pg_sched, add_schedule):
    add_schedule("ingest-force", "ingest", "hf", 3, 45)
    migrate_schedule(pg_sched, now=NOW)
    with pg_sched.cursor() as cur:
        cur.execute("UPDATE triggers SET timezone = 'Europe/London' "
                    "WHERE name = 'ingest-force'")
    pg_sched.commit()
    migrate_schedule(pg_sched, now=NOW, overwrite=True)
    assert trig_rows(pg_sched)["ingest-force"][4] == "UTC"


def test_migration_leaves_schedule_untouched(pg_sched):
    """Rollback during the cutover has to be "stop the new loop", not "restore
    from a backup"."""
    before = sched_rows(pg_sched)
    migrate_schedule(pg_sched, now=NOW)
    assert sched_rows(pg_sched) == before


# --- the result is usable ----------------------------------------------------

def test_every_migrated_row_validates_and_is_armed(pg_sched, add_schedule):
    add_schedule("weekly-thing", "command", "eval", 6, 30, weekday=2)
    rows = migrate_schedule(pg_sched, now=NOW)
    for r in rows:
        trig = tg.Trigger(name=r["name"], recipe=r["recipe"], cron=r["cron"],
                          timezone="UTC", enabled=r["enabled"])
        tg.validate(trig)
        assert r["next_fire_at"] is not None
        assert NOW < r["next_fire_at"] <= NOW + timedelta(days=7)


def test_migrated_triggers_are_immediately_tickable(pg_sched, add_schedule):
    """End to end: migrate, then let the tick fire one of the migrated rows into a
    real run. This is the cutover rehearsal."""
    from psycopg.types.json import Jsonb

    from windex.scheduler import tick

    add_schedule("ingest-cut", "ingest", "wiki", 3, 15)
    with pg_sched.cursor() as cur:
        cur.execute("""INSERT INTO recipes (name, source, spec, spec_hash)
                       VALUES ('wiki', 'wiki', %s, 'sha1:wiki')
                       ON CONFLICT (name) DO NOTHING""", (Jsonb({}),))
    pg_sched.commit()

    migrate_schedule(pg_sched, now=NOW)
    # 03:15 UTC the following morning — the instant the legacy row always meant.
    fired = tick(pg_sched, compile_tasks=lambda spec: [
        {"node": "sync", "kind": "discover", "module": "wiki.sync"},
    ], now=datetime(2026, 7, 25, 3, 15, tzinfo=UTC))
    assert "ingest-cut" in [f.trigger for f in fired.fired]
