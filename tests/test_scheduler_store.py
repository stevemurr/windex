"""The CRUD/read functions the `/admin/v1/triggers*` routes sit on."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from psycopg.types.json import Jsonb

from windex.scheduler import store
from windex.scheduler.fire import tick

UTC = timezone.utc
NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def nodes(spec):
    return [{"node": "run", "kind": "discover", "module": "test.discover"}]


@pytest.fixture()
def recipe(pg):
    with pg.cursor() as cur:
        cur.execute("""INSERT INTO recipes (name, source, spec, spec_hash)
                       VALUES ('wiki', 'wiki', %s, 'sha1:wiki')""", (Jsonb({}),))
    pg.commit()
    return "wiki"


# --- create / read / update / delete -----------------------------------------

def test_create_validates_and_arms(pg, recipe):
    out = store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                                    "cron": "0 3 * * *",
                                    "timezone": "Europe/London"}, now=NOW)
    assert out["next_fire_at"] == datetime(2026, 7, 25, 2, 0, tzinfo=UTC)  # 03:00 BST
    assert out["cadence"] == "cron 0 3 * * * (Europe/London)"

    got = store.get_trigger(pg, "nightly")
    assert got["timezone"] == "Europe/London" and got["enabled"] is True


def test_a_partial_edit_preserves_the_rest(pg, recipe):
    store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki", "cron": "0 3 * * *",
                              "timezone": "Europe/London", "jitter_seconds": 600,
                              "priority": 70}, now=NOW)
    store.upsert_trigger(pg, {"name": "nightly", "enabled": False}, now=NOW)
    got = store.get_trigger(pg, "nightly")
    assert got["enabled"] is False
    assert (got["timezone"], got["jitter_seconds"], got["priority"]) == \
        ("Europe/London", 600, 70)


def test_editing_the_cron_re_plans_the_next_fire(pg, recipe):
    """Leaving the old planned instant in place would make the next fire happen on
    the old schedule, which looks exactly like the edit was ignored."""
    store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                              "cron": "0 3 * * *"}, now=NOW)
    out = store.upsert_trigger(pg, {"name": "nightly", "cron": "0 21 * * *"}, now=NOW)
    assert out["next_fire_at"] == datetime(2026, 7, 24, 21, 0, tzinfo=UTC)


def test_scheduler_owned_fields_cannot_be_written(pg, recipe):
    """A client able to set `next_fire_at` can schedule a fire in the past, and the
    tick would read that as downtime."""
    with pytest.raises(ValueError, match="scheduler-owned"):
        store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                                  "cron": "0 3 * * *",
                                  "next_fire_at": "2020-01-01T00:00:00Z"}, now=NOW)


def test_a_string_false_does_not_enable_a_trigger(pg, recipe):
    """`bool("false")` is True. The route body is untyped, so this is reachable."""
    out = store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                                    "cron": "0 3 * * *", "enabled": "false"}, now=NOW)
    assert out["enabled"] is False
    with pytest.raises(ValueError, match="expected a boolean"):
        store.upsert_trigger(pg, {"name": "nightly", "enabled": "maybe"}, now=NOW)


def test_an_invalid_write_is_refused(pg, recipe):
    with pytest.raises(ValueError, match="not a known IANA zone"):
        store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                                  "cron": "0 3 * * *", "timezone": "Mars/Olympus"},
                             now=NOW)
    with pytest.raises(KeyError):
        store.get_trigger(pg, "nightly")


def test_create_requires_a_recipe(pg):
    with pytest.raises(ValueError, match="recipe is required"):
        store.upsert_trigger(pg, {"name": "orphan", "cron": "0 3 * * *"}, now=NOW)


def test_delete_keeps_the_run_history(pg, recipe):
    """`runs.recipe` is deliberately not a foreign key: deleting a schedule must
    not delete the record of what it did."""
    store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                              "cron": "0 3 * * *"}, now=NOW)
    tick(pg, compile_tasks=nodes, now=datetime(2026, 7, 25, 3, 0, tzinfo=UTC))

    assert store.delete_trigger(pg, "nightly") == {"deleted": "nightly"}
    with pytest.raises(KeyError):
        store.get_trigger(pg, "nightly")
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM runs")
        assert cur.fetchone()[0] == 1
    pg.rollback()


# --- listing -----------------------------------------------------------------

def test_list_reports_pause_state_alongside_the_schedule(pg, recipe):
    """"Why did nothing run" must be answerable from one response — correlating two
    endpoints client-side is how the console ended up showing an unexplained gap."""
    store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                              "cron": "0 3 * * *"}, now=NOW)
    with pg.cursor() as cur:
        cur.execute("INSERT INTO pauses (scope, reason) VALUES ('source:wiki', 'disk full')")
    pg.commit()

    row = store.list_triggers(pg, now=NOW)[0]
    assert row["paused"] is True
    assert (row["pause_scope"], row["pause_reason"]) == ("source:wiki", "disk full")
    assert row["source"] == "wiki" and row["valid"] is True


def test_list_surfaces_a_broken_row_instead_of_raising(pg, recipe):
    """`triggers` has no CHECK constraints, so a row hand-edited in psql is
    reachable. One bad row must not 500 the whole list."""
    with pg.cursor() as cur:
        cur.execute("""INSERT INTO triggers (name, recipe, type, cron)
                       VALUES ('broken', 'wiki', 'cron', '99 3 * * *')""")
    pg.commit()
    row = store.list_triggers(pg, now=NOW)[0]
    assert row["valid"] is False and "bad cron" in row["error"]


@pytest.mark.parametrize("entry, expected", [
    ({"cron": "0 3 * * *", "timezone": "UTC"}, "cron 0 3 * * * (UTC)"),
    ({"type": "interval", "interval_seconds": 30}, "every 30s"),
    ({"type": "interval", "interval_seconds": 300}, "every 5m"),
    ({"type": "interval", "interval_seconds": 7200}, "every 2h"),
    ({"type": "event", "event": "run.succeeded:wiki_sync"}, "on run.succeeded:wiki_sync"),
    ({"type": "manual"}, "manual only"),
])
def test_cadence_always_names_the_zone_for_a_cron(pg, recipe, entry, expected):
    """The old console rendered `daily · 03:00` with no zone at all, so an operator
    read it as local time and was wrong for seven months of the year."""
    out = store.upsert_trigger(pg, {"name": "probe", "recipe": "wiki", **entry}, now=NOW)
    assert out["cadence"] == expected


# --- the timezone conversion action -----------------------------------------

def test_convert_timezone_keeps_the_wall_clock_and_moves_the_instant(pg, recipe):
    """The explicit action the migration's UTC pinning leads to: 03:00 UTC becomes
    03:00 London, which is a different absolute instant, visibly."""
    before = store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                                       "cron": "0 3 * * *"}, now=NOW)
    assert before["next_fire_at"] == datetime(2026, 7, 25, 3, 0, tzinfo=UTC)

    after = store.convert_timezone(pg, "nightly", "Europe/London", now=NOW)
    assert after["timezone"] == "Europe/London"
    assert after["next_fire_at"] == datetime(2026, 7, 25, 2, 0, tzinfo=UTC)
    assert store.get_trigger(pg, "nightly")["cron"] == "0 3 * * *"   # unchanged


# --- the event feed ----------------------------------------------------------

def test_trigger_events_feed_explains_a_silent_night(pg, recipe):
    store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                              "cron": "0 3 * * *"}, now=NOW)
    with pg.cursor() as cur:
        cur.execute("INSERT INTO pauses (scope, reason) VALUES ('global', 'reindexing')")
    pg.commit()

    tick(pg, compile_tasks=nodes, now=datetime(2026, 7, 25, 3, 0, tzinfo=UTC))

    feed = store.trigger_events(pg, "nightly")
    assert [e["event"] for e in feed] == ["trigger.skipped"]
    assert feed[0]["data"]["reason"] == "reindexing"
    assert feed[0]["run_id"] is None       # the point is that no run was created

    assert store.trigger_events(pg, "someone-else") == []
    assert len(store.trigger_events(pg)) == 1


def test_the_feed_excludes_ordinary_run_lifecycle_events(pg, recipe):
    """`run.queued` belongs on the run, not in the "why is my schedule quiet"
    panel."""
    store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                              "cron": "0 3 * * *"}, now=NOW)
    tick(pg, compile_tasks=nodes, now=datetime(2026, 7, 25, 3, 0, tzinfo=UTC))
    assert store.trigger_events(pg) == []
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM run_events WHERE event = 'run.queued'")
        assert cur.fetchone()[0] == 1
    pg.rollback()


# --- round trip --------------------------------------------------------------

def test_a_trigger_created_through_the_store_fires(pg, recipe):
    store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                              "cron": "0 3 * * *", "priority": 70,
                              "params": {"days": 7}}, now=NOW)
    r = tick(pg, compile_tasks=nodes, now=datetime(2026, 7, 25, 3, 0, tzinfo=UTC))
    assert len(r.fired) == 1
    with pg.cursor() as cur:
        cur.execute("SELECT priority, params FROM runs WHERE id = %s", (r.fired[0].run_id,))
        assert cur.fetchone() == (70, {"days": 7})
    pg.rollback()
    assert store.get_trigger(pg, "nightly")["next_fire_at"] == \
        datetime(2026, 7, 26, 3, 0, tzinfo=UTC)


def test_disabling_a_trigger_stops_it_without_deleting_it(pg, recipe):
    store.upsert_trigger(pg, {"name": "nightly", "recipe": "wiki",
                              "cron": "0 3 * * *"}, now=NOW)
    store.upsert_trigger(pg, {"name": "nightly", "enabled": False}, now=NOW)
    assert not tick(pg, compile_tasks=nodes,
                    now=datetime(2026, 7, 25, 3, 0, tzinfo=UTC)).fired
    # Re-enabling re-arms it from the moment it is re-enabled.
    later = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    out = store.upsert_trigger(pg, {"name": "nightly", "enabled": True}, now=later)
    assert out["next_fire_at"] == later + timedelta(hours=15)
