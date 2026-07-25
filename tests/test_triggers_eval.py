"""Pure trigger evaluation: cron/DST, interval, jitter, validation, event vocabulary.

No database. These are the tests that have to exist for the DST fix to mean
anything — you cannot verify "03:00 stays 03:00 across the March switch" by
waiting for March.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from windex.scheduler import events as ev
from windex.scheduler import triggers as tg
from windex.scheduler.triggers import CRON, EVENT, INTERVAL, MANUAL, Trigger

UTC = timezone.utc


def cron_trigger(expr="0 3 * * *", tz="UTC", **kw) -> Trigger:
    return Trigger(name=kw.pop("name", "nightly"), recipe=kw.pop("recipe", "wiki"),
                   type=CRON, cron=expr, timezone=tz, **kw)


# --- DST: the whole reason croniter is a dependency -------------------------

def test_cron_holds_local_wall_clock_across_spring_forward():
    """`0 3 * * *` in Europe/London means 03:00 *London* on both sides of the
    March switch — so the stored absolute instants are 23 hours apart, not 24.

    This is the property the old `schedule` table could not express: `hour=3`
    compared against a naive `datetime.now()` is 03:00 UTC forever, which drifts
    an hour away from the operator's intent for seven months of the year.
    """
    trig = cron_trigger("0 3 * * *", "Europe/London")
    london = ZoneInfo("Europe/London")

    t = datetime(2026, 3, 27, 12, 0, tzinfo=UTC)
    fires = []
    for _ in range(3):
        t = tg.next_occurrence(trig, t)
        fires.append(t)

    # Stored values are absolute UTC, always.
    assert all(f.tzinfo is UTC for f in fires)
    assert [f.isoformat() for f in fires] == [
        "2026-03-28T03:00:00+00:00",   # GMT: 03:00 London == 03:00 UTC
        "2026-03-29T02:00:00+00:00",   # BST: 03:00 London == 02:00 UTC
        "2026-03-30T02:00:00+00:00",
    ]
    # Local wall clock is what stayed constant.
    assert {f.astimezone(london).strftime("%H:%M") for f in fires} == {"03:00"}
    # And the gap across the switch is a real 23 hours, not a bug.
    assert fires[1] - fires[0] == timedelta(hours=23)
    assert fires[2] - fires[1] == timedelta(hours=24)


def test_cron_fires_once_in_the_repeated_fall_back_hour():
    """01:30 America/New_York happens twice on 2026-11-01. croniter yields both;
    firing both would be a duplicate ingest an hour apart, which
    `runs_dedupe_live_uniq` cannot absorb once the first run has finished.
    `next_occurrence` suppresses the repeat."""
    trig = cron_trigger("30 1 * * *", "America/New_York")

    t = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
    fires = []
    for _ in range(4):
        t = tg.next_occurrence(trig, t)
        fires.append(t)

    assert [f.isoformat() for f in fires] == [
        "2026-10-31T05:30:00+00:00",   # EDT
        "2026-11-01T05:30:00+00:00",   # EDT — the FIRST 01:30, the repeat is skipped
        "2026-11-02T06:30:00+00:00",   # EST
        "2026-11-03T06:30:00+00:00",
    ]
    # One fire per calendar day, including the 25-hour one.
    days = [f.astimezone(ZoneInfo("America/New_York")).date() for f in fires]
    assert len(days) == len(set(days))


def test_cron_fires_after_the_spring_forward_gap():
    """02:30 America/New_York does not exist on 2026-03-08. The occurrence lands
    immediately after the gap rather than being dropped — vixie-cron's rule, and
    croniter's, so nothing here has to implement it."""
    trig = cron_trigger("30 2 * * *", "America/New_York")
    t = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)
    fires = [t := tg.next_occurrence(trig, t) for _ in range(3)]
    assert [f.isoformat() for f in fires] == [
        "2026-03-07T07:30:00+00:00",   # 02:30 EST
        "2026-03-08T07:00:00+00:00",   # 03:00 EDT — right after the gap
        "2026-03-09T06:30:00+00:00",   # 02:30 EDT
    ]


def test_next_occurrence_refuses_a_naive_datetime():
    """A naive datetime is the exact input that produced the UTC/local lie."""
    with pytest.raises(ValueError, match="aware datetime"):
        tg.next_occurrence(cron_trigger(), datetime(2026, 7, 24, 12, 0))


def test_utc_trigger_is_unaffected_by_a_dst_zone_elsewhere():
    """The migration pins everything to UTC; UTC has no switches, so consecutive
    fires are exactly 24 h apart through both March and November."""
    trig = cron_trigger("0 3 * * *", "UTC")
    for start in (datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
                  datetime(2026, 10, 30, 12, 0, tzinfo=UTC)):
        a = tg.next_occurrence(trig, start)
        b = tg.next_occurrence(trig, a)
        assert b - a == timedelta(hours=24)


# --- interval ----------------------------------------------------------------

def test_interval_is_relative_to_the_last_fire():
    trig = Trigger(name="drain", recipe="_embed", type=INTERVAL, interval_seconds=30)
    now = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    assert tg.next_occurrence(trig, now) == now + timedelta(seconds=30)


def test_interval_ignores_timezone_entirely():
    """An interval trigger is immune to DST by construction — which is why the
    always-on drains use one. 30 s is 30 s in any zone."""
    a = Trigger(name="drain", recipe="_embed", type=INTERVAL, interval_seconds=3600,
                timezone="America/New_York")
    # 01:30 EDT on the fall-back night; the wall clock is about to repeat.
    now = datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
    assert tg.next_occurrence(a, now) == now + timedelta(hours=1)


def test_event_and_manual_never_produce_a_fire_time():
    for trig in (Trigger(name="on-push", recipe="memory", type=EVENT,
                         event="source.pushed:memory"),
                 Trigger(name="adhoc", recipe="wiki", type=MANUAL)):
        assert tg.next_occurrence(trig, datetime.now(UTC)) is None
        assert tg.plan_next_fire(trig, datetime.now(UTC)) is None


# --- jitter ------------------------------------------------------------------

def test_jitter_is_bounded_deterministic_and_varies_per_occurrence():
    """Deterministic so `next_fire_at` does not jump every time anything
    recomputes it; varying per occurrence so it is jitter and not a fixed phase."""
    trig = cron_trigger("0 3 * * *", "UTC", jitter_seconds=900)
    t = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)

    offsets = []
    for _ in range(6):
        nominal = tg.next_occurrence(trig, t)
        planned = tg.plan_next_fire(trig, t)
        off = (planned - nominal).total_seconds()
        assert 0 <= off < 900
        # Recomputing the SAME occurrence gives the SAME answer.
        assert tg.plan_next_fire(trig, t) == planned
        offsets.append(off)
        t = planned
    assert len(set(offsets)) > 1, "a constant offset is a phase shift, not jitter"


def test_jitter_spreads_two_triggers_that_share_a_minute():
    """This is what replaces `_seed_schedule`'s hand-rolled 15-minute stagger."""
    now = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    planned = {name: tg.plan_next_fire(cron_trigger("0 3 * * *", name=name,
                                                    jitter_seconds=1800), now)
               for name in ("ingest-hf", "ingest-wiki", "ingest-docs", "ingest-hn")}
    assert len(set(planned.values())) == 4


def test_jitter_does_not_accumulate_drift():
    """Six nights of jittered fires must all land on the same nominal 03:00 grid.
    If the next occurrence were computed from the jittered value without croniter
    re-anchoring, the schedule would walk later every night."""
    trig = cron_trigger("0 3 * * *", "UTC", jitter_seconds=1800)
    t = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    for _ in range(6):
        planned = tg.plan_next_fire(trig, t)
        nominal = tg.next_occurrence(trig, t)
        assert nominal.strftime("%H:%M:%S") == "03:00:00"
        t = planned          # fire at the jittered instant, as the tick does


def test_zero_jitter_is_exactly_nominal():
    trig = cron_trigger("0 3 * * *", "UTC", jitter_seconds=0)
    now = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    assert tg.plan_next_fire(trig, now) == tg.next_occurrence(trig, now)


# --- validation --------------------------------------------------------------

@pytest.mark.parametrize("trig, match", [
    (Trigger(name="BAD NAME", recipe="wiki", cron="0 3 * * *"), "must match"),
    (Trigger(name="ok", recipe="", cron="0 3 * * *"), "recipe is required"),
    (Trigger(name="ok", recipe="wiki", type="whenever"), "type must be one of"),
    (Trigger(name="ok", recipe="wiki", cron="0 3 * * *", timezone="Mars/Olympus"),
     "not a known IANA zone"),
    (Trigger(name="ok", recipe="wiki", type=CRON, cron=None), "requires a cron"),
    (Trigger(name="ok", recipe="wiki", type=CRON, cron="0 3 * * * *"), "exactly 5 fields"),
    (Trigger(name="ok", recipe="wiki", type=CRON, cron="@daily"), "exactly 5 fields"),
    (Trigger(name="ok", recipe="wiki", type=CRON, cron="99 3 * * *"), "bad cron"),
    (Trigger(name="ok", recipe="wiki", type=INTERVAL, interval_seconds=None),
     "requires interval_seconds"),
    (Trigger(name="ok", recipe="wiki", type=INTERVAL, interval_seconds=30,
             jitter_seconds=30), "must be less than interval_seconds"),
    (Trigger(name="ok", recipe="wiki", type=EVENT, event=None), "requires an event"),
    (Trigger(name="ok", recipe="wiki", type=EVENT, event="run.exploded:wiki"),
     "unknown event kind"),
    (Trigger(name="ok", recipe="wiki", cron="0 3 * * *", jitter_seconds=-1),
     "jitter_seconds must be >= 0"),
])
def test_validate_rejects(trig, match):
    with pytest.raises(ValueError, match=match):
        tg.validate(trig)


def test_validate_accepts_the_shapes_that_matter():
    for trig in (
        cron_trigger("*/5 * * * *", "Europe/London"),
        cron_trigger("15 2 * * 0", "America/New_York", jitter_seconds=600),
        Trigger(name="drain", recipe="_embed", type=INTERVAL, interval_seconds=30),
        Trigger(name="chain", recipe="wiki", type=EVENT, event="run.succeeded:wiki_sync"),
        Trigger(name="boot-check", recipe="maintain", type=EVENT, event="boot"),
        Trigger(name="adhoc", recipe="wiki", type=MANUAL),
    ):
        tg.validate(trig)


def test_migrated_hyphenated_names_are_legal():
    """`_seed_schedule` names rows `ingest-hf`; the migration keeps the name, so
    the trigger-name rule has to accept a hyphen even though recipe names cannot."""
    tg.validate(cron_trigger(name="ingest-hf"))


# --- misfire -----------------------------------------------------------------

def test_misfire_grace_separates_tick_latency_from_downtime():
    now = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    late = cron_trigger(next_fire_at=now - timedelta(seconds=8))
    down = cron_trigger(next_fire_at=now - timedelta(hours=9))
    assert not tg.is_misfire(late, now)
    assert tg.is_misfire(down, now)
    assert not tg.is_misfire(cron_trigger(next_fire_at=None), now)


# --- the event vocabulary ----------------------------------------------------

@pytest.mark.parametrize("event, kind, arg", [
    ("run.succeeded:wiki_sync", "run.succeeded", "wiki_sync"),
    ("source.pushed:memory", "source.pushed", "memory"),
    ("unit.failed_threshold:smallweb", "unit.failed_threshold", "smallweb"),
    ("boot", "boot", None),
])
def test_parse_event_accepts_the_vocabulary(event, kind, arg):
    assert ev.parse_event(event) == (kind, arg)
    assert ev.validate_event(event) == event


@pytest.mark.parametrize("event, match", [
    ("", "non-empty"),
    ("deploy.finished:wiki", "unknown event kind"),
    ("run.succeeded", "requires an argument"),
    ("boot:now", "takes no argument"),
    ("source.pushed:Memory", "must match"),
    ("source.pushed:", "must match"),
    ("source.pushed:a:b", "must match"),
    ("source.pushed:../../etc", "must match"),
])
def test_parse_event_is_a_closed_vocabulary(event, match):
    with pytest.raises(ValueError, match=match):
        ev.parse_event(event)


def test_run_succeeded_is_a_chain_everything_else_is_an_event():
    """`chain` vs `event` in `runs.trigger` is what makes "this run happened
    because that one did" answerable — the thing `&&` in REFRESH_CHAINS loses."""
    assert ev.runs_trigger_column("run.succeeded:wiki_sync") == "chain"
    assert ev.runs_trigger_column("source.pushed:memory") == "event"
    assert ev.runs_trigger_column("boot") == "event"


def test_run_succeeded_event_constructor_validates():
    assert ev.run_succeeded_event("wiki_sync") == "run.succeeded:wiki_sync"
    with pytest.raises(ValueError):
        ev.run_succeeded_event("Wiki-Sync")
