"""Pure trigger evaluation — no database, no clock of its own.

Everything here is a function of (trigger row, `after` instant), which is the
whole point: the bug this subsystem exists to fix is a *calendar* bug, and a
calendar bug you cannot unit-test at 02:30 on the last Sunday in March is a
calendar bug you ship. `fire.py` owns the IO; this module owns the arithmetic.

Two rules are load-bearing and worth stating up front.

**`next_fire_at` is absolute.** It is computed by expanding the cron expression
against wall-clock time *in the trigger's IANA zone*, then converted to UTC before
it is ever stored. A stored local-naive timestamp is ambiguous for one hour a year
(the fall-back repeat) and non-existent for one hour a year (the spring-forward
gap); an absolute instant is neither, in any zone, forever.

**Calendar arithmetic is croniter's job, not ours.** DST, month lengths, leap
years and `0 3 * * 0` are exactly the class of problem where hand-rolled code is
right for eleven months. The only calendar decision made here is the fall-back
suppression below, and that is a *policy* choice croniter deliberately does not
make for us.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadCronError, croniter

# The four trigger types. `cron` and `interval` are time-driven and produce a
# `next_fire_at`; `event` and `manual` never do — they are woken by
# `fire.emit_event` and by a human respectively, and a non-NULL `next_fire_at` on
# one of those would be a lie the due-index would act on.
CRON, INTERVAL, EVENT, MANUAL = "cron", "interval", "event", "manual"
TYPES = (CRON, INTERVAL, EVENT, MANUAL)

# Trigger names are UI-visible stable ids and also carry the migrated `schedule`
# names, which contain hyphens (`ingest-hf`). Deliberately looser than
# custom_source.registry.NAME_RE (which governs *recipe* names) but still a
# closed character set: the name is echoed into log lines and event payloads.
TRIGGER_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{1,63}$")

# A 5-field crontab expression. croniter happily accepts 6-field (seconds) and
# 7-field (year) forms plus @hourly-style nicknames, and both are refused here:
# the value round-trips through a UI, a Swift client and `windex trigger` output,
# and "which dialect is this row" is not a question any of them should have to
# answer. One dialect, validated once, at the edge.
CRON_FIELDS = 5

# How late a fire may be and still count as "on time". Must be comfortably above
# the tick interval (10 s) so ordinary scheduling latency is never mistaken for
# downtime, and comfortably below any real outage. Anything later than this is a
# *misfire*, and what happens then is the `catch_up` decision.
DEFAULT_MISFIRE_GRACE = 90.0


@dataclass(frozen=True)
class Trigger:
    """One `triggers` row, normalized. Constructed from a DB row via `from_row`.

    Frozen because evaluation must never mutate the caller's view of the row: the
    fire transaction re-reads and locks the row itself, and a half-updated dict
    floating around is how "the scheduler fired with stale config" happens.
    """

    name: str
    recipe: str
    type: str = CRON
    cron: str | None = None
    interval_seconds: int | None = None
    timezone: str = "UTC"
    event: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    priority: int = 50
    jitter_seconds: int = 0
    catch_up: bool = False
    enabled: bool = True
    last_fired_at: datetime | None = None
    next_fire_at: datetime | None = None
    last_run_id: int | None = None

    # Column list for every SELECT in this package, in dataclass order. Kept here
    # so adding a field means touching one place, not five query strings.
    COLUMNS = ("name, recipe, type, cron, interval_seconds, timezone, event, params, "
               "priority, jitter_seconds, catch_up, enabled, last_fired_at, "
               "next_fire_at, last_run_id")

    @classmethod
    def from_row(cls, row: tuple) -> Trigger:
        return cls(*row)

    @property
    def is_timed(self) -> bool:
        return self.type in (CRON, INTERVAL)


def validate(trig: Trigger) -> None:
    """Raise ValueError if this row could not be evaluated, with a message a human
    can act on.

    Called on every write path *and* at the top of every fire. The second call is
    not redundant: `triggers` has no CHECK constraints (schema.sql keeps it
    permissive so a future trigger type is an INSERT, not a migration), so a row
    hand-edited with `psql` reaches the tick unvalidated. Better a per-trigger
    `trigger.failed` event than a traceback that kills the loop for every *other*
    trigger too — the 2026-07-17 lesson, where one dead component stalled indexing
    for ~36 h because nothing isolated the failure.
    """
    if not TRIGGER_NAME_RE.match(trig.name or ""):
        raise ValueError(
            f"trigger name {trig.name!r} must match {TRIGGER_NAME_RE.pattern}")
    if not trig.recipe:
        raise ValueError(f"trigger {trig.name}: recipe is required")
    if trig.type not in TYPES:
        raise ValueError(
            f"trigger {trig.name}: type must be one of {', '.join(TYPES)}, got {trig.type!r}")

    try:
        ZoneInfo(trig.timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(
            f"trigger {trig.name}: timezone {trig.timezone!r} is not a known IANA zone "
            f"({exc})") from exc

    if trig.type == CRON:
        if not trig.cron:
            raise ValueError(f"trigger {trig.name}: type=cron requires a cron expression")
        fields = trig.cron.split()
        if len(fields) != CRON_FIELDS:
            raise ValueError(
                f"trigger {trig.name}: cron must have exactly {CRON_FIELDS} fields "
                f"(m h dom mon dow), got {len(fields)}: {trig.cron!r}")
        try:
            croniter(trig.cron)
        except (CroniterBadCronError, ValueError) as exc:
            raise ValueError(f"trigger {trig.name}: bad cron {trig.cron!r}: {exc}") from exc
    elif trig.type == INTERVAL:
        if not trig.interval_seconds or trig.interval_seconds < 1:
            raise ValueError(
                f"trigger {trig.name}: type=interval requires interval_seconds >= 1")
    elif trig.type == EVENT:
        from windex.scheduler.events import validate_event  # circular at module scope

        if not trig.event:
            raise ValueError(f"trigger {trig.name}: type=event requires an event name")
        validate_event(trig.event)

    if trig.jitter_seconds < 0:
        raise ValueError(f"trigger {trig.name}: jitter_seconds must be >= 0")
    # Jitter wider than the period would skip occurrences outright rather than
    # spread them — the spread device silently becoming a drop device. Only
    # checkable for `interval` (a cron period is not a number), so cron carries
    # the same warning in `plan_next_fire`'s docstring instead.
    if trig.type == INTERVAL and trig.interval_seconds and \
            trig.jitter_seconds >= trig.interval_seconds:
        raise ValueError(
            f"trigger {trig.name}: jitter_seconds ({trig.jitter_seconds}) must be less "
            f"than interval_seconds ({trig.interval_seconds}) or occurrences are dropped, "
            f"not spread")
    if not (0 <= trig.priority <= 32767):
        raise ValueError(f"trigger {trig.name}: priority must be 0..32767")


def next_occurrence(trig: Trigger, after: datetime) -> datetime | None:
    """The next **nominal** (un-jittered) occurrence strictly after `after`, as an
    absolute UTC instant. None for `event`/`manual`, which are never time-driven.

    `after` must be timezone-aware; a naive datetime is the exact input that
    produced the UTC/local lie this package exists to fix, so it is refused rather
    than assumed to mean anything.

    Cron expansion happens on the wall clock of `trig.timezone`, which is what
    makes "03:00 every night" mean 03:00 to the person who typed it on both sides
    of a DST boundary — the absolute spacing between two consecutive fires is then
    23 h or 25 h, by design, not 24.

    **Fall-back suppression.** When the clock goes backward, one wall-clock reading
    happens twice, and croniter faithfully yields both — for `0 3 * * *` in
    Europe/London on the October switch that is two fires an hour apart. The
    second is a duplicate ingest that `runs_dedupe_live_uniq` cannot absorb,
    because by then the first run has usually finished and left the live set. So a
    candidate whose *naive local* time has not advanced past the base's is
    discarded and the next one taken. This is vixie-cron's rule for the repeated
    hour, and it means a wall-clock schedule always moves forward on the wall
    clock.

    *Accepted tradeoff:* a sub-hourly cron (`*/5 * * * *`) loses the repeated hour
    once a year in a DST zone. Anything that must tick regardless of what the wall
    clock is doing belongs on an `interval` trigger, which is immune by
    construction — that is exactly what the always-on drains use.
    """
    if after.tzinfo is None:
        raise ValueError(
            f"trigger {trig.name}: next_occurrence needs an aware datetime; a naive one "
            f"is the schedule.hour bug (see module docstring)")

    if trig.type == INTERVAL:
        assert trig.interval_seconds  # validate() guarantees it
        return (after + timedelta(seconds=trig.interval_seconds)).astimezone(timezone.utc)
    if trig.type != CRON:
        return None

    tz = ZoneInfo(trig.timezone)
    base_local = after.astimezone(tz)
    it = croniter(trig.cron, base_local)
    base_naive = base_local.replace(tzinfo=None)
    # Two iterations is enough for the fall-back repeat (one duplicate reading);
    # the bound exists so a pathological expression cannot spin the tick forever.
    for _ in range(64):
        cand_local = it.get_next(datetime)
        if cand_local.replace(tzinfo=None) > base_naive:
            return cand_local.astimezone(timezone.utc)
    raise ValueError(
        f"trigger {trig.name}: cron {trig.cron!r} produced no forward occurrence in "
        f"{trig.timezone} — refusing to loop")


def jitter_offset(name: str, nominal: datetime, jitter_seconds: int) -> timedelta:
    """A stable pseudo-random offset in [0, jitter_seconds) for one occurrence.

    Deliberately derived from `sha1(name|nominal)` rather than `random`, for two
    reasons that are both about the same property — recomputing must give the same
    answer:

    1. *Idempotence.* `next_fire_at` gets planned by the tick, by the migration and
       by an operator's edit. With `random` those disagree, so the displayed "next
       run" jumps every time anything touches the row and no test can assert on it.
    2. *Stability across restarts.* This is what replaces `_seed_schedule`'s
       hand-rolled 15-minute stagger. A restart mid-evening must not reshuffle the
       whole night's spread; the offset for a given occurrence is a property of the
       occurrence, not of the process that happened to compute it.

    Varying with `nominal` (not just `name`) keeps it real jitter rather than a
    fixed phase shift: two triggers that share a minute stay apart, and a single
    trigger's offset still moves night to night.
    """
    if jitter_seconds <= 0:
        return timedelta(0)
    seed = f"{name}|{nominal.astimezone(timezone.utc).isoformat()}".encode()
    # 8 bytes is far more entropy than a bounded seconds range needs; the modulo
    # bias against a jitter window of at most hours is unmeasurable.
    draw = int.from_bytes(hashlib.sha1(seed).digest()[:8], "big")
    return timedelta(seconds=draw % jitter_seconds)


def plan_next_fire(trig: Trigger, after: datetime) -> datetime | None:
    """The value to store in `next_fire_at`: the next nominal occurrence plus this
    trigger's jitter, absolute UTC. None for `event`/`manual`.

    Jitter does not accumulate. The *following* occurrence is computed from the
    actual fire instant (a jittered one), and croniter's next value from
    `03:00+7m` is still tomorrow's `03:00` — so the schedule stays anchored to the
    nominal grid as long as `jitter_seconds` is smaller than the period.
    `validate` enforces that for `interval`; for `cron` the period is not a number
    to compare against, so it is a documented constraint rather than a checked one.
    """
    nominal = next_occurrence(trig, after)
    if nominal is None:
        return None
    return nominal + jitter_offset(trig.name, nominal, trig.jitter_seconds)


def is_misfire(trig: Trigger, now: datetime,
               grace_seconds: float = DEFAULT_MISFIRE_GRACE) -> bool:
    """Is this trigger's due time so far in the past that the box must have been
    down (or the scope paused), rather than merely a tick late?

    The distinction is the entire catch-up policy: a due time 4 s in the past is
    normal scheduling latency and fires in both modes; a due time 9 h in the past
    is a *miss*, and `catch_up` decides whether to make it up.
    """
    if trig.next_fire_at is None:
        return False
    return (now - trig.next_fire_at).total_seconds() > grace_seconds
