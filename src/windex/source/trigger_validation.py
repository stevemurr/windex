"""Validation shared by Source trigger writes and the scheduler.

Trigger specifications are persisted as JSON so the database cannot express
the invariants that make each trigger type schedulable.  Keep those invariants
in one place: API/store writes reject bad input, while the scheduler uses the
same validator to quarantine rows written by older releases or direct SQL.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

TRIGGER_TYPES = frozenset({"cron", "interval", "event", "manual"})


class TriggerValidationError(ValueError):
    """A field-addressable validation failure suitable for an HTTP 422."""

    def __init__(self, path: tuple[str, ...], message: str) -> None:
        self.path = path
        self.message = message
        super().__init__(message)

    def api_detail(self) -> list[dict[str, Any]]:
        return [{
            "type": "value_error.trigger",
            "loc": ["body", *self.path],
            "msg": self.message,
        }]


def _error(path: tuple[str, ...], message: str) -> None:
    raise TriggerValidationError(path, message)


def _unknown_keys(
    spec: Mapping[str, Any], allowed: set[str], trigger_type: str,
) -> None:
    unknown = sorted(set(spec) - allowed)
    if unknown:
        _error(
            ("trigger_spec", unknown[0]),
            f"{trigger_type} trigger does not accept field {unknown[0]!r}",
        )


def _validate_interval(spec: Mapping[str, Any]) -> None:
    _unknown_keys(spec, {"seconds"}, "interval")
    if "seconds" not in spec:
        _error(
            ("trigger_spec", "seconds"),
            "interval trigger requires seconds",
        )
    seconds = spec["seconds"]
    if isinstance(seconds, bool) or not isinstance(seconds, int):
        _error(
            ("trigger_spec", "seconds"),
            "interval trigger seconds must be an integer",
        )
    if seconds < 1:
        _error(
            ("trigger_spec", "seconds"),
            "interval trigger seconds must be positive",
        )


def _validate_cron(spec: Mapping[str, Any]) -> None:
    _unknown_keys(spec, {"cron", "timezone"}, "cron")
    expression = spec.get("cron")
    if not isinstance(expression, str) or not expression.strip():
        _error(
            ("trigger_spec", "cron"),
            "cron trigger requires a five-field expression",
        )
    if len(expression.split()) != 5:
        _error(
            ("trigger_spec", "cron"),
            "cron trigger requires a five-field expression",
        )
    try:
        valid = croniter.is_valid(expression)
    except (TypeError, ValueError):
        valid = False
    if not valid:
        _error(
            ("trigger_spec", "cron"),
            "cron trigger expression is invalid",
        )

    timezone_name = spec.get("timezone")
    if not isinstance(timezone_name, str) or not timezone_name.strip():
        _error(
            ("trigger_spec", "timezone"),
            "cron trigger requires an IANA timezone",
        )
    try:
        ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError):
        _error(
            ("trigger_spec", "timezone"),
            f"unknown IANA timezone {timezone_name!r}",
        )


def _validate_event(spec: Mapping[str, Any]) -> None:
    event = spec.get("event")
    if not isinstance(event, str) or not event.strip():
        _error(
            ("trigger_spec", "event"),
            "event trigger requires a non-empty event name",
        )
    source = spec.get("source")
    if source is not None and (
        not isinstance(source, str) or not source.strip()
    ):
        _error(
            ("trigger_spec", "source"),
            "event trigger source must be a non-empty string",
        )


def _validate_manual(spec: Mapping[str, Any]) -> None:
    _unknown_keys(spec, set(), "manual")


def _validate_next_fire(
    trigger_type: str, next_fire_at: Any,
) -> None:
    if trigger_type in {"event", "manual"}:
        if next_fire_at is not None:
            _error(
                ("next_fire_at",),
                f"{trigger_type} trigger cannot set next_fire_at",
            )
        return
    if next_fire_at is None or isinstance(next_fire_at, datetime):
        if isinstance(next_fire_at, datetime) and next_fire_at.tzinfo is None:
            _error(
                ("next_fire_at",),
                "next_fire_at must include a timezone",
            )
        return
    if not isinstance(next_fire_at, str):
        _error(
            ("next_fire_at",),
            "next_fire_at must be an RFC 3339 timestamp",
        )
    try:
        parsed = datetime.fromisoformat(next_fire_at)
    except ValueError:
        _error(
            ("next_fire_at",),
            "next_fire_at must be an RFC 3339 timestamp",
        )
    if parsed.tzinfo is None:
        _error(
            ("next_fire_at",),
            "next_fire_at must include a timezone",
        )


def validate_trigger(
    trigger_type: Any,
    trigger_spec: Any,
    *,
    next_fire_at: Any = None,
) -> None:
    """Validate a complete trigger configuration without changing its shape."""

    if not isinstance(trigger_type, str) or trigger_type not in TRIGGER_TYPES:
        _error(
            ("trigger_type",),
            "trigger_type must be one of cron, interval, event, or manual",
        )
    if not isinstance(trigger_spec, Mapping):
        _error(
            ("trigger_spec",),
            "trigger_spec must be an object",
        )

    if trigger_type == "interval":
        _validate_interval(trigger_spec)
    elif trigger_type == "cron":
        _validate_cron(trigger_spec)
    elif trigger_type == "event":
        _validate_event(trigger_spec)
    else:
        _validate_manual(trigger_spec)
    _validate_next_fire(trigger_type, next_fire_at)


def scheduled_next_fire(
    trigger_type: str,
    trigger_spec: Mapping[str, Any],
    after: datetime,
) -> datetime | None:
    """Return the first scheduled deadline strictly after ``after``.

    Validation and cadence calculation live together so API/store re-arming
    and scheduler advancement cannot drift into subtly different behavior.
    Event and manual triggers deliberately have no clock deadline.
    """

    validate_trigger(trigger_type, trigger_spec)
    if trigger_type == "interval":
        return after + timedelta(seconds=trigger_spec["seconds"])
    if trigger_type == "cron":
        timezone = ZoneInfo(trigger_spec["timezone"])
        local = after.astimezone(timezone)
        return croniter(
            trigger_spec["cron"], local,
        ).get_next(datetime).astimezone(UTC)
    return None


__all__ = [
    "TRIGGER_TYPES",
    "TriggerValidationError",
    "scheduled_next_fire",
    "validate_trigger",
]
