"""Parse and range-clamp document publication dates.

Motivation (2026-07-22): several sources derive ``published_at`` from untrusted
input — RSS/Atom ``pubDate`` and trafilatura's *guessed* page date — which yields
absurd values (year 0001 from a default, year 2500 from an OCR/typo). Postgres
``timestamptz`` happily stores 4713 BC … 294276 AD, so nothing downstream bounds
it, and the garbage poisons the dashboard's min/max date-coverage range
(observed live: smallweb spanning 0001-01-01 … 2500-01-01).

``clamp_date`` gates a datetime to a plausible window; ``parse_and_clamp`` is a
drop-in for the copy-pasted ``_parse_ts``/``_parse_date`` helpers (parse ISO-8601,
then clamp). Out-of-range or unparseable → ``None`` — ``published_at`` is nullable
and every consumer already tolerates NULL.
"""

from datetime import datetime, timedelta, timezone

# Oldest plausible content across every source: arXiv 1991, Wikipedia 2001,
# HN 2006, CC-News ~2016. 1990 leaves headroom without admitting year-0001 /
# epoch-0 defaults. A single global bound is sufficient — no per-source override.
MIN_PUBLISHED = datetime(1990, 1, 1, tzinfo=timezone.utc)
# Grace for feed clock skew / mis-set publish timestamps.
MAX_FUTURE_SKEW = timedelta(days=2)


def clamp_date(dt: datetime | None, *, now: datetime | None = None) -> datetime | None:
    """Return ``dt`` if it falls within ``[MIN_PUBLISHED, now + MAX_FUTURE_SKEW]``,
    else ``None``. Naive datetimes are compared as UTC but returned unchanged, so
    only the range gate is added on top of the callers' existing behavior. ``now``
    is injectable for deterministic tests.
    """
    if dt is None:
        return None
    ref = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if ref < MIN_PUBLISHED or ref > now + MAX_FUTURE_SKEW:
        return None
    return dt


def parse_and_clamp(value: str | None, *, now: datetime | None = None) -> datetime | None:
    """Parse an ISO-8601 string (accepting a trailing ``Z``) then range-clamp it.
    Unparseable or out-of-range → ``None``.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    return clamp_date(dt, now=now)
