"""Pure unit tests for the shared date parse+clamp helper (Defect C guard)."""

from datetime import datetime, timedelta, timezone

from windex.dateparse import MIN_PUBLISHED, clamp_date, parse_and_clamp

UTC = timezone.utc
NOW = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)


def test_clamp_date_accepts_recent_date():
    dt = datetime(2026, 7, 1, tzinfo=UTC)
    assert clamp_date(dt, now=NOW) == dt


def test_clamp_date_rejects_year_0001():
    assert clamp_date(datetime(1, 1, 1, tzinfo=UTC), now=NOW) is None


def test_clamp_date_rejects_far_future():
    assert clamp_date(datetime(2500, 1, 1, tzinfo=UTC), now=NOW) is None


def test_clamp_date_boundary_min_published_kept_one_second_earlier_rejected():
    assert clamp_date(MIN_PUBLISHED, now=NOW) == MIN_PUBLISHED
    assert clamp_date(MIN_PUBLISHED - timedelta(seconds=1), now=NOW) is None


def test_clamp_date_accepts_within_future_skew_rejects_beyond():
    assert clamp_date(NOW + timedelta(days=1), now=NOW) is not None
    assert clamp_date(NOW + timedelta(days=3), now=NOW) is None


def test_clamp_date_passes_through_none():
    assert clamp_date(None, now=NOW) is None


def test_clamp_date_treats_naive_datetime_as_utc():
    naive = datetime(2026, 7, 1, 0, 0)  # no tzinfo
    assert clamp_date(naive, now=NOW) == naive


def test_parse_and_clamp_parses_z_suffix():
    assert parse_and_clamp("2026-07-20T10:00:00Z", now=NOW) == datetime(
        2026, 7, 20, 10, 0, tzinfo=UTC
    )


def test_parse_and_clamp_returns_none_for_garbage_string():
    assert parse_and_clamp("not-a-date", now=NOW) is None


def test_parse_and_clamp_returns_none_for_out_of_range_iso_string():
    # Parse succeeds, clamp rejects — the actual production bug.
    assert parse_and_clamp("0001-01-01T00:00:00", now=NOW) is None
    assert parse_and_clamp("2500-01-01T00:00:00Z", now=NOW) is None


def test_parse_and_clamp_returns_none_for_empty_or_none():
    assert parse_and_clamp("", now=NOW) is None
    assert parse_and_clamp(None, now=NOW) is None
