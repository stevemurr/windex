from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
import time
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from windex.api.app import admin
from windex.config import Settings
from windex.db.canonical import init_canonical_db
from windex.pipeline.events import append, list_events
from windex.pipeline.run_store import submit_source
from windex.source import scheduler as scheduler_module
from windex.source.scheduler import arm_unplanned, next_fire, tick
from windex.source.store import (
    TriggerValidationError,
    archive,
    create_trigger,
    list_triggers,
    set_paused,
    update_trigger,
)
from windex.source.trigger_validation import validate_trigger


@pytest.fixture
def canonical_conn():
    admin_dsn = "postgresql://windex:windex@127.0.0.1:5432/windex"
    try:
        root = psycopg.connect(admin_dsn, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("postgres not running")
    name = "windex_trigger_validation_" + uuid.uuid4().hex[:10]
    with root.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}"')
    conn = psycopg.connect(admin_dsn.rsplit("/", 1)[0] + "/" + name)
    try:
        init_canonical_db(conn, bootstrap_id="trigger-validation-test")
        yield conn
    finally:
        conn.close()
        with root.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s", (name,))
            cur.execute(f'DROP DATABASE "{name}"')
        root.close()


@pytest.mark.parametrize(
    ("trigger_type", "spec", "path"),
    [
        ("cron", {"cron": "* * * *", "timezone": "UTC"}, "cron"),
        ("cron", {"cron": "61 * * * *", "timezone": "UTC"}, "cron"),
        (
            "cron",
            {"cron": "0 * * * *", "timezone": "Mars/Olympus"},
            "timezone",
        ),
        ("interval", {"seconds": 0}, "seconds"),
        ("interval", {"seconds": -1}, "seconds"),
        ("interval", {"seconds": "60"}, "seconds"),
        ("interval", {"seconds": 2.5}, "seconds"),
        ("interval", {"seconds": True}, "seconds"),
        ("interval", {"interval_seconds": 60}, "interval_seconds"),
        ("event", {}, "event"),
        ("event", {"event": "document.changed", "source": ""}, "source"),
        ("manual", {"seconds": 10}, "seconds"),
    ],
)
def test_type_specific_trigger_validation_rejects_bad_values(
    trigger_type, spec, path,
):
    with pytest.raises(TriggerValidationError) as raised:
        validate_trigger(trigger_type, spec)
    assert raised.value.path[-1] == path


def test_store_rejects_invalid_create_and_validates_merged_update(
    canonical_conn,
):
    with pytest.raises(TriggerValidationError, match="positive"):
        create_trigger(canonical_conn, "hn", {
            "flow_name": "harvest",
            "trigger_type": "interval",
            "trigger_spec": {"seconds": 0},
        })
    assert list_triggers(canonical_conn, "hn") == []

    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 60},
    })
    with pytest.raises(TriggerValidationError, match="does not accept"):
        update_trigger(
            canonical_conn,
            "hn",
            trigger["id"],
            {"trigger_type": "cron"},
        )
    unchanged = list_triggers(canonical_conn, "hn")[0]
    assert unchanged["trigger_type"] == "interval"
    assert unchanged["trigger_spec"] == {"seconds": 60}


def _transaction_time(conn) -> datetime:
    with conn.cursor() as cur:
        cur.execute("SELECT now()")
        return cur.fetchone()[0]


def _deadline(trigger: dict) -> datetime | None:
    value = trigger["next_fire_at"]
    return datetime.fromisoformat(value) if value else None


def test_create_arms_enabled_schedules_and_leaves_other_triggers_unarmed(
    canonical_conn,
):
    instant = _transaction_time(canonical_conn)
    scheduled = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 90},
    })
    assert _deadline(scheduled) == instant + timedelta(seconds=90)

    event = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "event",
        "trigger_spec": {"event": "source.ready"},
    })
    manual = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "manual",
        "trigger_spec": {},
    })
    disabled = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 90},
        "enabled": False,
        "next_fire_at": "2099-01-01T00:00:00+00:00",
    })
    assert event["next_fire_at"] is None
    assert manual["next_fire_at"] is None
    assert disabled["next_fire_at"] is None


def test_cron_and_interval_cadence_edits_rearm_from_transaction_clock(
    canonical_conn,
):
    stale = "2099-01-01T00:00:00+00:00"
    cron = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "cron",
        "trigger_spec": {"cron": "0 0 * * *", "timezone": "UTC"},
        "next_fire_at": stale,
    })
    instant = _transaction_time(canonical_conn)
    cron_spec = {
        "cron": "30 9 * * 1-5",
        "timezone": "America/Los_Angeles",
    }
    cron = update_trigger(
        canonical_conn,
        "hn",
        cron["id"],
        {"trigger_spec": cron_spec},
    )
    assert _deadline(cron) == next_fire("cron", cron_spec, instant)
    assert cron["next_fire_at"] != stale

    interval = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 3600},
        "next_fire_at": stale,
    })
    instant = _transaction_time(canonical_conn)
    interval = update_trigger(
        canonical_conn,
        "hn",
        interval["id"],
        {"trigger_spec": {"seconds": 17}},
    )
    assert _deadline(interval) == instant + timedelta(seconds=17)
    assert interval["next_fire_at"] != stale


def test_disable_reenable_and_trigger_kind_changes_set_safe_deadlines(
    canonical_conn,
):
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 120},
    })
    trigger = update_trigger(
        canonical_conn, "hn", trigger["id"], {"enabled": False})
    assert trigger["enabled"] is False
    assert trigger["next_fire_at"] is None

    instant = _transaction_time(canonical_conn)
    trigger = update_trigger(
        canonical_conn, "hn", trigger["id"], {"enabled": True})
    assert _deadline(trigger) == instant + timedelta(seconds=120)

    trigger = update_trigger(canonical_conn, "hn", trigger["id"], {
        "trigger_type": "event",
        "trigger_spec": {"event": "source.changed"},
    })
    assert trigger["trigger_type"] == "event"
    assert trigger["next_fire_at"] is None

    instant = _transaction_time(canonical_conn)
    trigger = update_trigger(canonical_conn, "hn", trigger["id"], {
        "trigger_type": "cron",
        "trigger_spec": {"cron": "*/10 * * * *", "timezone": "UTC"},
    })
    assert _deadline(trigger) == next_fire(
        "cron", trigger["trigger_spec"], instant)


def test_explicit_deadline_wins_and_flow_only_edit_preserves_it(
    canonical_conn,
):
    trigger = create_trigger(canonical_conn, "ccnews", {
        "flow_name": "sync",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 300},
    })
    explicit = "2032-03-04T05:06:07+00:00"
    trigger = update_trigger(canonical_conn, "ccnews", trigger["id"], {
        "trigger_spec": {"seconds": 600},
        "next_fire_at": explicit,
    })
    assert _deadline(trigger) == datetime.fromisoformat(explicit)

    trigger = update_trigger(
        canonical_conn,
        "ccnews",
        trigger["id"],
        {"flow_name": "ingest"},
    )
    assert trigger["flow_name"] == "ingest"
    assert _deadline(trigger) == datetime.fromisoformat(explicit)


def test_effective_noop_patch_does_not_touch_trigger_row(canonical_conn):
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 60},
    })
    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE source_triggers
                  SET updated_at = '2020-01-02T03:04:05+00:00'
                WHERE id = %s""",
            (trigger["id"],),
        )
    canonical_conn.commit()

    unchanged = update_trigger(canonical_conn, "hn", trigger["id"], {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 60},
        "enabled": True,
        "next_fire_at": trigger["next_fire_at"],
    })
    assert unchanged == trigger
    with canonical_conn.cursor() as cur:
        cur.execute(
            "SELECT updated_at FROM source_triggers WHERE id = %s",
            (trigger["id"],),
        )
        assert cur.fetchone()[0] == datetime(
            2020, 1, 2, 3, 4, 5, tzinfo=UTC)
    canonical_conn.commit()


def test_canonical_api_returns_field_addressable_trigger_errors(
    canonical_conn, monkeypatch,
):
    from windex.api import app as app_module
    from windex.api import canonical

    settings = Settings(
        _env_file=None,
        pg_dsn="postgresql://unused",
        write_token="",
        serve_host="127.0.0.1",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(canonical, "get_settings", lambda: settings)
    monkeypatch.setattr(
        canonical.db,
        "pooled",
        lambda _dsn: nullcontext(canonical_conn),
    )
    client = TestClient(admin)

    cases = [
        (
            {"cron": "61 * * * *", "timezone": "UTC"},
            "cron",
            "expression is invalid",
        ),
        (
            {"cron": "0 * * * *", "timezone": "Mars/Olympus"},
            "timezone",
            "unknown IANA timezone",
        ),
    ]
    for spec, field, message in cases:
        response = client.post("/v1/sources/hn/triggers", json={
            "flow_name": "harvest",
            "trigger_type": "cron",
            "trigger_spec": spec,
            "enabled": True,
        })
        assert response.status_code == 422
        issue = response.json()["detail"][0]
        assert issue["loc"] == ["body", "trigger_spec", field]
        assert message in issue["msg"]

    for seconds in (0, -20, "often"):
        response = client.post("/v1/sources/hn/triggers", json={
            "flow_name": "harvest",
            "trigger_type": "interval",
            "trigger_spec": {"seconds": seconds},
            "enabled": True,
        })
        assert response.status_code == 422
        issue = response.json()["detail"][0]
        assert issue["loc"] == ["body", "trigger_spec", "seconds"]

    assert list_triggers(canonical_conn, "hn") == []


def test_canonical_api_preserves_explicit_deadline_and_null_resets_it(
    canonical_conn, monkeypatch,
):
    from windex.api import app as app_module
    from windex.api import canonical

    settings = Settings(
        _env_file=None,
        pg_dsn="postgresql://unused",
        write_token="",
        serve_host="127.0.0.1",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(canonical, "get_settings", lambda: settings)
    monkeypatch.setattr(
        canonical.db,
        "pooled",
        lambda _dsn: nullcontext(canonical_conn),
    )
    client = TestClient(admin)
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 60},
        "next_fire_at": "2099-01-01T00:00:00+00:00",
    })

    explicit = "2034-05-06T07:08:09+00:00"
    response = client.patch(
        f"/v1/sources/hn/triggers/{trigger['id']}",
        json={
            "trigger_spec": {"seconds": 120},
            "next_fire_at": explicit,
        },
    )
    assert response.status_code == 200
    assert datetime.fromisoformat(
        response.json()["next_fire_at"],
    ) == datetime.fromisoformat(explicit)

    # The production pool context commits the response projection read on exit;
    # this test's nullcontext uses the raw connection, so mirror that boundary.
    canonical_conn.commit()
    before = datetime.now(UTC)
    response = client.patch(
        f"/v1/sources/hn/triggers/{trigger['id']}",
        json={"next_fire_at": None},
    )
    after = datetime.now(UTC)
    assert response.status_code == 200
    reset = datetime.fromisoformat(response.json()["next_fire_at"])
    assert before + timedelta(seconds=120) <= reset
    assert reset <= after + timedelta(seconds=120)


def test_rearm_is_visible_to_scheduler_without_old_deadline_fire(
    canonical_conn,
):
    old_due = datetime.now(UTC) - timedelta(minutes=5)
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 60},
        "next_fire_at": old_due.isoformat(),
    })

    instant = _transaction_time(canonical_conn)
    trigger = update_trigger(canonical_conn, "hn", trigger["id"], {
        "trigger_spec": {"seconds": 3600},
    })
    assert _deadline(trigger) == instant + timedelta(hours=1)
    result = tick(canonical_conn, now=instant + timedelta(seconds=1))
    assert result.fired == []
    assert result.coalesced == []

    trigger = update_trigger(canonical_conn, "hn", trigger["id"], {
        "next_fire_at": old_due.isoformat(),
    })
    assert _deadline(trigger) == old_due
    result = tick(canonical_conn, now=instant + timedelta(seconds=2))
    assert [item["trigger_id"] for item in result.fired] == [trigger["id"]]


def _raw_trigger(
    conn,
    *,
    trigger_type: str,
    spec: dict,
    next_fire_at: datetime | None = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO source_triggers
                   (source_id, flow_name, trigger_type, trigger_spec,
                    enabled, next_fire_at)
               SELECT id, 'harvest', %s, %s, true, %s
                 FROM sources WHERE name = 'hn'
               RETURNING id""",
            (trigger_type, Jsonb(spec), next_fire_at),
        )
        trigger_id = cur.fetchone()[0]
    conn.commit()
    return trigger_id


def test_arm_unplanned_isolates_legacy_invalid_rows_and_commits_healthy_ones(
    canonical_conn,
):
    bad_id = _raw_trigger(
        canonical_conn,
        trigger_type="interval",
        spec={"seconds": 0},
    )
    healthy_id = _raw_trigger(
        canonical_conn,
        trigger_type="cron",
        spec={"cron": "*/5 * * * *", "timezone": "UTC"},
    )

    instant = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    assert arm_unplanned(canonical_conn, now=instant) == 1

    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT id, enabled, next_fire_at
                 FROM source_triggers WHERE id = ANY(%s) ORDER BY id""",
            ([bad_id, healthy_id],),
        )
        rows = {row[0]: row[1:] for row in cur.fetchall()}
    assert rows[bad_id] == (False, None)
    assert rows[healthy_id] == (
        True,
        datetime(2026, 7, 26, 12, 5, tzinfo=UTC),
    )

    events = list_events(
        canonical_conn, component="scheduler", source="hn", limit=20)
    invalid = [item for item in events if item["event"] == "trigger.invalid"]
    assert len(invalid) == 1
    assert invalid[0]["level"] == "error"
    assert invalid[0]["data"] == {
        "trigger_id": bad_id,
        "trigger_type": "interval",
        "error": "interval trigger seconds must be positive",
        "action": "disabled",
    }

    # The quarantined row is no longer retried and the connection remains
    # usable after the mixed batch committed.
    assert arm_unplanned(canonical_conn, now=instant) == 0
    with canonical_conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
    canonical_conn.commit()


def test_tick_quarantines_due_invalid_row_without_blocking_healthy_run(
    canonical_conn,
):
    instant = datetime.now(UTC)
    bad_id = _raw_trigger(
        canonical_conn,
        trigger_type="cron",
        spec={"cron": "broken", "timezone": "UTC"},
        next_fire_at=instant - timedelta(minutes=2),
    )
    healthy = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "interval",
        "trigger_spec": {"seconds": 3600},
        "next_fire_at": (instant - timedelta(minutes=1)).isoformat(),
    })

    result = tick(canonical_conn, now=instant)

    assert [item["trigger_id"] for item in result.failed] == [bad_id]
    assert result.failed[0]["disabled"] is True
    assert [item["trigger_id"] for item in result.fired] == [healthy["id"]]
    with canonical_conn.cursor() as cur:
        cur.execute(
            "SELECT enabled, next_fire_at FROM source_triggers WHERE id = %s",
            (bad_id,),
        )
        assert cur.fetchone() == (False, None)
        cur.execute(
            "SELECT count(*) FROM runs WHERE id = %s",
            (result.fired[0]["run_id"],),
        )
        assert cur.fetchone() == (1,)
    canonical_conn.commit()


def _append_test_event(
    conn,
    event: str,
    *,
    source: str | None = None,
    run_id: int | None = None,
) -> int:
    with conn.cursor() as cur:
        seq = append(
            cur,
            component="test",
            event=event,
            source_name=source,
            run_id=run_id,
        )
    conn.commit()
    assert seq is not None
    return seq


def _event_cursor(conn, trigger_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT after_seq FROM source_event_trigger_cursors
                WHERE trigger_id = %s""",
            (trigger_id,),
        )
        return cur.fetchone()[0]


def _test_dsn(conn) -> str:
    return (
        "postgresql://windex:windex@127.0.0.1:5432/"
        f"{conn.info.dbname}"
    )


def test_event_trigger_matches_exact_event_and_optional_source_without_rescan(
    canonical_conn,
):
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "event",
        "trigger_spec": {"event": "document.changed", "source": "upstream"},
    })
    wrong_event = _append_test_event(
        canonical_conn, "document.created", source="upstream")
    wrong_source = _append_test_event(
        canonical_conn, "document.changed", source="other")
    matching = _append_test_event(
        canonical_conn, "document.changed", source="upstream")

    first = tick(canonical_conn, event_scan_limit=2)
    assert first.fired == []
    assert _event_cursor(canonical_conn, trigger["id"]) == wrong_source
    assert wrong_event < wrong_source < matching

    second = tick(canonical_conn, event_scan_limit=2)
    assert len(second.fired) == 1
    fired = second.fired[0]
    assert fired["trigger_id"] == trigger["id"]
    assert fired["event_seq"] == matching
    assert _event_cursor(canonical_conn, trigger["id"]) == matching
    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT trigger_type, trigger_by, idempotency_key
                 FROM runs WHERE id = %s""",
            (fired["run_id"],),
        )
        assert cur.fetchone() == (
            "event",
            f"event-trigger:{trigger['id']}:event:{matching}",
            f"event-trigger:{trigger['id']}:{matching}",
        )
    canonical_conn.commit()


def test_new_and_edited_event_triggers_start_at_current_journal_tail(
    canonical_conn,
):
    historic = _append_test_event(
        canonical_conn, "source.changed", source="upstream")
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "event",
        "trigger_spec": {"event": "source.changed"},
    })
    assert _event_cursor(canonical_conn, trigger["id"]) >= historic
    assert tick(canonical_conn).fired == []

    stale_for_new_binding = _append_test_event(
        canonical_conn, "source.changed", source="upstream")
    trigger = update_trigger(canonical_conn, "hn", trigger["id"], {
        "trigger_spec": {"event": "source.ready"},
    })
    assert _event_cursor(canonical_conn, trigger["id"]) >= stale_for_new_binding
    assert tick(canonical_conn).fired == []

    fresh = _append_test_event(
        canonical_conn, "source.ready", source="upstream")
    fired = tick(canonical_conn).fired
    assert len(fired) == 1
    assert fired[0]["trigger_id"] == trigger["id"]
    assert fired[0]["event_seq"] == fresh


def test_event_submission_and_cursor_are_atomic_and_retry_is_idempotent(
    canonical_conn,
    monkeypatch,
):
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "event",
        "trigger_spec": {"event": "index.requested"},
    })
    event_seq = _append_test_event(canonical_conn, "index.requested")
    original_submit = scheduler_module.submit_source

    def crash_after_insert(*args, **kwargs):
        original_submit(*args, **kwargs)
        raise RuntimeError("simulated scheduler crash")

    monkeypatch.setattr(
        scheduler_module, "submit_source", crash_after_insert)
    failed = tick(canonical_conn)
    assert len(failed.failed) == 1
    assert failed.failed[0]["event_seq"] == event_seq
    assert _event_cursor(canonical_conn, trigger["id"]) < event_seq
    with canonical_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM runs WHERE idempotency_key = %s",
            (f"event-trigger:{trigger['id']}:{event_seq}",),
        )
        assert cur.fetchone()[0] == 0
    error_events = list_events(
        canonical_conn, component="scheduler", source="hn", limit=100)
    assert any(
        item["event"] == "trigger.event_error"
        and item["data"]["event_seq"] == event_seq
        for item in error_events
    )

    monkeypatch.setattr(scheduler_module, "submit_source", original_submit)
    retried = tick(canonical_conn)
    assert len(retried.fired) == 1
    run_id = retried.fired[0]["run_id"]
    assert _event_cursor(canonical_conn, trigger["id"]) == event_seq

    # A restored/stale cursor cannot duplicate the Run: its stable
    # idempotency key resolves the already-committed submission.
    with canonical_conn.cursor() as cur:
        cur.execute(
            """UPDATE source_event_trigger_cursors SET after_seq = %s
                WHERE trigger_id = %s""",
            (event_seq - 1, trigger["id"]),
        )
    canonical_conn.commit()
    replay = tick(canonical_conn)
    assert replay.fired[0]["run_id"] == run_id
    with canonical_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM runs WHERE idempotency_key = %s",
            (f"event-trigger:{trigger['id']}:{event_seq}",),
        )
        assert cur.fetchone()[0] == 1
    canonical_conn.commit()


def test_event_cursor_waits_for_lower_uncommitted_journal_sequence(
    canonical_conn,
):
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "event",
        "trigger_spec": {"event": "ordered.input"},
    })
    dsn = _test_dsn(canonical_conn)
    lower = psycopg.connect(dsn)
    higher = psycopg.connect(dsn)
    dispatcher = psycopg.connect(dsn)
    try:
        with lower.cursor() as cur:
            lower_seq = append(
                cur, component="test", event="ordered.input")
        assert lower_seq is not None

        with higher.cursor() as cur:
            higher_seq = append(
                cur, component="test", event="unrelated.input")
        higher.commit()
        assert higher_seq is not None and higher_seq > lower_seq

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(tick, dispatcher)
            time.sleep(0.1)
            # The exclusive dispatch lock waits instead of advancing over the
            # committed higher sequence while the lower one is still hidden.
            assert not pending.done()
            lower.commit()
            result = pending.result(timeout=10)

        assert len(result.fired) == 1
        assert result.fired[0]["trigger_id"] == trigger["id"]
        assert result.fired[0]["event_seq"] == lower_seq
        assert _event_cursor(canonical_conn, trigger["id"]) == lower_seq
    finally:
        lower.rollback()
        lower.close()
        higher.close()
        dispatcher.close()


def test_legacy_cursor_seeding_locks_trigger_before_journal(canonical_conn):
    trigger_id = _raw_trigger(
        canonical_conn,
        trigger_type="event",
        spec={"event": "legacy.input"},
    )
    dsn = _test_dsn(canonical_conn)
    locker = psycopg.connect(dsn)
    writer = psycopg.connect(dsn)
    dispatcher = psycopg.connect(
        dsn, application_name="event-cursor-lock-order-test")
    try:
        with locker.cursor() as cur:
            cur.execute(
                "SELECT id FROM source_triggers WHERE id = %s FOR UPDATE",
                (trigger_id,),
            )
            assert cur.fetchone() == (trigger_id,)

        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(tick, dispatcher)
            for _ in range(40):
                with canonical_conn.cursor() as cur:
                    cur.execute(
                        """SELECT wait_event_type FROM pg_stat_activity
                            WHERE pid = %s""",
                        (dispatcher.info.backend_pid,),
                    )
                    state = cur.fetchone()
                canonical_conn.commit()
                if state == ("Lock",):
                    break
                time.sleep(0.05)
            assert state == ("Lock",)

            # Cursor seeding is waiting on the trigger row and therefore
            # cannot already own the exclusive journal lock.  A normal event
            # writer must remain free to commit.
            with writer.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '750ms'")
                seq = append(
                    cur, component="test", event="legacy.input")
            writer.commit()
            assert seq is not None

            locker.commit()
            result = pending.result(timeout=10)

        assert result.fired == []
        assert _event_cursor(canonical_conn, trigger_id) >= seq
    finally:
        locker.rollback()
        locker.close()
        writer.rollback()
        writer.close()
        dispatcher.close()


def test_legacy_invalid_event_trigger_is_quarantined_once(canonical_conn):
    trigger_id = _raw_trigger(
        canonical_conn,
        trigger_type="event",
        spec={},
    )
    # The first pass safely establishes a tail cursor for the legacy row.
    tick(canonical_conn)
    _append_test_event(canonical_conn, "anything")

    result = tick(canonical_conn)
    assert result.failed == [{
        "trigger_id": trigger_id,
        "source": "hn",
        "error": "event trigger requires a non-empty event name",
        "disabled": True,
    }]
    with canonical_conn.cursor() as cur:
        cur.execute(
            "SELECT enabled FROM source_triggers WHERE id = %s",
            (trigger_id,),
        )
        assert cur.fetchone() == (False,)
    canonical_conn.commit()
    assert tick(canonical_conn).failed == []
    invalid = list_events(
        canonical_conn, component="scheduler", source="hn", limit=100)
    assert sum(
        item["event"] == "trigger.invalid"
        and item["data"]["trigger_id"] == trigger_id
        for item in invalid
    ) == 1


def test_event_triggers_skip_paused_archived_and_disabled_intervals(
    canonical_conn,
):
    paused = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "event",
        "trigger_spec": {"event": "paused.input"},
    })
    set_paused(canonical_conn, "hn", True, "maintenance")
    paused_seq = _append_test_event(canonical_conn, "paused.input")
    paused_result = tick(canonical_conn)
    assert paused_result.skipped == [{
        "trigger_id": paused["id"],
        "source": "hn",
        "event_seq": paused_seq,
        "reason": "source paused",
    }]
    set_paused(canonical_conn, "hn", False)
    assert tick(canonical_conn).fired == []

    disabled = create_trigger(canonical_conn, "ccnews", {
        "flow_name": "sync",
        "trigger_type": "event",
        "trigger_spec": {"event": "disabled.input"},
    })
    update_trigger(
        canonical_conn, "ccnews", disabled["id"], {"enabled": False})
    ignored = _append_test_event(canonical_conn, "disabled.input")
    assert tick(canonical_conn).fired == []
    update_trigger(
        canonical_conn, "ccnews", disabled["id"], {"enabled": True})
    assert _event_cursor(canonical_conn, disabled["id"]) >= ignored
    assert tick(canonical_conn).fired == []
    fresh = _append_test_event(canonical_conn, "disabled.input")
    enabled_result = tick(canonical_conn)
    assert any(
        item["trigger_id"] == disabled["id"]
        and item["event_seq"] == fresh
        for item in enabled_result.fired
    )

    archived = create_trigger(canonical_conn, "docs", {
        "flow_name": "sync",
        "trigger_type": "event",
        "trigger_spec": {"event": "archived.input"},
    })
    assert archive(canonical_conn, "docs") is True
    archived_seq = _append_test_event(canonical_conn, "archived.input")
    archived_result = tick(canonical_conn)
    assert any(
        item == {
            "trigger_id": archived["id"],
            "source": "docs",
            "event_seq": archived_seq,
            "reason": "source archived",
        }
        for item in archived_result.skipped
    )


def test_event_trigger_selection_is_fair_and_bounded_across_ticks(
    canonical_conn,
):
    first = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "event",
        "trigger_spec": {"event": "fair.input"},
    })
    second = create_trigger(canonical_conn, "ccnews", {
        "flow_name": "sync",
        "trigger_type": "event",
        "trigger_spec": {"event": "fair.input"},
    })
    event_seq = _append_test_event(canonical_conn, "fair.input")

    one = tick(canonical_conn, limit=1)
    two = tick(canonical_conn, limit=1)
    assert len(one.fired) == 1
    assert len(two.fired) == 1
    assert {
        one.fired[0]["trigger_id"],
        two.fired[0]["trigger_id"],
    } == {first["id"], second["id"]}
    assert one.fired[0]["event_seq"] == event_seq
    assert two.fired[0]["event_seq"] == event_seq


def test_active_source_run_coalesces_event_and_advances_cursor(
    canonical_conn,
):
    trigger = create_trigger(canonical_conn, "hn", {
        "flow_name": "harvest",
        "trigger_type": "event",
        "trigger_spec": {"event": "coalesce.input"},
    })
    active = submit_source(canonical_conn, "hn", flow="harvest")
    assert active is not None
    event_seq = _append_test_event(canonical_conn, "coalesce.input")

    result = tick(canonical_conn)
    assert result.coalesced == [{
        "trigger_id": trigger["id"],
        "source": "hn",
        "run_id": None,
        "event_seq": event_seq,
    }]
    assert _event_cursor(canonical_conn, trigger["id"]) == event_seq
    events = list_events(
        canonical_conn, component="scheduler", source="hn", limit=100)
    assert any(
        item["event"] == "trigger.event_coalesced"
        and item["data"]["event_seq"] == event_seq
        for item in events
    )


def test_event_triggered_run_events_are_suppressed_after_one_hop(
    canonical_conn,
):
    trigger = create_trigger(canonical_conn, "ccnews", {
        "flow_name": "sync",
        "trigger_type": "event",
        "trigger_spec": {"event": "run.queued"},
    })
    seed_run = submit_source(
        canonical_conn, "hn", flow="harvest", dedupe=False)
    assert seed_run is not None

    first = tick(canonical_conn)
    assert len(first.fired) == 1
    assert first.fired[0]["trigger_id"] == trigger["id"]
    event_run = first.fired[0]["run_id"]

    second = tick(canonical_conn)
    assert any(
        item["trigger_id"] == trigger["id"]
        and item["reason"] == "loop suppressed"
        for item in second.skipped
    )
    third = tick(canonical_conn)
    assert third.fired == []
    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT id FROM runs
                WHERE trigger_type = 'event' AND trigger_by LIKE %s
                ORDER BY id""",
            (f"event-trigger:{trigger['id']}:%",),
        )
        assert [row[0] for row in cur.fetchall()] == [event_run]
    skipped = list_events(
        canonical_conn, component="scheduler", source="ccnews", limit=100)
    assert any(
        item["event"] == "trigger.event_skipped"
        and item["data"]["reason"]
        == "event-triggered run causality is limited to one hop"
        for item in skipped
    )
    canonical_conn.commit()
