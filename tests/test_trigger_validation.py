from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from windex.api.app import admin
from windex.config import Settings
from windex.db.canonical import init_canonical_db
from windex.pipeline.events import list_events
from windex.source.scheduler import arm_unplanned, next_fire, tick
from windex.source.store import (
    TriggerValidationError,
    create_trigger,
    list_triggers,
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
