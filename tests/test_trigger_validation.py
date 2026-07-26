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
from windex.source.scheduler import arm_unplanned, tick
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
