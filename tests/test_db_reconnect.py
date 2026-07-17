"""Reconnection + stage-flag resilience (2026-07-17 gh-discover incident).

A transient host↔container postgres drop severed an in-flight connection and
crashed the whole discover sweep, then stage.__exit__ raised a *second* error
rolling back the dead connection — leaving gh_stage wedged at "discovery sweep"
on the dashboard. These tests pin the fixes: db.Reconnecting rides through the
drop, and stage resets the flag to idle even when the connection was lost."""

import psycopg
import pytest

import windex.db as db


def _scalar(conn: psycopg.Connection, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def _terminate(dsn: str, pid: int) -> None:
    """Kill a backend from a separate connection — the faithful reproduction of
    the server dropping the client mid-query (AdminShutdown, an OperationalError)."""
    with psycopg.connect(dsn, autocommit=True) as killer, killer.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))


def _control(dsn: str, key: str) -> str:
    with psycopg.connect(dsn) as conn:
        return db.get_control(conn, key, "MISSING")


def test_reconnecting_run_recovers_after_backend_terminated(pg_dsn, monkeypatch):
    monkeypatch.setattr(db.time, "sleep", lambda s: None)  # no real backoff waits
    rc = db.Reconnecting(pg_dsn)
    try:
        _terminate(pg_dsn, rc.conn.info.backend_pid)  # sever it, as the blip did
        # The op must succeed anyway: first attempt hits the dead socket, run()
        # reconnects and retries on a fresh connection.
        assert rc.run(lambda c: _scalar(c, "SELECT 42")) == 42
    finally:
        rc.close()


def test_reconnecting_run_raises_after_exhausting_attempts(pg_dsn, monkeypatch):
    # Postgres genuinely unreachable (not a blip): run() must give up and raise
    # the last error rather than loop forever.
    monkeypatch.setattr(db.time, "sleep", lambda s: None)
    rc = db.Reconnecting(pg_dsn, attempts=3)
    calls = []

    def always_drop(_conn):
        calls.append(1)
        raise psycopg.OperationalError("simulated persistent drop")

    with pytest.raises(psycopg.OperationalError):
        rc.run(always_drop)
    assert len(calls) == 3  # tried exactly `attempts` times, then surfaced it
    rc.close()


def test_stage_resets_flag_to_idle_after_connection_lost(pg_dsn, monkeypatch):
    # The core regression: a drop mid-stage must still leave the flag idle.
    monkeypatch.setattr(db.time, "sleep", lambda s: None)
    rc = db.Reconnecting(pg_dsn)
    try:
        with db.stage(rc, "gh_stage", "discovery sweep (search API)"):
            assert _control(pg_dsn, "gh_stage") == "discovery sweep (search API)"
            _terminate(pg_dsn, rc.conn.info.backend_pid)  # the 2026-07-17 drop
        # Before the fix this wedged at "discovery sweep"; now it reconnects to reset.
        assert _control(pg_dsn, "gh_stage") == "idle"
    finally:
        rc.close()


def test_stage_exit_does_not_raise_on_dead_raw_connection(pg_dsn):
    # A raw (non-Reconnecting) connection can't reset the flag once dead, but
    # exiting the stage must never raise a second exception that masks the
    # caller's original failure.
    conn = psycopg.connect(pg_dsn)
    try:
        with db.stage(conn, "gh_stage", "working"):
            conn.close()  # connection dies before the block exits
        # reaching here at all is the assertion: __exit__ swallowed the dead-conn error
    finally:
        if not conn.closed:
            conn.close()
