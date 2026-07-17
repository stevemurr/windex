import logging
import threading
import time
from collections.abc import Callable
from importlib.resources import files
from typing import TypeVar

import psycopg
from psycopg_pool import ConnectionPool

log = logging.getLogger("windex.db")

_T = TypeVar("_T")


def connect(dsn: str) -> psycopg.Connection:
    # fail fast when postgres is down — a wedged connect turned a service
    # outage into an 8h silent stall for the embed follower (2026-07-16)
    return psycopg.connect(dsn, connect_timeout=10)


class Reconnecting:
    """A single Postgres connection that transparently reconnects on a lost
    connection, for long-running one-shot jobs that hold one connection for the
    whole run (gh discover/scan/hydrate).

    A transient host↔container TCP drop — the Apple `container` port-forward
    blips CLAUDE.md and scripts/watchdog.sh warn about — severs an in-flight
    connection without taking Postgres down (server logs `connection to client
    lost`, client sees `server closed the connection unexpectedly`). Before this
    a single blip crashed the entire sweep (2026-07-17). `run()` retries each op
    on a fresh connection, so **every callable passed to it must be idempotent**
    — the discover ops already are (a read and two ON CONFLICT upserts), which
    CLAUDE.md requires of every job anyway.

    The pooled read paths (`pooled`) already ride through blips via the pool's
    per-checkout health check; this is the equivalent for the one-connection
    write jobs that can't use the pool (they hold the connection across a whole
    stage for the control-flag lifecycle)."""

    def __init__(self, dsn: str, attempts: int = 6,
                 base_backoff: float = 1.0, max_backoff: float = 30.0):
        self.dsn = dsn
        self.attempts = attempts
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.conn = connect(dsn)

    def __enter__(self) -> "Reconnecting":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    def run(self, fn: Callable[[psycopg.Connection], _T], attempts: int | None = None) -> _T:
        """Call fn(conn), returning its result; on a lost connection, reconnect
        and retry (idempotent fn required). Raises the last OperationalError if
        every attempt fails — postgres genuinely down, not a blip."""
        n = max(attempts or self.attempts, 1)
        last: psycopg.OperationalError | None = None
        for i in range(n):
            if i:
                time.sleep(min(self.base_backoff * 2 ** (i - 1), self.max_backoff))
                if not self._reconnect():
                    continue  # still unreachable — wait and try the next attempt
            try:
                return fn(self.conn)
            except psycopg.OperationalError as exc:
                last = exc
                log.warning("db op lost the connection (attempt %d/%d): %s",
                            i + 1, n, exc)
        assert last is not None  # n>=1 guarantees at least one failed attempt
        raise last

    def _reconnect(self) -> bool:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001 — a dead connection may raise on close
            pass
        try:
            self.conn = connect(self.dsn)
            return True
        except psycopg.OperationalError:
            return False  # postgres still down; caller retries after backoff

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass


def _run_db(conn: "psycopg.Connection | Reconnecting",
            fn: Callable[[psycopg.Connection], _T], attempts: int | None = None) -> _T:
    """Run fn against a raw connection or a Reconnecting wrapper uniformly."""
    if isinstance(conn, Reconnecting):
        return conn.run(fn, attempts=attempts)
    return fn(conn)


# Process-wide pools for the API's hot read paths. The 2026-07-16 timeout
# post-mortem: the dashboard's per-request connects (~1.6 fresh TCP connects/s
# per viewer) rolled the dice against a transient port-forward stall; pooled,
# established connections ride through blips and cap backend count.
_pools: dict[str, ConnectionPool] = {}
_pools_lock = threading.Lock()


def pool(dsn: str) -> ConnectionPool:
    with _pools_lock:
        p = _pools.get(dsn)
        if p is None:
            p = ConnectionPool(
                dsn, min_size=1, max_size=16, timeout=10, open=True,
                # health-check each checkout: a backend killed while the conn
                # sat idle in the pool (terminate bursts, restarts, forward
                # resets) is discarded + replaced instead of handed out dead
                # (post-mortem 2026-07-16: 7 one-shot 500s, self-healed on the
                # next request — this closes the gap at ~1 round-trip cost)
                check=ConnectionPool.check_connection,
                kwargs={"connect_timeout": 10},
            )
            _pools[dsn] = p
        return p


def pooled(dsn: str):
    """Context manager yielding a pooled connection (returned on exit)."""
    return pool(dsn).connection()


def init_db(conn: psycopg.Connection) -> None:
    schema = files("windex.db").joinpath("schema.sql").read_text()
    with conn.cursor() as cur:
        cur.execute(schema)
    conn.commit()


def get_control(conn: psycopg.Connection, key: str, default: str) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM control WHERE key = %s", (key,))
        row = cur.fetchone()
    return row[0] if row else default


def set_control(conn: psycopg.Connection, key: str, value: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO control (key, value) VALUES (%s, %s)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""",
            (key, value),
        )
    conn.commit()


class stage:
    """Context manager: publish a pipeline stage to the control table for the
    dashboard, resetting to idle on exit — including crashes, and a connection
    lost mid-run when `conn` is a Reconnecting wrapper."""

    def __init__(self, conn: "psycopg.Connection | Reconnecting", key: str, value: str):
        self.conn, self.key, self.value = conn, key, value

    def __enter__(self):
        _run_db(self.conn, lambda c: set_control(c, self.key, self.value))
        return self

    def _reset(self, c: psycopg.Connection) -> None:
        c.rollback()  # a job that died mid-transaction leaves an aborted tx
        set_control(c, self.key, "idle")

    def __exit__(self, *exc):
        # The stage flag MUST reset even if the job died mid-transaction or the
        # connection was lost, or the dashboard shows a stage that isn't running
        # (2026-07-17: gh_stage wedged at "discovery sweep" when rollback() raised
        # on a dead connection before the idle reset could run). And this must
        # never raise: a second exception here would mask the one the caller is
        # already unwinding. A Reconnecting conn resets on a fresh connection
        # (bounded retry — this is cleanup, not the hot path); a raw conn is
        # best-effort and simply logs if the connection is gone.
        try:
            _run_db(self.conn, self._reset, attempts=2)
        except Exception:  # noqa: BLE001 — includes a dead raw connection
            log.warning("stage %r: could not reset to idle (postgres unreachable)",
                        self.key)
        return False
