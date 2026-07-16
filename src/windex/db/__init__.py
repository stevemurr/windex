import threading
from importlib.resources import files

import psycopg
from psycopg_pool import ConnectionPool


def connect(dsn: str) -> psycopg.Connection:
    # fail fast when postgres is down — a wedged connect turned a service
    # outage into an 8h silent stall for the embed follower (2026-07-16)
    return psycopg.connect(dsn, connect_timeout=10)


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
    dashboard, resetting to idle on exit (including crashes)."""

    def __init__(self, conn: psycopg.Connection, key: str, value: str):
        self.conn, self.key, self.value = conn, key, value

    def __enter__(self):
        set_control(self.conn, self.key, self.value)
        return self

    def __exit__(self, *exc):
        self.conn.rollback()  # stage must reset even if the job died mid-transaction
        set_control(self.conn, self.key, "idle")
        return False
