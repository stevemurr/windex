from importlib.resources import files

import psycopg


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn)


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
