"""Fresh-schema initialization and contract-epoch guard."""

from __future__ import annotations

import secrets
from importlib.resources import files

import psycopg

from windex.pipeline.contracts import CONTRACT_EPOCH

SCHEMA_GENERATION = 2


class LegacySchemaError(RuntimeError):
    pass


class ContractEpochError(RuntimeError):
    pass


def _user_tables(conn: psycopg.Connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT tablename
                 FROM pg_tables
                WHERE schemaname = current_schema()
                  AND tablename NOT LIKE 'pg_%'""")
        return {row[0] for row in cur.fetchall()}


def inspect_generation(conn: psycopg.Connection) -> dict | None:
    if "windex_meta" not in _user_tables(conn):
        return None
    with conn.cursor() as cur:
        cur.execute(
            """SELECT schema_generation, contract_epoch, seed_hash, bootstrap_id
                 FROM windex_meta WHERE singleton""")
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "schema_generation": row[0],
        "contract_epoch": row[1],
        "seed_hash": row[2],
        "bootstrap_id": row[3],
    }


def init_canonical_db(
    conn: psycopg.Connection,
    *,
    bootstrap_id: str | None = None,
    seed: bool = True,
) -> dict:
    tables = _user_tables(conn)
    metadata = inspect_generation(conn)
    if tables and metadata is None:
        raise LegacySchemaError(
            "legacy or unknown Windex schema detected; normal init-db is "
            "non-destructive. Run the reviewed source-pipeline cutover command.")
    if metadata is not None:
        if metadata["schema_generation"] != SCHEMA_GENERATION:
            raise ContractEpochError(
                f"database schema generation {metadata['schema_generation']} is "
                f"not supported by this build ({SCHEMA_GENERATION})")
        if metadata["contract_epoch"] != CONTRACT_EPOCH:
            raise ContractEpochError(
                f"database contract epoch {metadata['contract_epoch']} is not "
                f"supported by this build ({CONTRACT_EPOCH})")
    schema = files("windex.db").joinpath("canonical.sql").read_text()
    resolved_id = (
        metadata["bootstrap_id"] if metadata is not None
        else bootstrap_id or secrets.token_hex(12)
    )
    try:
        with conn.cursor() as cur:
            cur.execute(schema)
            cur.execute(
                """INSERT INTO windex_meta
                       (singleton, schema_generation, contract_epoch, bootstrap_id)
                   VALUES (true, %s, %s, %s)
                   ON CONFLICT (singleton) DO UPDATE SET
                       updated_at = now()
                   RETURNING schema_generation, contract_epoch, seed_hash,
                             bootstrap_id""",
                (SCHEMA_GENERATION, CONTRACT_EPOCH, resolved_id),
            )
            row = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    result = {
        "schema_generation": row[0],
        "contract_epoch": row[1],
        "seed_hash": row[2],
        "bootstrap_id": row[3],
    }
    if seed:
        from windex.pipeline.bootstrap import seed_canonical

        seeded = seed_canonical(conn)
        result["seed_hash"] = seeded["seed_hash"]
    return result


__all__ = [
    "ContractEpochError",
    "LegacySchemaError",
    "SCHEMA_GENERATION",
    "init_canonical_db",
    "inspect_generation",
]
