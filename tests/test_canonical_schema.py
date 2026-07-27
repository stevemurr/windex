from __future__ import annotations

import hashlib
from pathlib import Path

import psycopg
import pytest

from windex.db.canonical import LegacySchemaError, init_canonical_db
from windex.pipeline.contracts import CONTRACT_EPOCH

ADMIN_DSN = "postgresql://windex:windex@127.0.0.1:5432/windex"
ROOT = Path(__file__).parents[1]
_KEY = hashlib.sha1(str(Path(__file__).resolve()).encode()).hexdigest()[:8]
DATABASE = f"windex_canonical_test_{_KEY}"
DSN = f"postgresql://windex:windex@127.0.0.1:5432/{DATABASE}"


@pytest.fixture(scope="module")
def canonical_conn():
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("postgres not running")
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {DATABASE} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {DATABASE}")
    conn = psycopg.connect(DSN)
    yield conn
    conn.close()
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {DATABASE} WITH (FORCE)")
    admin.close()


def test_fresh_schema_is_idempotent_and_epoch_guarded(canonical_conn):
    first = init_canonical_db(canonical_conn, bootstrap_id="pytest-bootstrap")
    second = init_canonical_db(canonical_conn, bootstrap_id="ignored")
    assert first == second
    assert first["contract_epoch"] == CONTRACT_EPOCH
    assert first["bootstrap_id"] == "pytest-bootstrap"


def test_canonical_schema_has_one_source_of_truth():
    database_package = ROOT / "src" / "windex" / "db"
    assert (database_package / "canonical.sql").is_file()
    assert not (database_package / "schema.sql").exists()
    assert 'joinpath("canonical.sql")' in (database_package / "canonical.py").read_text()


def test_canonical_schema_contains_no_legacy_control_plane(canonical_conn):
    with canonical_conn.cursor() as cur:
        cur.execute(
            """SELECT tablename FROM pg_tables
                WHERE schemaname = current_schema()""")
        tables = {row[0] for row in cur.fetchall()}
        cur.execute(
            """SELECT column_name
                 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'sources'"""
        )
        source_columns = {row[0] for row in cur.fetchall()}
    canonical_conn.rollback()
    assert {"pipelines", "pipeline_revisions", "sources", "operational_events"} <= tables
    assert "metadata" in source_columns
    assert not {
        "recipes", "recipe_revisions", "recipe_config",
        "custom_sources", "schedule", "triggers", "run_events",
    } & tables


def test_unknown_nonempty_schema_is_refused_without_mutation():
    database = f"{DATABASE}_legacy"
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("postgres not running")
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {database}")
    dsn = f"postgresql://windex:windex@127.0.0.1:5432/{database}"
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE recipes (name text primary key)")
            conn.commit()
            with pytest.raises(LegacySchemaError, match="non-destructive"):
                init_canonical_db(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('recipes'), to_regclass('pipelines')")
                assert cur.fetchone() == ("recipes", None)
            conn.rollback()
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)")
        admin.close()
