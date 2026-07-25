from __future__ import annotations

import uuid

import psycopg
import pytest

from windex.config import Settings
from windex.db.canonical import init_canonical_db
from windex.pipeline.run_store import get_run, submit_source
from windex.pipeline.store import (
    get_layout,
    get_pipeline,
    list_pipelines,
    publish_revision,
    put_layout,
)
from windex.source.store import (
    StaleSourceError,
    get_source,
    patch_settings,
    settings_projection,
)


@pytest.fixture
def canonical_conn():
    settings = Settings()
    admin = psycopg.connect(settings.pg_dsn, autocommit=True)
    name = "windex_pipeline_source_" + uuid.uuid4().hex[:10]
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}"')
    admin.close()
    parts = settings.pg_dsn.rsplit("/", 1)
    conn = psycopg.connect(parts[0] + "/" + name)
    try:
        init_canonical_db(conn, bootstrap_id="pipeline-source-test")
        yield conn
    finally:
        conn.close()
        admin = psycopg.connect(settings.pg_dsn, autocommit=True)
        with admin.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s", (name,))
            cur.execute(f'DROP DATABASE "{name}"')
        admin.close()


def test_bootstrap_and_layout_are_deterministic(canonical_conn):
    assert len(list_pipelines(canonical_conn)) == 11
    pipeline = get_pipeline(canonical_conn, "hn")
    assert pipeline and pipeline["version"] == 1
    layout = get_layout(canonical_conn, "hn", 1, "harvest")
    assert layout
    moved = {**layout["layout"], "annotations": [{"text": "note"}]}
    saved = put_layout(
        canonical_conn, "hn", 1, "harvest", moved, if_match=layout["etag"])
    assert saved["etag"] != layout["etag"]
    assert get_pipeline(canonical_conn, "hn")["spec_hash"] == pipeline["spec_hash"]


def test_noop_publish_and_stale_settings(canonical_conn):
    pipeline = get_pipeline(canonical_conn, "hn")
    revision = publish_revision(
        canonical_conn, "hn", pipeline["spec"],
        expected_version=1, expected_hash=pipeline["spec_hash"])
    assert revision["version"] == 1

    source = get_source(canonical_conn, "hn")
    assert source and source["pipeline_name"] == "hn"
    settings = settings_projection(canonical_conn, "hn")
    updated = patch_settings(
        canonical_conn, "hn", {"incremental_days": 3}, if_match=settings["etag"])
    assert updated["etag"] != settings["etag"]
    with pytest.raises(StaleSourceError):
        patch_settings(
            canonical_conn, "hn", {"incremental_days": 4}, if_match=settings["etag"])


def test_source_run_freezes_binding_and_index_continuation(canonical_conn):
    run_id = submit_source(canonical_conn, "hn", dedupe=False)
    run = get_run(canonical_conn, run_id, include_spec=True)
    assert run["source_snapshot"]["search_name"] == "hn"
    assert run["pipeline_name"] == "hn"
    assert run["tasks"][-1]["node"] == "__index__"
    assert run["tasks"][-1]["depends_on"]
