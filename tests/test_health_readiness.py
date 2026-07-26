from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from windex.api import app as app_module
from windex.api import readiness
from windex.config import Settings
from windex.pipeline.contracts import CONTRACT_EPOCH


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, query, _params=None):
        normalized = " ".join(query.split())
        if "FROM windex_meta" in normalized:
            self.row = self.conn.metadata
        elif "FROM run_tasks" in normalized:
            self.row = self.conn.worker
        elif "FROM source_triggers" in normalized:
            self.row = self.conn.scheduler
        else:
            raise AssertionError(f"unexpected readiness query: {normalized}")

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(
        self,
        *,
        metadata=(2, 2),
        worker=(0, 0, 0, 0),
        scheduler=(0, 0.0),
    ):
        self.metadata = metadata
        self.worker = worker
        self.scheduler = scheduler
        self.rolled_back = False
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def cursor(self):
        return _Cursor(self)

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _settings(**overrides):
    values = {
        "_env_file": None,
        "embed_endpoint": "http://embed.internal:8080",
        "embed_model": "configured-model",
        "embed_dim": 1024,
    }
    values.update(overrides)
    return Settings(**values)


def _database(monkeypatch, connection, *, statuses=()):
    captured = {}

    def connect(dsn, **kwargs):
        captured.update({"dsn": dsn, **kwargs})
        return connection

    monkeypatch.setattr(readiness.psycopg, "connect", connect)
    monkeypatch.setattr(
        readiness.source_store,
        "module_statuses",
        lambda _conn, enabled_only=False: list(statuses),
    )
    result = readiness._probe_database(_settings())
    return result, captured


def test_healthy_database_reports_schema_capacity_scheduler_and_locks(monkeypatch):
    result, captured = _database(
        monkeypatch,
        _Connection(worker=(0, 2, 2, 0), scheduler=(0, 0.0)),
        statuses=({"available": True},),
    )

    assert all(component["status"] == "ok" for component in result.values())
    assert result["schema"]["observations"] == {
        "schema_generation": 2,
        "contract_epoch": CONTRACT_EPOCH,
    }
    assert result["workers"]["observations"] == {
        "ready_tasks": 0,
        "running_tasks": 2,
        "live_workers": 2,
        "expired_leases": 0,
    }
    assert captured["connect_timeout"] == readiness._PROBE_TIMEOUT_S
    assert "statement_timeout=2000" in captured["options"]


def test_postgres_down_is_critical_and_redacts_exception(monkeypatch):
    secret = "postgresql://windex:do-not-leak@db.internal/windex"

    def unavailable(*_args, **_kwargs):
        raise RuntimeError(secret)

    monkeypatch.setattr(readiness.psycopg, "connect", unavailable)
    result = readiness._probe_database(_settings(pg_dsn=secret))

    assert result["postgres"]["status"] == "unavailable"
    assert result["postgres"]["critical"] is True
    assert all(
        result[name]["status"] == "unknown"
        for name in ("schema", "workers", "scheduler", "module_locks")
    )
    assert secret not in json.dumps(result)
    assert "do-not-leak" not in json.dumps(result)


def test_schema_mismatch_is_critical_without_hiding_postgres_reachability(
    monkeypatch,
):
    result, _captured = _database(
        monkeypatch,
        _Connection(metadata=(999, CONTRACT_EPOCH)),
    )

    assert result["postgres"]["status"] == "ok"
    assert result["schema"]["status"] == "unavailable"
    assert result["schema"]["critical"] is True
    assert result["schema"]["observations"]["schema_generation"] == 999


def test_ready_tasks_without_live_workers_are_degraded(monkeypatch):
    result, _captured = _database(
        monkeypatch,
        _Connection(worker=(7, 0, 0, 0)),
    )

    workers = result["workers"]
    assert workers["status"] == "degraded"
    assert workers["critical"] is False
    assert workers["observations"]["ready_tasks"] == 7
    assert "no live worker" in workers["summary"].lower()


def test_scheduler_lag_is_degraded_after_lateness_budget(monkeypatch):
    result, _captured = _database(
        monkeypatch,
        _Connection(scheduler=(3, readiness._SCHEDULER_LAG_WARN_S + 1)),
    )

    scheduler = result["scheduler"]
    assert scheduler["status"] == "degraded"
    assert scheduler["observations"]["due_triggers"] == 3
    assert scheduler["observations"]["max_lag_s"] == 31.0


def test_stranded_module_locks_are_degraded(monkeypatch):
    result, _captured = _database(
        monkeypatch,
        _Connection(),
        statuses=(
            {"available": True},
            {"available": False},
            {"available": False},
        ),
    )

    modules = result["module_locks"]
    assert modules["status"] == "degraded"
    assert modules["observations"] == {
        "stranded_sources": 2,
        "enabled_sources": 3,
    }


def test_qdrant_failure_is_critical_and_uses_short_timeout(monkeypatch):
    captured = {}

    class Client:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def get_collections(self):
            raise RuntimeError("qdrant secret response")

        def close(self):
            captured["closed"] = True

    import qdrant_client

    monkeypatch.setattr(qdrant_client, "QdrantClient", Client)
    result = readiness._probe_qdrant(_settings())

    assert result["status"] == "unavailable"
    assert result["critical"] is True
    assert captured["timeout"] == readiness._PROBE_TIMEOUT_S
    assert captured["check_compatibility"] is False
    assert captured["closed"] is True
    assert "secret" not in json.dumps(result)


def test_embedding_configuration_and_gateway_are_advisory(monkeypatch):
    unconfigured = readiness._probe_embedding(
        _settings(embed_model="placeholder", embed_dim=0))
    assert unconfigured["status"] == "unavailable"
    assert unconfigured["critical"] is False
    assert unconfigured["observations"]["configured"] is False

    monkeypatch.setattr(
        readiness.prom,
        "_gateway_probe",
        lambda _endpoint: (False, 0.01),
    )
    unavailable = readiness._probe_embedding(_settings())
    assert unavailable["status"] == "unavailable"
    assert unavailable["critical"] is False
    assert unavailable["observations"] == {
        "configured": True,
        "reachable": False,
        "gateway_required": True,
    }

    local = readiness._probe_embedding(_settings(embed_backend="st"))
    assert local["status"] == "ok"
    assert local["observations"] == {
        "configured": True,
        "gateway_required": False,
    }


def test_snapshot_is_cached_and_returns_isolated_values(monkeypatch):
    readiness.clear_cache()
    calls = []
    clock = {"now": 0.0}

    def collect(_settings):
        calls.append(True)
        return {
            "status": "ok",
            "ready": True,
            "checked_at": 1.0,
            "cache_ttl_s": readiness._CACHE_TTL_S,
            "components": {},
        }

    monkeypatch.setattr(readiness, "_collect", collect)
    monkeypatch.setattr(
        readiness.time,
        "monotonic",
        lambda: clock["now"],
    )
    first = readiness.snapshot(_settings())
    first["status"] = "mutated"
    second = readiness.snapshot(_settings())

    assert len(calls) == 1
    assert second["status"] == "ok"

    clock["now"] = readiness._CACHE_TTL_S + 0.01
    readiness.snapshot(_settings())
    assert len(calls) == 2


def test_snapshot_is_single_flight_and_cache_cardinality_is_bounded(monkeypatch):
    readiness.clear_cache()
    calls = []

    def collect(_settings):
        calls.append(True)
        time.sleep(0.02)
        return {
            "status": "ok",
            "ready": True,
            "checked_at": 1.0,
            "cache_ttl_s": readiness._CACHE_TTL_S,
            "components": {},
        }

    monkeypatch.setattr(readiness, "_collect", collect)
    settings = _settings()
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(
            lambda _index: readiness.snapshot(settings),
            range(16),
        ))

    assert len(calls) == 1
    assert all(result["status"] == "ok" for result in results)

    for index in range(readiness._CACHE_MAX_ENTRIES + 4):
        readiness.snapshot(_settings(pg_dsn=f"postgresql://db-{index}/windex"))
    assert len(readiness._cache) <= readiness._CACHE_MAX_ENTRIES


def test_snapshot_internal_failure_still_returns_decodable_degraded_health(
    monkeypatch,
):
    readiness.clear_cache()

    def broken(_settings):
        raise RuntimeError("internal secret must not escape")

    monkeypatch.setattr(readiness, "_collect", broken)
    result = readiness.snapshot(_settings(pg_dsn="postgresql://user:secret@db/windex"))

    assert result["status"] == "degraded"
    assert result["ready"] is False
    assert set(result["components"]) == {
        "postgres",
        "schema",
        "qdrant",
        "embedding",
        "workers",
        "scheduler",
        "module_locks",
    }
    assert "secret" not in json.dumps(result)


def test_collect_marks_advisory_outage_degraded_but_stays_ready(monkeypatch):
    database = {
        name: readiness._component(
            "ok",
            critical=name in ("postgres", "schema"),
            summary="ok",
        )
        for name in (
            "postgres",
            "schema",
            "workers",
            "scheduler",
            "module_locks",
        )
    }
    monkeypatch.setattr(readiness, "_probe_database", lambda _settings: database)
    monkeypatch.setattr(
        readiness,
        "_probe_qdrant",
        lambda _settings: readiness._component(
            "ok", critical=True, summary="ok"),
    )
    monkeypatch.setattr(
        readiness,
        "_probe_embedding",
        lambda _settings: readiness._component(
            "unavailable", critical=False, summary="down"),
    )

    result = readiness._collect(_settings())

    assert result["status"] == "degraded"
    assert result["ready"] is True


def test_health_remains_200_epoch_decodable_when_critical_dependency_is_down(
    monkeypatch,
):
    snapshot = {
        "status": "degraded",
        "ready": False,
        "checked_at": 1.0,
        "cache_ttl_s": 10.0,
        "components": {
            "postgres": readiness._component(
                "unavailable",
                critical=True,
                summary="Canonical Postgres is unreachable.",
            ),
        },
    }
    monkeypatch.setattr(app_module.readiness, "snapshot", lambda _settings: snapshot)
    monkeypatch.setattr(
        app_module,
        "get_settings",
        lambda: _settings(write_token="redacted-admin-token"),
    )

    response = TestClient(app_module.admin).get("/v1/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "degraded"
    assert body["contract_epoch"] == CONTRACT_EPOCH
    assert body["readiness"]["ready"] is False
    assert "redacted-admin-token" not in response.text
