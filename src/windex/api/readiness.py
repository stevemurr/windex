"""Bounded, cached readiness snapshot for the open admin health route.

This is deliberately not a second metrics collector.  It probes the two
critical state stores and reuses the worker/scheduler aggregates exported by
``windex.api.prom``.  The response contains only static summaries and numeric
observations: exception text, DSNs, endpoint URLs, and credentials never cross
the unauthenticated boundary.

``critical`` means the serving plane cannot truthfully operate without the
component (canonical Postgres/schema and Qdrant).  Advisory components can
degrade ingestion, scheduling, or dense retrieval while the service remains
ready for a useful subset of requests.  Any unhealthy component makes the
top-level status ``degraded``; only an unhealthy critical component makes
``ready`` false.
"""

from __future__ import annotations

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import psycopg

from windex.api import prom
from windex.config import Settings
from windex.db.canonical import SCHEMA_GENERATION
from windex.pipeline.contracts import CONTRACT_EPOCH
from windex.pipeline.store import seed_matrix_hash
from windex.source import store as source_store

_CACHE_TTL_S = 10.0
_CACHE_MAX_ENTRIES = 8
_PROBE_TIMEOUT_S = 2
_SCHEDULER_LAG_WARN_S = 30.0
_cache: dict[tuple[str, str, str, str, int], tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def _component(
    status: str,
    *,
    critical: bool,
    summary: str,
    **observations: int | float | bool | None,
) -> dict[str, Any]:
    return {
        "status": status,
        "critical": critical,
        "summary": summary,
        "observations": {
            key: value
            for key, value in observations.items()
            if value is not None
        },
    }


def _database_unknown(
    postgres: dict[str, Any],
    *,
    schema_status: str = "unknown",
    schema_summary: str = "Schema readiness could not be determined.",
) -> dict[str, dict[str, Any]]:
    return {
        "postgres": postgres,
        "schema": _component(
            schema_status,
            critical=True,
            summary=schema_summary,
        ),
        "canonical_seeds": _component(
            "unknown",
            critical=False,
            summary="Built-in Pipeline seed freshness could not be determined.",
        ),
        "workers": _component(
            "unknown",
            critical=False,
            summary="Worker capacity could not be determined.",
        ),
        "scheduler": _component(
            "unknown",
            critical=False,
            summary="Scheduler lateness could not be determined.",
        ),
        "module_locks": _component(
            "unknown",
            critical=False,
            summary="Frozen Module availability could not be determined.",
        ),
    }


def _probe_database(settings: Settings) -> dict[str, dict[str, Any]]:
    try:
        conn = psycopg.connect(
            settings.pg_dsn,
            connect_timeout=_PROBE_TIMEOUT_S,
            options=f"-c statement_timeout={_PROBE_TIMEOUT_S * 1000}",
        )
    except Exception:  # noqa: BLE001 - dependency health is data, not an API error
        return _database_unknown(_component(
            "unavailable",
            critical=True,
            summary="Canonical Postgres is unreachable.",
        ))

    try:
        with conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT schema_generation, contract_epoch, seed_hash
                             FROM windex_meta
                            WHERE singleton"""
                    )
                    metadata = cur.fetchone()
            except Exception:  # noqa: BLE001 - missing/old schema is a health result
                conn.rollback()
                return _database_unknown(
                    _component(
                        "ok",
                        critical=True,
                        summary="Canonical Postgres is reachable.",
                    ),
                    schema_status="unavailable",
                    schema_summary="Canonical schema metadata is unavailable.",
                )

            postgres = _component(
                "ok",
                critical=True,
                summary="Canonical Postgres is reachable.",
            )
            if metadata is None:
                return _database_unknown(
                    postgres,
                    schema_status="unavailable",
                    schema_summary="Canonical schema metadata is missing.",
                )
            generation, epoch, stored_seed_hash = metadata
            schema_ok = (
                generation == SCHEMA_GENERATION
                and epoch == CONTRACT_EPOCH
            )
            schema = _component(
                "ok" if schema_ok else "unavailable",
                critical=True,
                summary=(
                    "Canonical schema matches this build."
                    if schema_ok
                    else "Canonical schema does not match this build."
                ),
                schema_generation=int(generation),
                contract_epoch=int(epoch),
            )
            if not schema_ok:
                result = _database_unknown(
                    postgres,
                    schema_status="unavailable",
                    schema_summary="Canonical schema does not match this build.",
                )
                result["schema"] = schema
                return result

            try:
                expected_seed_hash = seed_matrix_hash(settings)
            except Exception:  # noqa: BLE001 - local seed probe failure is health data
                canonical_seeds = _component(
                    "unknown",
                    critical=False,
                    summary="Built-in Pipeline seed freshness could not be determined.",
                )
            else:
                seeds_current = stored_seed_hash == expected_seed_hash
                canonical_seeds = _component(
                    "ok" if seeds_current else "degraded",
                    critical=False,
                    summary=(
                        "Built-in Pipeline seeds match this build."
                        if seeds_current
                        else (
                            "Built-in Pipeline seeds are stale for this build; "
                            "run windex init-db before starting workers."
                        )
                    ),
                    matches_build=seeds_current,
                )

            try:
                with conn.cursor() as cur:
                    operational = prom.operational_snapshot(cur)
            except Exception:  # noqa: BLE001 - keep independent health visible
                workers = _component(
                    "unknown",
                    critical=False,
                    summary="Worker capacity could not be determined.",
                )
                scheduler = _component(
                    "unknown",
                    critical=False,
                    summary="Scheduler lateness could not be determined.",
                )
                conn.rollback()
            else:
                ready = int(operational["ready_tasks"])
                running = int(operational["running_tasks"])
                live = int(operational["live_workers"])
                expired = int(operational["expired_leases"])
                worker_stalled = expired > 0 or (ready > 0 and live == 0)
                workers = _component(
                    "degraded" if worker_stalled else "ok",
                    critical=False,
                    summary=(
                        "Ready work has no live worker capacity."
                        if ready > 0 and live == 0
                        else (
                            "One or more worker leases have expired."
                            if expired > 0
                            else "Worker capacity matches the current backlog."
                        )
                    ),
                    ready_tasks=ready,
                    running_tasks=running,
                    live_workers=live,
                    expired_leases=expired,
                )
                due = int(operational["due_triggers"])
                lag = float(operational["scheduler_lag_s"])
                scheduler_late = lag > _SCHEDULER_LAG_WARN_S
                scheduler = _component(
                    "degraded" if scheduler_late else "ok",
                    critical=False,
                    summary=(
                        "The Source scheduler is behind its trigger deadlines."
                        if scheduler_late
                        else "The Source scheduler is within its lateness budget."
                    ),
                    due_triggers=due,
                    max_lag_s=round(lag, 3),
                    lateness_budget_s=_SCHEDULER_LAG_WARN_S,
                )

            try:
                statuses = source_store.module_statuses(
                    conn,
                    enabled_only=True,
                )
            except Exception:  # noqa: BLE001 - local digest/probe failure
                conn.rollback()
                module_locks = _component(
                    "unknown",
                    critical=False,
                    summary="Frozen Module availability could not be determined.",
                )
            else:
                stranded = sum(
                    1 for item in statuses if not item["available"])
                module_locks = _component(
                    "degraded" if stranded else "ok",
                    critical=False,
                    summary=(
                        "Enabled Sources reference unavailable frozen Modules."
                        if stranded
                        else "Enabled Source Module locks are runnable."
                    ),
                    stranded_sources=stranded,
                    enabled_sources=len(statuses),
                )
            return {
                "postgres": postgres,
                "schema": schema,
                "canonical_seeds": canonical_seeds,
                "workers": workers,
                "scheduler": scheduler,
                "module_locks": module_locks,
            }
    finally:
        conn.close()


def _probe_qdrant(settings: Settings) -> dict[str, Any]:
    client = None
    try:
        from qdrant_client import QdrantClient

        client = QdrantClient(
            url=settings.qdrant_url,
            timeout=_PROBE_TIMEOUT_S,
            check_compatibility=False,
        )
        collections = client.get_collections().collections
    except Exception:  # noqa: BLE001 - dependency health is data
        return _component(
            "unavailable",
            critical=True,
            summary="Qdrant is unreachable.",
        )
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001 - probe result is already known
                pass
    return _component(
        "ok",
        critical=True,
        summary="Qdrant is reachable.",
        collections=len(collections),
    )


def _probe_embedding(settings: Settings) -> dict[str, Any]:
    configured = bool(
        settings.embed_model
        and settings.embed_model != "placeholder"
        and settings.embed_dim > 0
        and (
            settings.embed_backend == "st"
            or settings.embed_endpoint
        )
    )
    if not configured:
        return _component(
            "unavailable",
            critical=False,
            summary=(
                "Embedding is not configured; lexical search remains available."
            ),
            configured=False,
            reachable=False,
            gateway_required=settings.embed_backend != "st",
        )
    if settings.embed_backend == "st":
        return _component(
            "ok",
            critical=False,
            summary="In-process embedding is configured.",
            configured=True,
            gateway_required=False,
        )
    try:
        reachable, _duration = prom._gateway_probe(settings.embed_endpoint)
    except Exception:  # noqa: BLE001 - malformed endpoints are configuration state
        reachable = False
    return _component(
        "ok" if reachable else "unavailable",
        critical=False,
        summary=(
            "The embedding gateway is reachable."
            if reachable
            else "The embedding gateway is unreachable; lexical search remains available."
        ),
        configured=True,
        reachable=reachable,
        gateway_required=True,
    )


def _collect(settings: Settings) -> dict[str, Any]:
    # A short-lived pool keeps independent dependency timeouts concurrent
    # without leaving threads behind in processes that may later fork workers.
    with ThreadPoolExecutor(
        max_workers=3,
        thread_name_prefix="windex-health",
    ) as executor:
        database_future = executor.submit(_probe_database, settings)
        qdrant_future = executor.submit(_probe_qdrant, settings)
        embedding_future = executor.submit(_probe_embedding, settings)

        try:
            database = database_future.result()
        except Exception:  # noqa: BLE001 - preserve a decodable liveness response
            database = _database_unknown(_component(
                "unavailable",
                critical=True,
                summary="Canonical Postgres readiness failed.",
            ))
        try:
            qdrant = qdrant_future.result()
        except Exception:  # noqa: BLE001
            qdrant = _component(
                "unavailable",
                critical=True,
                summary="Qdrant readiness failed.",
            )
        try:
            embedding = embedding_future.result()
        except Exception:  # noqa: BLE001
            embedding = _component(
                "unknown",
                critical=False,
                summary="Embedding readiness could not be determined.",
            )

    components = {
        "postgres": database["postgres"],
        "schema": database["schema"],
        "canonical_seeds": database["canonical_seeds"],
        "qdrant": qdrant,
        "embedding": embedding,
        "workers": database["workers"],
        "scheduler": database["scheduler"],
        "module_locks": database["module_locks"],
    }
    ready = all(
        component["status"] == "ok"
        for component in components.values()
        if component["critical"]
    )
    status = (
        "ok"
        if all(component["status"] == "ok" for component in components.values())
        else "degraded"
    )
    return {
        "status": status,
        "ready": ready,
        "checked_at": time.time(),
        "cache_ttl_s": _CACHE_TTL_S,
        "components": components,
    }


def _fallback() -> dict[str, Any]:
    components = {
        "postgres": _component(
            "unknown",
            critical=True,
            summary="Canonical Postgres readiness could not be determined.",
        ),
        "schema": _component(
            "unknown",
            critical=True,
            summary="Schema readiness could not be determined.",
        ),
        "canonical_seeds": _component(
            "unknown",
            critical=False,
            summary="Built-in Pipeline seed freshness could not be determined.",
        ),
        "qdrant": _component(
            "unknown",
            critical=True,
            summary="Qdrant readiness could not be determined.",
        ),
        "embedding": _component(
            "unknown",
            critical=False,
            summary="Embedding readiness could not be determined.",
        ),
        "workers": _component(
            "unknown",
            critical=False,
            summary="Worker capacity could not be determined.",
        ),
        "scheduler": _component(
            "unknown",
            critical=False,
            summary="Scheduler lateness could not be determined.",
        ),
        "module_locks": _component(
            "unknown",
            critical=False,
            summary="Frozen Module availability could not be determined.",
        ),
    }
    return {
        "status": "degraded",
        "ready": False,
        "checked_at": time.time(),
        "cache_ttl_s": _CACHE_TTL_S,
        "components": components,
    }


def snapshot(settings: Settings) -> dict[str, Any]:
    """Return one cached readiness snapshot without exposing probe failures."""
    key = (
        settings.pg_dsn,
        settings.qdrant_url,
        settings.embed_endpoint,
        settings.embed_model,
        settings.embed_dim,
    )
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < _CACHE_TTL_S:
            return copy.deepcopy(hit[1])
        try:
            result = _collect(settings)
        except Exception:  # noqa: BLE001 - health itself obeys the never-500 rule
            result = _fallback()
        if key not in _cache and len(_cache) >= _CACHE_MAX_ENTRIES:
            oldest = min(_cache, key=lambda item: _cache[item][0])
            _cache.pop(oldest, None)
        _cache[key] = (time.monotonic(), result)
        return copy.deepcopy(result)


def clear_cache() -> None:
    """Test/reconfiguration hook; ordinary callers should use ``snapshot``."""
    with _cache_lock:
        _cache.clear()


__all__ = ["clear_cache", "snapshot"]
