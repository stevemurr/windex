"""Prometheus exposition for the contract-epoch 2 runtime.

Event metrics (HTTP RED, search, and query embedding) live in
``windex.metrics.REGISTRY`` and accumulate for the life of the API process.
State metrics are collected from the canonical database, Qdrant, the embedding
gateway, and local storage at scrape time.  The two registries have disjoint
family names and are concatenated into one text exposition.

The exporter follows a never-500 rule: dependency probes fail closed into
``windex_*_up 0`` while the rest of the page remains available.  This is
important because a monitor that returns 500 during the outage it is meant to
describe is not useful.
"""

from __future__ import annotations

import importlib.metadata
import logging
import os
import socket
import time
from urllib.parse import urlsplit

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
)
from prometheus_client.core import GaugeMetricFamily
from starlette.routing import Match

from windex import db, metrics
from windex.config import Settings
from windex.index.embed_breaker import CLOSED, HALF_OPEN, OPEN, breaker

log = logging.getLogger("windex.prom")

_gateway_cache: dict[str, tuple[float, bool, float]] = {}
_GATEWAY_TTL = 30.0
_scrape_cache: dict[tuple[str, str, str], tuple[float, bytes]] = {}
_SCRAPE_TTL = 10.0


def operational_snapshot(cur) -> dict[str, int | float]:
    """Read the small worker/scheduler state shared by metrics and health.

    Keep this to two aggregate queries.  The health route is intentionally
    unauthenticated and polled during pairing, so it must not repeat the full
    Prometheus corpus scan or turn each client into an observability load test.
    """
    cur.execute(
        """SELECT
               count(*) FILTER (WHERE state = 'ready'),
               count(*) FILTER (WHERE state = 'running'),
               count(DISTINCT lease_worker) FILTER (
                   WHERE state = 'running'
                     AND heartbeat_at >= now() - interval '60 seconds'),
               count(*) FILTER (
                   WHERE state = 'running'
                     AND lease_expires_at < now())
             FROM run_tasks
            WHERE state IN ('ready', 'running')"""
    )
    ready, running, live_workers, expired = cur.fetchone()
    cur.execute(
        """SELECT count(*),
                  coalesce(max(extract(
                      epoch FROM now() - next_fire_at)), 0)
             FROM source_triggers
            WHERE enabled
              AND trigger_type IN ('cron', 'interval')
              AND next_fire_at IS NOT NULL
              AND next_fire_at <= now()"""
    )
    due, lag = cur.fetchone()
    return {
        "ready_tasks": int(ready or 0),
        "running_tasks": int(running or 0),
        "live_workers": int(live_workers or 0),
        "expired_leases": int(expired or 0),
        "due_triggers": int(due or 0),
        "scheduler_lag_s": max(0.0, float(lag or 0)),
    }


def _gateway_probe(endpoint: str) -> tuple[bool, float]:
    """Probe only the configured endpoint's TCP port; never spend GPU work."""
    now = time.monotonic()
    hit = _gateway_cache.get(endpoint)
    if hit and now - hit[0] < _GATEWAY_TTL:
        return hit[1], hit[2]
    parts = urlsplit(endpoint)
    host = parts.hostname or "127.0.0.1"
    port = parts.port or (443 if parts.scheme == "https" else 80)
    started = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=2):
            up = True
    except OSError:
        up = False
    duration = time.monotonic() - started
    _gateway_cache[endpoint] = (now, up, duration)
    return up, duration


class WindexCollector:
    """Collect point-in-time state from the epoch-2 runtime."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def collect(self):
        yield self._build_info()
        yield from self._storage_metrics()
        yield from self._gateway_metrics()
        yield self._breaker_metric()
        yield from self._qdrant_metrics()
        yield from self._database_metrics()

    @staticmethod
    def _build_info() -> GaugeMetricFamily:
        try:
            version = importlib.metadata.version("windex")
        except Exception:  # noqa: BLE001 - source checkout, not an installed dist
            version = "unknown"
        family = GaugeMetricFamily(
            "windex_build_info",
            "Build information (always 1; the package version is a label).",
            labels=["version"],
        )
        family.add_metric([version], 1.0)
        return family

    def _storage_metrics(self):
        free = GaugeMetricFamily(
            "windex_storage_free_bytes",
            "Free bytes on the filesystem backing this storage tier.",
            labels=["tier"],
        )
        total = GaugeMetricFamily(
            "windex_storage_total_bytes",
            "Total bytes on the filesystem backing this storage tier.",
            labels=["tier"],
        )
        reserve = GaugeMetricFamily(
            "windex_storage_min_free_bytes",
            "Configured free-space reserve for this storage tier.",
            labels=["tier"],
        )
        ok = GaugeMetricFamily(
            "windex_storage_ok",
            "1 when the tier exists, is writable, and is above its reserve.",
            labels=["tier"],
        )
        tiers = (
            ("staging", self.settings.staging_dir),
            ("downloads", self.settings.downloads_dir),
        )
        for tier, path in tiers:
            try:
                stat = os.statvfs(path)
            except OSError as exc:
                log.warning("metrics: statvfs(%s) failed: %s", path, exc)
                ok.add_metric([tier], 0.0)
                continue
            free_bytes = stat.f_bavail * stat.f_frsize
            free.add_metric([tier], float(free_bytes))
            total.add_metric([tier], float(stat.f_blocks * stat.f_frsize))
            reserve.add_metric(
                [tier], float(self.settings.storage_min_free_bytes))
            healthy = (
                os.access(path, os.W_OK)
                and (
                    self.settings.storage_min_free_bytes <= 0
                    or free_bytes >= self.settings.storage_min_free_bytes
                )
            )
            ok.add_metric([tier], 1.0 if healthy else 0.0)
        yield free
        yield total
        yield reserve
        yield ok

    def _gateway_metrics(self):
        up, duration = _gateway_probe(self.settings.embed_endpoint)
        reachable = GaugeMetricFamily(
            "windex_gateway_up",
            "1 when the embedding endpoint accepts a TCP connection.",
        )
        reachable.add_metric([], 1.0 if up else 0.0)
        probe = GaugeMetricFamily(
            "windex_gateway_probe_duration_seconds",
            "Duration of the latest cached embedding-gateway TCP probe.",
        )
        probe.add_metric([], duration)
        yield reachable
        yield probe

    def _breaker_metric(self) -> GaugeMetricFamily:
        current = breaker.snapshot(self.settings)["state"]
        family = GaugeMetricFamily(
            "windex_query_breaker_state",
            "Query-embedding circuit-breaker state as a one-hot gauge.",
            labels=["state"],
        )
        for state in (CLOSED, OPEN, HALF_OPEN):
            family.add_metric([state], 1.0 if current == state else 0.0)
        return family

    def _qdrant_metrics(self):
        reachable = GaugeMetricFamily(
            "windex_qdrant_up",
            "1 when Qdrant answers the scrape-time probe.",
        )
        try:
            from qdrant_client import QdrantClient

            client = QdrantClient(url=self.settings.qdrant_url, timeout=5)
            collections = client.get_collections().collections
            points = GaugeMetricFamily(
                "windex_qdrant_points",
                "Indexed points in each physical Qdrant collection.",
                labels=["collection"],
            )
            for collection in collections:
                info = client.get_collection(collection.name)
                points.add_metric(
                    [collection.name], float(info.points_count or 0))
            client.close()
        except Exception as exc:  # noqa: BLE001 - dependency outage is a metric
            log.warning(
                "metrics: qdrant probe failed, serving windex_qdrant_up 0: %s",
                exc,
            )
            reachable.add_metric([], 0.0)
            yield reachable
            return
        reachable.add_metric([], 1.0)
        yield reachable
        yield points

    def _database_metrics(self):
        reachable = GaugeMetricFamily(
            "windex_db_up",
            "1 when the canonical Postgres database answers this scrape.",
        )
        try:
            with db.pooled(self.settings.pg_dsn) as conn:
                families = self._read_database(conn)
        except Exception as exc:  # noqa: BLE001 - dependency outage is a metric
            log.warning(
                "metrics: database probe failed, serving windex_db_up 0: %s",
                exc,
            )
            reachable.add_metric([], 0.0)
            yield reachable
            return
        reachable.add_metric([], 1.0)
        yield reachable
        yield from families

    @staticmethod
    def _read_database(conn) -> list[GaugeMetricFamily]:
        """Read bounded-cardinality state from canonical epoch-2 tables."""
        families: list[GaugeMetricFamily] = []
        with conn.cursor() as cur:
            cur.execute(
                """SELECT s.search_name, d.status, count(*)
                     FROM sources s
                     JOIN documents d ON d.source_id = s.id
                    GROUP BY s.search_name, d.status
                    ORDER BY s.search_name, d.status""")
            documents = GaugeMetricFamily(
                "windex_documents",
                "Canonical document rows by public Source name and state.",
                labels=["source", "status"],
            )
            for source, status, count in cur.fetchall():
                documents.add_metric([source, status], float(count))
            families.append(documents)

            cur.execute(
                """SELECT s.search_name,
                       count(*) FILTER (
                           WHERE d.indexed_at >= now() - interval '2 minutes'
                             AND d.id IS NOT NULL),
                       count(*) FILTER (
                           WHERE d.indexed_at >= now() - interval '10 minutes'
                             AND d.id IS NOT NULL)
                     FROM sources s
                     LEFT JOIN documents d
                       ON d.source_id = s.id
                      AND d.indexed_at >= now() - interval '10 minutes'
                    WHERE s.archived_at IS NULL AND s.enabled
                    GROUP BY s.search_name
                    ORDER BY s.search_name""")
            throughput = GaugeMetricFamily(
                "windex_embeds_per_minute",
                "Documents made searchable per minute over trailing windows.",
                labels=["source", "window"],
            )
            for source, count_2m, count_10m in cur.fetchall():
                throughput.add_metric(
                    [source, "2m"], float(count_2m or 0) / 2.0)
                throughput.add_metric(
                    [source, "10m"], float(count_10m or 0) / 10.0)
            families.append(throughput)

            cur.execute(
                """SELECT status, count(*)
                     FROM repos
                    GROUP BY status
                    ORDER BY status""")
            repos = GaugeMetricFamily(
                "windex_repos",
                "Canonical GitHub repository rows by state.",
                labels=["status"],
            )
            for status, count in cur.fetchall():
                repos.add_metric([status], float(count))
            families.append(repos)

            cur.execute(
                """SELECT s.search_name, u.store, u.status, count(*)
                     FROM source_units u
                     JOIN sources s ON s.id = u.source_id
                    GROUP BY s.search_name, u.store, u.status
                    ORDER BY s.search_name, u.store, u.status""")
            units = GaugeMetricFamily(
                "windex_source_units",
                "Canonical Source watermark units by Source, store, and state.",
                labels=["source", "store", "status"],
            )
            for source, store, status, count in cur.fetchall():
                units.add_metric([source, store, status], float(count))
            families.append(units)

            cur.execute(
                """SELECT state, count(*)
                     FROM runs
                    GROUP BY state
                    ORDER BY state""")
            runs = GaugeMetricFamily(
                "windex_runs",
                "Pipeline Runs by lifecycle state.",
                labels=["state"],
            )
            for state, count in cur.fetchall():
                runs.add_metric([state], float(count))
            families.append(runs)

            cur.execute(
                """SELECT coalesce(source_name, ''), lane, state, count(*)
                     FROM run_tasks
                    WHERE state IN ('pending', 'ready', 'running', 'blocked')
                    GROUP BY source_name, lane, state
                    ORDER BY source_name, lane, state""")
            tasks = GaugeMetricFamily(
                "windex_run_tasks",
                "Non-terminal Pipeline tasks by Source, lane, and state.",
                labels=["source", "lane", "state"],
            )
            for source, lane, state, count in cur.fetchall():
                tasks.add_metric([source, lane, state], float(count))
            families.append(tasks)

            cur.execute(
                """SELECT coalesce(source_name, ''), lane,
                          extract(epoch FROM now() - min(started_at)),
                          extract(epoch FROM now() - min(heartbeat_at))
                     FROM run_tasks
                    WHERE state = 'running'
                    GROUP BY source_name, lane
                    ORDER BY source_name, lane""")
            running_age = GaugeMetricFamily(
                "windex_task_running_age_seconds",
                "Age of the oldest running task in each Source/lane group.",
                labels=["source", "lane"],
            )
            heartbeat_age = GaugeMetricFamily(
                "windex_task_heartbeat_age_seconds",
                "Age of the stalest running-task heartbeat in each group.",
                labels=["source", "lane"],
            )
            for source, lane, age, heartbeat in cur.fetchall():
                if age is not None:
                    running_age.add_metric(
                        [source, lane], max(0.0, float(age)))
                if heartbeat is not None:
                    heartbeat_age.add_metric(
                        [source, lane], max(0.0, float(heartbeat)))
            families.extend((running_age, heartbeat_age))

            operational = operational_snapshot(cur)
            expired_leases = GaugeMetricFamily(
                "windex_worker_expired_leases",
                "Running task leases already past their expiry.",
            )
            expired_leases.add_metric(
                [], float(operational["expired_leases"]))
            claim_stalled = GaugeMetricFamily(
                "windex_worker_claim_stalled",
                "1 when a lease has expired, or ready work exists without a "
                "running task heartbeat in the last 60 seconds.",
            )
            claim_stalled.add_metric(
                [],
                1.0
                if (
                    operational["expired_leases"]
                    or (
                        operational["ready_tasks"]
                        and not operational["live_workers"]
                    )
                )
                else 0.0,
            )
            families.extend((expired_leases, claim_stalled))

            scheduler_due = GaugeMetricFamily(
                "windex_scheduler_due_triggers",
                "Enabled cron/interval triggers currently past next_fire_at.",
            )
            scheduler_due.add_metric(
                [], float(operational["due_triggers"]))
            scheduler_lag = GaugeMetricFamily(
                "windex_scheduler_max_lag_seconds",
                "Age of the most overdue enabled trigger, or zero.",
            )
            scheduler_lag.add_metric(
                [], float(operational["scheduler_lag_s"]))
            families.extend((scheduler_due, scheduler_lag))

            cur.execute(
                """SELECT extract(epoch FROM ts), data
                     FROM operational_events
                    WHERE component = 'maintenance'
                      AND event = 'storage.gc.completed'
                    ORDER BY seq DESC LIMIT 1"""
            )
            gc_row = cur.fetchone()
            gc_last_run = GaugeMetricFamily(
                "windex_storage_gc_last_run_timestamp_seconds",
                "Unix timestamp of the latest Pipeline storage GC pass.",
            )
            gc_deleted_files = GaugeMetricFamily(
                "windex_storage_gc_deleted_files",
                "Files removed by the latest Pipeline storage GC pass.",
                labels=["kind"],
            )
            gc_deleted_bytes = GaugeMetricFamily(
                "windex_storage_gc_deleted_bytes",
                "Bytes removed by the latest Pipeline storage GC pass.",
                labels=["kind"],
            )
            gc_errors = GaugeMetricFamily(
                "windex_storage_gc_errors",
                "File or database errors in the latest Pipeline storage GC pass.",
            )
            gc_capped = GaugeMetricFamily(
                "windex_storage_gc_cap_reached",
                "1 when the latest Pipeline storage GC pass reached a safety cap.",
                labels=["cap"],
            )
            if gc_row:
                timestamp, data = gc_row
                summary = data if isinstance(data, dict) else {}
                gc_last_run.add_metric([], float(timestamp or 0))
                deleted = summary.get("deleted")
                if isinstance(deleted, dict):
                    for kind, values in sorted(deleted.items()):
                        if not isinstance(values, dict):
                            continue
                        gc_deleted_files.add_metric(
                            [str(kind)], float(values.get("files") or 0))
                        gc_deleted_bytes.add_metric(
                            [str(kind)], float(values.get("bytes") or 0))
                gc_errors.add_metric(
                    [], float(summary.get("error_count") or 0))
                gc_capped.add_metric(
                    ["files"],
                    1.0 if summary.get("file_cap_reached") else 0.0,
                )
                gc_capped.add_metric(
                    ["bytes"],
                    1.0 if summary.get("byte_cap_reached") else 0.0,
                )
            else:
                gc_last_run.add_metric([], 0.0)
                gc_errors.add_metric([], 0.0)
                gc_capped.add_metric(["files"], 0.0)
                gc_capped.add_metric(["bytes"], 0.0)
            families.extend((
                gc_last_run,
                gc_deleted_files,
                gc_deleted_bytes,
                gc_errors,
                gc_capped,
            ))

            cur.execute(
                """SELECT known_item_ndcg, known_item_mrr,
                          golden_ndcg, golden_mrr, judge_ndcg
                     FROM search_quality
                    ORDER BY ts DESC LIMIT 1""")
            quality = cur.fetchone()
            ndcg = GaugeMetricFamily(
                "windex_search_quality_ndcg",
                "NDCG@k of the latest search-quality evaluation.",
                labels=["leg"],
            )
            mrr = GaugeMetricFamily(
                "windex_search_quality_mrr",
                "MRR of the latest search-quality evaluation.",
                labels=["leg"],
            )
            if quality:
                values = (
                    ("known_item", quality[0], quality[1]),
                    ("golden", quality[2], quality[3]),
                    ("judge", quality[4], None),
                )
                for leg, ndcg_value, mrr_value in values:
                    if ndcg_value is not None:
                        ndcg.add_metric([leg], float(ndcg_value))
                    if mrr_value is not None:
                        mrr.add_metric([leg], float(mrr_value))
            families.extend((ndcg, mrr))
        return families


def render(settings: Settings) -> bytes:
    """Return cached Prometheus text exposition for the configured runtime."""
    now = time.monotonic()
    key = (
        settings.pg_dsn,
        settings.qdrant_url,
        str(settings.active_data_root),
    )
    hit = _scrape_cache.get(key)
    if hit and now - hit[0] < _SCRAPE_TTL:
        return hit[1]
    state = CollectorRegistry()
    state.register(WindexCollector(settings))
    output = generate_latest(metrics.REGISTRY) + generate_latest(state)
    _scrape_cache[key] = (now, output)
    return output


class PrometheusMiddleware:
    """Raw ASGI HTTP RED instrumentation with bounded route-template labels.

    A raw wrapper preserves streaming responses.  ``/metrics`` is excluded so a
    Prometheus scrape never records itself.
    """

    def __init__(
        self,
        app,
        routes,
        label_prefix: str = "",
        skip_prefixes: tuple[str, ...] = (),
    ):
        self.app = app
        self.routes = routes
        self.label_prefix = label_prefix
        self.skip_prefixes = skip_prefixes
        self._flat = None
        self._flat_len = -1

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        if (
            scope["type"] != "http"
            or path == "/metrics"
            or any(path.startswith(prefix) for prefix in self.skip_prefixes)
        ):
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        status = {"code": 500}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        started = time.perf_counter()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            handler = self._handler(scope)
            metrics.HTTP_REQUESTS.labels(
                handler=handler,
                method=method,
                code=str(status["code"]),
            ).inc()
            metrics.HTTP_REQUEST_DURATION.labels(handler=handler).observe(
                time.perf_counter() - started)

    def _resolvable(self):
        if self._flat is None or self._flat_len != len(self.routes):
            flat = []
            for route in self.routes:
                expand = getattr(route, "effective_candidates", None)
                if callable(expand):
                    try:
                        flat.extend(expand())
                        continue
                    except Exception:  # noqa: BLE001 - fall back to the route
                        pass
                flat.append(route)
            self._flat = flat
            self._flat_len = len(self.routes)
        return self._flat

    def _handler(self, scope) -> str:
        for route in self._resolvable():
            try:
                match, _ = route.matches(scope)
            except Exception:  # noqa: BLE001 - an odd route cannot break metrics
                continue
            if match == Match.FULL:
                path = getattr(route, "path", None)
                if path:
                    return self.label_prefix + path
        return "__unmatched__"


__all__ = [
    "CONTENT_TYPE_LATEST",
    "PrometheusMiddleware",
    "WindexCollector",
    "operational_snapshot",
    "render",
]
