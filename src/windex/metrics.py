"""Process-global Prometheus registry + the persistent instruments.

This module is deliberately tiny and dependency-light — it imports only
prometheus_client and stdlib. That's what lets BOTH the API layer (the /metrics
exporter in api/prom.py, the RED middleware in api/app.py) and the lower index
layer (the query-embed leg in index/search.py) import it without a cycle and
without api/ ⇄ index/ layering inversion.

Two kinds of metric live in windex:
  * point-in-time STATE (row counts, is-a-loop-running, gateway reachable) —
    generated at scrape time by the custom collector in api/prom.py, which needs
    request-scoped Settings, so it is NOT registered here.
  * cumulative EVENT instruments (HTTP RED, search + query-embed counters and
    histograms) — long-lived objects that accumulate across scrapes. Those are
    the ones defined here, on REGISTRY, so a single process-global instance is
    shared by every call site that records into them.

REGISTRY is a private CollectorRegistry (not prometheus_client's global default)
so nothing we didn't ask for leaks in; the standard runtime collectors we DO
want (process_*, python_*) are attached explicitly below.
"""

from prometheus_client import (
    CollectorRegistry,
    Counter,
    GCCollector,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    disable_created_metrics,
)

# Drop the per-series `_..._created` timestamp gauges prometheus_client emits
# alongside every counter/histogram by default. They double the family count for
# no signal windex uses, and — being gauges named `_created` — they'd trip both
# the metric-name contract and the naming lint. Standard exporter hygiene.
disable_created_metrics()

REGISTRY = CollectorRegistry()

# Standard runtime collectors, attached to OUR registry (their default is the
# global one we deliberately don't use). These give Grafana python_info,
# python_gc_*, and (on Linux) process_cpu_seconds_total / process_resident_memory_
# bytes for free — the baseline any Prometheus exporter is expected to carry.
# NB: ProcessCollector reads /proc, so it emits NOTHING on the macOS box windex
# currently runs on; it's registered anyway so a Linux deploy lights up for free,
# and the python_* series prove the default collectors are wired regardless.
ProcessCollector(registry=REGISTRY)
PlatformCollector(registry=REGISTRY)
GCCollector(registry=REGISTRY)

# --- HTTP RED (rate / errors / duration) for the whole API surface ---
# `handler` is the ROUTE TEMPLATE (e.g. /v1/docs/{doc_id:path}), never the raw
# path — the raw path carries doc ids and query strings and would explode label
# cardinality. Set by the ASGI middleware in api/app.py.
HTTP_REQUESTS = Counter(
    "windex_http_requests",
    "Total HTTP requests handled, by route-template handler, method and status "
    "code (the /metrics scrape itself is excluded).",
    ["handler", "method", "code"],
    registry=REGISTRY,
)
HTTP_REQUEST_DURATION = Histogram(
    "windex_http_request_duration_seconds",
    "HTTP request latency in seconds, by route-template handler.",
    ["handler"],
    registry=REGISTRY,
)

# --- Search path (recorded in api/service.run_search — the one seam both the
# REST endpoint and the MCP tool go through) ---
SEARCH_REQUESTS = Counter(
    "windex_search_requests",
    "Total searches through service.run_search, by requested mode "
    "(hybrid|dense|lexical) and result (ok|degraded|error; degraded = hybrid "
    "fell back to lexical because the query embed timed out or the breaker was open).",
    ["mode", "result"],
    registry=REGISTRY,
)
SEARCH_DURATION = Histogram(
    "windex_search_duration_seconds",
    "End-to-end search latency in seconds (service.run_search, all sources fused).",
    registry=REGISTRY,
)

# --- Query-embed leg specifically (recorded in index/search.py) ---
# This leg carries the 8s deadline and the circuit breaker; its latency and
# failure rate are the operational story of hybrid search, so they get their own
# instruments rather than being buried in the search total. Breaker
# short-circuits are NOT counted here (no embed was attempted) — they're visible
# via windex_query_breaker_state.
QUERY_EMBED_DURATION = Histogram(
    "windex_query_embed_duration_seconds",
    "Query-embedding round-trip latency in seconds (only when an embed was "
    "actually attempted; breaker short-circuits are not observed here).",
    registry=REGISTRY,
)
QUERY_EMBED_FAILURES = Counter(
    "windex_query_embed_failures",
    "Total query-embedding failures (timeout / connection refused); pairs with "
    "windex_query_breaker_state going open.",
    registry=REGISTRY,
)
