"""Prometheus exposition for GET /metrics — windex's single, always-on exporter.

Why one endpoint in the serve process and nothing in the CLI jobs: almost every
state number Grafana needs is *state in Postgres* (documents/repos/control
counts), and the embed-loop/hydrate jobs are transient detached processes that
come and go — instrumenting each would mean a pushgateway plus a lifecycle to
babysit for series that go stale the moment a job exits. The API is the one
process that is always up and already holds a warm pooled connection, so it reads
that shared state at scrape time. The live signals that *aren't* in the DB (is a
loop running, is the gateway reachable) it derives from the same process
table / socket the console does.

Context for the alerts hanging off this endpoint: the dashboard is being split —
Grafana (fed by Prometheus) takes over metrics + alerting, the console keeps
search and the operational switches. On 2026-07-17 a 25-minute embedding-gateway
outage stalled indexing for ~36h and nothing alerted; the rules Grafana needs
(embeds stalled, a loop down, the gateway down) all hang off the series here.

Metric-name contract policy: these names/labels are a PUBLIC API — Grafana
dashboards and alert rules on the user's box query them by name. Treat them
additive-only, exactly like the /v1 REST contract: renaming or relabelling a
family silently breaks someone's dashboard. The golden test in tests/test_prom.py
is the guard; a failure there means extend the contract, don't rename.

Registry layout (see windex/metrics.py): cumulative event instruments (HTTP RED,
search + query-embed counters/histograms) and the standard process_/python_
runtime collectors live on a process-global `metrics.REGISTRY` so they accumulate
across scrapes. The point-in-time STATE metrics below need request-scoped
Settings, so they're produced by a custom Collector on a throwaway registry built
per render(). render() serialises both and concatenates (their family names are
disjoint) behind a ~10s cache so an aggressive scraper can't multiply DB load.
"""

import importlib.metadata
import logging
import socket
import time
from urllib.parse import urlsplit

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily
from starlette.routing import Match

# Re-exported so app.py sets the exposition media_type as prom.CONTENT_TYPE_LATEST
# and prometheus_client stays an implementation detail of this module.
from prometheus_client import CONTENT_TYPE_LATEST as CONTENT_TYPE_LATEST

from windex import db, metrics
from windex.api import jobs
from windex.cli import EMBED_SOURCES
from windex.config import Settings
from windex.index.embed_breaker import CLOSED, HALF_OPEN, OPEN, breaker

log = logging.getLogger("windex.prom")

# TCP-connect probe of the embedding gateway, cached (up + probe duration). A
# scrape must not open a fresh socket to the GPU host every time (Prometheus
# scrapes ~every 15s and an outage retry-storm could pile on), and 30s of
# staleness is well inside any useful "gateway down" alert-for window.
_gateway_cache: dict[str, tuple[float, bool, float]] = {}
_GATEWAY_TTL = 30.0

# Whole-exposition cache, keyed by DSN so a test settings pointed at another DB
# never serves the production scrape. 10s: shorter than a normal scrape interval,
# so a well-behaved Prometheus always gets a fresh page while a double-scrape
# rides the cache instead of re-hitting pg/qdrant.
_scrape_cache: dict[str, tuple[float, bytes]] = {}
_SCRAPE_TTL = 10.0


def _gateway_probe(endpoint: str) -> tuple[bool, float]:
    """TCP-connect probe to the embedding endpoint's host:port, returning
    (reachable, probe_seconds). Deliberately a bare connect, not a real embed: it
    answers "is the gateway reachable" without putting a single token of load on
    the GPU the bulk pipeline is saturating."""
    now = time.monotonic()
    hit = _gateway_cache.get(endpoint)
    if hit and now - hit[0] < _GATEWAY_TTL:
        return hit[1], hit[2]
    parts = urlsplit(endpoint)
    host = parts.hostname or "127.0.0.1"
    port = parts.port or (443 if parts.scheme == "https" else 80)
    t0 = time.monotonic()
    up = False
    try:
        with socket.create_connection((host, port), timeout=2):
            up = True
    except OSError:
        up = False  # refused / unresolved / timed out — all "gateway not answering"
    dur = time.monotonic() - t0
    _gateway_cache[endpoint] = (now, up, dur)
    return up, dur


# The embed-loop CLI names (EMBED_SOURCES keys) and the documents.source corpus
# vocabulary diverge for exactly two sources: the CLI calls them ccnews/gh, the
# corpus (and windex_documents{source}) calls them news/github. The Grafana
# dashboard joins per-source across windex_loop_up and windex_documents, so the
# `source` LABEL on loop_up (and on the embed-loop log series) MUST speak the
# corpus vocabulary or the news/github rows match nothing. Only the label is
# canonicalised; the pgrep patterns still use the CLI names. The other six
# sources are identical in both vocabularies.
_SOURCE_CANON = {"ccnews": "news", "gh": "github"}


def _canonical_source(cli_source: str) -> str:
    return _SOURCE_CANON.get(cli_source, cli_source)


def _log_source(stem: str) -> str:
    """Canonical corpus source for an embed-loop log file, or "" for any other
    log. ~/.windex/logs carries both naming conventions seen in the wild —
    '<src>-embed' (dashboard job names) and 'embed-<src>' / bare 'embed-loop'
    (manual starts) — so recognise all three, then map the CLI name into the
    documents vocabulary. Non-loop logs (serve, gh-discover, gh-hydrate,
    watchdog, …) get "" so their staleness series carry no source to join on."""
    if stem == "embed-loop":
        cli = "ccnews"  # jobs.py's name for the ccnews embed loop
    elif stem.startswith("embed-"):
        cli = stem[len("embed-"):]
    elif stem.endswith("-embed"):
        cli = stem[: -len("-embed")]
    else:
        return ""
    return _canonical_source(cli) if cli in EMBED_SOURCES else ""


class WindexCollector:
    """Assembles the STATE exposition at scrape time. Two never-500 firewalls:
    the DB-independent liveness metrics (loops, jobs, gateway, breaker) are
    yielded first and unconditionally so a Postgres outage still produces a valid
    page carrying `windex_db_up 0`; qdrant is probed separately so a vector-store
    outage degrades to `windex_qdrant_up 0` without touching the pg metrics. A
    scrape that 500s would blind the very monitor meant to catch the outage."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def collect(self):
        s = self.settings

        # --- liveness that reads only the process table (survives a DB outage) ---
        # embed loops, one series per source. A loop being down is the 2026-07-17
        # failure mode, so this must not depend on anything that can also be down.
        loop = GaugeMetricFamily(
            "windex_loop_up",
            "1 if an embed-loop process for this source is alive, else 0 (from the "
            "host process table). `source` uses the corpus vocabulary of "
            "windex_documents — the ccnews/gh CLI loops are labeled news/github so "
            "per-source dashboard joins line up.",
            labels=["source"])
        for job in jobs.embed_loop_jobs():
            # One registry: the loop set and their pgrep patterns come from
            # jobs.py — the same source `windex up`/`status` and the watchdog
            # use. Only the label is canonicalised (ccnews→news, gh→github).
            cli_source = job.argv[1]
            alive = 1.0 if jobs._pids(job.pattern) else 0.0
            loop.add_metric([_canonical_source(cli_source)], alive)
        yield loop

        # The long-running non-loop jobs from the same registry the console drives
        # (gh discover/hydrate, the ingest/harvest passes, …). Reusing jobs.JOBS
        # keeps the label set fixed and bounded; the embed loops are excluded here
        # because windex_loop_up already covers them keyed by source.
        job = GaugeMetricFamily(
            "windex_job_up", "1 if this registered non-loop job process is alive, else 0.",
            labels=["job"])
        for j in jobs.JOBS.values():
            if j.argv[0] == "embed-loop":
                continue
            job.add_metric([j.name], 1.0 if jobs._pids(j.pattern) else 0.0)
        yield job

        up, probe_s = _gateway_probe(s.embed_endpoint)
        gw = GaugeMetricFamily(
            "windex_gateway_up",
            "1 if the embedding gateway accepts a TCP connection, else 0 "
            "(a bare connect, no GPU load).")
        gw.add_metric([], 1.0 if up else 0.0)
        yield gw
        gwd = GaugeMetricFamily(
            "windex_gateway_probe_duration_seconds",
            "Wall time of the last embedding-gateway TCP probe, in seconds (cached ~30s).")
        gwd.add_metric([], probe_s)
        yield gwd

        # The query-embed breaker is process-global by design (index/embed_breaker.py):
        # one breaker models the health of the one embed_endpoint this process
        # serves. Exposed one-hot (the StateSet pattern, cf. kube_pod_status_phase)
        # because the breaker genuinely has three states and half_open is a
        # diagnostic worth seeing — exactly one series is 1.
        snap_state = breaker.snapshot(s)["state"]
        brk = GaugeMetricFamily(
            "windex_query_breaker_state",
            "Query-embed circuit-breaker state, one-hot: exactly one of "
            "state=closed|open|half_open is 1.",
            labels=["state"])
        for st in (CLOSED, OPEN, HALF_OPEN):
            brk.add_metric([st], 1.0 if snap_state == st else 0.0)
        yield brk

        # Raw mtimes, not staleness: Grafana computes `time() - <this>` so the
        # "log hasn't moved in N minutes" threshold lives in the alert rule, not
        # baked in here. `_timestamp_seconds` per the epoch-timestamp convention
        # (cf. process_start_time_seconds). Cardinality bounded by the fixed set
        # of log files.
        # `log` is the truthful filename (stem); `source` is the canonical corpus
        # source for embed-loop logs and "" for everything else. Prometheus treats
        # an empty label as absent, so non-loop logs effectively carry no source
        # while the dashboard's per-source staleness query joins the loop logs on
        # source=news|github|… . A source may map to several files (both
        # <src>-embed and embed-<src> exist historically) — the dashboard max()es.
        mtime = GaugeMetricFamily(
            "windex_log_last_modified_timestamp_seconds",
            "Unix mtime of each ~/.windex/logs/*.log; derive staleness as time() - "
            "this. embed-loop logs also carry a canonical `source` label (corpus "
            "vocabulary, matching windex_documents); other logs have source=\"\".",
            labels=["log", "source"])
        for path in sorted(jobs.LOG_DIR.glob("*.log")):
            try:
                mt = path.stat().st_mtime
            except OSError:
                continue  # file vanished between glob and stat — just omit it
            mtime.add_metric([path.stem, _log_source(path.stem)], mt)
        yield mtime

        try:
            version = importlib.metadata.version("windex")
        except Exception:  # noqa: BLE001 — not installed as a dist (editable/source run)
            version = "unknown"
        build = GaugeMetricFamily(
            "windex_build_info",
            "Build information as an info metric (always 1; windex version in the label).",
            labels=["version"])
        build.add_metric([version], 1.0)
        yield build

        # --- Qdrant, probed independently of Postgres (own never-500 firewall) ---
        yield from self._qdrant_metrics(s)

        # --- Postgres-backed metrics; a failure here degrades to windex_db_up 0 ---
        db_up = GaugeMetricFamily(
            "windex_db_up",
            "1 if this scrape could read Postgres; 0 degrades the page rather than failing it.")
        try:
            families = self._db_metrics(s)
        except Exception as exc:  # noqa: BLE001 — any pg failure must still yield a page
            log.warning("metrics: DB read failed, serving windex_db_up 0: %s", exc)
            db_up.add_metric([], 0.0)
            yield db_up
            return
        db_up.add_metric([], 1.0)
        yield db_up
        yield from families

    def _qdrant_metrics(self, s: Settings):
        """windex_qdrant_up + points per collection. Same never-500 rule as the DB
        block: qdrant unreachable ⇒ up 0 and the points series omitted."""
        qup = GaugeMetricFamily("windex_qdrant_up",
                                "1 if Qdrant answered this scrape, else 0 "
                                "(the points series is omitted when 0).")
        try:
            from qdrant_client import QdrantClient

            client = QdrantClient(url=s.qdrant_url, timeout=5)
            collections = client.get_collections().collections
            # One get_collection per collection (a handful; the 10s scrape cache
            # bounds how often this fans out). Label is the real collection name
            # (news__<model>), so a half-built collection is visible during a
            # model-swap reindex — the alias news_current points at one of these.
            points = GaugeMetricFamily(
                "windex_qdrant_points", "Indexed points per Qdrant collection.",
                labels=["collection"])
            for c in collections:
                info = client.get_collection(c.name)
                points.add_metric([c.name], float(info.points_count or 0))
        except Exception as exc:  # noqa: BLE001 — vector store down must not 500 the scrape
            log.warning("metrics: qdrant read failed, serving windex_qdrant_up 0: %s", exc)
            qup.add_metric([], 0.0)
            yield qup
            return
        qup.add_metric([], 1.0)
        yield qup
        yield points

    def _db_metrics(self, s: Settings) -> list[GaugeMetricFamily]:
        """Everything that needs Postgres. Built into a list before the caller
        yields windex_db_up so a mid-read failure flips it to 0 rather than
        emitting a half-populated page."""
        families: list[GaugeMetricFamily] = []

        # documents by source,status: a straight `GROUP BY source, status` on this
        # table is a full heap aggregate — EXPLAIN ANALYZE on the live 13.28M-row
        # table blew past a 20s statement_timeout (no index covers (source,status)).
        # So reuse the service layer's already-cached rollup (_pg_heavy, 600s TTL)
        # that /v1/stats feeds from — the ">250ms ⇒ fall back to the /v1/stats
        # aggregate" rule. 600s stale is fine: doc counts move slowly and the
        # alerts here fire on liveness/gateway, not on a live document total.
        from windex.api import service

        heavy = service._pg_heavy(s)
        docs = GaugeMetricFamily(
            "windex_documents",
            "Documents by source and pipeline status. Gauge, NOT a counter: a "
            "reindex moves rows embedded→deduped, so treat a rate() drop as a reset.",
            labels=["source", "status"])
        for source, by_status in heavy["docs"].items():
            for status, n in by_status.items():
                docs.add_metric([source, status], float(n))
        families.append(docs)

        # The rest are all cheap (repos is small; control is tiny) — one pooled
        # checkout so the scrape rides the pool's per-checkout health check.
        with db.pooled(s.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT status, count(*) FROM repos GROUP BY status")
            repos = GaugeMetricFamily(
                "windex_repos",
                "Repository rows by status. Gauge with the same reindex-reset "
                "caveat as windex_documents.",
                labels=["status"])
            for status, n in cur.fetchall():
                repos.add_metric([status], float(n))
            families.append(repos)

            cur.execute("SELECT key, value FROM control")
            flags = dict(cur.fetchall())

        paused = GaugeMetricFamily(
            "windex_indexing_paused", "1 if the indexing control flag is 'paused', else 0.")
        paused.add_metric([], 1.0 if flags.get("indexing", "running") == "paused" else 0.0)
        families.append(paused)

        # Busy/idle only, NOT the stage string. The free-text values ("2018-01-01..
        # 2018-12-31 p91", "extracting + filtering · batch 20260501-abcd1234") are
        # unbounded — one new time-series per batch — the classic Prometheus
        # cardinality-explosion antipattern. The console renders the human string;
        # here we only expose whether a stage is working.
        stage = GaugeMetricFamily(
            "windex_stage_busy",
            "1 if this pipeline stage is doing work (control value != 'idle'); the "
            "free-text stage string is deliberately omitted to bound label cardinality.",
            labels=["key"])
        for key, value in flags.items():
            if key.endswith("_stage"):
                stage.add_metric([key], 0.0 if value == "idle" else 1.0)
        families.append(stage)

        # Current embed throughput profile as an info-style metric: always 1, the
        # value carried in a label. The enum is bounded (env|polite|full), so a
        # label is safe here — unlike the stage strings above.
        profile = flags.get("embed_profile", "env")
        prof = GaugeMetricFamily(
            "windex_embed_profile_info",
            "Active embed throughput profile as an info metric (always 1; the "
            "profile is the label: env|polite|full).",
            labels=["profile"])
        prof.add_metric([profile], 1.0)
        families.append(prof)

        # NOTE: there is deliberately no windex_searches gauge backed by
        # count(*) of search_metrics. That table is retention-pruned (~30d, in
        # `windex daily`), so its rowcount DECREASES daily and rate() over it
        # reads the prune as negative garbage. Search throughput is instead the
        # real in-process counter windex_search_requests_total (windex/metrics.py):
        # it resets to 0 on restart, which rate() handles correctly as a counter
        # reset — the opposite and benign failure mode.

        return families


def render(settings: Settings) -> bytes:
    """Exposition bytes for GET /metrics, cached ~10s per DSN.

    Concatenates the process-global instrument/runtime registry with a freshly
    built state registry (disjoint family names, so concatenation is valid text
    exposition). The fresh registry per render is what keeps the state values
    scrape-fresh and lets the custom collector see the current request's Settings."""
    now = time.monotonic()
    key = settings.pg_dsn
    hit = _scrape_cache.get(key)
    if hit and now - hit[0] < _SCRAPE_TTL:
        return hit[1]
    state = CollectorRegistry()
    state.register(WindexCollector(settings))
    out = generate_latest(metrics.REGISTRY) + generate_latest(state)
    _scrape_cache[key] = (now, out)
    return out


class PrometheusMiddleware:
    """HTTP RED (rate/errors/duration) as a raw ASGI middleware — the write side
    of the exporter, kept here with the read side (render) so app.py carries only
    a one-line registration.

    Deliberately NOT starlette.BaseHTTPMiddleware: that wraps the response in a
    body-buffering bridge that breaks streaming, and windex serves an SSE stream
    (/v1/events) the console lives on. This wrapper only peeks at the
    response-start status line and never touches the body, so streaming is
    untouched. The `handler` label is the ROUTE TEMPLATE (e.g.
    /v1/docs/{doc_id:path}), never the raw path — a raw path carries doc ids and
    would blow up label cardinality. /metrics is excluded so the scrape doesn't
    instrument itself."""

    def __init__(self, app, routes):
        self.app = app
        self.routes = routes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") == "/metrics":
            await self.app(scope, receive, send)
            return
        method = scope.get("method", "GET")
        status = {"code": 500}  # if the app raises before responding, it's a 500

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        start = time.perf_counter()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            handler = self._handler(scope)
            elapsed = time.perf_counter() - start
            metrics.HTTP_REQUESTS.labels(
                handler=handler, method=method, code=str(status["code"])).inc()
            metrics.HTTP_REQUEST_DURATION.labels(handler=handler).observe(elapsed)

    def _handler(self, scope) -> str:
        # Resolve the template by re-matching the routes (Starlette 1.3 doesn't
        # stash the matched Route in scope). Unmatched paths (404s, random
        # probes) collapse to one bucket rather than one series each.
        for route in self.routes:
            try:
                match, _ = route.matches(scope)
            except Exception:  # noqa: BLE001 — a Mount/odd route shouldn't break metrics
                continue
            if match == Match.FULL:
                return getattr(route, "path", "__unmatched__")
        return "__unmatched__"
