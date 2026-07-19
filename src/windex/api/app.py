import asyncio
import time

import orjson
from datetime import datetime
from importlib.resources import files
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from starlette.concurrency import run_in_threadpool

from fastapi import Body

from windex.api import jobs, logs, prom, service
from windex.config import get_settings

STARTED_AT = time.time()  # serve-process uptime for the console

# No custom response class on purpose: handlers declare return types, so this
# FastAPI serializes straight to JSON bytes via pydantic-core (Rust) — its docs
# state that's faster than ORJSONResponse, which it deprecates. orjson is still
# used below for the SSE stream, which is hand-assembled outside response
# serialization (measured 5.8-9.4x over stdlib dumps there, 2026-07-19).
app = FastAPI(title="windex", version="0.1.0",
              description="Self-hosted web index for search agents")


@app.get("/", include_in_schema=False)
def dashboard() -> HTMLResponse:
    return HTMLResponse(
        files("windex.api").joinpath("dashboard.html").read_text(),
        headers={"Cache-Control": "no-cache"},  # single-file app; stale caches hide fixes
    )


@app.get("/v1/search")
def search(
    q: str = Query(min_length=1),
    source: Literal["news", "github", "wiki", "arxiv", "smallweb", "docs", "hn",
                    "hf", "all"] = "all",
    limit: int = Query(10, ge=1, le=50),
    mode: Literal["hybrid", "dense", "lexical"] = "hybrid",
    published_after: datetime | None = None,
    published_before: datetime | None = None,
    min_stars: int | None = None,
    min_points: int | None = Query(None, ge=0,
                                   description="Minimum HN points, e.g. 50"),
    language: str | None = None,
    category: str | None = Query(None, max_length=64,
                                 description="arXiv primary category, e.g. cs.LG"),
    outlet: str | None = Query(None, max_length=253,
                               description="Small Web feed host, e.g. example.com"),
    framework: str | None = Query(None, max_length=64,
                                  description="Docs framework, e.g. python or react"),
    root: str | None = Query(None, max_length=64,
                             description="HF doc root, e.g. transformers or agents-course"),
    kind: str | None = Query(None, max_length=16,
                             description="HF page kind: docs, learn or blog"),
) -> dict:
    return service.run_search(
        get_settings(), q, source=source, limit=limit, mode=mode,
        published_after=published_after, published_before=published_before,
        min_stars=min_stars, language=language, category=category, outlet=outlet,
        framework=framework, min_points=min_points, root=root, kind=kind,
    )


@app.get("/v1/docs/{doc_id:path}")
def get_doc(doc_id: str) -> dict:
    doc = service.get_document(get_settings(), doc_id)
    if doc is None:
        raise HTTPException(404, f"unknown document id: {doc_id}")
    return doc


@app.get("/v1/stats")
def stats() -> dict:
    return _stats_with_uptime(get_settings())


def _stats_with_uptime(settings) -> dict:
    body = service.get_stats(settings)
    body["activity"]["uptime_s"] = int(time.time() - STARTED_AT)
    return body


@app.get("/v1/metrics")
def metrics(minutes: int = Query(60, ge=1, le=43200)) -> dict:
    """Search-performance rollup: latency percentiles + hybrid→keyword
    degradation counts over the trailing window."""
    return service.get_search_metrics(get_settings(), minutes=minutes)


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics() -> Response:
    """Prometheus exposition (src/windex/api/prom.py). Not `/v1/*`: this is an
    ops scrape target for Grafana/Prometheus, deliberately outside the versioned
    agent-facing contract. Never 500s — a DB outage still returns a page with
    windex_db_up 0 (see the collector) so the very outage it should catch is
    visible rather than a scrape error."""
    return Response(prom.render(get_settings()), media_type=prom.CONTENT_TYPE_LATEST)


@app.get("/v1/recent")
def recent(limit: int = Query(30, ge=1, le=100)) -> list[dict]:
    return service.get_recent(get_settings(), limit=limit)


@app.get("/v1/timeseries")
def timeseries(minutes: int = Query(60, ge=5, le=1440)) -> list[dict]:
    return service.get_timeseries(get_settings(), minutes=minutes)


@app.post("/v1/control/{action}")
def control(action: Literal["start", "pause"]) -> dict:
    value = "running" if action == "start" else "paused"
    return {"indexing": service.set_control(get_settings(), value)}


@app.get("/v1/workers")
def workers() -> dict:
    return service.get_worker_activity(get_settings())


@app.get("/v1/logs")
def logs_list() -> list[dict]:
    return logs.list_logs()


@app.get("/v1/logs/{name}")
def logs_tail(
    name: str,
    lines: int = Query(200, ge=1, le=2000),
    grep: str | None = Query(None, max_length=200),
    level: Literal["info", "warn", "error"] | None = None,
) -> dict:
    try:
        return logs.tail(name, lines=lines, grep=grep, level=level)
    except KeyError:
        raise HTTPException(404, f"unknown log: {name}")


@app.get("/v1/jobs")
def jobs_list() -> list[dict]:
    return jobs.list_jobs()


@app.post("/v1/jobs/{name}/start")
def jobs_start(name: str, params: dict = Body(default={})) -> dict:
    try:
        return jobs.start(name, params)
    except KeyError:
        raise HTTPException(404, f"unknown job: {name}")
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


@app.post("/v1/jobs/{name}/stop")
def jobs_stop(name: str) -> dict:
    try:
        return jobs.stop(name)
    except KeyError:
        raise HTTPException(404, f"unknown job: {name}")


@app.post("/v1/throttle/{profile}")
def throttle(profile: Literal["polite", "full", "env"]) -> dict:
    """Embedding throughput profile — read by embedders at each pass, so it
    applies within about a minute without restarting anything."""
    return {"embed_profile": service.set_embed_profile(get_settings(), profile)}


@app.get("/v1/events")
async def events(ticks: int | None = Query(None, ge=1, le=100)) -> StreamingResponse:
    """SSE stream for the dashboard: `stats` every ~2s, `recent` only when it
    changes, `timeseries` every ~16s. REST endpoints remain the poll/agent API;
    `ticks` bounds the stream for tests."""
    settings = get_settings()

    async def gen():
        last_recent_key = None
        n = 0
        while True:
            stats = await run_in_threadpool(_stats_with_uptime, settings)
            yield f"event: stats\ndata: {orjson.dumps(stats).decode()}\n\n"
            recent = await run_in_threadpool(service.get_recent, settings, 25)
            key = (recent[0]["id"], recent[0]["indexed_at"]) if recent else ()
            if key != last_recent_key:
                last_recent_key = key
                yield f"event: recent\ndata: {orjson.dumps(recent).decode()}\n\n"
            if n % 8 == 0:
                series = await run_in_threadpool(service.get_timeseries, settings, 60)
                yield f"event: timeseries\ndata: {orjson.dumps(series).decode()}\n\n"
            if n % 3 == 0:
                job_state = await run_in_threadpool(jobs.list_jobs)
                yield f"event: jobs\ndata: {orjson.dumps(job_state).decode()}\n\n"
                log_sizes = await run_in_threadpool(logs.list_logs)
                yield f"event: logsizes\ndata: {orjson.dumps(log_sizes).decode()}\n\n"
            worker_state = await run_in_threadpool(service.get_worker_activity, settings)
            yield f"event: workers\ndata: {orjson.dumps(worker_state).decode()}\n\n"
            n += 1
            if ticks is not None and n >= ticks:
                return
            await asyncio.sleep(2)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# HTTP RED metrics (windex/api/prom.py). Registered last, after every route is
# defined, so the middleware's route-template resolver sees the full routing
# table (the live routes list, not a copy).
app.add_middleware(prom.PrometheusMiddleware, routes=app.router.routes)
