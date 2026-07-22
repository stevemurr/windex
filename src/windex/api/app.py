import asyncio
import time
import uuid

import orjson
from datetime import datetime
from importlib.resources import files
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Header, Query
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
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


# Vendored, no-build frontend assets (Preact console migration). Served locally
# — nothing here is fetched from a CDN or npm at runtime.
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


@app.get("/console-preview", include_in_schema=False)
def console_preview() -> HTMLResponse:
    """The in-progress Preact console (no build, vendored). Kept alongside the
    live `/` console until the migration is verified, then it takes over `/`."""
    return HTMLResponse(
        files("windex.api").joinpath("static/console.html").read_text(),
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/v1/search")
def search(
    q: str = Query(min_length=1),
    source: str = Query("all", description="news | github | wiki | arxiv | smallweb | "
                        "docs | hn | hf | memory | all, or a registered custom source name"),
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
    conversation_id: str | None = Query(None, max_length=64,
                                        description="Memory: scope recall to one conversation uuid"),
) -> dict:
    settings = get_settings()
    try:
        service.validate_source(settings, source)  # 422 on an unknown source
    except ValueError:
        raise HTTPException(422, f"unknown source: {source}")
    return service.run_search(
        settings, q, source=source, limit=limit, mode=mode,
        published_after=published_after, published_before=published_before,
        min_stars=min_stars, language=language, category=category, outlet=outlet,
        framework=framework, min_points=min_points, root=root, kind=kind,
        conversation_id=conversation_id,
    )


@app.get("/v1/docs/{doc_id:path}")
def get_doc(doc_id: str) -> dict:
    doc = service.get_document(get_settings(), doc_id)
    if doc is None:
        raise HTTPException(404, f"unknown document id: {doc_id}")
    return doc


# --- chat-memory write API (push-based source) -------------------------------
# The macOS app chunks each conversation and full-replace-pushes the whole chunk
# list here; windex stages parquet + reconciles the ledger (see
# memory_source.ingest). Opt-in bearer auth guards these three routes; reads
# (/v1/search, /v1/docs) stay open by design.

class MemoryChunk(BaseModel):
    index: int = Field(ge=0)
    text: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    message_range: tuple[int, int] | None = None


class MemoryPush(BaseModel):
    title: str = ""
    chunks: list[MemoryChunk] = Field(default_factory=list)


def require_write_token(authorization: str | None = Header(None)) -> None:
    """Bearer-token gate for the /v1/memory/* write side. No-op when
    WINDEX_WRITE_TOKEN is empty (open, trusted-LAN default); otherwise the
    request must carry `Authorization: Bearer <token>`."""
    token = get_settings().write_token
    if not token:
        return
    if authorization != f"Bearer {token}":
        raise HTTPException(401, "missing or invalid write token")


def _validate_push(conversation_id: str, body: MemoryPush) -> None:
    """422 the malformed pushes the ingest contract can't accept: a non-uuid
    conversation id, too many chunks, oversized chunk text, non-contiguous
    indices, or an over-budget body."""
    try:
        uuid.UUID(conversation_id)
    except ValueError:
        raise HTTPException(422, "conversation_id must be a UUID")
    from windex.memory_source.ingest import MAX_CHUNKS, MAX_TEXT_CHARS

    if len(body.chunks) > MAX_CHUNKS:
        raise HTTPException(422, f"too many chunks (max {MAX_CHUNKS})")
    if [c.index for c in body.chunks] != list(range(len(body.chunks))):
        raise HTTPException(422, "chunk indices must be exactly 0..n-1 in order")
    total = 0
    for c in body.chunks:
        if len(c.text) > MAX_TEXT_CHARS:
            raise HTTPException(422, f"chunk text too large (max {MAX_TEXT_CHARS} chars)")
        total += len(c.text)
    if total > 4_000_000:
        raise HTTPException(422, "push body too large (max ~4 MB of chunk text)")


@app.post("/v1/memory/conversations/{conversation_id}",
          dependencies=[Depends(require_write_token)])
def memory_push(conversation_id: str, body: MemoryPush) -> dict:
    """Full-replace one conversation's chat-memory chunks. Returns
    {conversation_id, chunks, staged, skipped, deleted}; staged+deleted>0 means
    work happened. 422 on a malformed push, 503 when staging isn't writable."""
    _validate_push(conversation_id, body)
    chunks = [c.model_dump() for c in body.chunks]
    try:
        return service.memory_replace(get_settings(), conversation_id.lower(),
                                      body.title, chunks)
    except OSError as exc:  # staging drive read-only / unmounted
        raise HTTPException(503, f"staging unavailable: {exc}")


@app.delete("/v1/memory/conversations/{conversation_id}",
            dependencies=[Depends(require_write_token)])
def memory_delete(conversation_id: str) -> dict:
    """Tombstone every chunk of a conversation. Idempotent (deleting nothing →
    deleted: 0)."""
    try:
        uuid.UUID(conversation_id)
    except ValueError:
        raise HTTPException(422, "conversation_id must be a UUID")
    return service.memory_delete(get_settings(), conversation_id.lower())


@app.get("/v1/memory/status", dependencies=[Depends(require_write_token)])
def memory_status() -> dict:
    """Corpus-wide memory rollup: conversation count, chunk counts by status,
    last embed time. The app's Settings status row + health probe."""
    return service.memory_status(get_settings())


# --- custom sources: registry CRUD (push-based, generalized memory source) ---
# POST/PATCH/DELETE are write-token gated like /v1/memory/*; GET reads stay open.
# A custom source reuses the documents ledger and the shared embed driver; the
# per-doc push + search endpoints are added alongside these.

class SourceCreate(BaseModel):
    name: str
    title: str = ""
    description: str = ""
    recipe: dict | None = None


class SourcePatch(BaseModel):
    title: str | None = None
    description: str | None = None
    recipe: dict | None = None


@app.post("/v1/sources", dependencies=[Depends(require_write_token)], status_code=201)
def source_create(body: SourceCreate) -> dict:
    """Register a custom source. 201 with its IndexInfo; 409 if it already
    exists; 422 for a malformed or reserved name."""
    from windex.custom_source.registry import DuplicateSource

    try:
        return service.custom_create(get_settings(), body.name, body.title,
                                     body.description, body.recipe)
    except DuplicateSource as exc:
        raise HTTPException(409, str(exc))
    except ValueError as exc:
        raise HTTPException(422, str(exc))


@app.get("/v1/sources")
def sources_list() -> dict:
    """Every registered custom source with doc counts: {"sources": [IndexInfo…]}."""
    return {"sources": service.custom_list(get_settings())}


@app.get("/v1/sources/{name}")
def source_get(name: str) -> dict:
    """One custom source's IndexInfo (recipe + doc_count + pending). 404 unknown."""
    info = service.custom_get(get_settings(), name)
    if info is None:
        raise HTTPException(404, f"unknown source: {name}")
    return info


@app.patch("/v1/sources/{name}", dependencies=[Depends(require_write_token)])
def source_patch(name: str, body: SourcePatch) -> dict:
    """Update a source's title/description/recipe (only the fields sent). 404
    unknown."""
    fields = {k: getattr(body, k) for k in ("title", "description", "recipe")
              if k in body.model_fields_set}
    info = service.custom_update(get_settings(), name, **fields)
    if info is None:
        raise HTTPException(404, f"unknown source: {name}")
    return info


@app.delete("/v1/sources/{name}", dependencies=[Depends(require_write_token)])
def source_delete(name: str) -> dict:
    """Delete a whole custom source: tombstone its docs, drop the registry row,
    remove its staging. Returns {"deleted": N}; 404 if the source is unknown."""
    res = service.custom_delete_source(get_settings(), name)
    if res is None:
        raise HTTPException(404, f"unknown source: {name}")
    return res


class CustomDoc(BaseModel):
    id: str                              # suffix; the stored id is <name>:<id>
    title: str = ""
    text: str
    url: str | None = None               # default custom://<name>/<id>
    published_at: datetime | None = None
    extra: dict | None = None            # opaque per-doc metadata, surfaced in search


class DocsPush(BaseModel):
    docs: list[CustomDoc] = Field(default_factory=list)


class DocsDelete(BaseModel):
    ids: list[str] = Field(default_factory=list)


def _validate_custom_docs(docs: list[CustomDoc]) -> None:
    """422 the malformed pushes the upsert contract can't accept: too many docs,
    an oversized text/extra, a bad id suffix, or an over-budget body."""
    from windex.custom_source.ingest import (
        MAX_BODY_CHARS, MAX_DOCS_PER_BATCH, MAX_EXTRA_BYTES, MAX_TEXT_CHARS, SUFFIX_RE,
    )

    if len(docs) > MAX_DOCS_PER_BATCH:
        raise HTTPException(422, f"too many docs (max {MAX_DOCS_PER_BATCH})")
    total = 0
    for d in docs:
        if not SUFFIX_RE.match(d.id):
            raise HTTPException(422, f"invalid doc id: {d.id!r}")
        if len(d.text) > MAX_TEXT_CHARS:
            raise HTTPException(422, f"doc text too large (max {MAX_TEXT_CHARS} chars)")
        if d.extra is not None and len(orjson.dumps(d.extra)) > MAX_EXTRA_BYTES:
            raise HTTPException(422, f"doc extra too large (max {MAX_EXTRA_BYTES} bytes)")
        total += len(d.text)
    if total > MAX_BODY_CHARS:
        raise HTTPException(422, "push body too large (max ~4 MB of doc text)")


@app.post("/v1/sources/{name}/docs", dependencies=[Depends(require_write_token)])
def source_push(name: str, body: DocsPush) -> dict:
    """Upsert docs into a custom source (changed-text delta staged + embedded;
    unchanged docs skipped). Returns {source, docs, staged, skipped}. 404 unknown
    source, 422 on a malformed push, 503 when staging isn't writable."""
    settings = get_settings()
    if service.custom_get(settings, name) is None:
        raise HTTPException(404, f"unknown source: {name}")
    _validate_custom_docs(body.docs)
    try:
        return service.custom_push(settings, name, [d.model_dump() for d in body.docs])
    except OSError as exc:  # staging drive read-only / unmounted
        raise HTTPException(503, f"staging unavailable: {exc}")


@app.post("/v1/sources/{name}/docs/delete", dependencies=[Depends(require_write_token)])
def source_delete_docs(name: str, body: DocsDelete) -> dict:
    """Tombstone specific docs by id suffix. Returns {"deleted": N}. 404 unknown
    source; idempotent (already-deleted / unknown ids don't count)."""
    settings = get_settings()
    if service.custom_get(settings, name) is None:
        raise HTTPException(404, f"unknown source: {name}")
    return service.custom_delete_docs(settings, name, body.ids)


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


@app.get("/v1/recent/embedded")
def recent_embedded(limit: int = Query(25, ge=1, le=100)) -> list[dict]:
    """Recently embedded (landed in Qdrant), newest first — console progress feed."""
    return service.recent_feed(get_settings(), "indexed_at", limit=limit)


@app.get("/v1/recent/indexed")
def recent_indexed(limit: int = Query(25, ge=1, le=100)) -> list[dict]:
    """Recently indexed (harvested/staged), newest first — console progress feed."""
    return service.recent_feed(get_settings(), "created_at", limit=limit)


@app.post("/v1/system/refresh-stats")
def refresh_stats() -> dict:
    """Force-drop the cached doc rollups so /metrics + /v1/stats recompute now
    (used after a bulk cleanup so dashboards reflect immediately)."""
    service.clear_doc_stats_cache()
    return {"ok": True}


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


@app.get("/v1/loops")
def loops_state() -> dict:
    """Per-source loop desired-state + running, and whether the supervisor is
    alive. Lightweight (pgrep + one control read) so the console control panel
    can poll it responsively, independent of the heavier /v1/stats."""
    return service.supervisor_status(get_settings())


@app.post("/v1/loops/{source}")
def loop_set(source: str, params: dict = Body(default={})) -> dict:
    """Turn an embed loop on/off (desired-state). `off` stops it and keeps it off
    — `windex up` and the watchdog both honor the flag, so it won't come back."""
    try:
        return service.set_loop_enabled(get_settings(), source, bool(params.get("enabled", True)))
    except KeyError:
        raise HTTPException(404, f"unknown source: {source}")


@app.post("/v1/ingest/{source}")
def ingest_set(source: str, params: dict = Body(default={})) -> dict:
    """Turn a source's auto-ingest on/off (desired-state). Off means the refresh
    sweep and the scheduler skip fetching it; a manual 'check now' still runs."""
    try:
        return service.set_ingest_enabled(get_settings(), source, bool(params.get("enabled", True)))
    except KeyError:
        raise HTTPException(404, f"unknown source: {source}")


@app.post("/v1/system/loops")
def loops_bulk(params: dict = Body(default={})) -> dict:
    """Bulk on/off for every embed loop ('start all' / 'stop all')."""
    return {"loops": service.set_all_loops_enabled(get_settings(), bool(params.get("enabled", True)))}


@app.post("/v1/system/up")
def system_up() -> dict:
    """Reconcile to desired state — detached `windex up` (starts enabled loops
    and serve that are down)."""
    return service.system_up(get_settings())


@app.post("/v1/system/restart")
def system_restart() -> dict:
    """Bounce the loops — stop every one, then `windex up` restarts the enabled."""
    return service.restart_loops(get_settings())


@app.post("/v1/system/refresh")
def system_refresh(params: dict = Body(default={})) -> dict:
    """Kick off a freshness sweep — detached `windex refresh [--source …]`."""
    return service.run_refresh(get_settings(), params.get("sources") or [])


@app.get("/v1/freshness")
def freshness_state() -> list[dict]:
    """Per-source indexed/pending counts + last embed-loop activity."""
    return service.freshness(get_settings())


@app.get("/v1/datasets/{source}/stats")
def dataset_stats(source: str) -> dict:
    """Per-dataset detail (freshness row-click): counts by pipeline status +
    content date range."""
    try:
        return service.dataset_stats(get_settings(), source)
    except KeyError:
        raise HTTPException(404, f"unknown source: {source}")


@app.get("/v1/schedule")
def schedule_state() -> list[dict]:
    """The editable schedule entries with running + last-run — what the console
    schedule editor reads (name, kind, target, hour, minute, weekday, enabled)."""
    return service.schedule_status(get_settings())


@app.put("/v1/schedule/{name}")
def schedule_upsert(name: str, params: dict = Body(default={})) -> dict:
    """Create or edit a schedule entry. Body: any of hour/minute/weekday/enabled
    /target/kind. Editing an existing entry preserves unspecified fields;
    creating a new one requires kind + target. 422 on an invalid entry."""
    try:
        return service.upsert_schedule(get_settings(), {**params, "name": name})
    except ValueError as exc:
        raise HTTPException(422, str(exc))


@app.delete("/v1/schedule/{name}")
def schedule_delete(name: str) -> dict:
    """Delete a schedule entry (404 if it doesn't exist)."""
    try:
        return service.delete_schedule(get_settings(), name)
    except KeyError:
        raise HTTPException(404, f"unknown scheduled job: {name}")


@app.post("/v1/schedule/{name}/run")
def schedule_run(name: str) -> dict:
    """Run a scheduled entry now (detached), ignoring the ingest desired-state
    flag (a manual run is an explicit 'check now')."""
    try:
        return service.run_scheduled(get_settings(), name)
    except KeyError:
        raise HTTPException(404, f"unknown scheduled job: {name}")


@app.get("/v1/activity")
def activity_state() -> list[dict]:
    """Watchable things for the log drawer: actions, loops, services — with
    running state, last activity, and crash flag. Tail any via GET /v1/logs/{name}."""
    return service.activity(get_settings())


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
