import asyncio
import hmac
import time
import uuid

import orjson
from datetime import datetime
from importlib.resources import files
from pathlib import Path
from typing import ClassVar, Literal

from fastapi import (APIRouter, Depends, FastAPI, HTTPException, Header, Query,
                     Request)
from fastapi.responses import (HTMLResponse, RedirectResponse, Response,
                               StreamingResponse)
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from fastapi import Body

from windex.api import jobs, logs, prom, service
from windex.api import models as m
from windex.config import get_settings

STARTED_AT = time.time()  # serve-process uptime for the console

# No custom response class on purpose: handlers declare return types, so this
# FastAPI serializes straight to JSON bytes via pydantic-core (Rust) — its docs
# state that's faster than ORJSONResponse, which it deprecates. orjson is still
# used below for the SSE stream, which is hand-assembled outside response
# serialization (measured 5.8-9.4x over stdlib dumps there, 2026-07-19).
app = FastAPI(title="windex", version="0.1.0",
              description="Self-hosted web index for search agents",
              # Operation ids become the handler name, so a generated client gets
              # `search(...)` rather than FastAPI's default `searchV1SearchGet(...)`.
              # Names must stay unique across routes — tests/test_api.py asserts it,
              # because a collision silently drops an operation from the schema.
              generate_unique_id_function=lambda route: route.name)

# The operational surface, defined once and served twice: at /admin/v1 (the real
# home, gated) and — until the console is deleted — at /v1 as a deprecated alias,
# so the existing dashboard keeps working through the whole migration.
#
# Two prefixes because they are two contracts with different lifetimes. /v1 is the
# agent-facing promise: additive-only, consumed by the MCP server and the memory
# push client, and it should shrink to exactly search + docs + push. The control
# plane will churn weekly while the native client is built. One version number
# cannot govern both.
ops = APIRouter()
# `admin` is constructed after require_admin is defined, below.


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


def _bearer_ok(authorization: str | None, token: str) -> bool:
    """Constant-time bearer check.

    `hmac.compare_digest` rather than `!=`: the timing signal is not a realistic
    threat on a trusted LAN, but it costs one line, and the previous form also
    compared the whole `"Bearer <tok>"` string — so it was case-sensitive about
    the scheme, which RFC 7235 says it must not be, and rejected an otherwise
    valid `bearer <tok>`.
    """
    if not authorization:
        return False
    scheme, _, value = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return False
    return hmac.compare_digest(value.strip().encode(), token.encode())


def require_write_token(authorization: str | None = Header(None)) -> None:
    """Bearer-token gate for the /v1/memory/* write side. No-op when
    WINDEX_WRITE_TOKEN is empty (open, trusted-LAN default); otherwise the
    request must carry `Authorization: Bearer <token>`."""
    token = get_settings().write_token
    if not token:
        return
    if not _bearer_ok(authorization, token):
        raise HTTPException(401, "missing or invalid write token")


# Paths under the /admin mount that answer without a token. Exactly one: the app
# has to be able to ask "are you there, and do you want a token" BEFORE it can
# pair. Kept as a set rather than a second unguarded router so that adding an
# admin route cannot accidentally land it outside the gate — the mount-level
# dependency is what makes "unauthenticated admin route" unrepresentable, and
# this is the single, visible exception to it.
ADMIN_OPEN_PATHS = frozenset({"/v1/health"})


def require_admin(request: Request, authorization: str | None = Header(None)) -> None:
    """Blanket gate for /admin/**.

    Two behaviours the /v1 side deliberately does not have:

    * **Fails closed off-loopback.** The admin surface can crawl a caller-chosen
      host and (later) clone a caller-chosen git URL. Served on the LAN with no
      token that is not a trusted-LAN default, it is an open SSRF proxy — so
      binding off-loopback without a token disables the surface with a fix-it
      message rather than silently serving it. Loopback stays open so `curl
      localhost:8100/admin/v1/jobs` on the box still works.
    * **On by default.** Gating is at the mount, not per route, because per-route
      opt-in is precisely why ~35 operational routes ended up ungated: nobody
      forgot on purpose, the mechanism made forgetting the default.
    """
    # Starlette keeps the FULL path in scope["path"] and records the mount in
    # root_path, so the mount prefix has to be stripped to compare against a
    # mount-relative allowlist. Doing it this way also keeps working if the app is
    # ever served unmounted (root_path empty).
    path = request.scope.get("path", "")
    root = request.scope.get("root_path", "")
    if root and path.startswith(root):
        path = path[len(root):] or "/"
    if path in ADMIN_OPEN_PATHS:
        return
    settings = get_settings()
    token = settings.write_token
    if not token:
        if settings.serve_host not in ("127.0.0.1", "::1", "localhost"):
            raise HTTPException(
                503, "admin API disabled: set WINDEX_WRITE_TOKEN, or bind "
                     "WINDEX_SERVE_HOST=127.0.0.1. Generate one with: "
                     "python -c 'import secrets;print(secrets.token_urlsafe(32))'")
        return                      # loopback + no token = trusted local dev
    if not _bearer_ok(authorization, token):
        raise HTTPException(401, "missing or invalid admin token")


admin = FastAPI(
    title="windex admin", version="1.0.0",
    description="windex control plane. Separately versioned from the /v1 agent API.",
    dependencies=[Depends(require_admin)],
    generate_unique_id_function=lambda route: route.name,
)


@admin.get("/v1/health", responses={200: {"model": m.Health}})
def admin_health() -> dict:
    """Unauthenticated liveness + capability probe — the one open admin route.

    A client needs to know a backend exists and whether it wants a token before it
    can pair, so this answers without one. It deliberately carries no state beyond
    that: no counts, no config, no source names.
    """
    settings = get_settings()
    return {
        "status": "ok",
        "service": "windex",
        "version": app.version,
        "auth_required": bool(settings.write_token),
        "started_at": STARTED_AT,
        "uptime_s": round(time.time() - STARTED_AT, 1),
    }


@admin.get("/v1/registry", responses={200: {"model": m.Registry}})
def admin_registry(response: Response) -> dict:
    """The module palette: port types, node kinds, and every module's config schema.

    The load-bearing endpoint for the native client — the graph editor renders its
    palette, its connection rules and every node inspector from this, so it
    hardcodes no vocabulary and a windex that gains a module needs no client
    release. ETag'd because a client caches it and revalidates.
    """
    from windex.recipe import registry

    doc = registry.describe()
    response.headers["ETag"] = f'W/"registry-{doc["registry_version"]}"'
    response.headers["Cache-Control"] = "no-cache"
    return doc


class RecipeDoc(BaseModel):
    """A recipe document. Deliberately untyped at this boundary: the schema is
    versioned INSIDE the document and validated by `recipe.parse`, which is the
    security boundary. Mirroring it in pydantic would be a second definition to
    keep in step, and the one that 422s would not be the one that matters."""

    model_config = ConfigDict(extra="allow")


@admin.post("/v1/recipes/validate", responses={200: {"model": m.ValidationReport}})
def admin_recipe_validate(body: dict) -> dict:
    """Parse + type-check a recipe. Pure: no network, no database, no filesystem.

    That purity is the point — an editor can call this on every keystroke, and it
    is what separates `validate` from `preview` (which fetches the seeds) and
    `dry-run` (which executes the graph against a counting sink).
    """
    from windex.recipe import parse as recipe_parse

    return recipe_parse.validate(body, get_settings())


@admin.get("/v1/recipes", responses={200: {"model": m.RecipeList}})
def admin_recipes(include_spec: bool = Query(False)) -> dict:
    """Every registered source, built-in and installed alike.

    They are the same kind of thing now — one table, one list, one editor. `spec` is
    omitted by default because a sidebar wants names and status, not eleven full
    DAGs.
    """
    from windex import db
    from windex.recipe import store

    with db.pooled(get_settings().pg_dsn) as conn:
        return {"recipes": store.list_recipes(conn, include_spec=include_spec)}


@admin.get("/v1/recipes/{name}", responses={200: {"model": m.Recipe}})
def admin_recipe(name: str) -> dict:
    """One recipe, with its graph. What the editor opens."""
    from windex import db
    from windex.recipe import store

    with db.pooled(get_settings().pg_dsn) as conn:
        got = store.get_recipe(conn, name)
    if got is None:
        raise HTTPException(404, f"unknown recipe: {name}")
    return got


@admin.get("/v1/recipes/{name}/tasks", responses={200: {"model": m.RecipeTasks}})
def admin_recipe_tasks(name: str, flow: str | None = Query(None)) -> dict:
    """The tasks a run of this recipe would fan out to — lane, dependencies,
    preconditions and progress weight per node.

    A dry look at placement without queueing anything, so the editor can show WHERE
    a node will run and what it waits on rather than making the author read source.
    """
    from windex import db
    from windex.recipe import compile as recipe_compile
    from windex.recipe import store

    with db.pooled(get_settings().pg_dsn) as conn:
        got = store.get_recipe(conn, name)
    if got is None:
        raise HTTPException(404, f"unknown recipe: {name}")
    try:
        tasks = recipe_compile.compile_tasks(got["spec"], flow=flow,
                                            settings=get_settings())
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    return {"recipe": name, "flow": flow, "tasks": tasks}


@admin.get("/v1/whoami", responses={200: {"model": m.WhoAmI}})
def admin_whoami() -> dict:
    """Gated echo, so pairing fails at setup with a clear message rather than on
    the first write."""
    return {"ok": True, "scopes": ["admin"],
            "auth_required": bool(get_settings().write_token)}


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


@ops.get("/metrics", responses={200: {"model": m.SearchMetrics}})
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


@ops.get("/recent", responses={200: {"model": list[m.RecentDoc]}})
def recent(limit: int = Query(30, ge=1, le=100)) -> list[dict]:
    return service.get_recent(get_settings(), limit=limit)


@ops.get("/recent/embedded", responses={200: {"model": list[m.RecentDoc]}})
def recent_embedded(limit: int = Query(25, ge=1, le=100)) -> list[dict]:
    """Recently embedded (landed in Qdrant), newest first — console progress feed."""
    return service.recent_feed(get_settings(), "indexed_at", limit=limit)


@ops.get("/recent/indexed", responses={200: {"model": list[m.RecentDoc]}})
def recent_indexed(limit: int = Query(25, ge=1, le=100)) -> list[dict]:
    """Recently indexed (harvested/staged), newest first — console progress feed."""
    return service.recent_feed(get_settings(), "created_at", limit=limit)


@ops.post("/system/refresh-stats", responses={200: {"model": m.ActionResult}})
def refresh_stats() -> dict:
    """Force-drop the cached doc rollups so /metrics + /v1/stats recompute now
    (used after a bulk cleanup so dashboards reflect immediately)."""
    service.clear_doc_stats_cache()
    return {"ok": True}


@ops.get("/timeseries", responses={200: {"model": list[m.TimeseriesPoint]}})
def timeseries(minutes: int = Query(60, ge=5, le=1440)) -> list[dict]:
    return service.get_timeseries(get_settings(), minutes=minutes)


@ops.post("/control/{action}", responses={200: {"model": m.ControlState}})
def control(action: Literal["start", "pause"]) -> dict:
    value = "running" if action == "start" else "paused"
    return {"indexing": service.set_control(get_settings(), value)}


@ops.get("/workers", responses={200: {"model": m.WorkersState}})
def workers() -> dict:
    return service.get_worker_activity(get_settings())


@ops.get("/logs", responses={200: {"model": list[m.LogSource]}})
def logs_list() -> list[dict]:
    return logs.list_logs()


@ops.get("/logs/{name}", responses={200: {"model": m.LogTail}})
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


@ops.get("/jobs", responses={200: {"model": list[m.JobInfo]}})
def jobs_list() -> list[dict]:
    return jobs.list_jobs()


@ops.post("/jobs/{name}/start", responses={200: {"model": m.ActionResult}})
def jobs_start(name: str, params: dict = Body(default={})) -> dict:
    try:
        return jobs.start(name, params)
    except KeyError:
        raise HTTPException(404, f"unknown job: {name}")
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


@ops.post("/jobs/{name}/stop", responses={200: {"model": m.ActionResult}})
def jobs_stop(name: str) -> dict:
    try:
        return jobs.stop(name)
    except KeyError:
        raise HTTPException(404, f"unknown job: {name}")


@ops.post("/throttle/{profile}", responses={200: {"model": m.ThrottleState}})
def throttle(profile: Literal["polite", "full", "env"]) -> dict:
    """Embedding throughput profile — read by embedders at each pass, so it
    applies within about a minute without restarting anything."""
    return {"embed_profile": service.set_embed_profile(get_settings(), profile)}


@ops.get("/loops", responses={200: {"model": m.LoopsState}})
def loops_state() -> dict:
    """Per-source loop desired-state + running, and whether the supervisor is
    alive. Lightweight (pgrep + one control read) so the console control panel
    can poll it responsively, independent of the heavier /v1/stats."""
    return service.supervisor_status(get_settings())


@ops.post("/loops/{source}", responses={200: {"model": m.ActionResult}})
def loop_set(source: str, params: dict = Body(default={})) -> dict:
    """Turn an embed loop on/off (desired-state). `off` stops it and keeps it off
    — `windex up` and the watchdog both honor the flag, so it won't come back."""
    try:
        return service.set_loop_enabled(get_settings(), source, bool(params.get("enabled", True)))
    except KeyError:
        raise HTTPException(404, f"unknown source: {source}")


@ops.post("/ingest/{source}", responses={200: {"model": m.ActionResult}})
def ingest_set(source: str, params: dict = Body(default={})) -> dict:
    """Turn a source's auto-ingest on/off (desired-state). Off means the refresh
    sweep and the scheduler skip fetching it; a manual 'check now' still runs."""
    try:
        return service.set_ingest_enabled(get_settings(), source, bool(params.get("enabled", True)))
    except KeyError:
        raise HTTPException(404, f"unknown source: {source}")


@ops.post("/system/loops", responses={200: {"model": m.ActionResult}})
def loops_bulk(params: dict = Body(default={})) -> dict:
    """Bulk on/off for every embed loop ('start all' / 'stop all')."""
    return {"loops": service.set_all_loops_enabled(get_settings(), bool(params.get("enabled", True)))}


@ops.post("/system/up", responses={200: {"model": m.ActionResult}})
def system_up() -> dict:
    """Reconcile to desired state — detached `windex up` (starts enabled loops
    and serve that are down)."""
    return service.system_up(get_settings())


@ops.post("/system/restart", responses={200: {"model": m.ActionResult}})
def system_restart() -> dict:
    """Bounce the loops — stop every one, then `windex up` restarts the enabled."""
    return service.restart_loops(get_settings())


@ops.post("/system/refresh", responses={200: {"model": m.ActionResult}})
def system_refresh(params: dict = Body(default={})) -> dict:
    """Kick off a freshness sweep — detached `windex refresh [--source …]`."""
    return service.run_refresh(get_settings(), params.get("sources") or [])


@ops.get("/freshness", responses={200: {"model": list[m.SourceFreshness]}})
def freshness_state() -> list[dict]:
    """Per-source indexed/pending counts + last embed-loop activity."""
    return service.freshness(get_settings())


@ops.get("/datasets/{source}/stats", responses={200: {"model": m.DatasetStats}})
def dataset_stats(source: str) -> dict:
    """Per-dataset detail (freshness row-click): counts by pipeline status +
    content date range."""
    try:
        return service.dataset_stats(get_settings(), source)
    except KeyError:
        raise HTTPException(404, f"unknown source: {source}")


@ops.get("/schedule", responses={200: {"model": list[m.ScheduleEntry]}})
def schedule_state() -> list[dict]:
    """The editable schedule entries with running + last-run — what the console
    schedule editor reads (name, kind, target, hour, minute, weekday, enabled)."""
    return service.schedule_status(get_settings())


@ops.put("/schedule/{name}", responses={200: {"model": m.ScheduleEntry}})
def schedule_upsert(name: str, params: dict = Body(default={})) -> dict:
    """Create or edit a schedule entry. Body: any of hour/minute/weekday/enabled
    /target/kind. Editing an existing entry preserves unspecified fields;
    creating a new one requires kind + target. 422 on an invalid entry."""
    try:
        return service.upsert_schedule(get_settings(), {**params, "name": name})
    except ValueError as exc:
        raise HTTPException(422, str(exc))


@ops.delete("/schedule/{name}", responses={200: {"model": m.ActionResult}})
def schedule_delete(name: str) -> dict:
    """Delete a schedule entry (404 if it doesn't exist)."""
    try:
        return service.delete_schedule(get_settings(), name)
    except KeyError:
        raise HTTPException(404, f"unknown scheduled job: {name}")


@ops.post("/schedule/{name}/run", responses={200: {"model": m.ActionResult}})
def schedule_run(name: str) -> dict:
    """Run a scheduled entry now (detached), ignoring the ingest desired-state
    flag (a manual run is an explicit 'check now')."""
    try:
        return service.run_scheduled(get_settings(), name)
    except KeyError:
        raise HTTPException(404, f"unknown scheduled job: {name}")


@ops.get("/activity", responses={200: {"model": list[m.ActivityItem]}})
def activity_state() -> list[dict]:
    """Watchable things for the log drawer: actions, loops, services — with
    running state, last activity, and crash flag. Tail any via GET /v1/logs/{name}."""
    return service.activity(get_settings())


@ops.get("/events",
         # No JSON body to describe: this is a stream. Declaring the
         # media type is the honest answer, and it stops the schema
         # advertising an empty object a client would try to decode.
         responses={200: {"content": {"text/event-stream": {}},
                           "description": "Server-sent events."}})
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


# --- source settings: runtime-editable per-source config ---------------------
# Reads are open like the rest of the console API; writes are write-token gated.
# The editable surface is the allowlist in settings_schema — secrets and DSNs are
# absent from it by construction, so they cannot be reached through here.


class SettingsPatch(BaseModel):
    values: dict


@ops.get("/settings", responses={200: {"model": m.SettingsAll}})
def source_settings_all() -> dict:
    """Every scope: field schema, effective value, and where the value came
    from (default | env | db)."""
    return {"scopes": service.all_source_settings(get_settings())}


@ops.get("/settings/{scope}", responses={200: {"model": m.SettingsScope}})
def source_settings_get(scope: str) -> dict:
    out = service.source_settings(get_settings(), scope)
    if out is None:
        raise HTTPException(404, f"no editable settings for scope: {scope}")
    return out


@ops.patch("/settings/{scope}", responses={200: {"model": m.SettingsScope}}, dependencies=[Depends(require_write_token)])
def source_settings_patch(scope: str, body: SettingsPatch) -> dict:
    """Set one or more overrides. 422 for an unknown key, a key belonging to a
    different scope, or a value of the wrong type; numbers are clamped to their
    declared range rather than rejected."""
    try:
        out = service.patch_source_settings(get_settings(), scope, body.values)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    if out is None:
        raise HTTPException(404, f"no editable settings for scope: {scope}")
    return out


@ops.delete("/settings/{scope}/{key}", responses={200: {"model": m.SettingsScope}},
            dependencies=[Depends(require_write_token)])
def source_settings_revert(scope: str, key: str) -> dict:
    """Drop one override so the key falls back to env, then the code default."""
    try:
        out = service.revert_source_setting(get_settings(), scope, key)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    if out is None:
        raise HTTPException(404, f"no editable settings for scope: {scope}")
    return out


# --- crawl: index an arbitrary web cluster from a seed link ------------------
# Writes (start/cancel) are write-token gated like /v1/sources mutations; reads
# stay open like the rest of the console API. A crawl runs for minutes, so the
# POST only QUEUES it — windex-loop-crawl executes it — and the control page at
# /crawl follows progress over SSE.


# The recipe sections, typed. These were `dict` — which meant a misspelled key
# was silently DROPPED: `{"scope": {"path_prfix": "/docs/"}}` validated, parsed to
# an empty scope, and crawled the whole host. `extra="forbid"` turns that into a
# 422 naming the bad key. It also makes the recipe visible in the OpenAPI schema,
# which a generated client needs in order to express a crawl at all.
#
# Every field is `| None` and dumping uses `exclude_none=True`, because in
# `crawl.recipe.parse` an ABSENT key and a null one are not the same thing:
# `drop_boilerplate` defaults to True when missing, and `path_prefix` missing
# means "the seed's own directory" while an explicit "" means "the whole host".
# Dropping nulls keeps "unset" unset, so those defaults still apply.
#
# Bounds deliberately live in `crawl.recipe.parse`, not here: it clamps to the
# operator's ceilings, and duplicating the numbers in a second place is how the
# two drift apart.

class CrawlScope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    same_host: bool | None = None
    path_prefix: str | None = None
    include: list[str] | None = None
    exclude: list[str] | None = None


class CrawlLimits(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_pages: int | None = None
    max_depth: int | None = None
    host_interval: float | None = None
    request_timeout: float | None = None
    max_page_bytes: int | None = None


class CrawlExtract(BaseModel):
    model_config = ConfigDict(extra="forbid")
    quality_filters: bool | None = None
    min_chars: int | None = None


class CrawlDedup(BaseModel):
    model_config = ConfigDict(extra="forbid")
    drop_boilerplate: bool | None = None
    prune: bool | None = None


class _RecipeBody(BaseModel):
    """The inline recipe shared by start and preview.

    `seed` is accepted as a singular alias for `seeds` so a one-link crawl — the
    headline use case — needs no array syntax. `version` is accepted because the
    console's "Re-run" posts a stored recipe back verbatim, and `Recipe.to_dict()`
    includes it; without it, `extra="forbid"` would break re-run.
    """

    model_config = ConfigDict(extra="forbid")

    version: int | None = None
    seed: str | None = None
    seeds: list[str] | None = None
    scope: CrawlScope | None = None
    limits: CrawlLimits | None = None
    extract: CrawlExtract | None = None
    dedup: CrawlDedup | None = None

    # ClassVar, not a field: pydantic would otherwise make an underscore-annotated
    # attribute a private attr, and a plain annotation a request body key.
    NOT_RECIPE: ClassVar[tuple[str, ...]] = ()

    def recipe(self) -> dict:
        return {k: v for k, v in self.model_dump(exclude_none=True).items()
                if k not in self.NOT_RECIPE}


class CrawlStart(_RecipeBody):
    source: str                       # custom-source name (created if absent)
    title: str = ""
    description: str = ""

    NOT_RECIPE: ClassVar[tuple[str, ...]] = ("source", "title", "description")


class CrawlPreview(_RecipeBody):
    pass


@app.get("/manage", include_in_schema=False)
def manage_console() -> HTMLResponse:
    """Source settings + crawling — its own space, not a dashboard tab."""
    return HTMLResponse(
        files("windex.api").joinpath("static/manage.html").read_text(),
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/crawl", include_in_schema=False)
def crawl_console() -> RedirectResponse:
    """The crawl page became a tab of /manage. Redirect rather than drop it —
    this URL is already bookmarked and referenced in the README."""
    return RedirectResponse("/manage#crawl", status_code=308)


@ops.post("/crawl/preview", responses={200: {"model": m.CrawlPreview}}, dependencies=[Depends(require_write_token)])
async def crawl_preview(body: CrawlPreview) -> dict:
    """Dry run: fetch only the seed(s) and report what WOULD be crawled.

    Write-gated despite writing nothing — it makes the server issue outbound
    requests to a caller-chosen host, which is the same capability as starting a
    crawl and should carry the same gate.
    """
    try:
        return await run_in_threadpool(service.crawl_preview, get_settings(), body.recipe())
    except ValueError as exc:
        raise HTTPException(422, str(exc))


@ops.post("/crawl", responses={202: {"model": m.CrawlQueued}}, dependencies=[Depends(require_write_token)], status_code=202)
async def crawl_start(body: CrawlStart) -> dict:
    """Queue a crawl. 202 + {run_id} — the worker picks it up; 422 on a bad
    recipe or source name."""
    try:
        return await run_in_threadpool(
            service.crawl_start, get_settings(), body.source, body.recipe(),
            body.title, body.description)
    except ValueError as exc:
        raise HTTPException(422, str(exc))


@ops.get("/crawl/runs", responses={200: {"model": m.CrawlRunList}})
def crawl_runs(source: str | None = None,
               limit: int = Query(50, ge=1, le=200)) -> dict:
    return {"runs": service.crawl_runs(get_settings(), source, limit)}


@ops.get("/crawl/runs/{run_id}", responses={200: {"model": m.CrawlRunDetail}})
def crawl_run_get(run_id: int) -> dict:
    run = service.crawl_run_get(get_settings(), run_id)
    if run is None:
        raise HTTPException(404, f"no such crawl run: {run_id}")
    return run


@ops.post("/crawl/runs/{run_id}/cancel", responses={200: {"model": m.CrawlCancelled}}, dependencies=[Depends(require_write_token)])
def crawl_cancel(run_id: int) -> dict:
    out = service.crawl_cancel(get_settings(), run_id)
    if out is None:
        raise HTTPException(409, "run is not pending or running")
    return out


@ops.get("/crawl/runs/{run_id}/events",
         # No JSON body to describe: this is a stream. Declaring the
         # media type is the honest answer, and it stops the schema
         # advertising an empty object a client would try to decode.
         responses={200: {"content": {"text/event-stream": {}},
                           "description": "Server-sent events."}})
async def crawl_events(run_id: int,
                       ticks: int | None = Query(None, ge=1, le=10_000)
                       ) -> StreamingResponse:
    """SSE for one crawl: `run` (status + stats) each tick, `urls` for frontier
    rows that reached a terminal state since the last event. Same shape as
    /v1/events; `ticks` bounds the stream for tests.

    URLs are streamed by a monotonic `seq` cursor rather than re-sent wholesale,
    so a long crawl's stream stays O(new rows) instead of O(frontier) per tick.
    The stream ends on its own once the run is finished AND its backlog has been
    flushed — an EventSource that closes cleanly is what stops the browser
    reconnecting forever to a run that ended.
    """
    settings = get_settings()

    async def gen():
        cursor, n, finished_seen = 0, 0, False
        while True:
            run = await run_in_threadpool(service.crawl_run_get, settings, run_id)
            if run is None:
                yield f"event: error\ndata: {orjson.dumps({'error': 'unknown run'}).decode()}\n\n"
                return
            yield f"event: run\ndata: {orjson.dumps(run).decode()}\n\n"
            while True:
                rows = await run_in_threadpool(service.crawl_run_urls, settings,
                                               run_id, cursor, 200)
                if not rows:
                    break
                cursor = rows[-1]["seq"]
                yield f"event: urls\ndata: {orjson.dumps(rows).decode()}\n\n"
                if len(rows) < 200:
                    break
            n += 1
            if ticks is not None and n >= ticks:
                return
            # One extra pass after the terminal status so the last transitions
            # are delivered before the stream closes.
            if run["status"] in ("done", "failed", "cancelled"):
                if finished_seen:
                    yield "event: end\ndata: {}\n\n"
                    return
                finished_seen = True
            await asyncio.sleep(1)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --- serve the operational surface twice ------------------------------------
# Must run after every @ops decorator above: include_router COPIES the routes as
# they stand, so a registration added later would silently not be served.
#
# The /v1 copy is marked deprecated in the schema rather than removed. Deleting it
# now would break the running console, and the whole point of the phased plan is
# that the console keeps working until the native client covers every screen —
# the alias is what makes stopping partway a supported outcome rather than being
# stranded.
admin.include_router(ops, prefix="/v1")
app.include_router(ops, prefix="/v1", deprecated=True)
app.mount("/admin", admin)

# HTTP RED metrics (windex/api/prom.py). Registered last, after every route is
# defined, so the middleware's route-template resolver sees the full routing
# table (the live routes list, not a copy).
#
# One instance per app. The parent skips /admin so an admin request is counted
# once, by the sub-app, against its real route template — without the skip the
# parent would also record it as the bare Mount path "/admin" and every admin
# endpoint would collapse into one series (and be double counted).
admin.add_middleware(prom.PrometheusMiddleware, routes=admin.router.routes,
                     label_prefix="/admin")
app.add_middleware(prom.PrometheusMiddleware, routes=app.router.routes,
                   skip_prefixes=("/admin",))
