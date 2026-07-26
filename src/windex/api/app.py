"""Windex contract-epoch 2 HTTP applications."""

from __future__ import annotations

import hmac
import time
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field

from windex.api import prom, service
from windex.api.canonical import data_router, router as canonical_router
from windex.api.module_admin import router as module_admin_router
from windex.config import get_settings
from windex.index.search import SearchBackendUnavailable
from windex.pipeline.contracts import CONTRACT_EPOCH

STARTED_AT = time.time()


def _operation_id(route) -> str:
    return route.name


app = FastAPI(
    title="windex",
    version="0.2.0",
    description="Self-hosted searchable Pipeline runtime",
    generate_unique_id_function=_operation_id,
)


def _bearer_ok(authorization: str | None, token: str) -> bool:
    if not authorization:
        return False
    scheme, _, value = authorization.partition(" ")
    return (
        scheme.lower() == "bearer"
        and hmac.compare_digest(value.strip().encode(), token.encode())
    )


def require_write_token(authorization: str | None = Header(None)) -> None:
    token = get_settings().write_token
    if token and not _bearer_ok(authorization, token):
        raise HTTPException(401, "missing or invalid write token")


def require_admin(
    request: Request, authorization: str | None = Header(None),
) -> None:
    path = request.scope.get("path", "")
    root = request.scope.get("root_path", "")
    if root and path.startswith(root):
        path = path[len(root):] or "/"
    if path == "/v1/health" or path.startswith("/v1/modules"):
        return
    settings = get_settings()
    if not settings.write_token:
        if settings.serve_host not in ("127.0.0.1", "::1", "localhost"):
            raise HTTPException(
                503, "admin API disabled: configure WINDEX_WRITE_TOKEN")
        return
    if not _bearer_ok(authorization, settings.write_token):
        raise HTTPException(401, "missing or invalid admin token")


def require_module_admin(
    request: Request, authorization: str | None = Header(None),
) -> None:
    token = get_settings().module_admin_token
    if not token:
        raise HTTPException(503, "module administration is not configured")
    if request.url.scheme != "https":
        raise HTTPException(
            426, "Module source upload requires HTTPS; loopback recovery uses "
                 "the module-approve CLI")
    if not _bearer_ok(authorization, token):
        raise HTTPException(403, "missing or invalid module_admin credential")


admin = FastAPI(
    title="windex admin",
    version="2.0.0",
    description="Contract-epoch 2 Pipeline and Source control plane",
    dependencies=[Depends(require_admin)],
    generate_unique_id_function=_operation_id,
)


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Capabilities(_Strict):
    pipelines: bool
    sources: bool
    generic_runs: bool
    source_ingest: bool
    operational_events: bool
    module_admin: bool
    secure_module_upload: bool
    module_runtimes: list[str]


class Health(_Strict):
    status: str
    service: str
    version: str
    contract_epoch: int
    supported_contract_epochs: list[int]
    schema_generation: int
    capabilities: Capabilities
    auth_required: bool
    started_at: float
    uptime_s: float


MessageRange = Annotated[
    list[Annotated[int, Field(ge=0)]],
    Field(min_length=2, max_length=2),
]


class SearchResult(_Strict):
    id: str | None
    score: float
    url: str | None = None
    title: str | None = None
    snippet: str | None = None
    source: str | None = None
    published_at: Any = None
    outlet: str | None = None
    stars: int | None = None
    language: str | None = None
    topics: list[str] | None = None
    pushed_at: Any = None
    lang: str | None = None
    incoming_links: int | None = None
    primary_category: str | None = None
    categories: list[str] | None = None
    authors: list[str] | None = None
    framework: str | None = None
    version: str | None = None
    attribution: Any = None
    points: int | None = None
    num_comments: int | None = None
    author: str | None = None
    target_url: str | None = None
    root: str | None = None
    kind: str | None = None
    conversation_id: str | None = None
    chunk_index: int | None = None
    message_range: MessageRange | None = None
    extra: dict[str, Any] | None = None


class SearchResponse(_Strict):
    query: str
    results: list[SearchResult]
    mode: str
    timings: dict[str, Any]
    took_ms: int


class DocumentResponse(_Strict):
    id: str
    source: str
    url: str
    title: str | None
    published_at: str | None
    lang: str | None
    status: str
    duplicate_of: str | None
    text: str | None
    message_range: MessageRange | None = None


@admin.get("/v1/health", response_model=Health)
def admin_health() -> dict[str, Any]:
    settings = get_settings()
    module_admin = bool(settings.module_admin_token)
    return {
        "status": "ok",
        "service": "windex",
        "version": app.version,
        "contract_epoch": CONTRACT_EPOCH,
        "supported_contract_epochs": [CONTRACT_EPOCH],
        "schema_generation": 2,
        "capabilities": {
            "pipelines": True,
            "sources": True,
            "generic_runs": True,
            "source_ingest": True,
            "operational_events": True,
            "module_admin": module_admin,
            "secure_module_upload": module_admin,
            "module_runtimes": ["python"],
        },
        "auth_required": bool(settings.write_token),
        "started_at": STARTED_AT,
        "uptime_s": round(time.time() - STARTED_AT, 1),
    }


@admin.get("/v1/whoami")
def admin_whoami() -> dict[str, Any]:
    return {
        "ok": True,
        "scopes": ["admin"],
        "auth_required": bool(get_settings().write_token),
        "contract_epoch": CONTRACT_EPOCH,
    }


@app.get(
    "/v1/search",
    response_model=SearchResponse,
    responses={
        503: {
            "description": (
                "The selected Source index, or every eligible source=all "
                "index, is temporarily unavailable.")
        },
    },
)
def search(
    q: str = Query(min_length=1),
    source: str = Query("all"),
    limit: int = Query(10, ge=1, le=50),
    mode: Literal["hybrid", "dense", "lexical"] = "hybrid",
    published_after: datetime | None = None,
    published_before: datetime | None = None,
    min_stars: int | None = None,
    min_points: int | None = Query(None, ge=0),
    language: str | None = None,
    category: str | None = Query(None, max_length=64),
    outlet: str | None = Query(None, max_length=253),
    framework: str | None = Query(None, max_length=64),
    root: str | None = Query(None, max_length=64),
    kind: str | None = Query(None, max_length=16),
    conversation_id: str | None = Query(None, max_length=64),
) -> dict[str, Any]:
    settings = get_settings()
    try:
        service.validate_source(settings, source)
    except ValueError:
        raise HTTPException(422, f"unknown source: {source}")
    try:
        return service.run_search(
            settings, q, source=source, limit=limit, mode=mode,
            published_after=published_after, published_before=published_before,
            min_stars=min_stars, language=language, category=category, outlet=outlet,
            framework=framework, min_points=min_points, root=root, kind=kind,
            conversation_id=conversation_id,
        )
    except SearchBackendUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc


@app.get("/v1/docs/{doc_id:path}", response_model=DocumentResponse)
def get_doc(doc_id: str) -> dict[str, Any]:
    result = service.get_document(get_settings(), doc_id)
    if result is None:
        raise HTTPException(404, f"unknown document id: {doc_id}")
    return result


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics() -> Response:
    """Prometheus scrape surface, deliberately outside both API contracts."""
    return Response(
        prom.render(get_settings()),
        media_type=prom.CONTENT_TYPE_LATEST,
    )


admin.include_router(canonical_router)
admin.include_router(
    module_admin_router, dependencies=[Depends(require_module_admin)])
app.include_router(data_router, dependencies=[Depends(require_write_token)])
app.mount("/admin", admin)

# The mounted admin app records its own full route templates.  The parent skips
# the mount so an admin request is neither double-counted nor collapsed to the
# bare "/admin" mount label.
admin.add_middleware(
    prom.PrometheusMiddleware,
    routes=admin.router.routes,
    label_prefix="/admin",
)
app.add_middleware(
    prom.PrometheusMiddleware,
    routes=app.router.routes,
    skip_prefixes=("/admin",),
)
