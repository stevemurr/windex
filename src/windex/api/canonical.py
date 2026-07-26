"""Contract-epoch 2 Pipeline/Source control-plane routes."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import orjson
from fastapi import (
    APIRouter,
    Body,
    Header,
    HTTPException,
    Query,
    Response,
)
from fastapi.responses import StreamingResponse
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from windex import db
from windex.config import get_settings
from windex.pipeline import registry
from windex.pipeline.contracts import ContractError
from windex.pipeline.events import facets, list_events
from windex.pipeline.overview import snapshot
from windex.pipeline import run_store
from windex.pipeline import store as pipeline_store
from windex.pipeline.spec import validate as validate_pipeline
from windex.source import store as source_store

router = APIRouter(prefix="/v1")
data_router = APIRouter(prefix="/v1")


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ValidationIssueModel(Strict):
    path: str
    code: str
    severity: Literal["error", "warning"]
    message: str


class ValidationReport(Strict):
    valid: bool
    issues: list[ValidationIssueModel]
    normalized: dict[str, Any] | None = None
    graph: dict[str, Any] | None = None
    contract: str | None = None


class BoundaryModel(Strict):
    id: str
    type: str
    required: bool = True
    max_items: int | None = None
    max_bytes: int | None = None


class KindDescriptor(Strict):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    id: str
    inp: str | None = Field(None, alias="in")
    out: str | None = None
    title: str
    help: str
    stateful: bool


class PortDescriptor(Strict):
    id: str
    title: str
    fields: list[str]


class PortTypeDescriptor(Strict):
    title: str
    fields: list[str]


class ParamDescriptor(Strict):
    key: str
    kind: str
    lo: float | None
    hi: float | None
    choices: list[str]
    label: str
    help: str
    type: str
    editor: str
    title: str
    description: str
    required: bool = False
    advanced: bool
    secret: bool
    stage: str = "runtime"
    enforce: str
    default: Any = None
    prefill: Any = None
    section: str | None = None
    unit: str | None = None
    enum_titles: list[str] | None = Field(None, alias="enumTitles")
    max_items: int | None = Field(None, alias="maxItems")
    max_length: int | None = Field(None, alias="maxLength")
    pattern: str | None = None
    clamp: str | None = None
    clamp_note: str | None = Field(None, alias="clampNote")
    locked_reason: str | None = Field(None, alias="lockedReason")
    depends_on: dict[str, Any] | None = Field(None, alias="dependsOn")
    allow: list[str] | None = None


class ModuleDescriptor(Strict):
    id: str
    kind: str
    version: str
    title: str
    summary: str
    stability: str
    capabilities: list[str]
    allowed_hosts: list[str]
    batched: bool
    thread_safe: bool
    lane: str
    preconditions: list[str]
    contract_roles: list[str]
    config: dict[str, list[ParamDescriptor]]
    fields: list[ParamDescriptor]
    implemented: bool
    implementation_digest: str


class RegistryResponse(Strict):
    contract: str
    registry_contract: str
    registry_version: int
    registry_digest: str
    ports: list[PortDescriptor]
    port_types: dict[str, PortTypeDescriptor]
    kinds: list[KindDescriptor]
    modules: list[ModuleDescriptor]
    always_before_load: list[str]


class PipelineCreate(Strict):
    name: str
    title: str = ""
    description: str = ""
    spec: dict[str, Any]
    author: str = ""
    note: str = ""


class PipelineRevisionCreate(Strict):
    spec: dict[str, Any]
    parent_version: int | None = None
    parent_hash: str | None = None
    author: str = ""
    note: str = ""


class LayoutWrite(Strict):
    flow_name: str
    layout: dict[str, Any]


class SourceCreate(Strict):
    name: str
    title: str = ""
    description: str = ""
    origin: dict[str, Any] = Field(default_factory=dict)
    pipeline_name: str
    pipeline_version: int | None = None
    search_name: str
    id_prefix: str
    collection_key: str
    search_profile: str
    include_in_all: bool = True
    state_namespace: str
    enabled: bool = True
    values: dict[str, Any] = Field(default_factory=dict)


class SourcePatch(Strict):
    title: str | None = None
    description: str | None = None
    origin: dict[str, Any] | None = None
    enabled: bool | None = None
    include_in_all: bool | None = None


class SettingsPatch(Strict):
    values: dict[str, Any]


class TriggerWrite(Strict):
    flow_name: str
    trigger_type: Literal["cron", "interval", "event", "manual"]
    trigger_spec: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    next_fire_at: str | None = None


class TriggerPatch(Strict):
    flow_name: str | None = None
    trigger_type: Literal["cron", "interval", "event", "manual"] | None = None
    trigger_spec: dict[str, Any] | None = None
    enabled: bool | None = None
    next_fire_at: str | None = None


class SourceUpgradePreviewRequest(Strict):
    target_version: int
    values: dict[str, Any] | None = None


class SourceUpgradeRequest(Strict):
    target_version: int
    values: dict[str, Any]
    confirmation_token: str


class PipelineRunCreate(Strict):
    version: int | None = None
    flow: str | None = None
    inputs: dict[str, Any] = Field(default_factory=dict)
    parameters: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(50, ge=0, le=100)
    dry_run: bool = False


class SourceRunCreate(Strict):
    flow: str | None = None
    overrides: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(50, ge=0, le=100)


class IngestDocument(Strict):
    id: str
    url: str
    text: str = Field(max_length=1_000_000)
    title: str = ""
    canonical_url: str | None = None
    published_at: str | None = None
    lang: str | None = None
    fields: dict[str, Any] = Field(default_factory=dict)
    deleted: bool = False


class IngestRequest(Strict):
    schema_version: Literal["windex.ingest/1"] = "windex.ingest/1"
    mode: Literal["delta", "full"] = "delta"
    # Names the replace scope for partition-replacing Sources. Ordinarily it is
    # derivable from the documents themselves, but an empty full push — the
    # delete path — has no document to read it from.
    partition: str | None = Field(default=None, max_length=256)
    documents: list[IngestDocument] = Field(max_length=10_000)


class PipelineModel(Strict):
    id: int
    name: str
    title: str
    description: str
    builtin: bool
    archived_at: str | None
    created_at: str
    updated_at: str
    head_revision_id: int | None
    version: int | None
    spec_hash: str | None
    spec: dict[str, Any] | None = None


class PipelinesResponse(Strict):
    pipelines: list[PipelineModel]


class PipelineRevisionModel(Strict):
    id: int
    pipeline_id: int
    pipeline_name: str
    version: int
    parent_revision_id: int | None
    spec: dict[str, Any]
    spec_hash: str
    registry_version: int
    registry_digest: str
    module_locks: dict[str, dict[str, Any]]
    author: str
    note: str
    created_at: str
    capability: dict[str, Any]


class PipelineRevisionsResponse(Strict):
    revisions: list[PipelineRevisionModel]


class TaskDescriptorModel(Strict):
    node: str
    kind: str
    module: str
    module_version: str
    module_digest: str
    executor: str
    lane: str
    config: dict[str, Any]
    depends_on: list[str]
    preconditions: list[str]
    captures: list[str]
    weight: float
    max_attempts: int
    lease_seconds: int


class TaskPreviewResponse(Strict):
    pipeline: str
    version: int
    spec_hash: str
    flow: str
    tasks: list[TaskDescriptorModel]
    modules_available: bool
    unavailable_modules: list[str]
    capability: dict[str, Any]


class LayoutResponse(Strict):
    flow: str
    layout: dict[str, Any]
    etag: str
    updated_at: str


class SourceModel(Strict):
    id: int
    name: str
    title: str
    description: str
    origin: dict[str, Any]
    pipeline_revision_id: int
    pipeline_name: str
    pipeline_version: int
    pipeline_hash: str
    search_contract_version: str
    search_name: str
    id_prefix: str
    collection_key: str
    search_profile: str
    include_in_all: bool
    state_namespace: str
    enabled: bool
    generation: int
    archived_at: str | None
    created_at: str
    updated_at: str
    values: dict[str, Any]
    values_hash: str
    paused: bool
    pause_reason: str
    paused_at: str | None
    etag: str
    ready: bool
    ingress: dict[str, Any] | None = None


class SourcesResponse(Strict):
    sources: list[SourceModel]


class DeploymentReport(ValidationReport):
    pipeline_hash: str | None = None


class SettingsProjection(Strict):
    source: str
    pipeline: str
    pipeline_version: int
    etag: str
    values: dict[str, Any]
    fields: list[dict[str, Any]]


class TriggerModel(Strict):
    id: int
    flow_name: str
    trigger_type: str
    trigger_spec: dict[str, Any]
    enabled: bool
    next_fire_at: str | None
    last_fired_at: str | None
    last_run_id: int | None


class TriggersResponse(Strict):
    triggers: list[TriggerModel]


class RunTaskModel(Strict):
    id: int
    node: str
    kind: str
    module: str
    module_version: str
    module_digest: str
    executor: str
    lane: str
    config: dict[str, Any]
    depends_on: list[str]
    preconditions: list[str]
    captures: list[str]
    state: str
    priority: int
    attempts: int
    max_attempts: int
    lease_worker: str | None
    lease_seconds: int
    lease_expires_at: str | None
    cursor: dict[str, Any]
    units_total: int
    units_done: int
    units_failed: int
    weight: float
    stats: dict[str, Any]
    started_at: str | None
    finished_at: str | None
    error: str | None


class RunModel(Strict):
    id: int
    source_id: int | None
    source_name: str | None
    pipeline_name: str
    pipeline_revision_id: int
    pipeline_version: int
    pipeline_hash: str
    flow_name: str
    source_snapshot: dict[str, Any] | None
    effective_config: dict[str, Any]
    explicit_inputs: dict[str, Any]
    module_locks: dict[str, dict[str, Any]]
    trigger_type: str
    trigger_by: str
    mode: str
    priority: int
    dedupe_key: str | None
    idempotency_key: str | None
    state: str
    cancel_requested: bool
    queued_at: str
    started_at: str | None
    finished_at: str | None
    updated_at: str
    progress: dict[str, Any]
    stats: dict[str, Any]
    error: str | None
    frozen_spec: dict[str, Any] | None = None
    tasks: list[RunTaskModel] = Field(default_factory=list)


class RunsResponse(Strict):
    runs: list[RunModel]


class QueuedRunResponse(Strict):
    run_id: int | None
    queued: bool
    coalesced: bool | None = None
    rerun_of: int | None = None


class RunOutputModel(Strict):
    boundary: str
    type: str
    value: Any
    size_bytes: int
    checksum: str
    created_at: str


class RunOutputsResponse(Strict):
    outputs: list[RunOutputModel]


class OperationalEventModel(Strict):
    seq: int
    ts: str
    level: str
    component: str
    source_name: str | None
    pipeline_name: str | None
    pipeline_version: int | None
    run_id: int | None
    task_id: int | None
    node: str | None
    module: str | None
    event: str
    message: str
    data: dict[str, Any]


class EventsResponse(Strict):
    events: list[OperationalEventModel]
    next_cursor: int


class SourceStatusResponse(Strict):
    source: str
    enabled: bool
    paused: bool
    latest_run: dict[str, Any] | None
    current_run: dict[str, Any] | None
    documents: dict[str, dict[str, Any]]
    last_success: str | None
    last_failure: str | None
    recent_error: str | None


class ResetPreviewResponse(Strict):
    generation: int
    documents: int
    state_units: int
    outstanding_tasks: int
    confirmation_token: str


class ResetQueuedResponse(Strict):
    source: str
    run_id: int
    generation: int
    planned_generation: int
    state: str


class UpgradePreviewResponse(Strict):
    source_id: int
    from_version: int
    target_version: int
    target_hash: str
    expected_etag: str
    candidate_hash: str
    candidate: dict[str, Any]
    retained: dict[str, Any]
    defaulted: dict[str, Any]
    removed: list[str]
    clamped: dict[str, Any]
    missing: list[str]
    install_stage_changed: list[str]
    state_impact: dict[str, Any]
    issues: list[ValidationIssueModel]
    confirmation_token: str | None
    valid: bool


class OverviewResponse(Strict):
    revision: int
    as_of: str
    health: dict[str, Any]
    runs: dict[str, Any]
    workers: dict[str, Any]
    sources: list[dict[str, Any]]
    schedules: list[dict[str, Any]]
    recent_documents: list[dict[str, Any]]
    totals: dict[str, Any]


class OperatorSettingsResponse(Strict):
    scope: str
    values: dict[str, Any]
    etag: str
    updated_at: str | None = None


class SecretReferenceModel(Strict):
    name: str
    provider: str
    configured: bool
    metadata: dict[str, Any]
    updated_at: str


class SecretsResponse(Strict):
    secrets: list[SecretReferenceModel]


class FacetsResponse(Strict):
    levels: list[str]
    components: list[str]
    sources: list[str]
    pipelines: list[str]
    nodes: list[str]
    modules: list[str]


class ActionResponse(Strict):
    ok: bool
    pipeline: str | None = None
    source: str | None = None
    archived: bool | None = None
    run_id: int | None = None


def _detail(exc: Exception) -> Any:
    if isinstance(exc, ContractError):
        return {
            "message": str(exc),
            "issues": [item.to_dict() for item in exc.issues],
        }
    if exc.args and isinstance(exc.args[0], dict):
        return exc.args[0]
    return str(exc)


def _raise(exc: Exception) -> None:
    if isinstance(exc, KeyError):
        raise HTTPException(404, "resource not found")
    if isinstance(exc, (
        pipeline_store.StalePipelineError, source_store.StaleSourceError,
    )):
        raise HTTPException(412, _detail(exc))
    if isinstance(exc, (
        source_store.SourceConflictError, run_store.RunConflictError,
    )):
        raise HTTPException(409, _detail(exc))
    raise HTTPException(422, _detail(exc))


def _push_contract(source: dict[str, Any]) -> dict[str, Any] | None:
    for flow_name, flow in (source.get("spec", {}).get("flows") or {}).items():
        for node in (flow.get("nodes") or {}).values():
            if node.get("uses") != "push.docs":
                continue
            config = node.get("with") or {}
            mode = str(config.get("mode") or "delta")
            maximum = config.get("max_docs", 10_000)
            if isinstance(maximum, str) and maximum.startswith("@param."):
                maximum = source.get("values", {}).get(
                    maximum.removeprefix("@param."), 10_000)
            return {
                "flow": flow_name,
                "mode": "full" if mode == "full_set" else "delta",
                "max_documents": min(int(maximum), 10_000),
            }
    return None


@router.get("/registry", response_model=RegistryResponse)
def canonical_registry(response: Response) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        registry.load_custom(conn)
    document = registry.describe()
    response.headers["ETag"] = f'"{document["registry_digest"]}"'
    return document


@router.post("/pipelines/validate", response_model=ValidationReport)
def pipeline_validate(body: dict[str, Any]) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        registry.load_custom(conn)
    return validate_pipeline(body, get_settings())


@router.get("/pipelines", response_model=PipelinesResponse)
def pipelines(include_archived: bool = False) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return {"pipelines": pipeline_store.list_pipelines(
            conn, include_archived=include_archived)}


@router.post("/pipelines", status_code=201, response_model=PipelineModel)
def pipeline_create(body: PipelineCreate) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            registry.load_custom(conn)
            return pipeline_store.create_pipeline(
                conn, **body.model_dump())
    except Exception as exc:
        _raise(exc)


@router.get("/pipelines/{name}", response_model=PipelineModel)
def pipeline_get(name: str) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        result = pipeline_store.get_pipeline(conn, name)
    if result is None:
        raise HTTPException(404, "Pipeline not found")
    return result


@router.get(
    "/pipelines/{name}/revisions", response_model=PipelineRevisionsResponse)
def pipeline_revisions(name: str) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        result = pipeline_store.list_revisions(conn, name)
        exists = pipeline_store.get_pipeline(conn, name) is not None
    if not exists:
        raise HTTPException(404, "Pipeline not found")
    if not result:
        return {"revisions": []}
    return {"revisions": result}


@router.get(
    "/pipelines/{name}/revisions/{version}",
    response_model=PipelineRevisionModel,
)
def pipeline_revision(name: str, version: int) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        result = pipeline_store.get_revision(conn, name, version)
    if result is None:
        raise HTTPException(404, "Pipeline revision not found")
    return result


@router.post(
    "/pipelines/{name}/revisions",
    status_code=201,
    response_model=PipelineRevisionModel,
)
def pipeline_revision_publish(
    name: str, body: PipelineRevisionCreate, if_match: str | None = Header(None),
) -> dict[str, Any]:
    expected_hash = body.parent_hash or (
        if_match.strip('"') if if_match else None)
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            registry.load_custom(conn)
            return pipeline_store.publish_revision(
                conn, name, body.spec, expected_version=body.parent_version,
                expected_hash=expected_hash, author=body.author, note=body.note)
    except Exception as exc:
        _raise(exc)


@router.get(
    "/pipelines/{name}/revisions/{version}/tasks",
    response_model=TaskPreviewResponse,
)
def pipeline_tasks(
    name: str, version: int, flow: str | None = None,
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return pipeline_store.task_preview(conn, name, version, flow=flow)
    except Exception as exc:
        _raise(exc)


@router.post("/pipelines/{name}/archive", response_model=ActionResponse)
def pipeline_archive(name: str) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        changed = pipeline_store.archive(conn, name)
    if not changed:
        raise HTTPException(404, "active Pipeline not found")
    return {"ok": True, "pipeline": name, "archived": True}


@router.get(
    "/pipelines/{name}/revisions/{version}/layout",
    response_model=LayoutResponse,
)
def pipeline_layout(
    name: str, version: int, flow: str = Query(...),
) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        result = pipeline_store.get_layout(conn, name, version, flow)
    if result is None:
        raise HTTPException(404, "layout not found")
    return result


@router.put(
    "/pipelines/{name}/revisions/{version}/layout",
    response_model=LayoutResponse,
)
def pipeline_layout_put(
    name: str, version: int, body: LayoutWrite,
    if_match: str = Header(...),
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return pipeline_store.put_layout(
                conn, name, version, body.flow_name, body.layout,
                if_match=if_match.strip('"'))
    except Exception as exc:
        _raise(exc)


@router.post("/sources/validate", response_model=DeploymentReport)
def source_validate(body: SourceCreate) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return source_store.validate_candidate(
            conn, body.model_dump(), settings=get_settings())


@router.get("/sources", response_model=SourcesResponse)
def sources(include_archived: bool = False) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return {"sources": source_store.list_sources(
            conn, include_archived=include_archived)}


@router.post("/sources", status_code=201, response_model=SourceModel)
def source_create(body: SourceCreate) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.create_source(
                conn, body.model_dump(), settings=get_settings())
    except Exception as exc:
        _raise(exc)


@router.get("/sources/{name}", response_model=SourceModel)
def source_get(name: str) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        result = source_store.get_source(conn, name, include_spec=True)
    if result is None:
        raise HTTPException(404, "Source not found")
    if result["origin"].get("ingress") == "push":
        contract = _push_contract(result)
        result["ingress"] = {
            "url": f"/v1/sources/{name}/ingest",
            "authentication_required": bool(get_settings().write_token),
            "max_documents": contract["max_documents"] if contract else 10_000,
            "max_text_bytes": 1_000_000,
            "modes": [contract["mode"]] if contract else [],
        }
    result.pop("spec", None)
    return result


@router.patch("/sources/{name}", response_model=SourceModel)
def source_patch(name: str, body: SourcePatch) -> dict[str, Any]:
    try:
        changes = body.model_dump(exclude_none=True)
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.patch_source(conn, name, changes)
    except Exception as exc:
        _raise(exc)


@router.post(
    "/sources/{name}/validate", response_model=DeploymentReport)
def source_validate_existing(name: str) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        source = source_store.get_source(conn, name)
        if source is None:
            raise HTTPException(404, "Source not found")
        return source_store.validate_candidate(
            conn, {
                **source,
                "pipeline_name": source["pipeline_name"],
                "pipeline_version": source["pipeline_version"],
            }, settings=get_settings(), exclude=name)


@router.post(
    "/sources/{name}/upgrade/preview",
    response_model=UpgradePreviewResponse,
)
def source_upgrade_preview(
    name: str, body: SourceUpgradePreviewRequest,
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.upgrade_preview(
                conn,
                name,
                body.target_version,
                values=body.values,
                settings=get_settings(),
            )
    except Exception as exc:
        _raise(exc)


@router.post("/sources/{name}/upgrade", response_model=SourceModel)
def source_upgrade(
    name: str,
    body: SourceUpgradeRequest,
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.upgrade(
                conn,
                name,
                body.target_version,
                body.values,
                body.confirmation_token,
                settings=get_settings(),
            )
    except Exception as exc:
        _raise(exc)


@router.post("/sources/{name}/archive", response_model=ActionResponse)
def source_archive(name: str) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        changed = source_store.archive(conn, name)
    if not changed:
        raise HTTPException(404, "active Source not found")
    return {"ok": True, "source": name, "archived": True}


@router.get(
    "/sources/{name}/settings", response_model=SettingsProjection)
def source_settings(name: str) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.settings_projection(
                conn, name, settings=get_settings())
    except Exception as exc:
        _raise(exc)


@router.patch(
    "/sources/{name}/settings", response_model=SettingsProjection)
def source_settings_patch(
    name: str, body: SettingsPatch, if_match: str = Header(...),
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.patch_settings(
                conn, name, body.values, if_match=if_match.strip('"'),
                settings=get_settings())
    except Exception as exc:
        _raise(exc)


@router.delete(
    "/sources/{name}/settings/{key}", response_model=SettingsProjection)
def source_setting_delete(
    name: str, key: str, if_match: str = Header(...),
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.delete_setting(
                conn, name, key, if_match=if_match.strip('"'),
                settings=get_settings())
    except Exception as exc:
        _raise(exc)


@router.get(
    "/sources/{name}/triggers", response_model=TriggersResponse)
def source_triggers(name: str) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return {"triggers": source_store.list_triggers(conn, name)}


@router.post(
    "/sources/{name}/triggers",
    status_code=201,
    response_model=TriggerModel,
)
def source_trigger_create(name: str, body: TriggerWrite) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.create_trigger(conn, name, body.model_dump())
    except Exception as exc:
        _raise(exc)


@router.patch(
    "/sources/{name}/triggers/{trigger_id}", response_model=TriggerModel)
def source_trigger_patch(
    name: str, trigger_id: int, body: TriggerPatch,
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.update_trigger(
                conn, name, trigger_id, body.model_dump(exclude_none=True))
    except Exception as exc:
        _raise(exc)


@router.delete(
    "/sources/{name}/triggers/{trigger_id}", response_model=ActionResponse)
def source_trigger_delete(name: str, trigger_id: int) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        deleted = source_store.delete_trigger(conn, name, trigger_id)
    if not deleted:
        raise HTTPException(404, "trigger not found")
    return {"ok": True}


@router.post("/sources/{name}/pause", response_model=SourceModel)
def source_pause(name: str, reason: str = Body("", embed=True)) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.set_paused(conn, name, True, reason)
    except Exception as exc:
        _raise(exc)


@router.post("/sources/{name}/resume", response_model=SourceModel)
def source_resume(name: str) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.set_paused(conn, name, False)
    except Exception as exc:
        _raise(exc)


@router.post(
    "/sources/{name}/reset/preview", response_model=ResetPreviewResponse)
def source_reset_preview(name: str) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.reset_preview(conn, name)
    except Exception as exc:
        _raise(exc)


@router.post(
    "/sources/{name}/reset",
    status_code=202,
    response_model=ResetQueuedResponse,
)
def source_reset(
    name: str, confirmation_token: str = Body(..., embed=True),
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.reset(conn, name, confirmation_token)
    except Exception as exc:
        _raise(exc)


@router.get(
    "/sources/{name}/status", response_model=SourceStatusResponse)
def source_status(name: str) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.status(conn, name)
    except Exception as exc:
        _raise(exc)


@router.get("/sources/{name}/runs", response_model=RunsResponse)
def source_runs(
    name: str, before_id: int | None = None, limit: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return {"runs": run_store.list_runs(
            conn, source=name, before_id=before_id, limit=limit)}


@router.post(
    "/sources/{name}/runs",
    status_code=202,
    response_model=QueuedRunResponse,
)
def source_run_create(name: str, body: SourceRunCreate) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            run_id = run_store.submit_source(
                conn, name, flow=body.flow, overrides=body.overrides,
                inputs=body.inputs, priority=body.priority, settings=get_settings())
        return {"run_id": run_id, "queued": run_id is not None,
                "coalesced": run_id is None}
    except Exception as exc:
        _raise(exc)


@router.post(
    "/pipelines/{name}/runs",
    status_code=202,
    response_model=QueuedRunResponse,
)
def pipeline_run_create(
    name: str,
    body: PipelineRunCreate,
    if_match: str | None = Header(None),
) -> dict[str, Any]:
    if body.version is None and if_match is None:
        raise HTTPException(
            428,
            "generic Pipeline Runs require an explicit revision or If-Match head",
        )
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            run_id = run_store.submit_pipeline(
                conn, name, version=body.version, flow=body.flow,
                inputs=body.inputs, parameters=body.parameters,
                expected_head=if_match.strip('"') if if_match else None,
                priority=body.priority, dry_run=body.dry_run,
                settings=get_settings())
        return {"run_id": run_id, "queued": True}
    except Exception as exc:
        _raise(exc)


@router.get("/runs", response_model=RunsResponse)
def runs(
    source: str | None = None, pipeline: str | None = None,
    state: str | None = None, before_id: int | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return {"runs": run_store.list_runs(
            conn, source=source, pipeline=pipeline, state=state,
            before_id=before_id, limit=limit)}


@router.get("/runs/{run_id}", response_model=RunModel)
def run_get(run_id: int, include_spec: bool = False) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        result = run_store.get_run(conn, run_id, include_spec=include_spec)
    if result is None:
        raise HTTPException(404, "Run not found")
    return result


@router.post("/runs/{run_id}/cancel", response_model=ActionResponse)
def run_cancel(run_id: int) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        changed = run_store.cancel(conn, run_id, by="admin API")
    if not changed:
        raise HTTPException(409, "Run is not active")
    return {"ok": True, "run_id": run_id}


@router.get("/runs/{run_id}/events", response_model=EventsResponse)
def run_events(
    run_id: int, after: int = 0, limit: int = Query(200, ge=1, le=1000),
) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        if run_store.get_run(conn, run_id) is None:
            raise HTTPException(404, "Run not found")
        events = list_events(conn, after=after, limit=limit, run_id=run_id)
    return {"events": events, "next_cursor": events[-1]["seq"] if events else after}


@router.get("/runs/{run_id}/outputs", response_model=RunOutputsResponse)
def run_outputs(run_id: int) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return {"outputs": run_store.outputs(conn, run_id)}


@router.get("/runs/{run_id}/artifacts/{artifact_id}")
def run_artifact(run_id: int, artifact_id: str) -> FileResponse:
    settings = get_settings()
    with db.pooled(settings.pg_dsn) as conn:
        metadata = run_store.artifact(conn, run_id, artifact_id)
    if metadata is None:
        raise HTTPException(404, "artifact not found or expired")
    root = (
        settings.data_root / "generations" / "current" / "artifacts"
    ).resolve()
    path = (root / metadata["relative_path"]).resolve()
    if root not in path.parents or not path.is_file():
        raise HTTPException(404, "artifact payload is unavailable")
    if path.stat().st_size != metadata["size_bytes"]:
        raise HTTPException(409, "artifact size does not match its manifest")
    return FileResponse(
        path, media_type=metadata["media_type"], filename=Path(path).name)


@router.post(
    "/runs/{run_id}/rerun",
    status_code=202,
    response_model=QueuedRunResponse,
)
def run_rerun(run_id: int) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            new_id = run_store.rerun(conn, run_id)
        return {"run_id": new_id, "rerun_of": run_id, "queued": True}
    except Exception as exc:
        _raise(exc)


@router.get("/settings", response_model=OperatorSettingsResponse)
def operator_settings() -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return source_store.get_operator_settings(conn)


@router.patch("/settings", response_model=OperatorSettingsResponse)
def operator_settings_patch(
    body: SettingsPatch, if_match: str = Header(...),
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.patch_operator_settings(
                conn, body.values, if_match=if_match.strip('"'))
    except Exception as exc:
        _raise(exc)


@router.delete(
    "/settings/{key}", response_model=OperatorSettingsResponse)
def operator_setting_delete(
    key: str, if_match: str = Header(...),
) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return source_store.delete_operator_setting(
                conn, key, if_match=if_match.strip('"'))
    except Exception as exc:
        _raise(exc)


@router.get("/secrets", response_model=SecretsResponse)
def secrets() -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return {"secrets": source_store.list_secrets(conn)}


@router.get("/overview", response_model=OverviewResponse)
def overview() -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return snapshot(conn, get_settings())


@router.get("/log-events", response_model=EventsResponse)
def log_events(
    after: int = 0, before: int | None = None,
    started_at: datetime | None = None, ended_at: datetime | None = None,
    limit: int = Query(200, ge=1, le=1000), level: str | None = None,
    component: str | None = None, source: str | None = None,
    pipeline: str | None = None, run_id: int | None = None,
    node: str | None = None, module: str | None = None, text: str | None = None,
) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        events = list_events(
            conn, after=after, before=before, started_at=started_at,
            ended_at=ended_at, limit=limit, level=level,
            component=component, source=source, pipeline=pipeline, run_id=run_id,
            node=node, module=module, text=text)
    return {"events": events, "next_cursor": events[-1]["seq"] if events else after}


@router.get("/log-events/facets", response_model=FacetsResponse)
def log_event_facets() -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return facets(conn)


def _event_stream(
    after: int,
    ticks: int | None,
    logs: bool,
    filters: Mapping[str, Any] | None = None,
) -> StreamingResponse:
    settings = get_settings()

    async def generate():
        cursor, count = after, 0
        while True:
            def read():
                with db.pooled(settings.pg_dsn) as conn:
                    return list_events(
                        conn, after=cursor, limit=500, **dict(filters or {}))

            events = await run_in_threadpool(read)
            if events:
                cursor = events[-1]["seq"]
                for event in events:
                    yield (
                        f"id: {event['seq']}\n"
                        f"event: {'log' if logs else event['event']}\n"
                        f"data: {orjson.dumps(event).decode()}\n\n")
            else:
                yield ": keepalive\n\n"
            count += 1
            if ticks is not None and count >= ticks:
                return
            await asyncio.sleep(1)

    return StreamingResponse(
        generate(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/events/stream")
def events_stream(
    after: int = 0, ticks: int | None = Query(None, ge=1, le=10_000),
    last_event_id: str | None = Header(None),
) -> StreamingResponse:
    cursor = int(last_event_id) if last_event_id and last_event_id.isdigit() else after
    return _event_stream(cursor, ticks, False)


@router.get("/log-events/stream")
def log_events_stream(
    after: int = 0, ticks: int | None = Query(None, ge=1, le=10_000),
    last_event_id: str | None = Header(None),
    level: str | None = None, component: str | None = None,
    source: str | None = None, pipeline: str | None = None,
    run_id: int | None = None, node: str | None = None,
    module: str | None = None, text: str | None = None,
    started_at: datetime | None = None, ended_at: datetime | None = None,
) -> StreamingResponse:
    cursor = int(last_event_id) if last_event_id and last_event_id.isdigit() else after
    return _event_stream(cursor, ticks, True, {
        "level": level, "component": component, "source": source,
        "pipeline": pipeline, "run_id": run_id, "node": node,
        "module": module, "text": text, "started_at": started_at,
        "ended_at": ended_at,
    })


@data_router.post(
    "/sources/{name}/ingest",
    status_code=202,
    response_model=QueuedRunResponse,
)
def source_ingest(
    name: str,
    body: IngestRequest,
    idempotency_key: str = Header(..., min_length=8, max_length=128),
) -> dict[str, Any]:
    total = sum(len(item.text.encode()) for item in body.documents)
    if total > 64 * 1024 * 1024:
        raise HTTPException(413, "ingest payload exceeds 64 MiB")
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            source = source_store.get_source(conn, name, include_spec=True)
            if source is None:
                raise KeyError(name)
            if source["origin"].get("ingress") != "push":
                raise run_store.RunConflictError("Source is not push-rooted")
            contract = _push_contract(source)
            if contract is None:
                raise run_store.RunConflictError(
                    "Source revision has no push.docs boundary")
            if body.mode != contract["mode"]:
                raise run_store.RunConflictError(
                    f"Source requires {contract['mode']!r} ingest semantics")
            if len(body.documents) > contract["max_documents"]:
                raise HTTPException(
                    413,
                    f"Source accepts at most {contract['max_documents']} documents")
            run_id = run_store.submit_source(
                conn, name, inputs={
                    "documents": {
                        "mode": body.mode,
                        "partition": body.partition,
                        "documents": [item.model_dump() for item in body.documents],
                    },
                }, settings=get_settings(), trigger_type="push",
                trigger_by="data API", idempotency_key=idempotency_key,
                dedupe=False,
            )
        return {"run_id": run_id, "queued": run_id is not None}
    except Exception as exc:
        _raise(exc)


__all__ = ["data_router", "router"]
