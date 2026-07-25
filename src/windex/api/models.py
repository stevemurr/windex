"""Response shapes for the control plane.

WHY THESE EXIST. Every handler used to be annotated `-> dict`, which FastAPI
renders as a bare `{}` in the schema. A generated client then gets typed requests
and untyped responses, so it hand-decodes every body — and nothing catches it when
a service function quietly changes shape.

WHY THEY ALL ALLOW EXTRA FIELDS. `response_model` FILTERS: a field the model does
not declare is silently dropped from the response. For a surface this wide,
declared-by-hand from functions that were never written against a schema, the odds
of missing one are high and the failure is invisible — the console just stops
showing a column. `extra="allow"` inverts that: a missed field still reaches the
client, and the model documents what is known rather than gating what is possible.
It also means additive server changes cannot break a response by omission.

So these are DESCRIPTIVE, not prescriptive. Fields are optional unless the value
is structural (an id, a name), because most of them come from left joins and
best-effort probes that legitimately return null. A model that demanded them would
be lying about the data.

`/v1/search` results and `/v1/stats` are deliberately NOT modelled — see
`SEARCH_IS_UNTYPED` at the bottom.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class _Loose(BaseModel):
    """Base: documents what is known, never filters what is not."""

    model_config = ConfigDict(extra="allow")


# --- generic ----------------------------------------------------------------

class ActionResult(_Loose):
    """A mutating call's acknowledgement. Shapes vary by action (each returns the
    state it just set), so only the common keys are declared."""

    ok: bool | None = None
    detail: str | None = None


class ControlState(_Loose):
    indexing: str            # running | paused


class ThrottleState(_Loose):
    embed_profile: str       # polite | full | env


# --- sources and pipeline state ---------------------------------------------

class SourceFreshness(_Loose):
    source: str
    indexed: int | None = None
    pending: int | None = None
    last_embed_ts: float | None = None
    last_update_ts: float | None = None


class LoopState(_Loose):
    source: str
    enabled: bool | None = None
    running: bool | None = None
    state: str | None = None          # up | down | disabled
    ingest_enabled: bool | None = None
    log: str | None = None
    # Empty across container boundaries — a PID means nothing outside its own
    # namespace, which is why loop liveness is a Postgres heartbeat instead.
    pids: list[int] = []


class LoopsState(_Loose):
    loops: list[LoopState] = []
    indexing_paused: bool | None = None
    watchdog_running: bool | None = None


class DatasetStats(_Loose):
    source: str
    total: int | None = None
    by_status: dict[str, int] = {}
    content_from: str | None = None
    content_to: str | None = None


class ActivityItem(_Loose):
    name: str                          # also the /logs/{name} key
    label: str | None = None
    group: str | None = None           # action | loop | service
    running: bool | None = None
    last_ts: float | None = None
    error: str | None = None


class WorkersState(_Loose):
    active: int | None = None
    stage: str | None = None


# --- jobs, schedule, logs ----------------------------------------------------

class JobInfo(_Loose):
    name: str
    title: str | None = None
    description: str | None = None
    category: str | None = None
    running: bool | None = None
    confirm: bool | None = None
    last_log: str | None = None
    pids: list[int] = []
    # Rendered by the same schema-driven form as settings and recipe config.
    params: dict[str, Any] = {}


class ScheduleEntry(_Loose):
    name: str
    kind: str | None = None            # ingest | command
    target: str | None = None
    hour: int | None = None
    minute: int | None = None
    weekday: int | None = None         # 0=Sun; null = every day
    enabled: bool | None = None
    last_run: str | None = None
    last_run_ts: float | None = None
    running: bool | None = None
    label: str | None = None
    cadence: str | None = None


class LogSource(_Loose):
    name: str
    title: str | None = None
    description: str | None = None
    category: str | None = None
    kind: str | None = None            # file | container
    available: bool | None = None
    size: int | None = None
    mtime: float | None = None


class LogTail(_Loose):
    name: str
    available: bool | None = None
    truncated: bool | None = None
    # Every line is redacted before it leaves the process; the API is LAN-exposed.
    lines: list[str] = []


# --- feeds and metrics -------------------------------------------------------

class RecentDoc(_Loose):
    id: str
    source: str | None = None
    title: str | None = None
    url: str | None = None
    # /recent reports when it was indexed; the embedded/indexed feeds report a
    # generic event time. Both are declared so one model serves all three.
    indexed_at: str | None = None
    ts: float | None = None


class TimeseriesPoint(_Loose):
    t: float | None = None
    docs: int | None = None
    ingested: int | None = None
    mb: float | None = None


class SearchMetrics(_Loose):
    window_minutes: int | None = None
    searches: int | None = None
    degraded: int | None = None
    degraded_pct: float | None = None
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    embed_p95_ms: float | None = None
    search_p95_ms: float | None = None
    by_source: dict[str, Any] = {}


# --- settings ----------------------------------------------------------------

class SettingsField(_Loose):
    """One control. A superset of `Param.describe()` plus the resolved value and
    where it came from — the client renders entirely from this and hardcodes no
    field. See windex/schema/param.py for what each attribute means."""

    key: str
    kind: str | None = None
    type: str | None = None
    editor: str | None = None
    title: str | None = None
    label: str | None = None
    description: str | None = None
    help: str | None = None
    lo: float | None = None
    hi: float | None = None
    choices: list[Any] = []
    required: bool | None = None
    advanced: bool | None = None
    secret: bool | None = None
    stage: str | None = None
    enforce: str | None = None         # clamp | reject
    clamp: str | None = None           # floor | ceiling | both
    clampNote: str | None = None
    lockedReason: str | None = None
    dependsOn: dict[str, Any] | None = None
    value: Any = None
    origin: str | None = None          # db | env | default | recipe


class SettingsScope(_Loose):
    scope: str
    fields: list[SettingsField] = []


class SettingsAll(_Loose):
    scopes: list[SettingsScope] = []


# --- crawl / runs -------------------------------------------------------------

class CrawlRun(_Loose):
    id: int
    source: str | None = None
    status: str | None = None          # pending|running|done|failed|cancelled
    requested_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    stats: dict[str, Any] | None = None
    error: str | None = None
    recipe: dict[str, Any] | None = None


class CrawlRunList(_Loose):
    runs: list[CrawlRun] = []


class CrawlRunDetail(CrawlRun):
    urls: dict[str, int] = {}          # frontier counts by status


class CrawlQueued(_Loose):
    run_id: int
    source: str | None = None
    recipe: dict[str, Any] | None = None


class CrawlCancelled(_Loose):
    run_id: int
    status: str | None = None


class CrawlPreview(_Loose):
    """A dry run over the seeds only: fetches, writes nothing.

    `suggest` is the affordance worth preserving — when every link lives under a
    different prefix than the seed, it proposes the one that would actually work,
    which turns a zero-result crawl into a one-click fix.
    """

    recipe: dict[str, Any] | None = None
    suggest: dict[str, Any] | None = None
    seeds: list[dict[str, Any]] = []
    in_scope: int | None = None
    urls: list[str] = []
    truncated: bool | None = None
    rejected: dict[str, int] = {}
    sample: dict[str, Any] | None = None


# --- recipe engine ------------------------------------------------------------

class ValidationReport(_Loose):
    valid: bool
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    normalized: dict[str, Any] | None = None
    graph: dict[str, Any] | None = None


class Registry(_Loose):
    """The module palette. The graph editor renders nodes, connection rules and
    every node inspector from this, so it is served whole and cached client-side
    against its ETag rather than queried piecemeal."""

    registry_version: int
    port_types: dict[str, Any] = {}
    kinds: list[dict[str, Any]] = []
    modules: list[dict[str, Any]] = []
    always_before_load: list[str] = []


class Recipe(_Loose):
    """One registered source. Built-ins and installed recipes are the same shape —
    that collapse is the point of the whole project."""

    name: str
    source: str | None = None
    kind: str | None = None
    title: str | None = None
    description: str | None = None
    spec: dict[str, Any] | None = None
    spec_hash: str | None = None
    version: int | None = None
    enabled: bool | None = None
    builtin: bool | None = None
    node_count: int | None = None
    created_at: str | None = None
    updated_at: str | None = None
    # Node ids and edges per flow, so an editor can lay out the graph without
    # walking the whole spec.
    flows: dict[str, Any] = {}
    # Every name a client might need for this source, so none is hardcoded — the
    # four scattered copies of {ccnews -> news} are what this replaces.
    search_name: str | None = None
    corpus_name: str | None = None
    loop_name: str | None = None


class RecipeList(_Loose):
    recipes: list[Recipe] = []


class RecipeTasks(_Loose):
    """What a run would fan out to. Placement, without queueing anything."""

    recipe: str
    flow: str | None = None
    tasks: list[dict[str, Any]] = []


class Health(_Loose):
    status: str
    service: str | None = None
    version: str | None = None
    auth_required: bool | None = None
    started_at: float | None = None
    uptime_s: float | None = None


class WhoAmI(_Loose):
    ok: bool
    scopes: list[str] = []
    auth_required: bool | None = None


# Deliberately unmodelled, and the reason belongs in the code rather than a commit
# message someone has to go looking for:
#
#   /v1/search results — RESULT_FIELDS (api/service.py) is intentionally sparse and
#     additive. A result carries only the fields its source has, and the union is
#     thirty-odd keys. Freezing that into a generated struct means a client
#     regeneration every time any source gains a field, and thirty optionals to
#     unwrap at the call site. Model it client-side as {id, score, extras} with
#     typed accessors instead.
#
#   /v1/stats — a deep nested rollup still being reshaped (the plan splits its
#     operational half onto /admin/v1). Typing it now would freeze a shape that is
#     known to be moving.
SEARCH_IS_UNTYPED = True
