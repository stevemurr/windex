# Source and Pipeline Implementation Plan

Status: backend BE-0 through BE-9 and macOS FE-0 through FE-7 implemented,
verified, and merged; live epoch-2 cutover acceptance pending
Date: 2026-07-25

## 1. Goal

Make Windex a general visual computation environment while preserving a clear,
safe path for building searchable corpora.

- A **Pipeline** is a reusable named graph with immutable revisions.
- A **Pipeline revision** is an immutable, content-addressed snapshot of graph
  semantics, parameter declarations, and locked module versions.
- A **Source** is a configured deployment pinned to one Pipeline revision. It
  owns an external origin, searchable corpus identity, configuration, schedule,
  durable state, and run history.
- A Pipeline may exist without a Source.
- Multiple Sources may use the same Pipeline revision with independent
  configuration, state, output identity, and runs.
- A Pipeline can back a Source only when it satisfies the versioned
  **Search Source contract**.
- A **Run** freezes the Pipeline revision, Source binding when present, effective
  configuration, explicit inputs, and module versions.

`Recipe` is removed from the product and final backend model. This is a clean
cutover: legacy recipe tables, packages, routes, DTOs, and tests may be replaced
rather than adapted.

Marketplace work is deferred. Remove it from the active API and client during
this cutover; no marketplace migration or compatibility work belongs to this
plan.

Backward compatibility is explicitly out of scope. The current Postgres database,
Qdrant collections/aliases, run history, settings rows, and installed/custom
source records are disposable for this bootstrap. Prefer a clean schema and
correct model over dual reads, adapters, or data migration. Files under
`WINDEX_DATA_ROOT` may be reused only through an intentional import/reindex path;
preserving them is not a requirement.

## 2. Locked product decisions

1. The primary UI vocabulary is Sources, Pipelines, Modules, Nodes, Flows, Runs,
   and Events.
2. Pipeline is the named graph lineage; Pipeline revision is the Git-commit-like
   immutable snapshot.
3. Source is a deployed searchable-corpus integration, not a synonym for
   Pipeline.
4. Publishing a Pipeline revision never silently upgrades a Source.
5. Source configuration changes affect future Runs only.
6. Manual node positions, groups, and annotations synchronize through the
   backend but do not affect the executable hash or create a Pipeline revision.
   Zoom, pan, selection, and inspector state remain local to each Mac.
7. Custom Python/Bash Modules are local, owner-authored, explicitly approved,
   immutable after approval, and sandboxed. They are not marketplace content.
8. Built-in bootstrap definitions should choose intentional stable search names
   and document ID prefixes, but no migration from current database identities is
   required.
9. No old API, database, or client compatibility layer is required. This
   decision supersedes the previous additive-migration assumption for this
   redesign.

## 3. Domain invariants

### 3.1 Pipeline

- Has a stable name and a sequence of immutable revisions.
- A semantic edit publishes revision `N+1`.
- Revisions cannot be overwritten.
- A revision records its parent, normalized spec, semantic hash, registry
  version, and exact custom Module version/digest locks.
- A Pipeline may be generic or may satisfy one or more execution contracts.
- A generic Flow may declare typed boundary inputs and outputs from the registry
  port vocabulary.

### 3.2 Source

- Has an immutable stable name/state namespace.
- Points to exactly one active Pipeline revision.
- Owns effective values for that revision's declared parameters.
- Owns search identity separately from execution identity:
  - Source/state name, for example `gh`
  - Public search/document source, for example `github`
  - Collection/profile, for example `repos`
- Owns schedule, pause/enabled state, refresh policy, cursor/watermark state,
  corpus counts, and run history.
- Search identity fields become immutable after documents or Runs exist. Changing
  them requires creating a new Source or explicitly resetting that Source's
  corpus.
- Archiving/disabling is distinct from destructive corpus deletion.

### 3.3 Search Source contract

The Search Source contract has two distinct checks.

**Pipeline Source capability** is graph-only. It determines whether a published
revision can be selected in the Create Source flow:

- The Pipeline has a valid external ingress. A Source is pull-rooted or
  push-rooted, not both.
- Stable document identity and provenance are produced.
- Every document-producing terminal path reaches the platform-owned searchable
  output continuation.
- Refresh/push behavior and durable state ownership are declared.
- All referenced Module definitions declare compatible contract roles.

**Source deployment validation** combines a capable revision with one concrete
Source binding. It gates creation and upgrade:

- Required parameter values and secret references are supplied.
- All referenced Module implementations are available and approved.
- Search name, ID prefix, collection/profile, and state namespace are uniquely
  owned.
- Operator ceilings, network policy, preconditions, and Module capabilities are
  satisfied.

Contract diagnostics must have stable `path`, `code`, `severity`, and `message`
fields so the graph editor can focus the relevant Node or Source field.

### 3.4 Run

- A Source Run freezes Source identity, Pipeline revision/hash, effective
  configuration, run overrides, graph, and Module locks.
- A generic Pipeline Run freezes its revision, explicit inputs, parameters, and
  Module locks, and has no Source binding.
- Source Run deduplication uses `source + flow`, never Pipeline name alone.
- Editing a Source or publishing a Pipeline while a Run is active cannot alter
  that Run.
- **Re-run** means execute the historic frozen revision/configuration.
- **Run latest** means execute the Source's current active revision and current
  configuration.
- Historic Runs survive Source or Pipeline archival.

## 4. Backend implementation track

The backend agent owns this section. This plan authorizes a coordinated breaking
cutover across the backend and macOS client. Do not spend implementation time on
legacy API adapters, dual reads/writes, or historical-data migration.

Develop the breaking backend/OpenAPI/macOS work on an integration branch. The
BE phases are implementation checkpoints, not independently deployable mainline
states. Remove Recipe routes and land the new contract only as one coherent,
tested contract-epoch slice with Source-aware execution and the matching client;
do not leave `main` between old and new models.

Primary backend touchpoints:

- `src/windex/recipe/`: move/refactor parser, ports, registry, compiler, store,
  run store, runners, and wire codec into the Pipeline domain.
- `src/windex/api/app.py`, `models.py`, `service.py`, and `logs.py`: canonical
  APIs, projections, streaming, and removal of Recipe/Marketplace routes.
- `src/windex/db/schema.sql`: clean canonical schema and reset/bootstrap support.
- `src/windex/scheduler/` and `src/windex/worker/`: Source-aware triggers,
  freezes, task context, state, and execution.
- `src/windex/modules/`, `src/windex/embed/`, and `src/windex/index/`: Source
  bindings, staging-to-searchable completion, and profile-driven routing.
- `compose.yaml` and deployment scripts: dedicated custom-Module sandbox and
  destructive bootstrap procedure.
- `tests/test_recipe_*`, scheduler/worker/API/index tests: replace Recipe
  coverage with Pipeline/Source contract and isolation coverage.

### BE-0 — Freeze the canonical contracts

#### Work

- Add internal domain DTOs for Pipeline, PipelineRevision, SourceDeployment,
  SourceStatus, SourceConfiguration, OverviewSnapshot, OperationalEvent, and
  structured ValidationIssue.
- Define `windex.pipeline/1`, including:
  - parameter declarations using the existing `Param` vocabulary;
  - named Flows, Nodes, Edges, and typed boundary inputs/outputs;
  - refresh entry points;
  - no corpus identity or executable code in the reusable graph spec.
- Define the normalized boundary wire shape: boundary IDs and nominal types,
  Edge references to boundaries, input cardinality/size limits, durable input
  injection, output/artifact persistence, and output retrieval.
- Define `windex.search-source/1` as the two-stage capability/deployment
  validator described above.
- Extend registry descriptors with any contract roles needed to identify
  ingress, state, staging, and searchable sinks without hardcoding Module names
  in the client.
- Publish Kind, Port, Module, capability, contract-role, and `Param` descriptors
  as typed OpenAPI schemas. Do not carry forward opaque
  `additionalProperties` registry DTOs.
- Make the Search Source terminal model a platform-owned indexing continuation
  inside the Source Run. `succeeded` means searchable; staging and embedding are
  intermediate task/progress states. This decision is part of the frozen Run,
  event, status, and Overview contracts.
- Define the Pipeline semantic hash over the normalized graph, Pipeline contract
  version, registry contract version, and all effective built-in/custom Module
  implementation version/digest locks.
- Give built-in Modules an implementation version/build digest just like custom
  Modules so an exact historic Run never silently resolves to changed code.
- Add a `contract_epoch` to health/capabilities. The backend and macOS client
  refuse unsupported epochs before mutation and explain that a matched app/
  server pair is required.
- Replace `windex.recipe` with the Pipeline domain during the cutover. Working
  parser/compiler logic may be moved and mechanically adapted, but the completed
  tree and API should not expose a second Recipe model.
- Decide error status conventions:
  - `409` or `412` for stale revision/layout/config writes;
  - `422` for structured semantic validation failures;
  - `409` for missing/unapproved Modules or an unsafe Source transition.

#### Exit criteria

- The new API schemas and reset/cutover behavior are documented and covered by
  contract tests before persistence changes begin.
- New validation responses always identify a field or graph path where
  applicable.
- Indexing completion, generic boundary I/O, hash inputs, and contract epoch are
  settled rather than deferred to later DTO work.

### BE-1 — Separate Pipeline lineage from Source deployment

#### Persistence

Replace the old control-plane schema with canonical, idempotently bootstrapped
tables in `src/windex/db/schema.sql`:

```text
pipelines
  id, name UNIQUE, title, description, head_revision_id,
  builtin, archived_at, created_at, updated_at

pipeline_revisions
  id, pipeline_id, version, parent_revision_id,
  spec, spec_hash, registry_version, module_locks,
  author, note, created_at
  UNIQUE(pipeline_id, version)
  UNIQUE(pipeline_id, spec_hash)

pipeline_layouts
  pipeline_revision_id, flow_name, layout, layout_etag, updated_at
  PRIMARY KEY(pipeline_revision_id, flow_name)

sources
  id, name UNIQUE, title, description, origin,
  pipeline_revision_id,
  search_contract_version,
  search_name UNIQUE, id_prefix UNIQUE, collection_key UNIQUE,
  search_profile, include_in_all,
  state_namespace UNIQUE,
  enabled, generation, archived_at,
  created_at, updated_at

source_config
  source_id PRIMARY KEY, values, values_hash, updated_at

source_triggers
  id, source_id, flow_name, trigger_type, trigger_spec,
  enabled, next_fire_at, created_at, updated_at

source_control
  source_id PRIMARY KEY, paused, pause_reason, paused_at, updated_at

operator_settings
  scope PRIMARY KEY, values, values_hash, updated_at

secret_references
  name PRIMARY KEY, provider, configured, metadata, updated_at

operational_events
  seq, ts, level, component,
  source_name, pipeline_name, pipeline_version,
  run_id, task_id, node, module,
  event, message, data
```

The clean schema omits old Recipe/custom-source tables. Normal `init-db` and
startup must never contain destructive DROP logic. Add a schema-generation/
contract-epoch guard that initializes an empty database and refuses a legacy
schema with instructions to run the separately reviewed cutover command.
Pipeline revisions must be immutable through the store API and database
privileges/guards.

Use explicit foreign keys: Sources restrict deletion of their pinned revision;
Pipeline/Source archival is a state change; Source-owned config, controls, and
triggers may cascade only on an explicit hard delete; Runs/events never cascade
from live-definition archival or deletion. The initial API exposes archive and
corpus reset, not hard delete.

Replace execution storage with the canonical shape:

```text
runs
  source_id nullable
  source_name nullable
  pipeline_name
  pipeline_revision_id
  pipeline_version
  source_snapshot
  effective_config
  frozen_spec

run_tasks
  module_version
  module_digest
  executor
```

Do not carry `recipe` or `recipe_version` columns into the new schema. Preserve
the domain property that live-definition archival does not cascade into Runs.

Before reset approval, inventory and explicitly define every retained canonical
subsystem—not only the new control-plane tables—including documents, durable
Source state/units, task units and outputs/artifacts, Run events, scheduler
leases, worker fairness, operator settings, secret-reference metadata, search
metrics, specialized Source stores, indexes, foreign keys, partitions, partition
rolling, and retention. Nothing may rely implicitly on a legacy table definition
that the reset removes.

Create the durable global `operational_events` cursor, indexes, partitions, and
retention here. BE-4 instruments transitions; BE-6 and BE-7 project the same
journal for live state and Console behavior.

#### Bootstrap

Implement deterministic seed/bootstrap code:

1. Convert the eleven built-in YAML definitions to canonical Pipeline seed
   definitions.
2. Seed the intended built-in Sources and their search/state bindings.
3. Seed a generic pushed-documents Pipeline; create custom Sources through the
   canonical Source API after bootstrap.
4. Seed triggers/pauses only where they are part of the desired new defaults.
5. Seed no historic Runs, custom source records, settings overrides, or
   marketplace installations.

Check in a built-in seed matrix containing, for every seed:

- Pipeline and initial revision;
- whether a Source is created;
- Source/search/state names and ID prefix;
- collection key and search profile/version;
- `include_in_all`;
- pull/push ingress;
- defaults and trigger/Flow definitions.

`custom` is a pushed-documents Pipeline template, not a seeded searchable Source.
On future releases, an identical built-in hash is a no-op; a changed definition
creates a new immutable revision and leaves existing Sources pinned.

#### Exit criteria

- A clean database initializes repeatably to the intended built-in Pipelines and
  Sources.
- No legacy Recipe/custom-source tables or DTOs are required at runtime.
- Two Sources can point to one Pipeline revision without sharing config or
  durable state.
- A legacy schema is refused without destructive mutation, and the same clean
  bootstrap produces the same schema/seed hashes.

### BE-2 — Add canonical Pipeline APIs

Add:

```text
GET    /admin/v1/pipelines
POST   /admin/v1/pipelines
GET    /admin/v1/pipelines/{name}
POST   /admin/v1/pipelines/validate

GET    /admin/v1/pipelines/{name}/revisions
GET    /admin/v1/pipelines/{name}/revisions/{version}
POST   /admin/v1/pipelines/{name}/revisions
GET    /admin/v1/pipelines/{name}/revisions/{version}/tasks
POST   /admin/v1/pipelines/{name}/archive

GET    /admin/v1/pipelines/{name}/revisions/{version}/layout
PUT    /admin/v1/pipelines/{name}/revisions/{version}/layout
```

- Revision creation requires a parent version/hash or `If-Match`. If
  `parent_hash` and `If-Match` coexist they must be identical; `parent_version`
  is an independent guard and all supplied guards must match the current head.
- Creating a revision is the only semantic graph write; there is no update of a
  published revision.
- A no-op semantic publish should return the existing hash/version rather than
  inventing a duplicate revision.
- Layout has its own ETag and can change without changing the Pipeline revision
  or semantic hash.
- A new revision inherits the prior layout by Node ID; new Nodes receive
  deterministic auto-layout positions.
- Task preview compiles the selected immutable revision and reports placement,
  preconditions, Module availability, and Source-contract roles.

Delete `/admin/v1/recipes*` from the final API. Update all server callers and the
macOS client to use Pipelines and Sources in the same cutover.

#### Exit criteria

- Concurrent stale publication is rejected atomically.
- Moving a Node changes layout but not `spec_hash` or version.
- No Recipe route or Recipe DTO remains in the generated OpenAPI contract.

### BE-3 — Add canonical Source APIs and dynamic settings

Add:

```text
GET    /admin/v1/sources
POST   /admin/v1/sources
POST   /admin/v1/sources/validate
GET    /admin/v1/sources/{name}
PATCH  /admin/v1/sources/{name}
POST   /admin/v1/sources/{name}/validate
POST   /admin/v1/sources/{name}/upgrade/preview
POST   /admin/v1/sources/{name}/upgrade
POST   /admin/v1/sources/{name}/archive

GET    /admin/v1/sources/{name}/settings
PATCH  /admin/v1/sources/{name}/settings
DELETE /admin/v1/sources/{name}/settings/{key}

GET    /admin/v1/sources/{name}/triggers
POST   /admin/v1/sources/{name}/triggers
PATCH  /admin/v1/sources/{name}/triggers/{trigger_id}
DELETE /admin/v1/sources/{name}/triggers/{trigger_id}
POST   /admin/v1/sources/{name}/pause
POST   /admin/v1/sources/{name}/resume

POST   /admin/v1/sources/{name}/reset/preview
POST   /admin/v1/sources/{name}/reset

GET    /admin/v1/sources/{name}/status
GET    /admin/v1/sources/{name}/runs
POST   /admin/v1/sources/{name}/runs

GET    /admin/v1/settings
PATCH  /admin/v1/settings
DELETE /admin/v1/settings/{key}
GET    /admin/v1/secrets
```

At this phase, Source list/detail contains:

- identity and provenance;
- pinned Pipeline name, version, and hash;
- configuration readiness and missing requirements;
- enabled/paused state, refresh Flow, triggers, and next trigger;
- Module/precondition availability.

BE-5 enriches this projection with current/latest Run, progress,
staged/embedding/searchable counts with `as_of`, last success/failure, and recent
error. Do not invent those fields from legacy freshness heuristics.

Settings responses combine the active revision's `Param` schema with configured
and effective values. Each field returns value origin (`source`, `default`, or
`unset`), stage, clamp information, and secret-set state. Secret values are
never returned. Source and operator-setting writes require `If-Match` against
their values hash.

Revision upgrade semantics:

- Retain and revalidate compatible same-key values.
- Materialize new defaults.
- Remove obsolete keys from the candidate config.
- Return all clamped or changed values explicitly.
- Block activation when new required values are missing.
- Make revision switch plus normalized config one atomic write.
- Install-stage changes require an impact preview/confirmation; ordinary
  runtime settings use PATCH.
- Upgrade preview returns retained/defaulted/removed/clamped/missing values,
  state impact, an expected Source/config ETag, and a short-lived confirmation
  token consumed by the atomic upgrade.

`_global` remains operator-owned. Replace hard-coded per-source settings with
Pipeline parameters in the new built-in definitions; no database value migration
is required. Global settings and secret references have their own typed schemas,
ETags, bootstrap defaults, and write scopes.

Each trigger names an applicable Flow and schedule/event type. Pause/resume is
Source-wide; disabling a trigger does not disable the Source. A push Source
detail response includes the canonical ingress URL, authentication requirement,
payload limits, and full-set/delta behavior.

Source search/state identity is permanently immutable. Corpus reset clears
documents, vectors, Source state, and outstanding work but does not unlock
identity changes. The reset preview returns exact affected counts and a typed
confirmation token; execution is asynchronous and emits normal events.

#### Exit criteria

- Two Sources using the same revision expose identical schemas but independent
  effective values.
- Config changes affect only future Runs.
- Settings and Source detail receive the same normalized response.
- Stale Source/config/settings writes fail without partial mutation.
- Scheduling, pause/resume, archive, and corpus reset are independent, observable
  lifecycle operations.

### BE-4 — Make compilation and execution deployment-aware

Make Source-aware compilation the canonical compiler entry point:

```text
compile_source(pipeline_revision, source, run_overrides)
  -> frozen graph, effective config, tasks, module locks
```

Replace Recipe-oriented `TaskContext` properties with Pipeline and Source
identities:

- Pipeline name/version/hash
- Source name and state namespace
- Search source, ID prefix, collection, and search profile
- Frozen effective configuration

Update execution in this order:

1. State/discover/collect Modules use Source `state_namespace`.
2. Staging uses frozen `search_name`, `id_prefix`, and `search_profile`, not
   mutable graph corpus fields.
3. Embedding/index routing uses frozen collection/profile data.
4. Search source discovery reads canonical Source deployments.
5. Source triggers and pauses submit Source Runs.
6. Push endpoints queue Source Runs rather than bypassing the graph.
7. Every Run/Task transition appends to the durable global operational-event
   journal in the same transaction as its state change.

Add generic Pipeline execution for graphs that do not require a Source:

```text
POST /admin/v1/pipelines/{name}/runs
POST /admin/v1/sources/{name}/runs
GET  /admin/v1/runs
GET  /admin/v1/runs/{id}
POST /admin/v1/runs/{id}/cancel
GET  /admin/v1/runs/{id}/events
GET  /admin/v1/runs/{id}/outputs
GET  /admin/v1/runs/{id}/artifacts/{artifact_id}
POST /admin/v1/runs/{id}/rerun
```

- Generic Pipeline Runs require an explicit revision or `If-Match` head
  precondition, Flow, typed inputs, and parameter values.
- The compiler converts boundary inputs into durable task-unit values before
  work is claimable. Terminal boundary values are stored with type, size,
  checksum, and bounded inline/artifact representation.
- Large outputs use a platform-owned artifact store with explicit size,
  retention, and download limits; they are never smuggled through events.
- Source Runs derive contract bindings from the Source.
- Exact Re-run uses the historic frozen snapshot; Run latest remains a separate
  Source action.

Define the replacement push data plane:

```text
POST /v1/sources/{name}/ingest
```

- Versioned, authenticated payload with documented document/chunk schema.
- Returns `202` with Run ID.
- Enforces request/chunk/text limits before queueing.
- Requires an idempotency key and defines delta versus full-set behavior.
- Injects the payload into the declared push Flow boundary.
- Rejects pull Sources and Sources whose pinned revision lacks the matching
  boundary.

Keep `/v1/search` as the canonical query API for the new system, with Source
choices and inclusion policy driven entirely by canonical Source records.

#### Exit criteria

- Two Sources sharing a revision cannot coalesce, share watermarks, mix document
  IDs, or write to one another's collection.
- New frozen Runs remain readable and reproducible after live definitions are
  archived.
- Generic Pipeline Runs reject Source-only Modules unless all required bindings
  are supplied.
- Boundary inputs and outputs round-trip durably across worker restart.
- Push retries with the same idempotency key do not duplicate work.

### BE-5 — Close the searchable-output contract

Replace the current split where `ledger.stage` ends a graph Run and separate
embedding loops make content searchable. A Source Run owns a platform indexing
continuation:

```text
graph tasks -> staged -> embedding/indexing continuation -> searchable -> succeeded
```

- The continuation is a visible platform Task in the same Run, even though it is
  not a user-placeable Node.
- It uses the frozen Source search profile, collection key, and model/runtime
  configuration.
- The Run remains active through indexing. Source Run `succeeded` means the
  committed output is queryable.
- Staging, embedding, alias publication, and search verification emit structured
  intermediate events and progress.
- Generic Pipeline Runs without the Search Source contract do not receive this
  continuation and finish when their declared outputs commit.

Replace hard-coded source routing with Source `search_profile` metadata. Port the
desired built-in profile-specific filters and payload schemas into the new model.
BE-5 also enriches Source status with current/latest Run, weighted progress,
staged/embedding/searchable/failed counts and timestamps, last success/failure,
and recent error.

#### Exit criteria

- The Search Source contract has a real searchable terminal condition.
- A completed Source operation can be traced from origin through staging and
  indexing to queryable output.
- Seeded built-in search behavior and inclusion/privacy rules match the newly
  declared Source profiles.

### BE-6 — Add live Overview and global Run state

Add:

```text
GET /admin/v1/overview
GET /admin/v1/events/stream
```

The snapshot includes:

- service and storage/vector health;
- control/pause state and degraded modes;
- worker pools, lanes, queue pressure, and failed preconditions;
- active, queued, blocked, and recent Runs;
- weighted task progress, explicitly indeterminate when totals are unknown;
- Source staging/indexing/searchability status;
- schedules, next triggers, recent failures, and recent indexed documents;
- document/vector totals and throughput with `as_of` timestamps.

The control-plane stream is a multiplexed, low-volume projection over the
durable operational-event cursor. It carries Run/Task transitions, Source
status/control/config invalidations, Pipeline revision publication, layout
changes, worker/precondition changes, and an Overview revision. It supports
cursor replay after reconnect. High-volume Console following remains a separate
BE-7 stream.

Compute live progress from `run_tasks`; store the final aggregate at completion
instead of hot-updating the parent Run on every heartbeat.

#### Exit criteria

- A newly queued Run appears in the stream within one event interval.
- All terminal and blocked transitions are observable without knowing the Run ID
  in advance.
- REST reconciliation and SSE projections converge to the same state.

### BE-7 — Add structured Console events

Extend `run_events` or add an adjacent operational-event table with:

```text
seq, ts, level, component,
source_name, pipeline_name, pipeline_version,
run_id, task_id, node, module,
event, message, data
```

Add authenticated admin-only APIs:

```text
GET /admin/v1/log-events
GET /admin/v1/log-events/stream
GET /admin/v1/log-events/facets
```

Support cursor/time pagination and server filters for level, component, Source,
Pipeline, Run, Node/Module, and text. Support `Last-Event-ID`, keepalives,
bounded replay, retention, payload-size limits, and redaction before persistence
or egress.

Raw Postgres/Qdrant/host log tails remain secondary diagnostic adapters until a
production-safe Podman/journald collector exists. Do not expose filesystem paths
or container sockets to the API service.

#### Exit criteria

- Reconnect from a cursor produces no gaps or duplicates.
- Source, Pipeline, Run, Node, and component filters compose.
- Token-shaped and configured secrets never appear in stored or returned events.

### BE-8 — Add approved local Python/Bash Modules

Do not begin this phase until scoped authentication and sandbox isolation are
designed and reviewed. This is a post-core extension and does not block the
BE-9/FE-7 Source/Pipeline cutover; it may be delivered afterward with FE-8.

First implement the secure module-admin channel:

- Serve code-authoring endpoints only over HTTPS with a documented certificate
  pinning or explicit trust-on-first-use flow for the Mac client.
- Issue a separately scoped `module_admin` credential; the ordinary Windex admin
  token cannot upload or approve code.
- Publish supported scopes, secure-upload availability, and server-advertised
  runtimes through health/capabilities.
- Record module-admin actions in the operational-event audit trail.
- Provide a loopback CLI approval path for recovery; it does not weaken the HTTPS
  requirement for remote source upload.

Add immutable Module/version storage with runtime, kind, port types, `Param`
schema, requested capabilities, allowed hosts, source digest, approval state,
resource limits, and audit metadata.

Lifecycle:

```text
draft -> validate -> test with fixtures -> approve -> available
                                      \-> reject/revoke
```

Initial restrictions:

- Advertise Python only at first; add Bash later using the same proven protocol.
- `transform` and optionally `extract` kinds only.
- Existing nominal wire types only.
- No arbitrary dependency installation.
- No network, database, Qdrant, Source state, host files, or secrets by default.
- JSONL stdin/stdout through the existing wire codec.
- Every output is decoded, bounded, and type-validated by the platform.
- Approved versions are immutable and their digest is frozen into Pipeline and
  Run snapshots.

Execute only in a dedicated rootless sandbox service with a read-only root,
bounded temporary storage, empty/minimal environment, dropped capabilities,
no-new-privileges, no host/container socket, and CPU, memory, process, duration,
and output limits.

Module-code writes require a separately scoped credential. Refuse code upload
over plaintext HTTP.

Update the registry ETag from installed Module definitions/digests rather than a
fixed constant.

#### Exit criteria

- Unapproved or revoked Module versions cannot create new Runs.
- A hostile fixture cannot reach the network, credentials, host files, database,
  Qdrant, or container runtime.
- Limits terminate runaway code and emit a structured failure event.
- No marketplace/catalog path installs executable code.
- Revocation blocks every new execution, including exact historic Re-run; history
  remains inspectable and the API returns structured `module_revoked` conflict.

### BE-9 — Destructive bootstrap and cutover

Implement a dedicated, fail-closed cutover command/runbook. Normal `init-db`
never performs this reset.

#### Preflight before maintenance

1. Build, test, tag, and record the immutable backend image/commit.
2. Regenerate OpenAPI and build/test the matching contract-epoch macOS client.
3. Inventory and validate operator config, secrets, model settings, storage
   roots, and bootstrap credentials.
4. Dry-run the reset command. It prints exact:
   - Postgres host/database/schema;
   - Qdrant endpoint and Windex-owned aliases/collections from an ownership
     manifest;
   - new generation-scoped path under
     `WINDEX_DATA_ROOT/generations/<bootstrap-id>`;
   - old generation path that will be quarantined, not immediately erased.
5. Reject empty targets, wildcards, unresolved environment variables, shared
   service-wide targets, filesystem roots, or the whole `WINDEX_DATA_ROOT`.
6. Require an explicit reviewed production confirmation containing the resolved
   bootstrap ID and target manifest. The reset is irreversible; no historical
   backup is required.

#### Maintenance and reset

1. Disable writes/triggers and enter maintenance mode.
2. Drain or cancel workers, then stop scheduler, workers, embedding/indexing,
   and API in that order.
3. Verify no Windex writer or active database session remains.
4. Start only the required Postgres/Qdrant infrastructure.
5. Run the reset with durable phase markers:
   `preflight -> postgres_reset -> qdrant_reset -> filesystem_generation ->
   schema_bootstrap -> seed -> verified`.
6. Drop/recreate only the resolved Windex Postgres database/schema and
   enumerated Windex-owned Qdrant aliases/collections.
7. Initialize the new generation-scoped filesystem namespace. Reuse raw
   downloads/staged files only through an explicit verified importer; never let
   stale relative references enter the new ledger.
8. Bootstrap the canonical schema and seed matrix; verify schema, contract epoch,
   and seed hashes.

Every phase is idempotently resumable. On partial failure, the command reads its
marker and either completes the remaining stores or recreates the entire new
generation; it never reconnects a partially reset old/new combination.

#### Controlled restart

1. Start API/workers with all Source triggers disabled.
2. Verify health, contract epoch, registry, Pipeline/Source seeds, Overview
   stream, Console stream, and redaction.
3. Run one bounded Source end to end and verify it reaches searchable output.
4. Query the result through `/v1/search`.
5. Enable Sources/triggers individually while observing queues and events.
6. Quarantine the prior filesystem generation until fresh-search verification;
   prune it only through a separate exact-target operation.

Treat old Runs, settings overrides, custom Sources, vectors, and control-plane
records as disposable. Production requires image rebuild/recreate, not a service
restart, because source is baked into the image.

No destructive reset is performed as part of writing this plan. The backend
handoff owns the reset procedure and its execution timing.

## 5. macOS implementation track

The macOS track can begin its domain/UI work while BE-1 through BE-3 are in
progress. Backend response models and OpenAPI artifacts are the integration
boundaries.

Primary frontend touchpoints:

- `clients/macos/WindexApp/App/AppModel.swift` and
  `Views/AppShellView.swift`: shared BackendSession and navigation.
- `Views/SourcesView.swift` and `Views/RecipesView.swift`: split into canonical
  Source and Pipeline workspaces.
- `Views/OverviewView.swift`, `RunsView.swift`, `LogsView.swift`,
  `SettingsView.swift`, and `SearchView.swift`: shared live projections.
- `Packages/WindexKit/Sources/WindexKit/API/` and `Models/`: new transports and
  domain adapters; removal of Recipe/Marketplace surfaces.
- `Packages/WindexKit/Sources/WindexUI/SchemaForm/`: reuse value editors and add
  Node-binding adapters without turning the value form into a schema author.
- `clients/macos/DESIGN.md`, `README.md`, and `HANDOFF.md`: terminology,
  interaction, and delivery-state updates.

### FE-0 — Update terminology and design guidance

- Update `clients/macos/DESIGN.md`, `README.md`, and `HANDOFF.md`.
- Remove Recipe from primary copy and navigation.
- Remove Marketplace from navigation and delete its unused client transport,
  models, fixtures, and tests during the cutover.
- Use this navigation order:
  1. Overview
  2. Sources
  3. Pipelines
  4. Runs
  5. Logs
  6. Search
  7. Settings
- Define consistent copy for:
  - Publish revision
  - Use as Source
  - Upgrade Source
  - Re-run
  - Run latest
  - staged / embedding / searchable

#### Exit criteria

- No ordinary UI says Recipe or Marketplace.
- Pipeline and Source descriptions match the domain definitions in this plan.

### FE-1 — Introduce a shared backend session

Create a connection-scoped `BackendSession` owned by `AppModel`:

```swift
@MainActor @Observable
final class BackendSession {
    let registry: RegistryStore
    let pipelines: PipelineStore
    let sources: SourceStore
    let runs: RunStore
    let overview: OverviewStore
    let logs: LogStore
    let events: LiveEventHub
}
```

- Put authoritative state in shared stores rather than screen-local polling
  models.
- `LiveEventHub` owns the multiplexed low-volume control-plane SSE connection,
  cursor resume, backoff, cancellation, authentication failure, and REST
  reconciliation. `LogStore` owns a separate high-volume Console stream/cursor.
- Reconcile at connection, foreground, reconnect, and after mutations.
- Check `contract_epoch` before constructing the session or allowing mutations;
  show a matched app/server upgrade requirement on mismatch.
- Optimistically apply only reversible presentation state and temporary
  queued-Run placeholders. Pipeline publication, Source creation/upgrade,
  settings, archive/reset, and Module approval wait for authoritative server
  responses.
- Preserve stale known data during refresh.
- Inject the session through the SwiftUI environment instead of threading
  client/backend arguments through every view.

Suggested structure:

```text
WindexApp/App/BackendSession.swift
WindexApp/App/LiveEventHub.swift
WindexApp/Stores/RegistryStore.swift
WindexApp/Stores/PipelineStore.swift
WindexApp/Stores/SourceStore.swift
WindexApp/Stores/RunStore.swift
WindexApp/Stores/OverviewStore.swift
WindexApp/Stores/LogStore.swift
```

#### Exit criteria

- One control-plane stream drives Overview, Sources, Pipelines, and Runs; one
  independently bounded Console stream drives Logs.
- Queueing a Run updates every relevant screen immediately.
- SSE failure degrades visibly to bounded polling without discarding known data.

### FE-2 — Add typed WindexKit Pipeline and Source surfaces

Add domain adapters and transports:

```text
WindexKit/Models/Pipeline.swift
WindexKit/Models/SourceDeployment.swift
WindexKit/Models/OverviewSnapshot.swift
WindexKit/Models/OperationalEvent.swift
WindexKit/API/PipelinesAPI.swift
WindexKit/API/SourcesAPI.swift
```

- Regenerate DTOs after each backend contract slice.
- Decode typed generated registry DTOs for kinds, ports, Modules, capabilities,
  contract roles, and fields, then project them into editor-friendly domain
  types. Do not retain opaque registry dictionaries at the wire boundary.
- Expose no Recipe terminology to WindexApp.
- Populate Search choices from canonical deployed Sources. Keep `all` only when
  the server provides it so privacy/inclusion policy remains authoritative.

#### Exit criteria

- WindexApp consumes Pipeline/Source domain models only.
- No Recipe or Marketplace API/model remains in WindexKit.

### FE-3 — Build read-only Sources and Pipelines workspaces

#### Sources

Refactor Sources into a catalogue and detail workspace with:

- Overview
- Settings
- Runs
- Activity
- Pipeline

Activity is initially recent Run events and becomes the full filtered Console
projection after FE-7.

Show Source identity, pinned Pipeline revision, Search Source readiness,
configuration completeness, schedule/pause state, staged/indexing/searchable
counts, current/latest Run, and recent failure.

The Pipeline section links to the immutable pinned revision. Editing happens in
the Pipelines workspace, not inside Source detail.

#### Pipelines

Replace the raw Recipe screen with:

- Pipeline catalogue
- Revision selector/history
- Read-only graph
- deployment backlinks such as “used by 3 Sources”
- validation/contract summary
- advanced normalized definition view

Published revisions are semantically read-only but layout-editable. Layout-only
saves use the per-Flow layout ETag and never publish a semantic revision. “New
revision” creates a mutable semantic draft from a selected revision.

#### Exit criteria

- Two Sources using one revision appear independently.
- The Pipeline revision shows both Source deployments.
- Built-in provenance is metadata, not a different interaction model.

### FE-4 — Build the Pipeline draft model and composer

Add SwiftUI-independent editor types:

```text
PipelineDraft
FlowDraft
PipelineBoundaryDraft
NodeDraft
EdgeDraft
ParameterDefinitionDraft
NodeConfigValue
PipelineValidationIssue
PipelineLayout
```

`NodeConfigValue` must distinguish:

- literal value;
- compatible Pipeline/Source parameter reference;
- allowed secret reference.

`PipelineEditorModel` owns base revision/hash, mutable draft, selection, dirty
state, undo/redo commands, local validation, debounced server validation,
publishing, and local crash recovery. Store unpublished drafts under Application
Support keyed by Pipeline and base revision.

Composer capabilities:

- create, select, rename, duplicate, and delete Flows;
- edit typed Flow boundaries and choose refresh entry points;
- searchable Module palette grouped by registry kind;
- pan/zoom canvas;
- SwiftUI Node views for hit testing/accessibility and `Canvas` edges;
- add, move, rename, duplicate, connect, disconnect, and delete;
- typed boundary and Node ports;
- compatible-target highlighting;
- local type, cycle, bounds, and dangling-path checks;
- deterministic auto-layout plus synchronized manual positions;
- grouping and annotations;
- keyboard selection, connect, delete, duplicate, undo, and redo;
- click-through validation diagnostics.

Inspector:

- Reuse `SchemaForm` for literal Module values.
- Add a `NodeInspectorModel` for literal/parameter/secret binding mode.
- Add a separate parameter-definition editor; `SchemaForm` edits values and
  must not be stretched into authoring schemas.

Search Source rails:

- Show Origin, Computation, and Searchable output requirements.
- Allow incomplete drafts.
- Present a live readiness checklist from local and server validation.
- Enable “Use as Source” when the revision has Pipeline Source capability; the
  creation wizard performs full Source deployment validation after identity,
  settings, secrets, and policy are bound.

Publishing:

- Requires server validation and expected parent/hash.
- Server-normalized spec becomes local truth.
- Existing Sources remain pinned.
- Layout writes use their independent ETag.

Generic Run:

- Published Pipeline revisions expose Run.
- Select a Flow, enter values for typed boundary inputs and declared parameters,
  choose dry-run/priority, and submit the explicit revision.
- Show bounded inline outputs and artifact downloads in Run detail.
- Source-only requirements disable generic Run with a structured explanation.

#### Exit criteria

- A user can clone a revision, add/configure/connect Nodes, publish a valid new
  revision, and see existing Sources remain unchanged.
- Invalid intermediate graphs never reach executable revision storage.
- Reopening the revision restores node positions without changing its hash.
- A source-less Pipeline Run accepts declared inputs and returns durable declared
  outputs.

### FE-5 — Build Source creation, upgrade, and settings

Source creation:

1. Name the Source and define immutable search/corpus identity.
2. Choose a Pipeline revision with Pipeline Source capability.
3. Fill the revision's parameter schema.
4. Resolve preconditions and secret references.
5. Validate the complete deployment.
6. Create atomically.

Source upgrade:

1. Select another revision.
2. Preview compatible retained values, defaults, removed keys, clamps, missing
   requirements, and install-stage impact.
3. Edit the candidate configuration.
4. Validate.
5. Upgrade revision and configuration atomically after confirmation.

Settings:

- The Source Settings tab and global Settings screen use the same SourceStore
  state and schema-driven form.
- Global Settings contains:
  - System, backed by `_global` operator settings;
  - Sources, generated from Source deployments.
- PATCH responses replace local values so server clamps/defaults remain truth.
- Secrets show configured/unconfigured state but never a value.
- Keep one configuration edit draft per Source across Source detail and global
  Settings. Submit the Source values hash with each write.
- On `409/412`, offer reload, compare, and reapply; never silently overwrite a
  concurrent edit.

Lifecycle:

- Edit schedule/event triggers and select their refresh Flow.
- Enable, pause, resume, and archive a Source.
- For push Sources, show copyable ingress URL, authentication, limits, and
  delta/full-set instructions.
- Keep corpus reset separate from archive. Show server-provided affected
  document/vector/state/work counts, require typed confirmation, and follow its
  asynchronous Run/events.

#### Exit criteria

- Create two Sources from one revision with different values and corpus identity.
- Run both and observe isolated state and effective configuration.
- Editing a Source setting does not alter an active or historic Run.
- Upgrade cannot partially move the revision pointer before config validation.
- Trigger, pause, archive, and reset controls remain distinct and reflect their
  server-confirmed state.

### FE-6 — Rebuild Overview and Runs around shared live state

Overview should be an all-up operational display using typography and tables, not
dashboard-card sprawl:

- system state and live uptime;
- throughput and indexed documents;
- active, queued, blocked, and recent Runs;
- worker/lane activity and queue pressure;
- Source revision, counts, current progress, last success, and last failure;
- staged/embedding/searchable distinction;
- recent failures and documents;
- storage, database, vector, and service availability;
- clear live/degraded connection state.

Runs:

- Show Pipeline and immutable revision for every Run; show Source when bound and
  “generic Pipeline Run” otherwise.
- Keep per-Task progress and events.
- Render declared inputs, terminal outputs, and artifacts for generic Runs.
- Make Re-run and Run latest separate actions.
- Deep-link from a Run to its Source, Pipeline revision, Node, and filtered Logs.

#### Exit criteria

- A queued Run appears across Overview, Sources, and Runs within two seconds.
- Running, blocked, failed, cancelled, and succeeded states update without
  navigation.
- With SSE unavailable, a visible degraded mode reconciles within five seconds.

### FE-7 — Replace Logs with a Console-style event viewer

- Use a bounded structured ring buffer.
- Load cursor-based history and follow authenticated SSE.
- Deduplicate across reconnects.
- Provide follow/pause, jump to newest, clear local view, copy, and export.
- Use a virtualized table with time, level, Source, Pipeline/Run, component, and
  message.
- Filter by time, level, component, Source, Pipeline, Run, Node/Module, and text.
- Support saved filter presets.
- Selecting an event opens structured metadata without exposing secrets.
- Client-side filtering covers the buffered window; server-side queries cover
  history and high volume.

#### Exit criteria

- Events append without refresh.
- Pausing stops viewport movement without stopping collection.
- Reconnect produces no visible duplicates or gaps.
- Deep links open a correctly prefiltered Console.

### FE-8 — Add the local Module editor

This phase depends on BE-8 and secure transport.

- Launch “New Module” from the Pipeline palette.
- Edit name, server-advertised runtime, kind, input/output types, parameter
  schema, capabilities, resource limits, and source. Show Python initially; show
  Bash only when the backend advertises it.
- Provide fixture input and structured output preview.
- Expose validate, test, approve, revoke, and immutable version history.
- Make requested capabilities and effective sandbox restrictions explicit.
- Approved versions appear in the normal registry palette with provenance/trust
  status.
- Store the separate `module_admin` credential in Keychain, discover scopes,
  reauthenticate before approval, and require the pinned/trusted HTTPS channel.
  Plaintext connections disable source upload/approval and offer the loopback
  approval instructions.

#### Exit criteria

- An owner can author, test, approve, place, and run a sandboxed transform Module.
- Editing approved code creates a new version.
- Existing Pipeline revisions remain pinned to the prior digest.

### FE-9 — Accessibility, recovery, and performance hardening

- Keyboard-only graph authoring and focus order.
- VoiceOver names for Nodes, ports, Edges, status, and validation.
- Reduced-motion canvas and streaming updates.
- Draft crash recovery and explicit discard.
- Multi-window/stale revision and layout conflict handling.
- Offline registry cache and stale-data behavior.
- Canvas performance at 64 Nodes/128 Edges.
- Log buffer performance under sustained event volume.

## 6. Backend/frontend dependency matrix

| Backend deliverable | Frontend work unblocked |
|---|---|
| BE-0 contract epoch, typed registry, validation/boundary contracts | FE-1 epoch handshake and FE-4 local editor models against fixtures |
| BE-1 canonical schema/event foundation | Backend integration tests; no screen transport by itself |
| BE-2 Pipeline APIs, revisions, concurrency, and layout | FE-2 Pipeline transport, FE-3 Pipeline catalogue, FE-4 publish/layout |
| BE-3 Source, trigger, global settings, and secret APIs | FE-2 Source transport, FE-3 Sources, FE-5 lifecycle/settings |
| BE-4 Source/generic Runs, outputs, and push ingress | FE-4 generic Run, FE-5 push/run isolation, FE-6 Run detail |
| BE-5 searchable completion state | FE-3/FE-6 staged-versus-searchable display |
| BE-6 Overview/multiplexed control stream | FE-1 LiveEventHub, layout invalidation, and FE-6 Overview |
| BE-7 structured event history/SSE | FE-3 Activity and FE-7 Console |
| BE-8 scoped secure Module registry/sandbox lifecycle | FE-8 Module authentication/editor |
| BE-9 reviewed reset/bootstrap | Integrated production acceptance for FE-0 through FE-7 |

FE-0, the shared-state skeleton in FE-1, local Pipeline draft types, canvas
mechanics, auto-layout, undo/redo, and local port validation can begin from BE-0
fixtures and the existing `Param`/registry concepts while backend contracts are
developed. Do not scaffold new UI against `/recipes` or legacy Source DTOs.

## 7. Test plan

### Backend

- Fresh bootstrap is idempotent and creates only canonical Pipeline/Source state.
- Recipe, legacy custom-source, and Marketplace tables/routes are absent.
- Health rejects an incompatible contract epoch before mutation.
- Every seeded built-in Source appears exactly once.
- Identical built-in seed hashes are no-ops; changed seeds create revisions
  without moving Sources.
- One Pipeline revision backs two Sources with distinct configuration, state,
  IDs, and collections.
- Publishing creates `N+1`; stale publication changes nothing.
- Layout changes do not change Pipeline version/hash.
- Per-Flow layout writes conflict independently.
- Source upgrade is atomic and deterministic across retained/defaulted/removed/
  clamped/missing settings.
- Stale Source, Source config, and operator settings writes change nothing.
- Secret references never appear as secret values.
- Source triggers, pause/resume, archive, and reset obey distinct state machines.
- Source Runs freeze revision, Source binding, defaults, persisted settings,
  overrides, and Module locks.
- Exact Re-run and Run latest have distinct verified behavior.
- Duplicate Source Runs coalesce by Source and Flow, not Pipeline.
- Pipeline Source capability and Source deployment validation are tested
  separately.
- Source contract rejects mixed ingress, missing searchable paths, missing
  requirements, unavailable Modules, and identity conflicts.
- Generic Pipeline boundary inputs/outputs and artifacts survive worker restart.
- Push ingress enforces limits, binding compatibility, and idempotency.
- A Source transitions honestly through staged, embedding, and searchable.
- Overview and Run SSE replay without lost or duplicated transitions.
- Log cursor replay, compound filtering, retention, and redaction work.
- Custom Module sandbox isolation and resource limits are adversarially tested.
- Revoked Modules block new and exact historic re-execution.
- Fresh Postgres and Qdrant state can complete bootstrap, ingest, index, and
  search without any legacy rows.
- Reset preflight rejects broad/unowned targets; each phase resumes idempotently
  after injected partial failure.

### WindexKit and macOS

- Generated DTO conformance and transport fixtures for each new endpoint.
- Generated OpenAPI/Swift operations contain no Recipe or Marketplace surface.
- Contract-epoch mismatch blocks the session before writes.
- Pipeline draft encode/decode and normalized server round-trip.
- Port compatibility, cycle detection, Node deletion, and undo/redo.
- Multi-Flow create/rename/delete, boundary editing, refresh entry points, and
  Flow selection.
- Deterministic auto-layout, layout-only editing of semantic revisions, and
  per-Flow layout ETag conflicts.
- Literal/parameter/secret Node bindings.
- Pipeline parameter-definition editing.
- Pipeline Source capability versus complete Source deployment validation.
- Generic Pipeline Run inputs, outputs, artifacts, and source-less Run display.
- Two Sources sharing one revision.
- Source config editing, revision-upgrade reconciliation, and settings conflict
  reload/compare/reapply.
- Source scheduling, pause/resume, archive, push instructions, and corpus reset.
- Shared event reduction and REST reconciliation.
- SSE reconnect, cursor deduplication, cancellation, and unauthorized handling.
- Overview mutation propagation.
- Log compound filters, pause/follow, and bounded-buffer behavior.
- Search picker populated from canonical Sources.
- `SearchSource.builtIn` and legacy per-Source `SettingsScope` assumptions are
  absent; only canonical Sources and global operator settings populate them.
- Scoped Module upload authentication, TLS gating, and server-advertised runtime
  behavior.
- Keyboard commands, VoiceOver labels/focus, and reduced motion.
- Regression test that app strings, accessibility labels, errors, fixtures, and
  ordinary navigation contain neither Recipe nor Marketplace.

### End-to-end acceptance scenario

1. Open a reusable documentation Pipeline revision.
2. Create `team_docs` and `public_docs` Sources from it with different URLs,
   schedules, IDs, and collections.
3. Publish a new Pipeline revision; verify neither Source moves.
4. Upgrade only `team_docs`, resolving one new required setting.
5. Run both Sources and verify independent config, watermarks, progress, events,
   staged content, indexing, and search results.
6. Re-run the older `public_docs` Run exactly and separately Run latest.
7. Rearrange the Pipeline canvas on one Mac and verify another Mac receives the
   layout without a semantic revision.
8. Disconnect SSE, verify visible degraded polling, reconnect by cursor, and
   observe no missing/duplicate events.
9. Run a generic Source-less Flow with typed inputs and verify its declared
   outputs/artifacts.
10. Pause, resume, reschedule, and reset a bounded test Source while confirming
    each state/event independently.
11. Author and approve a local Python transform Module, publish a Pipeline
   revision pinned to its digest, and run it in the sandbox.

## 8. Delivery gates

For every backend contract slice:

```sh
uv run pytest
uv run python scripts/dump-openapi.py -o clients/macos/Packages/WindexKit/openapi.json
uv run python scripts/dump-openapi.py --which admin -o clients/macos/Packages/WindexKit/openapi-admin.json
uv run python scripts/dump-openapi.py --check
```

For every macOS slice:

```sh
cd clients/macos/Packages/WindexKit && swift test
cd clients/macos
xcodebuild -project Windex.xcodeproj -scheme Windex \
  -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO test
```

The backend owns authoritative OpenAPI JSON. The macOS track regenerates and
commits Swift DTOs after pulling each backend contract slice.

Before the coherent cutover merge/deployment:

- backend and macOS report the same `contract_epoch`;
- fresh-schema bootstrap and the built-in seed matrix pass;
- a reset dry-run resolves only allowlisted Windex-owned targets;
- BE-0 through BE-7 and FE-0 through FE-7 pass the end-to-end scenario on a
  disposable environment;
- the immutable backend image and matching macOS build are recorded before
  production maintenance starts.

Do not begin custom-code execution before its authentication and sandbox gates
are met. Marketplace remains out of scope.
