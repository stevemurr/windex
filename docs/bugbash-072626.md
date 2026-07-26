# Windex application bug bash — 2026-07-26

This report covers the full Windex product: backend control plane, Pipeline and
Source lifecycle, scheduler, workers, ingestion, indexing, search, operations,
and the macOS client.

No application or deployment changes were made during the investigation.

## Executive summary

The bug bash found:

- 12 high-priority defects
- 15 medium-priority defects
- 1 external consumer integration issue

The repository is generally healthy: all 286 backend tests passed, Ruff passed,
and both OpenAPI artifacts were reproducible. The findings are primarily
runtime, lifecycle, failure-recovery, and missing-coverage defects rather than a
generally broken build.

## Live state observed during the bash

- Public hybrid search was working. The final probe returned a result in 116 ms.
- GitHub, Hacker News, and both CC-News extraction lanes were progressing.
- There were no `error` or `critical` operational events during the final
  one-hour observation window.
- Wiki task 175 had run for more than 3,390 seconds, consumed roughly 4 GB and
  two CPU cores, and completed zero durable units.
- Prometheus was receiving a 404 from every `/metrics` scrape.
- One active admin client generated 14,107 HTTP requests in 20 minutes.
- The worktree was clean and synchronized with `origin/main`.

## Application areas

Areas are concentrated pieces of application functionality. The following areas
were included in the bash.

| Area | Concentrated functionality | Findings |
|---|---|---|
| Public API and contracts | Authentication, health, epoch pairing, errors, caching | H05, M05–M08 |
| Pipeline management | Validation, publication, revisions, layouts | H08, M09–M10, M13 |
| Source lifecycle | Deployment, settings, upgrades, status | M02–M04, M07, M12 |
| Triggers and scheduling | Cron, interval, and event dispatch | H06–H07, M01–M02 |
| Runs and artifacts | Submission, progress, history, outputs | M04, H10 |
| Worker runtime | Claims, leases, slicing, cancellation, recovery | H01–H02 |
| Pull connectors | GitHub, HN, Wiki, arXiv, HF, docs, Small Web | H01; others healthy in live sampling |
| Push ingestion | Memory and generic custom documents | H09, M14 |
| Ledger and replacement | Staging, tombstones, metadata, coverage | H04, H09–H10 |
| Embedding and vector storage | Qdrant indexing and deletion | H04 |
| Search and retrieval | Fan-out, degradation, document fetching | H05, M11 |
| Module registry and administration | Locks, approval, sandbox execution | No additional defect found |
| Operations | Bootstrap, deployment, health, metrics, retention | H03, H10, M06, M15 |
| macOS session and state | SSE, reconciliation, stores, transport | H11–H12, M08, M12–M13 |
| macOS workspaces | Search, Sources, Pipelines, Runs, Console | H06, M01, M11 |
| Documentation and integration | Runbooks and consumer handoffs | M15 plus external consumer issue |

## High-priority findings

### H01 — Wiki extraction is effectively unsliced

`cirrus.articles` loads and materializes an entire shard, never uses its declared
`chunk_rows` setting, and cannot yield while parsing. This was the live task
monopolizing the only `cpu_heavy` lane.

Evidence:

- [`cirrus_articles`](../src/windex/modules/extract.py#L435)
- [`cirrus.articles` registry declaration](../src/windex/pipeline/registry.py#L427)

### H02 — A non-cooperative task cannot self-heal

The runtime only warns when a module exceeds its slice deadline. The kill
watchdog watches database `yield_requested`, not deadline expiry. Its grace
period defaults to disabled and cannot be configured through the worker
environment mapping.

Cancellation sets `yield_requested`, but a module that never calls
`should_yield()` will continue running unless the worker process is killed.

Evidence:

- [Overdue slice warning](../src/windex/worker/execute.py#L141)
- [Hung-task watchdog](../src/windex/worker/supervisor.py#L262)
- [Worker environment mapping](../src/windex/worker/config.py#L120)

### H03 — Prometheus observability is absent

The live `/metrics` endpoint returns 404 on every scrape. Metrics objects remain
in the code, but the route, point-in-time collector, and HTTP RED middleware
referenced by the metrics module are absent.

Evidence:

- [API application construction](../src/windex/api/app.py#L26)
- [Metrics module](../src/windex/metrics.py#L1)

### H04 — Failed Qdrant deletion can leave permanent ghost results

The ledger clears a tombstoned document's `embedded_model` and `indexed_at`
markers before attempting the Qdrant deletion. All Qdrant deletion errors are
then suppressed.

The index retry path only selects non-searchable documents with a non-null
embedding marker, so the failed vector is never retried. Search reads Qdrant
without cross-checking the ledger and can continue returning the deleted point.

Evidence:

- [Ledger tombstoning](../src/windex/modules/load.py#L302)
- [Best-effort vector deletion](../src/windex/modules/load.py#L360)
- [Index retry selection](../src/windex/pipeline/indexing.py#L130)

### H05 — Search outages can appear as successful empty searches

Every per-collection exception is suppressed, including when the caller
explicitly requests one Source. The response remains HTTP 200 and is not marked
degraded, making an unavailable index indistinguishable from a valid query with
no matches.

Evidence:

- [Collection search error handling](../src/windex/index/search.py#L408)

### H06 — Event triggers are inert

Event triggers are accepted by the API and exposed in the macOS trigger editor,
but the scheduler only scans cron and interval triggers. No event-dispatch path
exists elsewhere in the backend.

Evidence:

- [Accepted trigger types](../src/windex/api/canonical.py#L203)
- [Scheduler trigger selection](../src/windex/source/scheduler.py#L43)

### H07 — One malformed new schedule can stop all scheduling

Trigger creation does not validate cron syntax, time zone, or interval values.
New triggers normally begin without `next_fire_at`.

`arm_unplanned()` processes every new schedule in one transaction. One invalid
trigger raises before the normal scheduler tick runs, aborts the transaction,
and repeats on every scheduler iteration.

Evidence:

- [Trigger creation](../src/windex/source/store.py#L374)
- [Unplanned trigger arming](../src/windex/source/scheduler.py#L121)

### H08 — Source capability validation is not topology-aware

The validator counts ingress and staging modules globally. A disconnected
staging node can make a Pipeline appear Source-capable even when a
document-producing path never reaches platform staging.

Terminal extract or transform paths are not rejected; the special terminal check
only considers nodes whose declared kind is `load`.

Evidence:

- [Source capability validation](../src/windex/pipeline/validation.py#L17)

### H09 — Metadata-only document changes are silently ignored

Document change detection is primarily based on the text hash. Updates to URL,
publication time, language, custom fields, stars, points, memory metadata, and
similar payload values can complete successfully without updating Postgres or
Qdrant.

Evidence:

- [Ledger staging change detection](../src/windex/modules/load.py#L443)

### H10 — Runtime and superseded staging data lack garbage collection

Terminal wire artifacts, coverage files, superseded parquet batches, and
historic download data accumulate.

The current generation contained approximately 3.3 GB of `_pipeline_runs`
staging plus a retained 22 GB failed Wiki download. Existing maintenance only
handles declared `run_artifacts`. Terminal download cleanup has no scheduled
historic sweep, and runtime staging artifacts have no equivalent collector.

Evidence:

- [Runtime wire-artifact storage](../src/windex/modules/common.py#L42)
- [Artifact maintenance](../src/windex/source/scheduler.py#L160)
- [Terminal download cleanup helper](../src/windex/pipeline/run_store.py#L448)

### H11 — macOS reconciliation causes an API request storm

Every control event triggers a complete reload of:

- The module registry
- Global Run history
- Every Source
- Per-Source Run history, settings, triggers, and status
- Every Pipeline revision
- Every flow layout for every revision
- Logs, facets, and Overview

One live admin client generated 14,107 requests in 20 minutes, including about
90 complete refreshes. The cost grows indefinitely as revision and Source
history grows.

Evidence:

- [Full reconciliation](../clients/macos/WindexApp/App/BackendSession.swift#L366)
- [Pipeline and layout loading](../clients/macos/WindexApp/App/BackendSession.swift#L392)
- [Source loading](../clients/macos/WindexApp/App/BackendSession.swift#L441)
- [Control-event reconciliation](../clients/macos/WindexApp/App/BackendSession.swift#L557)

### H12 — macOS can permanently drop reconciliation events

`refreshAll()` returns immediately while another refresh is running, without
recording that another pass is required.

An event or successful mutation arriving after its relevant store was already
loaded can therefore remain invisible until another unrelated event, reconnect,
or foreground refresh occurs.

Evidence:

- [Refresh exclusion](../clients/macos/WindexApp/App/BackendSession.swift#L366)
- [Reconciliation scheduling](../clients/macos/WindexApp/App/BackendSession.swift#L587)

## Medium-priority findings

### M01 — Editing a trigger does not re-arm it

Changing trigger cadence preserves the old `next_fire_at`. The edited schedule
may not take effect until the previous deadline.

Evidence:

- [Trigger update](../src/windex/source/store.py#L400)

### M02 — Source upgrades ignore trigger compatibility

Upgrade preview validates the target Pipeline and Source settings but does not
check existing trigger flow names against the target revision.

An upgrade can remove a configured flow, after which its due trigger repeatedly
fails to submit.

Evidence:

- [Source upgrade planning](../src/windex/source/store.py#L571)

### M03 — Deleting some Source settings is a no-op

Deleting an optional setting without a default removes it from a local copy, but
then calls the patch path. The patch path merges the supplied values back into
the original configuration, reintroducing the deleted key.

Evidence:

- [Source setting deletion](../src/windex/source/store.py#L299)
- [Source setting merge](../src/windex/source/store.py#L268)

### M04 — Source status can hide active Runs

Source status derives `current_run` solely from the highest-ID Run. Sources can
run different flows concurrently because deduplication is per Source and flow.

A newer completed flow can therefore make `current_run` null while an older flow
is still active.

Evidence:

- [Source status Run selection](../src/windex/source/store.py#L789)

### M05 — The Source document-count limit returns the wrong status

Exceeding a Source's document-count limit raises HTTP 413 internally, but the
route's broad exception handler remaps it to HTTP 422.

The separate 64 MiB payload limit is outside the broad handler and correctly
remains 413.

Evidence:

- [Source ingest route](../src/windex/api/canonical.py#L1364)
- [Canonical exception mapping](../src/windex/api/canonical.py#L639)

### M06 — HTTP health always reports `ok`

The HTTP health endpoint does not check Postgres, Qdrant, the embedding gateway,
workers, the scheduler, or module locks.

It is sufficient for contract-epoch pairing, but it is unsafe as a readiness or
service-health signal despite returning `status: ok`.

Evidence:

- [HTTP health response](../src/windex/api/app.py#L172)

### M07 — Every Source projection hardcodes `ready: true`

A Source pinned to unavailable module implementations still appears ready in
the normal Source projection. Module-health endpoints provide separate
diagnostics, but the primary readiness field contradicts them.

Evidence:

- [Source projection](../src/windex/source/store.py#L52)

### M08 — Registry conditional requests are ignored

The registry response includes an ETag, and the macOS client sends
`If-None-Match`, but the backend does not inspect it or return HTTP 304.

The client therefore redownloads and decodes the complete registry during every
full reconciliation.

Evidence:

- [Registry endpoint](../src/windex/api/canonical.py#L672)
- [macOS registry cache](../clients/macos/Packages/WindexKit/Sources/WindexKit/API/RegistryAPI.swift#L56)

### M09 — Pipeline publication permits unsafe concurrent writes

Both parent revision guards are optional. A client can publish without supplying
an expected parent version, parent hash, or `If-Match` value.

Concurrent stale clients then create successive revisions instead of rejecting
the stale writer.

Evidence:

- [Pipeline revision request](../src/windex/api/canonical.py#L161)
- [Pipeline publication route](../src/windex/api/canonical.py#L745)

### M10 — Publishing an old spec does not move the Pipeline head

When a requested semantic hash already exists, publication returns the old
revision but does not update `head_revision_id`.

An attempted semantic rollback can therefore report success while the active
head remains unchanged.

Evidence:

- [Existing-revision publication path](../src/windex/pipeline/store.py#L252)

### M11 — Concurrent macOS searches can display stale results

Search requests have no generation token or cancellation. The search field's
submit handler can create overlapping tasks, allowing an older response to
overwrite a newer query and clear its loading state.

Evidence:

- [macOS search operation](../clients/macos/WindexApp/Views/SearchView.swift#L34)

### M12 — One broken Source can poison the complete Source store

Per-Source history and detail failures are partly tolerated, but settings,
triggers, and status are not isolated. One failed request aborts the entire
Source load and leaves partially updated ETags/triggers alongside stale
deployments.

Evidence:

- [macOS Source loading](../clients/macos/WindexApp/App/BackendSession.swift#L441)

### M13 — One broken Pipeline can poison the complete Pipeline store

One revision-list or decoding failure aborts every later Pipeline. Individual
layout failures are silently ignored, leaving a partially populated layout
cache with no error attached to the affected revision.

Evidence:

- [macOS Pipeline loading](../clients/macos/WindexApp/App/BackendSession.swift#L392)

### M14 — Memory `message_range` is not retained

`message_range` is accepted and placed into the Pipeline document fields, but
the memory parquet mapping omits it. Public search and document response models
also have no field for it.

Evidence:

- [Memory identity mapping](../src/windex/modules/receive.py#L103)
- [Memory parquet mapping](../src/windex/modules/load.py#L136)

### M15 — Primary operator documentation describes the removed stack

The README and runbooks still describe pre-epoch-2 operations, including:

- Per-Source embed loops
- `windex up`, `windex down`, and `windex status`
- Legacy custom Source CRUD and document endpoints
- Apple `container` deployment and launchd supervision
- Metrics and job-control endpoints that no longer exist

These are primary operator-facing documents, so following them on the current
backend produces incorrect commands and API calls.

Evidence:

- [README quickstart](../README.md#L159)
- [Operations runbook](operations.md#L8)
- [Custom Sources handoff](custom-sources.md#L10)

## External integration issue

This was not counted as a Windex application defect.

During the final 70-minute log window:

- A consumer called the removed `/v1/memory/conversations/{id}` endpoint and
  received HTTP 404.
- The canonical `/v1/sources/memory/ingest` endpoint received both a valid HTTP
  202 request and a malformed HTTP 422 request.

At least one running memory consumer or test build had therefore not completely
adopted the epoch-2 ingestion contract.

## Verification performed

- `uv run pytest -q`: 286 passed
- `uv run ruff check src tests`: passed
- `uv run python scripts/dump-openapi.py --check`: passed
- Public health probe: HTTP 200, contract epoch 2
- Public hybrid-search probe: succeeded
- Public invalid-Source probe: HTTP 422
- Live Postgres inspection of Runs, tasks, triggers, documents, and operational
  events
- Live worker, scheduler, API, Prometheus scrape, container resource, and
  filesystem inspection
- Static audit of backend and macOS source

Swift and Xcode were not available on the Spark host, so the macOS findings were
validated through source inspection and live request traces rather than by
rerunning the native test suites during this bash.
