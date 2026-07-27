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

## Remediation ledger

Status: **27/27 original findings resolved in code.** Resolved means the focused
fix and regression coverage are merged; live activation still requires the
coordinated release below. Commit links identify the focused fix commits rather
than their merge commits.

The expanded backend suite passed **450/450** at backend integration head
`9871fd4`. Ruff, both OpenAPI reproducibility checks, canonical/schema parity,
Compose validation, operator-document checks, and tracked JSON/YAML parsing
also passed. All later changes are macOS-only. Swift coverage is checked in, but
package and native Xcode tests remain a release gate on a Mac.

### High-priority findings

| ID | Focused fix and result | Release and consumer impact |
|---|---|---|
| H01 | [`6f3997b`](https://github.com/stevemurr/windex/commit/6f3997b849424efe570727b2d499f26059e5ec4f) — Wiki extraction now commits bounded decoded-byte checkpoints and resumes through a persisted bzip2 block index. | Rebuild `indexed-bzip2`; run `init-db` and upgrade affected Sources because `extract.py` Module locks change. |
| H02 | [`4e13b31`](https://github.com/stevemurr/windex/commit/4e13b3170bf254b8830b6d987d030ec31f2aae29) — finite hard deadlines terminate and recover non-cooperative slices, cancellation, and shutdown. | Rebuild and restart workers. No client contract change. |
| H03 | [`8fd3470`](https://github.com/stevemurr/windex/commit/8fd34704545a3bd71f1dc412d89e4ebd007eb9f2) — restored `/metrics`, HTTP RED instrumentation, canonical-state collectors, dashboard, and alerts. | Rebuild the API and provision the current Prometheus/Grafana assets. |
| H04 | [`1181166`](https://github.com/stevemurr/windex/commit/118116689fceb411f6a291f12e02e7d529b66bea) — tombstones retain a durable vector marker until Qdrant confirms deletion, so `platform.index` can retry. | `load.py` relock and Source upgrades required. A one-time audit may be useful for ghosts created by pre-fix failures. |
| H05 | [`b587aa7`](https://github.com/stevemurr/windex/commit/b587aa76d703df16214b6d8ba48f2037622b9764) — unavailable explicit Sources and total fan-out failure are HTTP 503; partial `source=all` results are degraded. | macOS must surface degraded/unavailable search. Memory search consumers must treat HTTP 503 as retryable. |
| H06 | [`aebba7f`](https://github.com/stevemurr/windex/commit/aebba7fed774e48fb73923fb27f5fb88f10431ab) — event triggers consume the journal through durable cursors with atomic submission, fairness, idempotency, and one-hop loop suppression. | Additive cursor-table migration. Quiesce old event writers and the scheduler during `init-db`; restart only the new image. |
| H07 | [`0664f44`](https://github.com/stevemurr/windex/commit/0664f448160b7689d610e2c7de7d6d241c205c8e) — trigger writes validate type-specific values; invalid persisted rows are isolated and quarantined. | Rebuild API/scheduler. Clients can receive structured HTTP 422 for invalid trigger fields. |
| H08 | [`21b0624`](https://github.com/stevemurr/windex/commit/21b062467d23949c604aa9edde5025978bfd1177) — Source capability requires a real ingress-to-staging path and rejects document-producing terminal branches. | Invalid Pipeline publication can now fail validation. No DTO change. |
| H09 | [`c1360e9`](https://github.com/stevemurr/windex/commit/c1360e99158046c0ba5f8ea84e22441436cd5fda) — metadata fingerprints and acknowledgements drive payload-only Qdrant refresh without re-embedding unchanged text. | Additive schema plus `load.py` relock. Re-run or re-push documents whose earlier metadata corrections must be repaired. |
| H10 | [`2ac8f0e`](https://github.com/stevemurr/windex/commit/2ac8f0e705e0c6f8546809a29ab2b035d4246353) — scheduler maintenance safely collects terminal staging, superseded batches, coverage files, and retained downloads under caps. | Rebuild scheduler/API and review the new GC environment defaults and metrics. |
| H11 | [`b73d933`](https://github.com/stevemurr/windex/commit/b73d933128c6bf052de6aeed4821aa175e236027) — macOS classifies control events and reconciles only unique affected resources. | macOS rebuild and native tests required. No backend or memory-consumer change. |
| H12 | [`03ecc26`](https://github.com/stevemurr/windex/commit/03ecc26638c19eb8ee718b884a26306b282f2fbd) — reconciliation drains mid-pass invalidations, coalesces storms, lets full refresh dominate, and rejects stale lifecycle work. | macOS rebuild and native tests required. No backend or memory-consumer change. |

### Medium-priority findings

| ID | Focused fix and result | Release and consumer impact |
|---|---|---|
| M01 | [`d717ea7`](https://github.com/stevemurr/windex/commit/d717ea77c995511ac6bef8502fde2065339505fb) — cadence edits and disable/re-enable transitions re-arm schedules transactionally. | Rebuild API/scheduler. macOS edits take effect immediately without a client update. |
| M02 | [`5f27574`](https://github.com/stevemurr/windex/commit/5f27574287ac8eae088bc976170e2f5dac08c745) — Source upgrade preview/submission rejects target revisions that remove trigger-bound Flows. | Rebuild API. Existing macOS upgrade UI can surface the structured issue. |
| M03 | [`d3086b1`](https://github.com/stevemurr/windex/commit/d3086b1f2e0eb64b75d385749197ef02e2d74d01) — optional Source setting deletion performs exact replacement instead of merging the key back. | Server-side behavior fix only. |
| M04 | [`1be42f0`](https://github.com/stevemurr/windex/commit/1be42f05e332062498f14d2768f3b681cb3a5564) — Source status selects the newest active Run across Flows. | Server-side behavior fix; macOS status becomes truthful without a DTO update. |
| M05 | [`d728da0`](https://github.com/stevemurr/windex/commit/d728da0cb48fd68ce7c54bdae469f157b1330911) — ingest preserves intentional HTTP statuses, including 413, and no longer disguises internal failures as validation errors. | Memory consumers must split or reduce batches on HTTP 413. |
| M06 | [`16c74a3`](https://github.com/stevemurr/windex/commit/16c74a36f7da517ad1396e0a455e6b39c1728b0f) — health reports cached, redacted dependency readiness while retaining HTTP 200 and `contract_epoch=2`. | Pair by epoch first, then inspect optional readiness. The coordinated Swift DTO update below is mandatory. |
| M07 | [`110d86f`](https://github.com/stevemurr/windex/commit/110d86f8ebfe430d1b2a0f38cda9c6ea2b8f763c) — Source projections batch-check frozen locks and report `ready: false` for unavailable implementations. | macOS may now truthfully show a Source as not ready; no client code change. |
| M08 | [`0f02b12`](https://github.com/stevemurr/windex/commit/0f02b12eb6b21d61e6364ae2e884893c9f152e35) — registry GET honors strong/weak `If-None-Match` and returns an empty HTTP 304. | Existing macOS registry caching benefits automatically. |
| M09 | [`2f343a6`](https://github.com/stevemurr/windex/commit/2f343a6469f08533ded91f467d514d4cdbd12cea) — publication of an existing Pipeline requires a parent guard and rejects stale or contradictory writers. | Pipeline clients must handle HTTP 428/412 and send parent version/hash or strong `If-Match`; current macOS already does. |
| M10 | [`08b0fcc`](https://github.com/stevemurr/windex/commit/08b0fcc100b34219a31b9a7a5be82a95d16bea27) — publishing an existing semantic revision atomically moves the head; 201 means created and 200 means rollback/no-op. | Pipeline clients must accept both successful 200 and 201 responses. |
| M11 | [`0ba93fd`](https://github.com/stevemurr/windex/commit/0ba93fdb077d8c85bd5d386496b53f567119af56) — macOS search ownership generations prevent stale responses or errors from replacing newer input. | macOS rebuild and native tests required. |
| M12 | [`5ae848a`](https://github.com/stevemurr/windex/commit/5ae848a294be96020f5fdb947930ef9b439e93f5) — macOS assembles each Source projection atomically, preserves exact last-known-good state, and attaches scoped diagnostics. | macOS rebuild and native tests required. No backend, DTO, or memory-consumer change. |
| M13 | [`179909e`](https://github.com/stevemurr/windex/commit/179909e3f2364edd9ea553567fe5566b92407f91) — macOS isolates Pipeline/revision/layout failures, validates response identity and membership, retains only complete exact snapshots, and recovers diagnostics per Flow. | macOS rebuild and native tests required. No backend, DTO, or memory-consumer change. |
| M14 | [`afdcf49`](https://github.com/stevemurr/windex/commit/afdcf49a8e67227b0302eb03a9f1861a8c60258a) — memory `message_range` survives validation, parquet, Qdrant payload, search, and document detail. | Rebuild/relock and upgrade memory. Fully re-push historical conversations only if their old ranges must be populated. |
| M15 | [`77a11f3`](https://github.com/stevemurr/windex/commit/77a11f33d0ea3d18b58662761bfbce949421574d) — primary operator, Source, cutover, and memory documentation describes only epoch 2. | Documentation-only; macOS and memory teams should use the updated handoffs. |

### Post-bash release finding — strict Swift health decoding

This is a release finding, not a 28th original bug-bash finding.

[`4f52792`](https://github.com/stevemurr/windex/commit/4f5279204f5bc4ecf412ac3021ca4f9e940c4b47)
regenerates the macOS admin DTOs for M06 readiness. The generated `Health`
decoder enforces `additionalProperties: false`; a pre-M06 WindexKit build
therefore rejects a readiness-bearing health response instead of ignoring it.
The fix adds typed readiness structures and healthy/degraded pairing fixtures.
Shipping the regenerated macOS DTO is a hard cutover gate. Any memory consumer
with an equally strict health decoder must also accept optional `readiness`.

## Coordinated activation

- Rebuild from the final exact `main` SHA. Do not clear Postgres or Qdrant.
- Stop automatic submissions, finish or cancel old Runs, and stop the old
  worker before applying the new image's additive `windex init-db`.
- Changed runner files affect frozen implementation digests. All nine deployed
  Sources must be previewed and upgraded before the new worker and scheduler
  start.
- Start the new API and Module sandbox first, check epoch/readiness and Module
  health, upgrade the Source fleet, then start the worker and scheduler.
- Validate `/metrics`, readiness, Module locks, recent errors, a bounded Run and
  search, and a synthetic memory push/search/delete cycle.
- Requeue cancelled GitHub, Hacker News, and Wiki work after Source upgrades.
  Let the CC-News interval trigger resume its work without a duplicate manual
  submission.

## Consumer notifications

The macOS team must pull final `main`, regenerate the checked-in DTOs
byte-for-byte, run WindexKit package tests and native Xcode tests, verify both
OpenAPI artifacts, and perform the active-code legacy scan.

Memory consumers must:

- use only `POST /v1/sources/memory/ingest`;
- provide `partition` for an empty full-set deletion;
- poll the returned Run after HTTP 202;
- split or reduce batches on HTTP 413 and retry search HTTP 503;
- pair on `contract_epoch=2` before interpreting optional readiness; and
- accept optional `message_range`, re-pushing history only when backfill is
  required.

The observed caller of removed `/v1/memory/conversations/{id}` remains an
external consumer migration issue. No legacy endpoint should be restored.
