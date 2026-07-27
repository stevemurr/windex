# Windex epoch-2 operations

This is the production runbook for the canonical Pipeline/Source backend.
Production is the Linux rootless-Podman project in `compose.yaml`; application
source is baked into `localhost/windex-app:latest`.

The steady-state process set is:

| Compose service | Responsibility |
|---|---|
| `postgres` | canonical metadata, immutable revisions, Runs, task leases, watermarks, and document ledger |
| `qdrant` | dense/sparse vectors and searchable payloads |
| `windex-serve` | public search/data API, authenticated admin API, health, and metrics |
| `windex-source-scheduler` | cron/interval/event trigger dispatch, trigger repair, journal retention, and Pipeline storage GC |
| `windex-worker` | one leased, sliced Pipeline worker pool for every Source |
| `windex-module-sandbox` | isolated execution service for approved local Modules |

There are no per-Source ingestion or embedding processes. A Source Run freezes
one Pipeline revision and the worker executes its graph, including the
`platform.index` continuation that makes staged documents searchable.

## First start

Review `.env` before starting. In particular, set the local-NVMe
`WINDEX_DATA_ROOT` and `WINDEX_STACK_DATA`, the embedding endpoint/model/dimension,
and `WINDEX_WRITE_TOKEN`.

```sh
cp .env.example .env
podman-compose -p windex -f compose.yaml build
podman-compose -p windex -f compose.yaml up -d postgres qdrant
podman-compose -p windex -f compose.yaml run --rm windex-serve init-db
podman-compose -p windex -f compose.yaml up -d
```

The one-off `init-db` command is intentionally before the scheduler and worker.
It applies additive schema changes, verifies contract epoch 2/schema generation
2, creates the active filesystem generation, and seeds built-in Pipelines and
Sources. It refuses an unknown or pre-epoch-2 database without modifying it.
The destructive migration for such a database is documented separately in
[the cutover runbook](source-pipeline-cutover-runbook.md).

Useful lifecycle commands:

```sh
podman-compose -p windex -f compose.yaml ps
podman-compose -p windex -f compose.yaml logs --tail 200 windex-serve
podman-compose -p windex -f compose.yaml logs -f windex-worker
podman-compose -p windex -f compose.yaml stop -t 40
podman-compose -p windex -f compose.yaml up -d
```

Stopping containers does not remove Postgres, Qdrant, or corpus data. Do not use
a project teardown as a routine restart.

## Health and compatibility

`GET /admin/v1/health` is unauthenticated so pairing and monitors can always
read compatibility. It remains HTTP 200 and retains `contract_epoch: 2` even
when a dependency is down.

```sh
B=http://127.0.0.1:8100
curl -fsS "$B/admin/v1/health" | jq .
uv run windex health --embed
```

Interpret the response in two independent steps:

1. `contract_epoch == 2` establishes client/backend compatibility.
2. `readiness.ready` establishes serving readiness.

Top-level `status: degraded` includes advisory failures and does not itself mean
the API is incompatible or wholly unavailable. Postgres/schema and Qdrant are
critical. The embedder, worker capacity, scheduler lateness, and frozen Module
locks are advisory because lexical search or unaffected Sources may still be
useful. The snapshot is cached for ten seconds and its summaries are redacted.

For authenticated detail:

```sh
TOKEN="${WINDEX_WRITE_TOKEN:?export WINDEX_WRITE_TOKEN first}"
AUTH="Authorization: Bearer $TOKEN"

curl -fsS "$B/admin/v1/module-health" -H "$AUTH" | jq .
curl -fsS "$B/admin/v1/overview" -H "$AUTH" | jq .
curl -fsS "$B/admin/v1/sources/ccnews/status" -H "$AUTH" | jq .
```

`module-health.status: degraded` means at least one enabled Source is pinned to
a Pipeline revision whose frozen Module implementation is unavailable in this
build. It is not repaired by restarting a worker.

## Metrics and logs

Prometheus scrapes the unversioned, unauthenticated `GET /metrics` endpoint:

```sh
curl -fsS "$B/metrics" | grep -E \
  '^windex_(db_up|qdrant_up|gateway_up|runs|run_tasks|worker_claim_stalled|scheduler_max_lag_seconds|storage_gc_errors)'
```

The exporter reports canonical Source/Run/task state, dependency availability,
search request/error/latency metrics, the query-embedding breaker, storage
headroom, and Pipeline storage cleanup. It deliberately has no synthetic
per-Source process gauges. The checked-in scrape configuration, dashboard, and
alerts are described in [`ops/README.md`](../ops/README.md).

Container stdout/stderr is the process log:

```sh
podman-compose -p windex -f compose.yaml logs --since 1h windex-source-scheduler
podman-compose -p windex -f compose.yaml logs --since 1h windex-worker
podman-compose -p windex -f compose.yaml logs --since 1h windex-module-sandbox
```

Structured application history is in the operational journal:

```sh
curl -fsS "$B/admin/v1/log-events?level=error&limit=200" -H "$AUTH" | jq .
curl -N "$B/admin/v1/log-events/stream?level=error" -H "$AUTH"
```

Use journal facets at `GET /admin/v1/log-events/facets`, or filter by
`component`, `source`, `pipeline`, `run_id`, `node`, `module`, time range, and
text.

## Running a Source

Manual Source execution uses
`POST /admin/v1/sources/{name}/runs`; status and history remain generic Runs.
List the concrete Source deployment and its available Pipeline revision before
starting work:

```sh
source_json=$(curl -fsS "$B/admin/v1/sources/ccnews" -H "$AUTH")
version=$(jq -r .pipeline_version <<<"$source_json")
jq . <<<"$source_json"
curl -fsS "$B/admin/v1/pipelines/ccnews/revisions/$version/tasks?flow=sync" \
  -H "$AUTH" | jq .
```

Queue one Flow:

```sh
run=$(
  curl -fsS -X POST "$B/admin/v1/sources/ccnews/runs" \
    -H "$AUTH" -H 'Content-Type: application/json' \
    -d '{"flow":"sync","priority":50}' |
  jq -r .run_id
)
curl -fsS "$B/admin/v1/runs/$run" -H "$AUTH" | jq .
curl -N "$B/admin/v1/log-events/stream?run_id=$run" -H "$AUTH"
```

HTTP 202 means queued, not completed. A duplicate active Source/Flow may
coalesce, in which case `queued` is false and `run_id` is null. Follow the Run
to `succeeded`; `failed` includes the terminal error and the Run event stream
contains node/module context.

Cancel only an active Run:

```sh
curl -fsS -X POST "$B/admin/v1/runs/$run/cancel" -H "$AUTH"
```

Pulled Pipelines expose these current Flow sequences:

| Source | Order |
|---|---|
| `ccnews` | `sync`, then `ingest` |
| `gh` | `discover`, then `hydrate`, then `compose` |
| `wiki` | `sync`, then `ingest` |
| `smallweb` | `sync`, then `poll` |
| `docs` | `sync`, then `ingest` |
| `hf` | `sync`, then `crawl` |
| `arxiv` | `harvest` |
| `hn` | `harvest` |

Queue dependent Flows only after the prior Run succeeds. Every successful
Source-bound Flow automatically schedules the index continuation; no separate
embedding command is required.

Pause a Source to reject new manual, push, or scheduled Runs without discarding
its configuration:

```sh
curl -fsS -X POST "$B/admin/v1/sources/ccnews/pause" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"reason":"operator maintenance"}'
curl -fsS -X POST "$B/admin/v1/sources/ccnews/resume" -H "$AUTH"
```

Pausing does not cancel an already active Run. Cancel or let it drain
explicitly.

## Triggers

Triggers bind to a Flow on the Source's pinned revision:

```sh
# Daily calendar schedule.
curl -fsS -X POST "$B/admin/v1/sources/hn/triggers" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"flow_name":"harvest","trigger_type":"cron",
       "trigger_spec":{"cron":"30 4 * * *","timezone":"America/Los_Angeles"}}'

# Fixed interval.
curl -fsS -X POST "$B/admin/v1/sources/arxiv/triggers" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"flow_name":"harvest","trigger_type":"interval",
       "trigger_spec":{"seconds":86400}}'
```

Cron expressions have exactly five fields and require an IANA timezone.
Interval seconds are positive integers. Editing cadence or re-enabling a
trigger re-arms it from the transaction time instead of retaining a stale
deadline. A `manual` binding is valid configuration but has no automatic
deadline and is not dispatched by the scheduler. Invalid persisted rows are
disabled and emit `trigger.invalid`.

An event trigger matches an exact operational event name and, optionally, one
exact `source_name`:

```sh
curl -fsS -X POST "$B/admin/v1/sources/docs/triggers" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"flow_name":"sync","trigger_type":"event",
       "trigger_spec":{"event":"run.succeeded","source":"hf"}}'
```

Event cursors are durable and per trigger. A new or materially edited event
trigger starts after the journal tail visible at that moment; it does not
replay historical events. Dispatch advances the cursor atomically with Run
submission, coalesces an already active Source/Flow, and never treats scheduler
bookkeeping as input. Events caused by an event-triggered Run are limited to one
hop to prevent feedback loops. Paused, disabled, or archived Sources skip and
advance rather than accumulating an unbounded replay.

## Pipeline and Source changes

Pipeline revisions are immutable. Publishing against an existing Pipeline must
name the expected head with `parent_version`, `parent_hash`, or one strong
`If-Match` value:

```sh
head=$(curl -fsS "$B/admin/v1/pipelines/custom" -H "$AUTH")
version=$(jq -r .version <<<"$head")
hash=$(jq -r .spec_hash <<<"$head")
revision=$(
  curl -fsS "$B/admin/v1/pipelines/custom/revisions/$version" -H "$AUTH"
)
body=$(jq -c \
  '{spec:.spec,author:"operator",note:"idempotent publication check"}' \
  <<<"$revision")

curl -fsS -X POST "$B/admin/v1/pipelines/custom/revisions" \
  -H "$AUTH" -H "If-Match: \"$hash\"" \
  -H 'Content-Type: application/json' \
  -d "$body"
```

Omitting the guard returns HTTP 428; a stale guard returns HTTP 412. HTTP 201
means a new immutable revision was created. HTTP 200 means the request moved
the head to an existing semantic revision (rollback) or was an idempotent
no-op. Never infer revision creation from a generic 2xx.

A Source stays pinned until an explicit preview and upgrade. The preview
validates settings plus every enabled and disabled trigger Flow, returns the
exact candidate values, and issues a short-lived confirmation token:

```sh
preview=$(
  curl -fsS -X POST "$B/admin/v1/sources/memory/upgrade/preview" \
    -H "$AUTH" -H 'Content-Type: application/json' \
    -d '{"target_version":2,"values":null}'
)
jq . <<<"$preview"
jq -e '.valid == true and .confirmation_token != null' <<<"$preview"

upgrade_body=$(jq -c \
  '{target_version,values:.candidate,confirmation_token}' <<<"$preview")
curl -fsS -X POST "$B/admin/v1/sources/memory/upgrade" \
  -H "$AUTH" -H 'Content-Type: application/json' -d "$upgrade_body"
```

Use the actual `latest_pipeline_version` from the Source's
`/module-status`; version `2` above is illustrative. Submit the preview's exact
candidate. A settings, revision, or trigger edit makes the token stale rather
than silently applying an outdated plan.

## Rebuild and deploy

A code change requires a new image and container recreation. Use this order for
schema, scheduler, worker, Module, and API changes:

1. Quiesce external API writers and the macOS client.
2. Inspect active Runs. Let them finish or cancel them deliberately.
3. Build and test the exact commit.
4. Stop the scheduler, worker, API, and sandbox.
5. Run the new image's `init-db`.
6. Start the new API and sandbox only.
7. Upgrade every Source whose frozen Modules are unavailable.
8. Start the new worker, then the Source scheduler.
9. Verify health, metrics, a bounded Run, and a search result before releasing
   clients.

```sh
git pull --ff-only origin main
B=http://127.0.0.1:8100
TOKEN="${WINDEX_WRITE_TOKEN:?export WINDEX_WRITE_TOKEN first}"
AUTH="Authorization: Bearer $TOKEN"
uv sync --all-extras
uv run pytest
uv run ruff check src tests
uv run python scripts/dump-openapi.py --check
uv run python scripts/dump-openapi.py --which admin --check

podman-compose -p windex -f compose.yaml build
podman-compose -p windex -f compose.yaml stop -t 40 \
  windex-source-scheduler windex-worker windex-serve windex-module-sandbox

podman-compose -p windex -f compose.yaml run --rm windex-serve init-db
podman-compose -p windex -f compose.yaml up -d --no-deps --force-recreate \
  windex-module-sandbox windex-serve

curl -fsS "$B/admin/v1/health" | jq .
curl -fsS "$B/admin/v1/module-health" -H "$AUTH" | jq .
# Preview and upgrade each Source reported above before continuing.

podman-compose -p windex -f compose.yaml up -d --no-deps --force-recreate \
  windex-worker
podman-compose -p windex -f compose.yaml up -d --no-deps --force-recreate \
  windex-source-scheduler
podman-compose -p windex -f compose.yaml ps
```

`serve` also runs idempotent initialization before accepting traffic, but the
explicit one-off is the deployment barrier: it ensures the schema and built-in
revision publication complete before any new scheduler or worker starts.

Module locks are frozen per Pipeline revision. When an in-tree implementation
changes (for example `push.docs` or `ledger.stage`), `init-db` publishes a new
built-in revision because the semantic hash includes implementation locks.
It intentionally leaves Sources pinned. Starting workers before upgrading those
Sources produces `module_revoked` failures; restarting without the upgrade
cannot fix that state. Historic active Runs retain their old locks, so either
finish them with a matching old worker before the deployment or cancel and
rerun them on the upgraded Source.

The current schema includes durable event-trigger cursors, metadata fingerprints
for payload-only refreshes, memory message ranges, and DB-aware Pipeline storage
cleanup. The current API also adds dependency readiness and mandatory Pipeline
head preconditions. Consequently, a coordinated release must include:

- backend schema initialization before scheduler/worker start;
- Source re-lock/upgrade for every Pipeline whose Module digest changed;
- macOS OpenAPI/DTO regeneration whenever either checked-in OpenAPI document
  changes;
- client pairing based on `contract_epoch`, then readiness—not top-level
  `status == "ok"`;
- memory-consumer use of the canonical ingest endpoint and Run polling; and
- a complete re-push of old conversations if historical `message_range` values
  must become searchable.

The memory request and repair procedure is in
[`docs/memory-ingest-contract.md`](memory-ingest-contract.md).

## Storage retention

The Source scheduler runs bounded Pipeline storage cleanup at
`WINDEX_PIPELINE_GC_INTERVAL_SECONDS`. A candidate file is removed only when:

- its owning Run is terminal and older than
  `WINDEX_PIPELINE_GC_TERMINAL_RETENTION_SECONDS`;
- the file is older than `WINDEX_PIPELINE_GC_MIN_FILE_AGE_SECONDS`;
- no active Run, task output, capture, coverage row, or
  `documents.text_ref` references it; and
- it resolves under a managed staging/download root and is not a symlink.

`keep: true` downloads, unknown ownership, active data, run artifacts, and paths
outside managed roots are retained. Each pass is capped by
`WINDEX_PIPELINE_GC_MAX_FILES_PER_TICK` and
`WINDEX_PIPELINE_GC_MAX_BYTES_PER_TICK`. Errors are isolated for retry and
reported in `windex_storage_gc_*` metrics plus `storage.gc.completed` events.
There is no cleanup HTTP command; keep the scheduler running.

## Incident checks

- **Search returns HTTP 503 for one Source** — that Source's Qdrant index is
  unavailable. Retry with backoff; do not convert it to an empty successful
  result. `source=all` may return explicit partial degradation if other Sources
  remain available.
- **Ready work but no progress** — inspect
  `windex_worker_claim_stalled`, expired leases, worker logs, and the Run's
  tasks/events. Restarting is not a substitute for resolving unavailable
  Module locks.
- **Scheduler lag** — inspect due triggers, scheduler logs, and
  `trigger.invalid`; malformed legacy rows are quarantined instead of poisoning
  the tick.
- **Embedding gateway down** — indexing waits/backs off and hybrid search
  degrades to lexical through the query breaker. Confirm
  `windex_gateway_up`, then the Run/task state.
- **Low storage** — ingestion refuses new staging before consuming the reserve.
  Check both storage tiers and GC metrics; never manually delete a path still
  referenced by `documents.text_ref`.
- **Memory push returned HTTP 202 but data is absent** — poll the returned Run.
  HTTP 202 only confirms queueing. Envelope and boundary errors that the API
  can determine are synchronous HTTP 422, but Module-level validation can still
  fail the accepted Run. Identity, Module-lock, and embedding failures remain
  visible on that Run.
