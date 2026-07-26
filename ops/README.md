# Windex operations metrics and alerting

Windex exposes Prometheus metrics from the always-on epoch-2 API process. The
self-hosted Prometheus and Grafana services run outside this compose project and
scrape `GET /metrics` on port 8100.

| File | Purpose |
|---|---|
| `prometheus/windex-scrape.yml` | Prometheus scrape job |
| `grafana/dashboards/windex.json` | Importable `windex ops` dashboard |
| `grafana/alerting/windex-rules.yml` | Provisionable Grafana alert rules |

## Exporter behavior

`GET /metrics` is an unversioned, unauthenticated operations endpoint. It is
deliberately outside the public and admin OpenAPI contracts, even when the admin
API requires a token. The endpoint:

- exports standard Python/process metrics and cumulative HTTP/search metrics;
- computes point-in-time state from canonical Sources, Runs, tasks, Source
  units, Postgres, Qdrant, the embedding gateway, and local storage;
- caches a rendered page for ten seconds to bound scrape load;
- returns a valid page when Postgres or Qdrant is down, setting the corresponding
  `windex_*_up` gauge to zero;
- excludes its own scrape from HTTP request counters; and
- uses FastAPI route templates, never document IDs or other raw paths, as HTTP
  metric labels.

Epoch 2 has one leased Pipeline worker pool. It does not have per-Source ingest
or embedding loops, so the exporter intentionally does not recreate
`windex_loop_up`, `windex_job_up`, or other process-loop gauges from the retired
stack.

## Prometheus setup

Add `prometheus/windex-scrape.yml` to the external Prometheus
`scrape_configs`, set its target to the Spark's reachable port 8100 address, and
reload Prometheus. Confirm that the `windex` target is `UP` in Prometheus before
importing the dashboard.

The API container must bind port 8100 to the host. `compose.yaml` already does
this for `windex-serve`.

## Readiness contract

`GET /admin/v1/health` is an unauthenticated, cached liveness/capability
response. It always remains HTTP 200 and carries `contract_epoch`, including
during an outage, so a temporary dependency failure cannot masquerade as an
incompatible backend during macOS pairing.

The additive `readiness` object checks Postgres and its schema metadata, Qdrant,
embedding configuration/reachability, work-sensitive worker capacity, Source
scheduler lateness, and enabled Sources' frozen Module locks. Postgres/schema
and Qdrant are **critical**: their failure sets `readiness.ready` to false.
Embedding, workers, scheduler, and Module locks are **advisory** because lexical
search or unaffected Sources remain useful. Any unhealthy component sets the
top-level `status` to `degraded`, including advisory failures. Summaries are
static/redacted and observations contain only bounded counts and booleans.
Snapshots are cached for 10 seconds to keep pairing and probes from creating a
dependency thundering herd.

## Grafana setup

Import `grafana/dashboards/windex.json` and bind `${DS_PROMETHEUS}` to the
external Prometheus datasource. The Source variable is populated from
`label_values(windex_documents, source)`.

For alerts, either copy `grafana/alerting/windex-rules.yml` into Grafana's
provisioning directory or recreate the rules in the UI. Replace
`REPLACE_WITH_PROMETHEUS_DS_UID` with the actual Prometheus datasource UID.

## Metric contract

Metric names and labels are consumed by the checked-in dashboard and alert
rules. Changes must update all three together.

### Canonical runtime state

| Metric | Labels | Meaning |
|---|---|---|
| `windex_documents` | `source`, `status` | Document ledger rows by public Source name and canonical state (`staged`, `embedding`, `searchable`, `failed`, `deleted`) |
| `windex_embeds_per_minute` | `source`, `window` | Documents made searchable per minute over the trailing `2m` or `10m` window |
| `windex_repos` | `status` | GitHub repository rows by state |
| `windex_source_units` | `source`, `store`, `status` | Canonical Source watermark units |
| `windex_runs` | `state` | Runs by lifecycle state |
| `windex_run_tasks` | `source`, `lane`, `state` | Non-terminal Pipeline tasks |
| `windex_task_running_age_seconds` | `source`, `lane` | Age of the oldest running task in each group |
| `windex_task_heartbeat_age_seconds` | `source`, `lane` | Age of the stalest heartbeat in each running group |
| `windex_worker_expired_leases` | — | Running task leases already past expiry |
| `windex_worker_claim_stalled` | — | `1` when a lease expired, or ready work exists without a heartbeat within 60 seconds |
| `windex_scheduler_due_triggers` | — | Enabled cron/interval triggers past their planned fire time |
| `windex_scheduler_max_lag_seconds` | — | Age of the most overdue trigger |

The worker claim gauge is deliberately work-sensitive: an idle worker with no
ready tasks is not reported as failed. Use container health in addition to this
metric when process liveness independent of work availability is required.

### Dependencies and storage

| Metric | Labels | Meaning |
|---|---|---|
| `windex_db_up` | — | Canonical Postgres answered the scrape |
| `windex_qdrant_up` | — | Qdrant answered the scrape |
| `windex_qdrant_points` | `collection` | Point count per physical collection |
| `windex_gateway_up` | — | Embedding endpoint accepted a TCP connection |
| `windex_gateway_probe_duration_seconds` | — | Duration of the cached gateway probe |
| `windex_storage_ok` | `tier` | Storage exists, is writable, and is above its reserve |
| `windex_storage_free_bytes` | `tier` | Available storage bytes |
| `windex_storage_total_bytes` | `tier` | Total storage bytes |
| `windex_storage_min_free_bytes` | `tier` | Configured free-space reserve |
| `windex_storage_gc_last_run_timestamp_seconds` | — | Latest DB-recorded Pipeline storage cleanup pass |
| `windex_storage_gc_deleted_files` | `kind` | Files removed in the latest cleanup pass |
| `windex_storage_gc_deleted_bytes` | `kind` | Bytes removed in the latest cleanup pass |
| `windex_storage_gc_errors` | — | Isolated errors in the latest cleanup pass |
| `windex_storage_gc_cap_reached` | `cap` | Latest pass reached its `files` or `bytes` safety budget |
| `windex_query_breaker_state` | `state` | One-hot query embedding breaker state |
| `windex_build_info` | `version` | Build identity |

The Source scheduler also performs DB-aware cleanup of Pipeline-owned
transients. It considers wire and coverage files, sliced extract/fetch scratch,
`<source>/pipeline/<run>/` parquet batches, and disposable `http.download`
outputs. A file is removed only when its Run is terminal and past
`WINDEX_PIPELINE_GC_TERMINAL_RETENTION_SECONDS`, the file itself is past
`WINDEX_PIPELINE_GC_MIN_FILE_AGE_SECONDS`, and no active Run, task output,
capture, coverage row, or `documents.text_ref` references it. `keep: true`
downloads, unknown ownership, symlinks, paths outside the managed roots, and
the separately retained `run_artifacts` store are never collected by this
pass. File and byte budgets bound each pass; isolated failures are retried on a
later pass and recorded as `storage.gc.completed` operational events.

### Request and search events

| Metric | Labels | Meaning |
|---|---|---|
| `windex_http_requests_total` | `handler`, `method`, `code` | HTTP requests by bounded route template |
| `windex_http_request_duration_seconds` | `handler` | HTTP request latency |
| `windex_search_requests_total` | `mode`, `result` | Search results (`ok`, `degraded`, or `error`) |
| `windex_search_duration_seconds` | — | End-to-end search latency |
| `windex_query_embed_duration_seconds` | — | Query embedding latency |
| `windex_query_embed_failures_total` | — | Query embedding failures |
| `windex_search_quality_ndcg` | `leg` | NDCG@k from the latest evaluation |
| `windex_search_quality_mrr` | `leg` | MRR from the latest evaluation |

## Primary alerts

- `EmbedsStalled`: staged backlog exceeds 1,000 while the ten-minute searchable
  rate remains zero.
- `WorkerClaimsStalled`: ready work exists without a recent task heartbeat.
- `GatewayDown`, `DbDown`, and `QdrantDown`: dependency probes remain down.
- `StorageLow`: a storage tier is missing, read-only, or below its reserve.
- `QueryBreakerOpen`: hybrid search is persistently falling back to lexical.
- `ApiHighErrorRate` and `SearchErrorRate`: sustained error ratios above their
  traffic-gated thresholds.
- `SearchQualityRegression`: latest known-item NDCG falls below the configured
  baseline.

No alert delivery channel is configured by these files; configure a Grafana
contact point and notification policy separately.
