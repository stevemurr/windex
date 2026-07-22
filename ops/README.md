# windex ops — metrics & alerting

windex's metrics console is **self-hosted Prometheus + Grafana running on the
services box (`192.168.1.237`)** — the same host as the LiteLLM gateway. windex
does **not** run its own Prometheus/Grafana; it only *exposes* metrics, and the
existing stack scrapes them. This directory holds the config you paste into that
stack:

| File | What it is |
|------|------------|
| `prometheus/windex-scrape.yml` | Scrape job to add to the services-box Prometheus. |
| `grafana/dashboards/windex.json` | The "windex ops" dashboard (import into Grafana). |
| `grafana/alerting/windex-rules.yml` | The nine alert rules (provision or recreate in UI). |

## The dashboard at a glance

`grafana/dashboards/windex.json` (uid `windex-ops`) is built to answer *what is
broken and when did it break* without hunting:

- **Alerts & uptime** — an **Active alerts** panel listing whatever is
  firing/pending (scoped to `app=windex`), beside an **uptime state-timeline**
  with one row per dependency (gateway / Postgres / Qdrant) and per embed-loop, so
  an outage's blast radius and exact timing are visible at a glance (red = the
  signal read 0 in that interval).
- **Fleet health** — KPI stat tiles: the three dependencies, the query breaker,
  indexing paused, total backlog, embeds/min (goes **red at 0** — the stall
  signature), and a backlog ETA.
- **Throughput / Search / Search quality** — backlog-vs-embedded and embeds/min;
  search rate, degraded+error share, query + query-embed latency, query-embed
  failures; and the `windex eval` NDCG/MRR-by-leg trends (0.8 threshold line =
  the SearchQualityRegression alert).
- **API internals** (collapsed) — HTTP rate, 5xx/4xx error rate, and p95 by
  handler; background-job uptime; gateway probe duration.
- **Source: $source** — a per-source row (repeats over `label_values(windex_documents,
  source)`): backlog vs embedded, embeds/min, loop up/down, and embed-log
  staleness (yellow >5m, red >15m — a wedged-but-alive loop).

The dashboard also carries a **`windex alerts` annotation layer** (Grafana
datasource, tag `windex`) that overlays alert state-changes onto the time panels
*if* Grafana's alert-state annotations are enabled; it is harmless/empty otherwise.

## The exporter

`GET /metrics` on the windex API, served by the always-on `windex serve` process
on **this Mac at `:8100`** (`--host 0.0.0.0`, so it's reachable on the LAN).
Everything is generated at scrape time from Postgres state + the process table +
the filesystem — there is no pushgateway and nothing to run in the CLI jobs. The
exposition is cached ~10s so an aggressive scraper can't multiply DB load.

## Wire up Prometheus (on 192.168.1.237)

1. Paste the `windex-scrape.yml` job into the `scrape_configs:` list of that
   box's `prometheus.yml`. Its target is **this Mac: `192.168.1.231:8100`**.
2. Reload Prometheus: `curl -X POST http://localhost:9090/-/reload` (needs
   `--web.enable-lifecycle`), or send it SIGHUP, or restart it.
3. Confirm at `http://192.168.1.237:9090/targets` — the `windex` target should be
   **UP**. If it's `DOWN` with a *connection refused/timeout* (not a 404), see
   Networking notes below.

## Wire up Grafana (on 192.168.1.237)

**Dashboard:** Dashboards > New > Import > upload `grafana/dashboards/windex.json`
(or paste it). It has a `Prometheus` (`${DS_PROMETHEUS}`) datasource variable, so
Grafana asks which datasource to bind at import — pick the box's Prometheus. No
uid editing needed. The `$source` variable and the repeated per-source row fill
in automatically from `label_values(windex_documents, source)`.

**Alerts:** either
- **provision** — copy `grafana/alerting/windex-rules.yml` into that Grafana's
  `provisioning/alerting/` dir, replace `REPLACE_WITH_PROMETHEUS_DS_UID` with the
  Prometheus datasource uid (`GET /api/datasources`), and restart Grafana; or
- **UI** — Alerting > Alert rules > New, recreate each rule with the same query +
  threshold, picking the Prometheus datasource from the dropdown.

There is no notification channel on this box (no SMTP/webhook), so alerts fire
**in-UI only** on the default contact point. Add a contact point when a channel
exists.

## Networking notes

- **This Mac's IP is DHCP-assigned** (`192.168.1.231` as of 2026-07-19). It can
  change on a new lease/reboot and break the scrape. For stability, add a DHCP
  reservation on the LAN router or a DNS A record, and update the scrape target.
  The mDNS name `MacBook-Pro-33.local:8100` also works from hosts that resolve
  mDNS — but a **containerized** Prometheus usually can't resolve `*.local`, so
  prefer the IP or a real DNS name there.
- **The macOS Application Firewall is ON** on this Mac (block-all and stealth are
  both off). Inbound TCP 8100 from the services box must be allowed: if the
  Prometheus target shows *connection refused/timeout*, allow the `python`/
  `windex serve` binary in System Settings > Network > Firewall, or
  `sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add <python-bin>
  --unblockapp <python-bin>`.

## Metric contract

The dashboard and alerts are built against these series (exposed by
`src/windex/api/prom.py`). Names/labels are a contract — don't rename without
updating both this dir and the exporter.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `windex_documents` | gauge | `source`, `status` | Document rows by source and status (`deduped`, `embedded`, `duplicate`, …). |
| `windex_repos` | gauge | `status` | Repo rows by status. |
| `windex_loop_up` | gauge | `source` | 1 if the source's embed-loop process is alive. |
| `windex_job_up` | gauge | `job` | 1 if a registered non-loop job is alive. |
| `windex_indexing_paused` | gauge | — | 1 if the indexing control flag is `paused`. |
| `windex_stage_busy` | gauge | `key` | 1 if a pipeline stage is not idle. |
| `windex_embed_profile_info` | gauge | `profile` | Always 1; active embed profile in the label. |
| `windex_gateway_up` | gauge | — | 1 if the embedding endpoint accepts a TCP connection. |
| `windex_gateway_probe_duration_seconds` | gauge | — | Duration of the last gateway TCP probe. |
| `windex_query_breaker_state` | gauge | `state` | One-hot query-embed breaker state (`closed`/`open`/`half_open`). |
| `windex_log_last_modified_timestamp_seconds` | gauge | `log`, `source`* | Unix mtime of each `~/.windex/logs/*.log`. *Embed-loop logs also carry a canonical `source` label (the `windex_documents` vocabulary: `embed-ccnews` → `news`, `embed-gh` → `github`); other logs have no `source`. `windex_loop_up{source}` uses the same canonical vocabulary. |
| `windex_db_up` | gauge | — | 1 if the scrape could read Postgres. |
| `windex_qdrant_up` | gauge | — | 1 if Qdrant is reachable. |
| `windex_qdrant_points` | gauge | `collection` | Point count per Qdrant collection. |
| `windex_build_info` | gauge | `version` | Always 1; build version in the label. |
| `windex_search_requests_total` | counter | `mode`, `result` | Search requests (`result` = `ok`/`degraded`/`error`). |
| `windex_search_duration_seconds` | histogram | — | Search request latency. |
| `windex_query_embed_duration_seconds` | histogram | — | Query-embed latency. |
| `windex_query_embed_failures_total` | counter | — | Query-embed failures. |
| `windex_http_requests_total` | counter | `handler`, `method`, `code` | HTTP requests. |
| `windex_http_request_duration_seconds` | histogram | `handler` | HTTP request latency. |
| `process_*`, `python_*` | — | — | Standard prometheus_client runtime series. |

## Alert rules

Nine rules, all carrying `labels.app: windex` (the dashboard's Active-alerts panel
filters on it) and a `severity`. The liveness/outage rules use `noDataState: NoData`
(a scrape that stops is itself a signal); the rate/quality rules use `OK` (no
traffic, or an eval that hasn't run, is not a fault and must not page).

| Rule | Severity | Fires when | For |
|------|----------|-----------|-----|
| `EmbedsStalled` | critical | embed throughput == 0 **and** backlog > 1000 (the 2026-07-17 incident detector) | 15m |
| `GatewayDown` | critical | `windex_gateway_up == 0` | 5m |
| `DbDown` | critical | `windex_db_up == 0` | 5m |
| `QdrantDown` | critical | `windex_qdrant_up == 0` | 5m |
| `LoopDown` | warning | `windex_loop_up == 0` (per source) | 10m |
| `QueryBreakerOpen` | warning | `windex_query_breaker_state{state="open"} == 1` (hybrid degrading to lexical) | 10m |
| `ApiHighErrorRate` | warning | 5xx share > 5% at > 0.1 rps | 10m |
| `SearchErrorRate` | warning | `result="error"` share > 5% (not the benign degraded fallback) | 10m |
| `SearchQualityRegression` | warning | `windex_search_quality_ndcg{leg="known_item"} < 0.8` (baseline ~0.93) | 30m |

The rate/quality rules gate on a minimum request rate and use `clamp_min` on the
denominator so a `0/0` (or a single stray error in a quiet window) can't fire them.

## Search quality (relevance)

Distinct from the latency series above: `windex eval` measures *relevance*
(NDCG@k / MRR / Recall@k) over a known-item proxy + a curated golden set (+ an
optional LLM judge), writes a row to `search_quality`, and the exporter surfaces
the latest run. Runs nightly via the scheduler (`eval` @ 06:30) and on demand
(`windex eval`). Details: `docs/search-overhaul-plan.md` Phase 0.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `windex_search_quality_ndcg` | gauge | `leg` | NDCG@k of the latest eval, by leg (`known_item` \| `golden` \| `judge`). |
| `windex_search_quality_mrr` | gauge | `leg` | MRR of the latest eval, by leg. |

**Grafana panels** — the **Search quality** row of the dashboard plots
`windex_search_quality_ndcg` and `_mrr` with legend `{{leg}}`. The `known_item`
leg is the always-on health line; `golden` tracks the curated regression anchors;
`judge` appears only when the LLM judge is set.

**Alert** — `SearchQualityRegression` (in `windex-rules.yml`) fires when
`windex_search_quality_ndcg{leg="known_item"} < 0.8` for 30m. Tune the 0.8
threshold to the observed baseline (currently ~0.93).
