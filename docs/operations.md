# windex operations

How to start, run, and operate windex. Human- and agent-facing: the dashboard is
the human control surface, the REST API + `windex` CLI are the agent/headless
surface, and both drive the same service layer. Metrics/alerting live in the
remote Grafana (192.168.1.237); this doc is the operational runbook.

## TL;DR — start it

```bash
windex up          # containers → serve (:8100) → the 8 embed loops, in order
windex status      # what's up (add --json for machine output)
windex down        # stop serve + loops (keeps containers; --stop-containers to stop those too)
```

`windex up` is **idempotent** and **health-gated**: it preflights the external
mount, brings up the postgres/qdrant containers (`scripts/dev.sh up`), waits for
them to answer, applies the schema + Qdrant collections, then starts serve and
the loops — **skipping anything already running**. Re-run it any time; it only
starts what's missing. Flags: `--no-serve`, `--no-loops`, `--source <s>`
(repeatable), `--foreground`, `--timeout <s>`.

For unattended operation you don't run `windex up` by hand — the supervisor does
(see [Supervision & at-boot](#supervision--at-boot)).

## Process map — what should be running

| Process | How it's started | pgrep pattern | Log | Health signal |
|---|---|---|---|---|
| `windex-postgres` container | `dev.sh up` (via `windex up`) | — | `container logs` | `windex status` · TCP :5432 |
| `windex-qdrant` container | `dev.sh up` (via `windex up`) | — | `container logs` | `windex status` · TCP :6333 |
| `windex serve` (REST + dashboard + `/metrics` on :8100) | `windex up` | `windex serve --host` | `~/.windex/logs/serve.log` | `windex status` · `windex_*` scrape |
| 8 embed loops (`windex embed-loop <src>`) | `windex up` | `windex embed-loop <src>` | `~/.windex/logs/<job>.log` | `windex_loop_up{source}` |
| watchdog (supervisor) | launchd agent / `nohup` | `watchdog.sh` | `~/.windex/watchdog.log` | its own heartbeat line |

**Supervised set = serve + the 8 embed loops.** The one-shot ingest/harvest/daily
jobs are not supervised (they run and exit; scheduled via launchd calendar
agents). Sources: `ccnews, gh, wiki, arxiv, smallweb, docs, hn, hf`.

## Health checks

```bash
windex status --json     # authoritative; works even when serve is down. Top-level
                         # "up": bool and "down": [absent supervised members]
windex health            # postgres + qdrant (+ --embed to ping the embedder)
curl -s localhost:8100/metrics | grep -E '^windex_(loop_up|job_up|gateway_up|db_up|qdrant_up)'
```

- **Dashboard**: `http://<box>:8100/` — live stats, per-source counts, log viewer,
  and the operational switches. The header carries a **Metrics ↗** link to Grafana
  when `WINDEX_GRAFANA_URL` is set.
- **Grafana** (192.168.1.237): dashboards + the `LoopDown` / `EmbedsStalled`
  alert rules (`ops/grafana/alerting/windex-rules.yml`). windex only exposes
  `/metrics`; the remote box scrapes it.

## Day-2 ops

| Task | CLI | REST |
|---|---|---|
| Pause / resume indexing | `windex` control flag | `POST /v1/control/{pause,start}` |
| Embed throughput profile | — | `POST /v1/throttle/{polite,full,env}` |
| Start / stop a pipeline job | `windex <job>` | `POST /v1/jobs/{name}/{start,stop}` (whitelist: `jobs.py`) |
| List jobs + running state | `windex status` | `GET /v1/jobs` |
| Rebuild the index from parquet | `windex reindex <source>` | `reindex` job (confirm-gated) |
| Store maintenance (VACUUM; weekly reindex) | `windex maintain [--reindex]` | `maintain` job |

Pause and throttle are read by the embedders at each pass, so they take effect
within ~a minute without restarting anything.

## Supervision & at-boot

The watchdog (`scripts/watchdog.sh`, v4) is the supervisor. When the data plane
is healthy it reads `windex status --json` and restarts any absent supervised
member with idempotent `windex up`. Guards against restart storms: a 2-cycle
debounce, a 5-per-600s rate cap (then alert-only — the Grafana alerts page a
human), and a bounded backstop that stops re-launching a serve that won't stay
up. It still guards the two containers exactly as v3 did (3-failure debounce,
mount-loss guard). On start it waits for the external mount, then `windex up`.

Run it durably via a launchd LaunchAgent:

```bash
bash deploy/install-launchd.sh              # installs the supervisor + calendar agents
bash deploy/install-launchd.sh --uninstall  # removes them
```

- **LaunchAgent, not Daemon** — the external volume, the Apple `container`
  per-user runtime, the venv and `.env` all live in the user session.
- A LaunchAgent runs only in a logged-in GUI session. For the supervisor to come
  up **at boot**, enable **Automatic login** (System Settings → Users & Groups).
- The plist's `PATH` includes `/usr/local/bin` (where `container` lives) — launchd's
  default PATH omits it.

Manual start without launchd: `nohup bash scripts/watchdog.sh &`.

## Recurring schedule

Installed by `deploy/install-launchd.sh` as launchd calendar agents (chosen over
cron: they run a **missed** job on wake, and don't need the deprecated macOS
cron). All jobs are idempotent.

| When | Job |
|---|---|
| 02:15 daily | `windex daily` (news + github freshness) |
| 03:30 daily | arxiv harvest + embed |
| 04:00 daily | smallweb sync + poll + embed |
| 04:30 daily | hn harvest + embed |
| 05:00 Sun | wiki sync + ingest + embed |
| 05:30 daily | docs sync + ingest + embed |
| 05:45 daily | `windex maintain` |
| 06:00 daily | hf sync + crawl + embed |
| 06:15 Sun | `windex maintain --reindex` |

## Log locations

- `~/.windex/logs/serve.log` — the API server (uvicorn).
- `~/.windex/logs/<job>.log` — each embed loop and one-shot job (jobs-registry names).
- `~/.windex/watchdog.log` — supervisor decisions + container health + heartbeat.
- `~/.windex/logs/supervisor.out` — launchd-level stdout/err for the supervisor.
- Rotation: `deploy/newsyslog-windex.conf` (`sudo cp` it into `/etc/newsyslog.d/`).

## Incident playbook

- **A loop or serve died** → the watchdog restarts it within a couple of cycles
  (`~/.windex/watchdog.log` logs the action). If the rate cap tripped, it's
  alerting-only — a member is crash-looping; investigate its log.
- **External mount lost** → the watchdog stops the containers to limit corruption
  and waits for the mount to return, then `dev.sh up`. `windex up` also refuses to
  start if the mount is absent.
- **Embedding gateway outage** → loops never exit; they back off and keep probing,
  self-healing when the gateway returns (the 2026-07-17 lesson). Search degrades
  hybrid→lexical via the query-embed breaker meanwhile.
- **Wedged container** → `dev.sh`'s `run_or_start` recreates a container that
  can't `exec`.
- **Rebuild vectors** → extracted text in parquet is the source of truth:
  `windex reindex <source>` drops the collection and re-embeds; the loops repopulate.

## Agent-oriented surface

An AI agent or script operates windex through the CLI and REST API (same service
core as the dashboard — a GUI is the wrong shape for an agent). Everything below
is machine-drivable:

- **State**: `windex status --json` (top-level `up`, `down`), `GET /v1/stats`,
  `GET /v1/metrics`, `GET /v1/jobs`, `GET /v1/logs` + `GET /v1/logs/{name}`.
- **Control**: `POST /v1/control/{pause,start}`, `POST /v1/throttle/{profile}`,
  `POST /v1/jobs/{name}/{start,stop}` (typed, bounded params — the whitelist in
  `src/windex/api/jobs.py` is the security boundary; the API is LAN-exposed).
- **Search**: `GET /v1/search`, `GET /v1/docs/{id}`; MCP tools `search_index` /
  `get_document` (`windex serve-mcp`).
- **Lifecycle**: `windex up` / `down` / `status` for bring-up and teardown.
