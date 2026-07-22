# windex: portable podman stack, Spark consolidation & search overhaul

## Context

Two user reports drove this, and the fix converged on a hardware + packaging change:
- **"arXiv 'attention' returns junk."** Verified root cause = **data coverage**: the arxiv corpus ends **2017-12-28** (no transformer-era papers; "Attention Is All You Need" isn't indexed). Plus machinery gaps: the qwen3-embedding **query instruction prefix is empty**, ranking is plain RRF with **no reranker**, the exposed `score` is a meaningless RRF reciprocal, and `source=all` ties rank-1 across collections.
- **Search timeouts >30s.** Verified root cause = **capacity, not compute**: the Mac's Qdrant container is capped at **10GB** (`scripts/dev.sh` `-m 10G`), so the 21GB int8 vector set spills to mmap off a **saturated external SSD** (~5s idle, p95 46s under load). Compounded by a **serial fan-out over 8 collections**, each with a 30s timeout.

Hardware makes the real fix tractable: the user has an **NVIDIA DGX Spark** (GB10 Grace Blackwell, **128GB unified memory**), confirmed to be **192.168.1.237** — the box windex *already* calls for embeddings and which also hosts a **reranker** and an **LLM**. Decision: package windex as a **portable Podman stack** and **consolidate onto the Spark**.

## Target architecture

- **A fully portable Podman stack** — windex app + **Postgres** + **Qdrant** — env-driven, no host assumptions, runnable on macOS or Linux (multi-arch arm64 OCI images).
- **Deployed on the Spark** (the appliance): Postgres + Qdrant + `windex serve` + embed loops, co-located with the already-hosted embedder + reranker + LLM. Search becomes all-loopback (embed → search → rerank).
- **Bulk data** (`WINDEX_DATA_ROOT`, parquet + crawl staging, 100s of GB): stays on the **Mac's external disk for now, NFS-mounted on the Spark**; env-configurable to move to Spark-attached storage later.
- **Postgres is part of the stack** and travels with it (no SQLite — see rationale). The search path is DB-free, but ingest needs PG.
- **Vector store: Qdrant, RAM-resident** (int8, `always_ram=true`, `rescore=true`). **No Milvus/cuVS.**

## Decisions (locked, with rationale)

- **Podman everywhere** (replaces Apple `container`). Runs on macOS + the Spark's aarch64 Linux, Docker-CLI-compatible, rootless, no daemon. *This supersedes CLAUDE.md's "Apple container CLI, not Docker / no compose" constraint — CLAUDE.md + `scripts/dev.sh` + `scripts/watchdog.sh` (which shell out to `container`) must be updated as part of the work.*
- **Keep Postgres; no SQLite.** PG is the ingest backbone — **~27M rows**: `documents` (14.3M — the ledger driving every crawl→dedup→embed status transition + `text_ref` into parquet), `minhash_bands` (12.9M — dedup), per-source resumable harvest watermarks (`arxiv_windows`, `warc_files`, `hn_windows`, `gh_shards`, …), `control`, `schedule`. `search_metrics` is 235 rows — metrics are trivial. **42 concurrent raw-connection writers** (the embed loops + jobs) rule out SQLite's single-writer model, and PG-specific features are used (`percentile_cont`, array `= ANY()`, `VACUUM/REINDEX CONCURRENTLY`). The search path never touches PG (`search.py` is Qdrant-only), so PG is off the query hot path but essential to ingest.
- **Qdrant RAM-resident on the Spark; skip Milvus + cuVS.** The whole corpus is **5.21M vectors × 4096-dim = ~21GB int8** (43GB fp16 / 85GB fp32) — fits in 128GB beside the 3 models. Use **int8** (~30GB resident) for headroom; recall is fine at 4096-dim (`docs/store-tuning.md`); with vectors in RAM, `rescore=true` is a cheap RAM re-read that recovers int8 fine-ranking. The bottleneck was the 10GB **capacity cap** forcing disk mmap — not compute or bandwidth (M1 Max ≈ 400 GB/s already exceeds the Spark's 273 GB/s). RAM-residency alone → tens of ms. **cuVS is wrong here**: needs Milvus (etcd + object store + WAL; unverified aarch64+Blackwell image), would run search on the one GPU already serving all 3 models over the same 273 GB/s bus (vs Qdrant on the 20 idle Grace CPU cores), and by Amdahl only shaves a minority stage — co-location saves more wall-clock. Revisit only at ~10× scale/QPS.
- **Reranker = wiring, not a standup.** The Spark already serves one; add an `HttpReranker` mirroring `HttpEmbedder`.
- **Query rewriting = deprioritized.** Prefix + reranker first; a gated **0.6B** sparse-side expander only if recall gaps remain (the big LLM is ~3.5s/query on the Spark — disqualified from the hot path).

## Distinction that shapes the migration: **rebuild the index, migrate the DB**
- The **Qdrant index is disposable** (rebuildable from parquet-persisted text) → build fresh on the Spark, `always_ram=true`, no copy.
- The **Postgres state is NOT** — it's the crawl/dedup/watermark idempotency; losing it means re-crawling. → **migrate via `pg_dump` → restore** on the Spark.

## Execution model (hybrid)
- **Code/config (Phases 0–3) — on the Mac, committed to the repo.** The `windex eval` harness, relevance fixes, `HttpReranker`, Podman-ization, and config — authored here with native tooling and validated against the running services + `uv run pytest`. This plan is also persisted as a checked-in **`docs/search-overhaul-plan.md`** runbook so the Spark can `git pull` it.
- **Deployment (Phases 3.5–5) — a Claude Code agent running ON the Spark**, which pulls the repo + reads the runbook: preflight repair, `pg_dump`→restore, stand up the Podman stack, mount the bulk-data disk, rebuild the index, arxiv backfill — native filesystem + GPU + mount access there. The Mac session SSHes only for light verification/glue.

## Phases (cheap-first)

> **Every relevance phase below (1, 2, 5, 7) reports a before/after on the Phase 0 harness.** We currently have *no* way to prove a ranking change helped — that's why the yardstick is built first.

### Phase 0 — Search-quality evaluation harness (build FIRST; measure the baseline)
Today windex has **no quality metrics** — `search_metrics` records only latency + the `degraded` flag; no relevance judgments, no click/selection capture, no eval set. Build the yardstick before touching ranking.
- **Eval set — three complementary sources (there is no click data):**
  - **Curated golden set** (~30–100 `(query → relevant doc-id)` pairs across sources, incl. regression anchors like `"attention is all you need" → arxiv:1706.03762`). Deterministic; catches obvious regressions; can gate merges.
  - **LLM-as-judge** — grade `(query, result)` relevance with the Spark's hosted LLM (self-hosted, scales to a larger auto-labeled query set); the **reranker doubles as a relevance scorer**.
  - **Known-item / title-as-query recall proxy** — sample docs, query by title, check the doc ranks in top-k. Label-free index/embedding-health signal that scales to any corpus.
- **Metrics:** NDCG@10, MRR, Recall@k, Precision@k — broken out **by source and by mode** (hybrid/dense/lexical) — plus a known-item-rank distribution.
- **Harness:** a `windex eval` command runs the set through `/v1/search`, computes the metrics, and persists each run (tagged with config/commit) to a `search_quality` table so runs are comparable over time.
- **Periodic job, wired into the control surface** (reuses the scheduler + console built this month): register `eval` as a **`command` schedule entry** — add `eval` to `_SCHED_CMD`/`_SCHED_LOG` in `service.py`, seed a nightly default in `db/_seed_schedule`, add the `windex eval` command — so `windex scheduler` runs it on a cadence and it shows up in the console **Scheduled Jobs editor** (run-now / edit cadence / read logs) and in `windex status`. Quality is measured automatically, not ad hoc.
- **Grafana / Prometheus:** each run updates **Prometheus gauges** (`windex_search_quality_ndcg` / `_mrr` / `_recall`, labeled by source + mode) exported from the `prom.py` collector (read from the latest `search_quality` row); add a **search-quality panel** (NDCG/MRR/Recall trends) to the Grafana dashboard in `ops/`, beside the latency panels, plus an optional **alert** rule when quality drops below a threshold (a relevance regression pages just like `EmbedsStalled`/`LoopDown`).
- **(Optional, later)** a lightweight agent-feedback endpoint (which result was used) to grow the judgment set over time — agents are the consumers, so that's the natural online signal.

### Phase 1 — Relevance quick wins (no infra; ship now on the current Mac Qdrant)
1. **Set the qwen3-embedding query instruction prefix** — empty today (`config.py:55`), off-distribution for an instruction-trained embedder (~1–5% retrieval loss per model card). Set `WINDEX_EMBED_QUERY_PREFIX="Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: "`; already concatenated at `search.py:254`. **One line, highest ROI.**
2. **Fix `source=all` ranking** — normalize per-collection scores before the global sort (they tie rank-1 today): `search.py:297-307`.
3. **Return a meaningful `score`** — surface a normalized similarity, not the raw RRF reciprocal: `search.py:204`, `service.py:61`. Collapse the dead identical fusion branch at `search.py:194-203`.

### Phase 2 — Reranker integration (wire to the Spark's existing model; fixes the precision complaint)
- `Reranker` interface + `HttpReranker` mirroring `Embedder`/`HttpEmbedder` (`embed/base.py`, `embed/http.py`, `build_reranker()` in `embed/__init__.py`), `WINDEX_RERANK_*` config in `config.py`.
- Slot in after the fan-out, before the sort/truncate (`search.py:303-305`); **over-fetch** `limit×K` per collection (`search.py:301`); reuse the embed circuit-breaker/timeout so a rerank failure degrades to the fused order (`search.py:234-269`).

### Phase 3 — Portable Podman stack (do on the Mac first; makes migration a redeploy)
- Replace Apple `container` with **Podman** in `scripts/dev.sh` and `scripts/watchdog.sh`; update CLAUDE.md.
- Define the stack as multi-arch OCI: **Postgres** + **Qdrant** official images + a **windex app image** (aarch64). Orchestrate with Podman (a pod / quadlet / `podman kube` — not docker-compose).
- Make everything env-driven and host-agnostic: `WINDEX_QDRANT_URL`, PG DSN, `WINDEX_DATA_ROOT`, embed/rerank endpoints, `WINDEX_SERVE_HOST`. Verify the identical stack runs on macOS and Linux.

### Phase 3.5 — Pre-migration preflight: verify + repair the ingest state, clean the corpus (GATE before Phase 4)
The Postgres state is the non-rebuildable part, and a live spot-check already found problems — verify and repair **before** `pg_dump` and the rebuild.
- **Reclaim stranded watermarks** — found live: **1 `arxiv_windows` + 32 `warc_files` stuck in `processing`** (killed-mid-harvest during the incident; they silently skip data until reclaimed). Run each source's `reclaim_stale`/status path, clear stragglers, and reset the stale `*_stage` display flags (`control` still shows gh/news mid-work text from the stopped loops).
- **Ledger ↔ index consistency** — confirmed healthy: `documents.status='embedded'` (5,208,001) ≈ Qdrant points (5,208,009). Note the **8.96M `deduped` backlog** (staged, never embedded) → the Spark rebuild/catch-up embeds ~14.2M docs total (big one-time job the Spark is built for). Spot-check for `text_ref` orphans (rows → missing parquet) and `minhash_bands`↔`documents` consistency.
- **Post-restore gate** — after `pg_dump`→restore on the Spark, row counts match and an embed loop **resumes cleanly** against the migrated watermarks (dry-run) before any traffic flips.
- **Unicode poison sweep (all 8 sources)** — the embed-time `strip_smuggled` makes the **rebuilt vectors clean regardless**, but we only measured gh (3 heavy + ~13.7K mild-benign). Scan every source's parquet to quantify; since we're rebuilding, **also strip smuggling from the STORED text on re-stage** (so `/v1/docs` + the LLM-judge see clean text) and sanitize the doc-fetch/eval read paths. Confirm the 3 known gh poison repos embed clean.

### Phase 4 — Spark cutover: RAM-resident index + co-located serve (the latency fix)
- **De-risk first:** `getconf PAGE_SIZE` on the Spark (Grace/aarch64 may be 64KB → known Qdrant/jemalloc issues; use a matching build); confirm the Qdrant arm64 image runs under Podman there.
- **Mount the Mac's external disk on the Spark** (NFS); point `WINDEX_DATA_ROOT` at the mount.
- **Migrate Postgres**: `pg_dump` on the Mac → restore into the Spark's PG container (preserves the 27M-row ingest state).
- **Qdrant collections RAM-resident** (`ensure_collection`, `qdrant.py:88-90`): int8 `always_ram=True`, `on_disk=False`, no memory cap. Flip `rescore=True` (`search.py:178`).
- **Parallelize the 8-collection fan-out** (`search.py:297-303`) + add a **per-query Qdrant `timeout=`** (`search.py:195-203`).
- **Rebuild the index on the Spark**: `windex reindex <source>` (`cli.py:892`) — the Spark's embed loops read parquet from the NFS mount, embed via the local models, upsert to the local Qdrant (all Spark-side). Fold in Phase 5 so it's **one rebuild**.
- **Move `windex serve` to the Spark** — hot path becomes loopback embed→search→rerank (vs ~6.5ms WiFi hops + passage transfer). Search runs on the Grace CPU; GPU stays reserved for the models. Budget: ~30–120ms, sub-100ms target.

### Phase 5 — arxiv coverage: extend 2018→present (folds into the Phase 4 rebuild)
- arxiv is a live **OAI-PMH harvester** (not a dump); nothing bounds it to 2017 — the backfill just stopped and the daily cron only covers a trailing 7 days (`arxiv/harvest.py`). First run `windex arxiv status` to see why incremental never advanced.
- **`windex arxiv harvest --from-year 2018`** (→ present): idempotent per-year windows, auto re-stage + re-embed; rate-limited 1 req/3s (multi-hour crawl). Repair the `arxiv:1706.03762` point-gap (its neighbors exist; the Phase 4 reindex recovers a dropped point).

### Phase 6 — Latency polish
- PG pool (`db/__init__.py:114-123`): raise `max_size`/segment a reserved pool for the metric-write path (fixes the dashboard-path `PoolTimeout`). Sparse BM25 prefetch breadth (`limit×4`, `search.py:189`).

### Phase 7 — Conditional: query expansion (only if recall gaps remain after Phases 1–2)
- Re-measure "attention" after prefix + reranker. If a real recall gap remains: a small **Qwen3-0.6B** doing constrained keyword expansion (3–6 terms) on the **sparse leg only** (`search.py:36-38,295`), gated to short queries, cached, skip-on-timeout — or offline Doc2Query at index time (folds into a rebuild). Not HyDE, not full-rewrite, never the dense query.

## Migration mechanics
- **SSH to the Spark** to stand up the Podman stack (or build/pull the images there). Bulk data via **NFS mount** of the Mac disk (no scp of parquet — the rebuild reads it from the mount).
- **Cutover**: bring the Spark stack up alongside the Mac, migrate PG, rebuild Qdrant + backfill arxiv, validate, then flip `serve`/clients to the Spark. Keep the Mac stack as rollback until validated.

## Verification
- **Portability:** the same Podman stack comes up on macOS and the Spark; `windex status --json` healthy on both.
- **Relevance (measured, not eyeballed):** `windex eval` NDCG@10 / MRR / Recall@k improve phase-over-phase on the Phase 0 harness (golden set + LLM-as-judge + known-item), broken out by source/mode; spot-check "attention", "attention is all you need", "transformer self-attention"; `score` is a real similarity; after Phase 5 `GET /v1/docs/arxiv:1706.03762` resolves and appears top-k for its title.
- **Latency:** `GET /v1/metrics` `search_p95` → sub-100ms after cutover; a cold query is tens of ms; confirm parallel fan-out via `/v1/search` `timings`.
- **DB integrity:** post-`pg_dump` restore, row counts match (`documents` 14.3M, `minhash_bands` 12.9M) and an embed loop resumes cleanly against migrated watermarks.
- **Tests:** `uv run pytest`; add coverage for `source=all` normalization, score semantics, `HttpReranker` + rerank-timeout degradation (mirror `tests/test_embed_*`).
- **Quality is operated, not ad hoc:** the `eval` job appears in the console Scheduled Jobs editor (run-now / edit cadence / read logs), fires on schedule via `windex scheduler`, and the Grafana search-quality panel shows NDCG/MRR/Recall trending after each run (with the regression alert armed).
- **Ops:** self-hosted/OSS throughout; new config = `WINDEX_QDRANT_URL`, `WINDEX_RERANK_*`, `WINDEX_DATA_ROOT` (NFS mount); no Milvus, no cuVS, Podman not docker-compose.
