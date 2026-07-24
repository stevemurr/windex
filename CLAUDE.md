# windex

Self-hosted web index (CC-News articles + GitHub projects) that search agents query to find
and link to things. Full design/plan: `~/.claude/plans/i-want-to-build-functional-knuth.md`.

## Hard constraints
- **Everything self-hosted and open source.** No proprietary SaaS (no BigQuery, no hosted
  query services). Only external touchpoints: Common Crawl bucket, GH Archive downloads,
  GitHub API for README hydration.
- **Production is Linux + rootless podman-compose** on the Spark (DGX GB10), project `windex`,
  via `compose.yaml` + `Containerfile`. The image BAKES the source (`COPY src ./src`), so a
  code change needs a rebuild + recreate, not a restart. `scripts/dev.sh` and Apple's
  `container` CLI are the old macOS dev path — don't treat them as the deployment.
- **All bulk data is on the local NVMe**, never a network share and never /tmp:
  `WINDEX_DATA_ROOT` (default `/home/murr/windex-data`) holds parquet staging + downloads;
  `WINDEX_STACK_DATA` (default `/home/murr/windex-stack`) holds pgdata + qdrant storage.
  compose binds `WINDEX_DATA_ROOT` to the **same path** inside the container, so it means one
  thing everywhere. Staging is the source of truth for document text and is read on the embed
  hot path, so keep it local — it was on a CIFS share until 2026-07-24 and that put a network
  round-trip in that path plus a boot race in the startup sequence.
  `documents.text_ref` is stored RELATIVE to `staging_dir`, so relocating the tree is an rsync
  plus a `WINDEX_DATA_ROOT` change — never a ledger rewrite.
- **The embedding model is user-supplied** (WINDEX_EMBED_* in .env). Never hardcode a model;
  everything flows through the `Embedder` interface (src/windex/embed/). Extracted text and
  embeddings are persisted to parquet so a model swap is re-embed + Qdrant alias flip, never
  a re-crawl.

## Stack
- Python 3.12, `uv` for env/deps (`uv sync`, extras: pipeline/api/st/dev), typer CLI (`windex`).
- Postgres = metadata + state watermarks (warc_files, gharchive_files) + dedup ledgers.
- Qdrant = vectors, one collection per model (`news__<model>`, `repos__<model>`) behind
  aliases `news_current`/`repos_current`. Hybrid = dense (user model) + sparse BM25 (fastembed).
- Pipeline reuses datatrove (FineWeb blocks) — don't hand-roll extraction/quality/dedup.

## Commands
- `podman-compose -p windex -f compose.yaml up -d` — the whole stack (prod)
- `scripts/dev.sh up` — macOS dev only: postgres:5432 + qdrant:6333
- Metrics: `windex serve` exposes Prometheus `/metrics` on :8100, scraped by the user's self-hosted Prometheus/Grafana on 192.168.1.237 (config to paste in `ops/`).
- `uv run windex init-db` / `health` / `ensure-collections`
- `uv run pytest`

## Conventions
- Stable doc ids: `news:<hash>`, `gh:owner/repo` — these are the public API ids, don't change.
- API contract is /v1, additive-only (see plan Phase 3).
- State transitions are idempotent: every job must be safely re-runnable (watermark tables).
