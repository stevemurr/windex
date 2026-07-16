# windex

Self-hosted web index (CC-News articles + GitHub projects) that search agents query to find
and link to things. Full design/plan: `~/.claude/plans/i-want-to-build-functional-knuth.md`.

## Hard constraints
- **Everything self-hosted and open source.** No proprietary SaaS (no BigQuery, no hosted
  query services). Only external touchpoints: Common Crawl bucket, GH Archive downloads,
  GitHub API for README hydration.
- **This machine uses Apple's `container` CLI, not Docker.** Services via `scripts/dev.sh
  up|down|status|psql`. There is no compose; don't add docker-compose workflows.
- **Bulk downloads and parquet staging live on `/Volumes/External/windex`** (WINDEX_DATA_ROOT).
  Never stage large files on the internal disk or in /tmp.
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
- `scripts/dev.sh up` — start postgres:5432 + qdrant:6333
- `uv run windex init-db` / `health` / `ensure-collections`
- `uv run pytest`

## Conventions
- Stable doc ids: `news:<hash>`, `gh:owner/repo` — these are the public API ids, don't change.
- API contract is /v1, additive-only (see plan Phase 3).
- State transitions are idempotent: every job must be safely re-runnable (watermark tables).
