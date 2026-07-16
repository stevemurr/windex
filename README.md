# windex

Self-hosted web index for search agents: CC-News (fresh daily) + GitHub projects (metadata +
README). Hybrid search (dense + BM25, RRF) over Qdrant, metadata/state in Postgres, served
over REST (`/v1`) and MCP. Everything open source and self-hosted; bring your own embedding
model behind an OpenAI/TEI-style endpoint.

## Quickstart

```sh
scripts/dev.sh up          # postgres + qdrant via Apple `container` CLI
cp .env.example .env       # fill in WINDEX_EMBED_* (model, endpoint, dim)
uv sync --all-extras
uv run windex init-db
uv run windex ensure-collections
uv run windex health --embed
```

## CC-News (freshness pattern: backfill + daily incremental, same code)

```sh
uv run windex ccnews sync --days 90     # record pending WARCs
uv run windex ccnews run                # download → extract/filter → dedup → embed
uv run windex ccnews status
```

## GitHub (projects + READMEs, not code)

```sh
# bootstrap candidates from the star-rich GH Archive window (pre 2025-10 Events API change)
uv run windex gh sync-hours --start 2024-10-01 --end 2025-10-01
uv run windex gh scan
# post-2025-10 star discovery via Search API sweeps
uv run windex gh discover --created-from 2025-10-01
# metadata + READMEs (needs WINDEX_GITHUB_TOKENS)
uv run windex gh hydrate
uv run windex gh embed
```

## Serve

```sh
uv run windex serve        # REST: /v1/search, /v1/docs/{id}, /v1/stats (OpenAPI at /docs)
uv run windex serve-mcp    # MCP tools: search_index, get_document
```

## Daily cron

```sh
uv run windex daily        # news sync+process+embed, gh tail+hydrate refresh; idempotent
```

Bulky data (WARC/event downloads, parquet staging) lives under `WINDEX_DATA_ROOT`
(default `/Volumes/External/windex`).

## Reproducibility: rebuild from any layer

Each layer is derivable from the one beneath it; nothing below the vector store
is ever mutated by a rebuild.

| Lost / corrupted | Source of truth | Rebuild |
|---|---|---|
| Vector index (Qdrant) | staged parquet + Postgres ledger | `windex reindex all` then `windex ccnews embed-loop` + `windex gh embed` — no re-crawl, no re-extraction |
| Embedding model swap | same | same (new collection per model, alias flips when ready) |
| Postgres | pg_dump backups + watermark re-sync | restore dump; or re-run `ccnews sync` + `run` (dedup makes re-processing idempotent) |
| Everything | the public datasets | `init-db` → `ccnews sync/run` → `gh sync-hours/scan/discover/hydrate` → embed — the full flow is the recovery procedure |

Battle-tested 2026-07-16: an external-drive detach corrupted the news collection;
`reindex` + the embed loop rebuilt it from parquet with zero re-crawling.
