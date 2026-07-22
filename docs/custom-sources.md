# Custom sources — handoff notes

Branch `feat/custom-sources` (off `origin/main` @ 181671e, 2026-07-22) implements
push-based **custom sources**: dynamic per-corpus indexes (email, notes, …) that
the LLMChat app's agent creates and loads at runtime — no per-corpus migrations.
Authored on the MacBook in a worktree; the LLMChat client side is already built
against the contract below.

## What's on the branch (one commit per slice)

1. **Registry + CRUD API** — `custom_sources` table (schema.sql, additive
   CREATE TABLE IF NOT EXISTS), `custom_source/registry.py` (name regex
   `^[a-z][a-z0-9_]{1,31}$`, reserved = built-in SOURCES ∪ {all, github, gh,
   ccnews, custom}), routes `POST/GET /v1/sources`,
   `GET/PATCH/DELETE /v1/sources/{name}`. Writes bearer-gated
   (`require_write_token`), reads open. The `recipe` jsonb column stores the
   app's refresh recipe server-side so a scheduled refresh needs only the name.
2. **Ingest** — `custom_source/ingest.py`: **upsert + explicit delete** (NOT
   memory's full-replace; absent ids are never implicitly tombstoned).
   Per-batch parquet `staging/custom/<name>/<uuid>.parquet` (tmp+rename),
   text_hash delta ledger (re-push of unchanged docs → `skipped`), generalized
   tombstones. Routes `POST /v1/sources/{name}/docs`,
   `POST /v1/sources/{name}/docs/delete`. Limits: 500 docs/batch, 16k chars
   text, 2KB extra. `memory_source` is untouched.
3. **Embed** — ONE aggregate `custom-embed` loop drains every registered custom
   source (`EMBED_SOURCES["custom"]`, `PUSH_SOURCES = {memory, custom}`);
   `windex custom embed|list|status` one-shots; `ensure-collections` covers
   registered custom collections; `CUSTOM_PAYLOAD_INDEXES` fallback in
   qdrant.py (doc_id keyword + published_at datetime).
4. **Search** — `/v1/search` `source` accepts any registered custom name
   (validate_source: static vocab + ~15s registry TTL cache; unknown → 422,
   preserving the bogus-source contract). Custom sources are **excluded from
   `source=all`** (same personal-data rationale as memory). `RESULT_FIELDS`
   gains `extra`. MCP `search_index` validates the same way.

Test suite: `uv run pytest -x -q` → 225 passed / 295 skipped / 0 failed on the
authoring machine — **Postgres/Qdrant were down there, so every [live-service]
test in `tests/test_custom_sources.py` skipped**. First task on a box with live
dev services: run the full suite (the skips should become passes), then the
curl E2E below.

## Contract the LLMChat client expects (already shipped app-side)

- `POST /v1/sources` `{name, title?, description?, recipe?}` → 200/201
  IndexInfo `{name, title, description, recipe, doc_count, pending, …}`;
  409 duplicate; 422 invalid/reserved.
- `GET /v1/sources` → `{"sources": [IndexInfo…]}`
- `GET /v1/sources/{name}` → IndexInfo (recipe + counts included)
- `PATCH /v1/sources/{name}` `{recipe}` → IndexInfo
- `DELETE /v1/sources/{name}` → `{"deleted": N}` (tombstone-all + collection +
  staging + registry row)
- `POST /v1/sources/{name}/docs` `{"docs": [{id, text, title?, url?,
  published_at?, extra?}]}` → `{…, "staged": N, "skipped": N}`
- `POST /v1/sources/{name}/docs/delete` `{"ids": […]}` → `{"deleted": N}`
- `GET /v1/search?q=…&source=<name>&mode=hybrid&limit=…&published_after=…` →
  usual results shape (+`extra`); `GET /v1/docs/{escaped-id}` for full text.

Keep these shapes stable (additive-only) — the app's `WindexIndexClient`
decodes them.

## Ops to enable

1. `uv run windex init-db` (applies `custom_sources`, idempotent)
2. Start/verify the `custom-embed` loop (`windex up` picks it from JOBS, or
   `windex loop custom on`)
3. `uv run windex ensure-collections`

Known deferred gaps (cosmetic): console freshness/dataset_stats rows key on
real source names, so the aggregate `custom` loop row shows zeroed counts;
`reindex` doesn't know custom names (re-embed = status-reset UPDATE drained by
the loop); parquet compaction of superseded batches.

## E2E curl (once serve is up; add `Authorization: Bearer $TOKEN` to writes)

```sh
B=http://127.0.0.1:8100
curl -sX POST $B/v1/sources -H 'content-type: application/json' -d '{"name":"e2etest","title":"E2E"}'
curl -sX POST $B/v1/sources -H 'content-type: application/json' -d '{"name":"e2etest"}' -o /dev/null -w '%{http_code}\n'  # 409
curl -sX POST $B/v1/sources -H 'content-type: application/json' -d '{"name":"memory"}' -o /dev/null -w '%{http_code}\n'   # 422
D='{"docs":[{"id":"1","title":"Flight","text":"flight confirmation to tokyo","extra":{"from":"airline"}},{"id":"2","title":"Hotel","text":"hotel booking in shibuya"},{"id":"3","title":"Notes","text":"remember the JR pass"}]}'
curl -sX POST $B/v1/sources/e2etest/docs -H 'content-type: application/json' -d "$D"   # staged:3
curl -sX POST $B/v1/sources/e2etest/docs -H 'content-type: application/json' -d "$D"   # skipped:3
uv run windex custom embed
curl -s $B/v1/sources/e2etest                                          # doc_count:3, pending:0
curl -s "$B/v1/search?source=e2etest&q=flight+confirmation"            # hit, carries extra
curl -s "$B/v1/search?source=all&q=flight+confirmation"                # e2etest absent
curl -s "$B/v1/search?source=bogus&q=x" -o /dev/null -w '%{http_code}\n'  # 422
curl -sX POST $B/v1/sources/e2etest/docs/delete -H 'content-type: application/json' -d '{"ids":["1"]}'
curl -sX DELETE $B/v1/sources/e2etest
```

## Branch-state context

The MacBook's main checkout sits on `feat/search-overhaul` (local-only rerank/
deploy/eval commits + a dirty tree); this branch deliberately bases off
`origin/main` and does not touch that work. Reconciliation order is the box
owner's call.
