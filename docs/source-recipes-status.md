# Source recipes — status

Plan: `~/.claude/plans/i-want-to-look-cheeky-muffin.md`. Last updated 2026-07-24.
Integrated with frontend `main` through `a0037f8`; 961 tests passing. Production image
`3ebd60f3abd6` is live across all 13 app containers; the full 15-container stack
is healthy with zero restarts.

## Done

| Phase | | |
|---|---|---|
| 0 | `Param` schema, typed request models, `hmac.compare_digest`, operation ids, `dump-openapi.py` | ✅ |
| 1★ | Corpus moved to local NVMe; storage-tier metrics + `StorageLow`; CIFS boot guard retired | ✅ |
| 1 | Additive DDL — recipes, source_units, runs/tasks/units/events, triggers, pauses | ✅ |
| 3 | `/admin/v1` sub-app, auth on by default, fail-closed off-loopback, separate schemas | ✅ |
| 6 | Recipe engine — ports, 42-module registry, parser, `compile_tasks` | ✅ |
| 7a | **All 11 sources registered as recipes**, openable via the API | ✅ |
| 7b | Runner foundation — durable typed edge stream; 6 generic discover/catalog/collect modules | ✅ first slice |
| 8 | Worker pool — claim/lease/slice, lanes, WFQ, slot recycling, memory ceiling | ✅ merged |
| 9 | Trigger scheduler — croniter-backed cron in IANA tz, pause-aware, atomic fire | ✅ merged |
| ~~2, 4, 5~~ | Watermark migration | **removed** — superseded by the reset decision |
| eval | Fixed 50-query docs+hf 8B baseline, five repeats and exact anchor/query hashes | ✅ |

Response schemas cover 39 of 41 admin operations; the two exceptions are SSE
streams, which declare `text/event-stream`. Guarded by tests so the untyped
surface cannot regrow.

## Not done

**Module implementations are in progress.** The runtime now has a crash-safe,
typed edge stream in `task_units.outputs`, including fan-in/fan-out lineage and
replay-safe consumption. Six generic modules execute:
`static.once`, `state.pending`, `list.lines`, `list.json_manifest`,
`list.path_manifest_gz`, and `store.upsert`. The remaining 36 declarations still
resolve to the explicit "declared but not yet implemented" error. This is still
the bulk of the remaining work: moving each source's fetch/extract/load behaviour
out of its package and behind a module.

Then, in order: recipe CRUD writes (`PUT`/`POST`) · runs API + SSE · **reset +
clean ingest** (the reproducibility proof) · marketplace · Swift client (on a Mac)
· delete the web console.

## Live now for the Swift client

Base `http://<host>:8100`. Auth `Authorization: Bearer $WINDEX_WRITE_TOKEN` on
everything under `/admin`.

| | |
|---|---|
| `GET /admin/v1/health` | open — probe before pairing; reports `auth_required` |
| `GET /admin/v1/whoami` | validate a token at setup |
| `GET /admin/v1/registry` | 42 modules, 8 kinds, port lattice. ETag'd. The editor's whole palette |
| `GET /admin/v1/recipes` | all 11 sources |
| `GET /admin/v1/recipes/{name}` | one recipe with its flows/nodes/edges — what the editor opens |
| `GET /admin/v1/recipes/{name}/tasks` | placement per node: lane, deps, preconditions, weight |
| `POST /admin/v1/recipes/validate` | pure, no IO — safe to call per keystroke |
| `GET /admin/v1/settings` | drives `SchemaForm`; real data |
| `/admin/v1/{loops,freshness,activity,jobs,schedule,logs,stats,timeseries}` | real data |
| `GET /v1/search`, `/v1/docs/{id}` | docs + hf are searchable; other source collections await rebuild |

**Search is partial and that is not a client bug.** The docs and hf collections
were rebuilt to secure the model-comparison baseline (18,108 + 3,816 points).
The corpus survived intact (17.49M documents, 31 G of parquet), but the remaining
source vectors are still deferred to the planned rebuild. `reset` now only
deletes collections it owns, proven by test.

Not built yet: recipe writes, runs, marketplace.

## Open decisions

1. **4B comparison** — the 8B side is now banked in
   `docs/eval-baseline-qwen3-8b-docs-hf.json`: 50 fixed anchors, five repeats,
   mean NDCG@10 `0.90872` and MRR `0.8913`. The identical anchors/query hash must
   be used after a 4B subset reindex.
2. **Indexing is paused.** Deliberate — the loops were embedding a backlog that the
   reset discards. Resume with `POST /v1/control/start`.
3. **4B/NVFP4 timing** — the rebuild is the moment to switch, so the corpus is
   embedded once. NVFP4 has already caused one measured regression on the rerank
   path, hence (1).
