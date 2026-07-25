# Source recipes — status

Last updated 2026-07-25.

## Outcome

The recipe backend is executable end to end. All 42 registered modules have
in-tree runners, all 11 built-in sources compile without unavailable modules,
and the generic run API executes their real discover, fetch, catalog, extract,
transform, collect, and load behavior.

The clean reset is complete. It removed 17,493,427 ledger documents, run/task
history, source watermarks, Windex-owned Qdrant collections, and transient
staging/download artifacts while preserving settings, recipes, schedules, and
custom-source registration. The replacement corpus is being built only through
the recipe runners.

The embedding service now runs `Forturne/Qwen3-Embedding-4B-NVFP4` as
`qwen3-embedding-4b` with 2,560-dimensional vectors. The fixed 8B baseline is
banked in `docs/eval-baseline-qwen3-8b-docs-hf.json`; the paired 4B result is
recorded in `docs/eval-comparison-qwen3-4b-docs-hf.json`. Across five exact,
rerank-off repeats, 4B measured mean NDCG@10 `0.90636` and MRR `0.88800`,
versus 8B's `0.90872` and `0.89130`. The deltas (`-0.26%` and `-0.37%`) are
inside normal repeat variance; Hit@10 was unchanged, and HF improved. The
production decision is therefore to keep 4B.

## Shipped backend

| Area | Status |
|---|---|
| Registry | 42 implemented modules, 8 kinds, closed port lattice |
| Recipes | 11 built-ins; list/open/validate/config/revision operations |
| Runs | submit/list/detail/cancel, JSON events, typed SSE |
| Runtime | durable typed edges, fan-in/fan-out, leases, yielding, WFQ lanes |
| Source state | durable frontiers, watermarks, stale leases, bounded run snapshots |
| Safety | host allowlists, SSRF checks, census guards, partial-run prune protection |
| Marketplace | inert catalogs, install/update metadata, executable-state reporting |
| Native client | pairing, recipe editor, Galley, marketplace, and operations screens |

Notable runtime guarantees proven during the clean rebuild:

- module and recipe defaults are frozen into both node config and run params;
- a lock forbids changing a field but does not reject the frozen field itself;
- GitHub and arXiv pagination use bounded daily resume units;
- crawl BFS frontiers persist per URL across worker yields;
- crawl page-budget or fetch failures propagate an incomplete-census marker, so
  partial results may stage but may not tombstone unseen documents;
- Hugging Face roots are atomic replace boundaries, while completed fetch units
  checkpoint before the next long unit starts;
- CC News uses monthly manifests and a true run-wide WARC cap;
- zero-page or missing Hugging Face manifests remain observable without forming
  a permanent crawl loop.

## Clean rollout

| Source | Clean recipe path |
|---|---|
| docs | 18,108 documents rebuilt and embedded |
| hf | 831 posts + 2,985 documentation pages rebuilt for the fixed comparison pool |
| ccnews | monthly manifest sync; bounded 8-WARC ingest verified |
| wiki | current dated Cirrus shard catalog and real shard download verified |
| smallweb | 36,271-feed frontier; dead TLS/expired feeds isolated per unit |
| hn | historical window harvest active |
| gh | daily Search API frontier + Archive tail active; hydrate/compose follow discovery |
| arxiv | daily OAI frontier active |
| memory/custom/crawl | push/crawl runners available; crawl frontier is crash-resumable |

Historical backfills continue as ordinary durable runs. They are not prerequisites
for the docs+HF model comparison and survive worker/container restarts.

## Live API for the Swift client

Base `http://<host>:8100`. Everything under `/admin` requires
`Authorization: Bearer $WINDEX_WRITE_TOKEN`, except the pairing health probe.

| Endpoint | Purpose |
|---|---|
| `GET /admin/v1/health` | pairing probe and `auth_required` |
| `GET /admin/v1/whoami` | validate a paired token |
| `GET /admin/v1/registry` | module palette, kinds, ports, placement |
| `GET /admin/v1/recipes` | all registered recipes |
| `GET /admin/v1/recipes/{name}` | one recipe and its flow DAGs |
| `GET /admin/v1/recipes/{name}/tasks` | compiled placement and preconditions |
| `POST /admin/v1/recipes/validate` | pure validation |
| `POST/PUT /admin/v1/recipes` | validated writes and revision history |
| `/admin/v1/runs` | submit/history/detail/cancel/events/SSE |
| `/admin/v1/marketplace` | bundled and operator-mounted catalogs |
| `GET /admin/v1/settings` | SchemaForm data |
| `/admin/v1/{loops,freshness,activity,jobs,schedule,logs,stats,timeseries}` | operations data |
| `GET /v1/search`, `/v1/docs/{id}` | search and document retrieval |

## Verification

- Full suite: 1,003 tests passed.
- Production registry: 42/42 modules report `implemented: true`.
- Production recipe list: 11/11 built-ins.
- Admin health: `status: ok`, authentication required.
- Qdrant docs pool: 18,108 4B points.
- Qdrant HF pool: 3,816 4B points.
- Fixed comparison: identical anchor and query hashes; 4B NDCG@10 `0.90636`,
  MRR `0.88800`; decision `keep-4b`.

The remaining operational follow-up is credential rotation after the frontend
has validated pairing with the deployed server. Signed macOS notarization also
remains environment-dependent because this checkout has no Apple signing
identity.
