# Postgres + Qdrant tuning (investigated live 2026-07-16)

Read-only inspection under full five-source ingest load. Headline findings:
- **Checkpoint storm**: 46 of 55 checkpoints WAL-forced (54GB WAL vs 1GB max_wal_size);
  ~27% of wall-clock spent in checkpoint writes on the already-saturated external disk.
- **Embed-backlog claims seq-scanned** 2.57M rows + sort per batch (no matching index).
- **minhash_bands autovacuum never fires** (rolling deletes < the 20%-of-9.4M default trigger).
- **Qdrant queries rescore against f32 vectors on disk** by default (quantization
  `rescore=true`) — a per-query disk read that competes with checkpoints/optimizer.
- Qdrant memory: 400MB of 10GB used at ~140k vectors; posture fine to ~10-15M, then
  binary quantization is the scale path.

## Applied 2026-07-16 (online, no restart)
- `ALTER SYSTEM`: max_wal_size=6GB, checkpoint_timeout=15min, wal_compression=zstd,
  **synchronous_commit=off** (safe: pipeline is idempotent, parquet is source of truth;
  loses ≤1s of commits on crash, never corrupts — `fsync`/`full_page_writes` untouched),
  work_mem=16MB, maintenance_work_mem=256MB, autovacuum_work_mem=128MB, jit=off.
  shared_buffers=1GB staged (applies at next PG restart).
- Per-table autovacuum on minhash_bands + documents (also codified in schema.sql).
- Partial index `documents_embed_backlog_idx (source, created_at) WHERE status='deduped'`
  (in schema.sql; built CONCURRENTLY in prod).
- bgwriter stats reset for before/after measurement.
- search.py: quantization `rescore=false` + explicit `hnsw_ef=96` in query params.
- service.py: stats cache split — full-scan aggregates (source/status group-by,
  outlets distinct, coverage) at 60s TTL; cheap live signals stay 10s.
- `windex maintain`: VACUUM ANALYZE the churn tables nightly; `--reindex` weekly
  rebuilds btree indexes >50MB whose pgstatindex leaf density < 70%, CONCURRENTLY,
  one at a time (crontab lines in README). Never during backfill bursts.

- Ledger probes: dropped the redundant `source = 'x'` predicate from the
  `id = ANY(...)` probes in all 7 sources. It forced
  documents_source_published_idx (rows=1 estimate — hn/docs/github are absent
  from the source MCV list) and scanned every row of the source: 244s vs 63ms
  on the pkey plan. Regression test in tests/test_scaffold.py. Do NOT
  reintroduce it, and do NOT "fix" this with ANALYZE — a rare source value will
  always tempt the planner back.
- Stats: outlets tile rewritten count(DISTINCT ...) -> count(*) over GROUP BY
  (the sort spilled ~10MB/s of temp files); _PG_HEAVY_TTL 60s -> 600s;
  get_timeseries cached 30s (uncached full scan, once per SSE viewer).
- Hot objects: shared BM25 singleton (index/sparse.py) replacing per-pass
  construction in 7 sources; module-level QdrantClient; query BM25 vector
  hoisted to once per search; ensure_collection only creates missing payload
  indexes.

Measured 2026-07-17 after the above (60s deltas, live): disk reads 505 ->
125 MB/s, temp spill 10 -> 0 MB/s, cache hit 14.9% -> 89.4%, longest active
query 244s -> 0s. Re-baseline before chasing anything below.

## Pending (code changes)
- Stats: incremental rollup table if the 600s heavy pass ever hurts.
- Query-embed circuit breaker: degraded searches burn a flat 8s
  (embed_query_timeout) waiting on a GPU saturated by design; p95 9.2s,
  22% degraded. After N consecutive timeouts skip the dense leg for a cooldown.
- Qdrant `wait=True` on bulk upserts (client default in all 7 embed paths):
  354ms avg / 36.5s max per upsert, serial in the embed worker thread. NOT a
  free flag flip — the pass commits status='embedded' right after, so wait=False
  risks docs marked embedded whose vectors never landed, never retried.
  Decoupling upsert from the embed thread is the safer shape.
- Embed pass starves the GPU: per text_ref the main thread does a full
  pq.read_table() (no column/filter pushdown; wiki refs avg 332MB) then filters
  in memory, and cf.as_completed() is a barrier per ref. Prefetch the next ref
  while the current embeds. get_document() has the right idiom (filters=).
- shared_buffers: still 128MB, pending_restart=true — the staged 1GB never took
  effect (needs a PG restart; documents heap cache hit was 5.78%).
- hn/docs collections still on max_segment_size/max_optimization_threads=null
  while the other five carry the backfill posture (300000/1).
- Payload indexes built but never filtered on: news.lang, wiki.title,
  wiki.incoming_links, repos.pushed_at, doc_id on all seven.
- During backlog embed: qdrant max_optimization_threads=1 + bounded max_segment_size
  (avoid 81s optimizer stalls); revert to defaults at steady state.
- Verify-then-drop: documents_status_idx (superseded by partial), canonical_url_idx
  (idx_scan=0, function-wrapped in queries). Keep text_hash_idx (dedup probe).
- Cleanup: leftover *__pytest-model* collections/snapshots in qdrant.

## Do NOT (saturated-external-disk rules)
fsync/full_page_writes off; smaller/more-frequent checkpoints; higher
effective_io_concurrency; scheduled Qdrant snapshots (parquet is the recovery source);
REINDEX/VACUUM FULL during backfill bursts; HNSW on_disk or global defer-HNSW;
shared_buffers >25% of the 4GB VM.

## Measurement plan (before/after each change)
1. checkpoints_req:timed ratio + checkpoint_write_time %wall (baseline 46:9, ~27%).
2. Backlog-claim EXPLAIN: seq+sort 2.57M rows → bounded index scan.
3. Heap cache-hit ratio (baseline 6.5% → target high-90s after shared_buffers restart).
4. Qdrant query avg/max under embed load (baseline 14ms/205ms) after rescore=false.
5. PUT/optimizer spikes (baseline PUT max 36.5s, optimizer max 81s) after segment bounds.
6. Daily bloat trend on minhash_bands/documents (gate for the maintain job).
