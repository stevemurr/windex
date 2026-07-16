# Wikipedia data source decision (researched 2026-07-16)

All facts verified against live listings on 2026-07-16, not memory.

## Decision: new CirrusSearch index dumps (primary)

`https://dumps.wikimedia.org/other/cirrus_search_index/YYYYMMDD/index_name=enwiki_content/`

- 64 bzip2 shards `enwiki_content-YYYYMMDD-{00000..00063}.json.bz2`, ~610MB each,
  **~39.2GB total**; `_SUCCESS` marker gates completeness.
- **Weekly** (Saturdays), only ~4 weeks retained → always re-baseline from the newest
  complete snapshot; each snapshot is a full index, so re-ingest is idempotent.
- Elasticsearch bulk pairs: action line (`_id` = page id) + document line with
  **pre-extracted plaintext** (`text`), `opening_text` (lead), `title`, `timestamp`
  (revision ts), `version` (revision id), `incoming_links` (popularity signal —
  `popularity_score` no longer exists), `category`, `namespace`, `wikibase_item`.
- Change detection: text_hash ledger (same pattern as news) → only deltas re-embed.

## Rejected / fallback

| Source | Why not primary |
|---|---|
| `dumps.wikimedia.org/other/cirrussearch/` | **DEPRECATED 2026-01-07**, last dir 20251229 |
| Official XML `pages-articles-multistream` (24.7GB bz2) | raw wikitext → extraction quality pain (effort 4–5/5); zero-third-party fallback only |
| Enterprise HTML on the public mirror | frozen at 2025-03-20; fresh data requires an account (conflicts with self-hosted constraint) |
| HF `wikimedia/structured-wikipedia` (34.6GiB parquet, May 2026) | **fallback A** — official, has URL/page id/rev timestamp; "beta", cadence unguaranteed |
| HF `omarkamali/wikipedia-monthly` (`latest.en`) | **fallback B** — easiest, but single-maintainer, no per-article timestamp |
| HF `wikimedia/wikipedia` | data frozen at Nov 2023 |
| Kiwix ZIM / DBpedia / Wikidata | not article-prose sources |

## Operational caveat

The 4-week retention means a >4-week outage can't be backfilled from this dir —
re-baseline from the newest snapshot (safe: full index) or use a dated XML dump.
