# Hacker News source (researched 2026-07-16)

Verified live. Decision: **stories-only lightweight source; Algolia API primary,
community parquet mirror as backfill accelerator** (effort 2-3/5). Awaiting build go.

## Sources
- **Algolia HN API** (`hn.algolia.com/api/v1`, free, no auth, ~10k req/hr/IP):
  `search_by_date` + `numericFilters=created_at_i>=X,<Y` gives clean UTC day windows;
  `tags=story` excludes comments and dead/deleted. Hard cap 1000 hits/query → busy days
  (verified 1,172 stories on 2026-07-15) need sub-window splitting. Primary for the
  daily tail AND the authoritative backfill option (~10-15k queries ≈ 1-2h for 2006→now).
- **open-index/hacker-news** (HuggingFace, ODC-By 1.0, no auth): full 48.87M-item mirror,
  12.2GB of monthly parquet + 5-minute live blocks; filter `type==1 AND NOT dead AND NOT
  deleted` for ~6.1M stories. Fast backfill path (download-and-filter, zero API load);
  single-maintainer risk → treat as accelerator/backup, not the tail.
- Official Firebase API: authoritative, real-time, no rate limit — but no date/type
  queries (48.9M individual GETs for backfill). Single-item hydration fallback only.
- BigQuery public dataset (ruled out: hosted; historically stale) and ClickHouse example
  parquet (frozen pre-2023): documented, not used.

## Document model
- One doc per STORY. `hn:<id>`; canonical url `news.ycombinator.com/item?id=<id>`
  (the discussion is the stable link target); external target `url` as a field.
- Text = title + `story_text` (Ask/Show/self posts only). Fields: points, num_comments,
  author, created_at. Points/comment-count usable later as query-time ranking boost.
- **Skip comments entirely**: 87% of items, conversational, low-signal for link-finding.
- Standalone source, not a join onto existing docs: most HN targets aren't in windex,
  and a cross-source join would break the per-source watermark pattern.
- Volume: ~1,000-1,500 stories/day; lifetime ≈ 6.1M (≈3.8M live in Algolia). Text volume
  tiny (titles + occasional self-text) → embedding cost negligible vs news/wiki.

## Pattern mapping
Backfill: open-index monthly parquet keyed (year, month) watermarks — minutes-hours;
or Algolia day-windowed walk. Daily: Algolia yesterday-UTC window (~1-3 queries, split
when >1000 hits), upsert by id; optionally re-pull trailing ~2 days to refresh scores.
