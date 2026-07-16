-- windex schema. Idempotent: applied via `windex init-db` on every deploy.

CREATE TABLE IF NOT EXISTS documents (
    id             text PRIMARY KEY,          -- stable API id: news:<hash> | gh:owner/repo | wiki:<page_id> | arxiv:<paper_id>
    source         text NOT NULL,             -- news | github | wiki | arxiv
    url            text NOT NULL,
    canonical_url  text,
    title          text,
    published_at   timestamptz,
    lang           text,
    text_hash      text,                      -- sha1 of normalized text (exact dedup)
    status         text NOT NULL DEFAULT 'extracted',  -- extracted | deduped | embedded | duplicate | deleted
    duplicate_of   text,                      -- id of canonical doc when near-dup
    embedded_model text,
    indexed_at     timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS documents_canonical_url_idx ON documents (canonical_url);
CREATE INDEX IF NOT EXISTS documents_text_hash_idx ON documents (text_hash);
CREATE INDEX IF NOT EXISTS documents_source_published_idx ON documents (source, published_at);
CREATE INDEX IF NOT EXISTS documents_status_idx ON documents (status);

-- Freshness watermark for CC-News: one row per WARC file ever seen.
CREATE TABLE IF NOT EXISTS warc_files (
    path         text PRIMARY KEY,            -- crawl-data/CC-NEWS/yyyy/mm/CC-NEWS-*.warc.gz
    status       text NOT NULL DEFAULT 'pending',  -- pending | processing | done | failed
    doc_counts   jsonb,                       -- per-stage in/out stats
    processed_at timestamptz
);
CREATE INDEX IF NOT EXISTS warc_files_status_idx ON warc_files (status);

CREATE TABLE IF NOT EXISTS repos (
    repo_id           bigint PRIMARY KEY,     -- GitHub numeric id (stable across renames)
    full_name         text NOT NULL UNIQUE,
    stars             integer,
    star_events       integer DEFAULT 0,      -- WatchEvent count from archive scan (candidate signal)
    description       text,
    topics            text[],
    primary_language  text,
    default_branch    text,
    pushed_at         timestamptz,
    readme_fetched_at timestamptz,
    status            text NOT NULL DEFAULT 'candidate'  -- candidate | hydrated | embedded | gone | below_threshold
);
CREATE INDEX IF NOT EXISTS repos_status_idx ON repos (status);
CREATE INDEX IF NOT EXISTS repos_stars_idx ON repos (stars);

-- Freshness watermark for GH Archive: one row per hourly file.
CREATE TABLE IF NOT EXISTS gharchive_files (
    name         text PRIMARY KEY,            -- 2026-07-14-23.json.gz
    status       text NOT NULL DEFAULT 'pending',
    processed_at timestamptz
);
CREATE INDEX IF NOT EXISTS gharchive_files_status_idx ON gharchive_files (status);

-- Freshness watermark for Wikipedia CirrusSearch dumps: one row per shard file
-- of the newest _SUCCESS-complete weekly snapshot. Each snapshot is a full
-- index, so sync re-baselines from the newest date; the documents.text_hash
-- ledger keeps re-ingests to the changed-article delta.
CREATE TABLE IF NOT EXISTS wiki_dumps (
    name         text PRIMARY KEY,            -- enwiki_content-YYYYMMDD-NNNNN.json.bz2
    dump_date    text,                        -- YYYYMMDD snapshot the shard belongs to
    status       text NOT NULL DEFAULT 'pending',  -- pending | processing | done | failed
    bytes        bigint,                      -- shard size (bandwidth accounting)
    doc_counts   jsonb,                       -- per-shard in/staged/skipped stats
    processed_at timestamptz
);
CREATE INDEX IF NOT EXISTS wiki_dumps_status_idx ON wiki_dumps (status);

-- Freshness watermark for arXiv OAI-PMH harvest: one row per date window.
-- The full corpus is chunked into independently restartable per-year windows
-- (backfill) plus a rolling incremental window; a window is only 'done' once its
-- resumption-token chain completes. OAI resumption tokens expire at the next
-- 00:00 UTC, so an interrupted window is safely re-harvested from its start
-- (the documents.text_hash ledger keeps re-harvests to the changed-paper delta).
CREATE TABLE IF NOT EXISTS arxiv_windows (
    from_date    text NOT NULL,               -- YYYY-MM-DD OAI `from` (inclusive)
    until_date   text NOT NULL,               -- YYYY-MM-DD OAI `until` (inclusive)
    status       text NOT NULL DEFAULT 'pending',  -- pending | processing | done | failed
    token        text,                        -- last resumption token seen (progress only)
    pages        integer DEFAULT 0,
    records      integer DEFAULT 0,           -- records seen (incl. tombstones)
    staged       integer DEFAULT 0,           -- delta rows staged to parquet + ledger
    deleted      integer DEFAULT 0,           -- tombstones applied
    processed_at timestamptz,
    PRIMARY KEY (from_date, until_date)
);
CREATE INDEX IF NOT EXISTS arxiv_windows_status_idx ON arxiv_windows (status);

-- Rolling-window LSH index for near-dup detection across daily batches.
CREATE TABLE IF NOT EXISTS minhash_bands (
    band_idx  smallint NOT NULL,
    band_hash bigint NOT NULL,
    doc_id    text NOT NULL,
    day       date NOT NULL,
    PRIMARY KEY (band_idx, band_hash, doc_id)
);
CREATE INDEX IF NOT EXISTS minhash_bands_day_idx ON minhash_bands (day);

-- Idempotent column additions (schema.sql is our migration file).
ALTER TABLE documents ADD COLUMN IF NOT EXISTS text_ref text;  -- staging parquet holding this doc's text

-- Recently-indexed feed (/v1/recent, dashboard ticker)
CREATE INDEX IF NOT EXISTS documents_indexed_at_idx
    ON documents (indexed_at DESC) WHERE indexed_at IS NOT NULL;

-- Bandwidth accounting (dashboard rate metrics)
ALTER TABLE warc_files ADD COLUMN IF NOT EXISTS bytes bigint;
ALTER TABLE gharchive_files ADD COLUMN IF NOT EXISTS bytes bigint;

-- Control plane (dashboard start/pause; workers poll between batches)
CREATE TABLE IF NOT EXISTS control (
    key   text PRIMARY KEY,
    value text NOT NULL
);
