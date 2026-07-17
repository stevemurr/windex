-- windex schema. Idempotent: applied via `windex init-db` on every deploy.

CREATE TABLE IF NOT EXISTS documents (
    id             text PRIMARY KEY,          -- stable API id: news:<hash> | gh:owner/repo | wiki:<page_id> | arxiv:<paper_id> | smallweb:<hash> | docs:<slug>/<path> | hn:<item_id> | hf:<path>
    source         text NOT NULL,             -- news | github | wiki | arxiv | smallweb | docs | hn | hf
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

-- Shard ledger for the GitHub Search discovery sweep: one row per completed
-- leaf shard, so a crashed sweep resumes without re-paginating finished
-- windows (the in-memory split deque is not a checkpoint — 2026-07-16 crash).
CREATE TABLE IF NOT EXISTS gh_shards (
    from_date      date NOT NULL,
    to_date        date NOT NULL,
    star_threshold integer NOT NULL,             -- done at T=10 is not done at T=5
    repos          integer DEFAULT 0,
    processed_at   timestamptz DEFAULT now(),
    PRIMARY KEY (from_date, to_date, star_threshold)
);

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

-- Feed registry for the Kagi Small Web source. This is windex's only FETCH-based
-- source: sync.py seeds this table from smallweb.txt (github.com/kagisearch/smallweb,
-- MIT); poll.py polls each active feed with a conditional GET (etag/last_modified),
-- parses it, and stages new posts. fail_count accrues on consecutive failures and
-- flips status to 'dead' at the cap (reset on any success/304); feeds that drop off
-- the upstream list become 'removed' (the row + poll watermark survive a reappearance).
CREATE TABLE IF NOT EXISTS feeds (
    url           text PRIMARY KEY,           -- RSS/Atom feed URL from smallweb.txt
    host          text NOT NULL,              -- feed host (payload outlet for its posts)
    etag          text,                       -- conditional-GET validator (If-None-Match)
    last_modified text,                       -- conditional-GET validator (If-Modified-Since)
    last_polled   timestamptz,                -- poll watermark (drives rotation order)
    last_status   integer,                    -- last HTTP status seen (200/304/…; progress only)
    items_seen    integer NOT NULL DEFAULT 0, -- cumulative posts staged from this feed
    fail_count    integer NOT NULL DEFAULT 0, -- consecutive failures
    status        text NOT NULL DEFAULT 'active',  -- active | dead | removed
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS feeds_status_idx ON feeds (status);
CREATE INDEX IF NOT EXISTS feeds_last_polled_idx ON feeds (last_polled);

-- Freshness watermark for programming docs (DevDocs pre-built bundles): one row
-- per docset ever seen in the manifest (https://devdocs.io/docs.json). The
-- manifest's per-docset `mtime` is THE upstream freshness signal: a docset is
-- pending when it is in the configured seed list and mtime > ingested_mtime.
-- Ingest is full-replace per slug (no per-page deltas upstream); the
-- documents.text_hash ledger keeps a refresh to the changed-page delta, and
-- pages that vanished from the new bundle are tombstoned. attribution is the
-- upstream license HTML — stored here and carried into search payloads.
CREATE TABLE IF NOT EXISTS docsets (
    slug           text PRIMARY KEY,            -- e.g. python~3.14, javascript
    release        text,                        -- upstream version (e.g. 3.14.6)
    mtime          bigint,                      -- upstream freshness watermark (unix)
    db_size        bigint,                      -- db.json bytes (bandwidth accounting)
    attribution    text,                        -- upstream license/attribution HTML
    status         text NOT NULL DEFAULT 'pending',  -- pending | processing | done | failed
    ingested_mtime bigint,                      -- mtime last fully ingested (NULL = never)
    doc_counts     jsonb,                       -- per-docset pages/staged/skipped/deleted stats
    processed_at   timestamptz
);
CREATE INDEX IF NOT EXISTS docsets_status_idx ON docsets (status);

-- Freshness watermark for Hacker News: one row per [from_ts, until_ts) epoch
-- window — calendar months for the backfill (drained from either the Algolia
-- API or the open-index parquet mirror; same staging flow), plus a rolling
-- trailing-days window for the tail. Algolia hard-caps any query at 1000 hits,
-- so a window is FETCHED by recursively halving over-cap sub-ranges but staged
-- and marked as one unit. The trailing window is re-armed on every run: the
-- documents.text_hash ledger keeps unchanged stories from re-embedding, while
-- their points/num_comments payloads are refreshed in place (set_payload).
CREATE TABLE IF NOT EXISTS hn_windows (
    from_ts      bigint NOT NULL,             -- created_at_i >= (inclusive, unix UTC)
    until_ts     bigint NOT NULL,             -- created_at_i <  (exclusive, unix UTC)
    status       text NOT NULL DEFAULT 'pending',  -- pending | processing | done | failed
    queries      integer DEFAULT 0,           -- Algolia requests issued (incl. cap splits)
    hits         integer DEFAULT 0,           -- stories seen
    staged       integer DEFAULT 0,           -- changed-text delta rows staged to parquet + ledger
    refreshed    integer DEFAULT 0,           -- unchanged stories with a points payload refresh
    processed_at timestamptz,
    PRIMARY KEY (from_ts, until_ts)
);
CREATE INDEX IF NOT EXISTS hn_windows_status_idx ON hn_windows (status);

-- Freshness watermark for Hugging Face docs/courses (huggingface.co): one row
-- per doc root from sitemap-doc.xml (52 — the shard is COMPLETE, unlike the
-- models/datasets/spaces/papers shards, which are recency windows and must never
-- be used as a frontier; see docs/huggingface-source.md).
--
-- The per-root llms.txt (a titled index of every page as a .md link) is THE
-- freshness signal: a root is pending when its llms_hash differs from the
-- ingested_hash the crawl last completed. That gate is load-bearing rather than
-- an optimization — HF's `pages` rate-limit bucket is 1 req/3s, so a naive
-- re-sweep would cost 3.3 HOURS every night, and a conditional GET wouldn't help
-- (a 304 still spends a request). Hashing 52 llms.txt files costs ~3 minutes.
--
-- Pending-ness deliberately does NOT consult `status` (see hf/sync.py:
-- pending_roots): status is progress reporting, so a job killed mid-root leaves
-- a row in 'processing' that is still pending and simply re-crawls. There is no
-- stale claim to reclaim.
CREATE TABLE IF NOT EXISTS hf_roots (
    root          text PRIMARY KEY,            -- docs/transformers | learn/agents-course
    kind          text NOT NULL,               -- docs | learn
    url           text NOT NULL,               -- sitemap loc
    lastmod       text,                        -- sitemap lastmod (progress only)
    llms_hash     text,                        -- sha1 of llms.txt (NULL = no llms.txt)
    ingested_hash text,                        -- llms_hash last fully crawled (NULL = never)
    pages         integer,                     -- .md links llms.txt lists
    version       text,                        -- observed vX.Y.Z (recorded, NOT in doc ids)
    license       text,                        -- per-root upstream license ("" = unchecked)
    status        text NOT NULL DEFAULT 'pending',  -- pending | processing | done | partial | failed | no_llms
    doc_counts    jsonb,                       -- per-root pages/staged/skipped/failed stats
    processed_at  timestamptz
);
CREATE INDEX IF NOT EXISTS hf_roots_status_idx ON hf_roots (status);

-- Freshness watermark for the Hugging Face blog: one row per post from
-- sitemap-blog.xml (829, spanning 2020-02-14 → today — the complete archive).
-- The sitemap's lastmod is the watermark; a post whose lastmod advances past
-- ingested_lastmod is pending again (an edited post re-extracts, and the
-- documents.text_hash ledger decides whether that costs a re-embed). Slugs are
-- not always flat: org-authored posts are namespaced (nvidia/some-post).
CREATE TABLE IF NOT EXISTS hf_posts (
    slug             text PRIMARY KEY,         -- blog slug, may contain '/' (nvidia/foo)
    url              text NOT NULL,
    lastmod          text NOT NULL DEFAULT '', -- sitemap lastmod (upstream watermark)
    ingested_lastmod text,                     -- lastmod last fully crawled (NULL = never)
    status           text NOT NULL DEFAULT 'pending',  -- pending | done | failed
    processed_at     timestamptz
);
CREATE INDEX IF NOT EXISTS hf_posts_status_idx ON hf_posts (status);

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

-- Embed-backlog claim: every embed batch selects the oldest N 'deduped' rows
-- per source — without this partial index that's a seq scan + sort over
-- millions of rows per batch (measured; see docs/store-tuning.md)
CREATE INDEX IF NOT EXISTS documents_embed_backlog_idx
    ON documents (source, created_at) WHERE status = 'deduped';

-- Autovacuum: minhash_bands' rolling deletes never reach the default 20%
-- trigger at ~10M rows; documents churns millions of status UPDATEs during
-- backlog burn-down (see docs/store-tuning.md)
ALTER TABLE minhash_bands SET (autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 50000, autovacuum_vacuum_cost_delay = 0);
ALTER TABLE documents SET (autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 10000);

-- Bandwidth accounting (dashboard rate metrics)
ALTER TABLE warc_files ADD COLUMN IF NOT EXISTS bytes bigint;
-- First-seen-by-discovery timestamp (never updated on conflict); NULL for rows
-- that predate the column or arrived via the archive scan / tail.
ALTER TABLE repos ADD COLUMN IF NOT EXISTS discovered_at timestamptz;
ALTER TABLE gharchive_files ADD COLUMN IF NOT EXISTS bytes bigint;

-- Control plane (dashboard start/pause; workers poll between batches)
CREATE TABLE IF NOT EXISTS control (
    key   text PRIMARY KEY,
    value text NOT NULL
);

-- Search-performance metrics: one narrow row per run_search call (REST and MCP
-- both route through service.run_search). No query text by design — privacy
-- and row width; q_hash (sha1 prefix) still surfaces repeated-query patterns.
-- `windex daily` caps retention at 30 days.
CREATE TABLE IF NOT EXISTS search_metrics (
    ts             timestamptz NOT NULL DEFAULT now(),
    source         text NOT NULL,              -- news | github | … | all
    mode_requested text NOT NULL,              -- hybrid | dense | lexical (as asked, pre-degrade)
    degraded       boolean NOT NULL DEFAULT false,  -- hybrid fell back to keyword-only
    q_hash         text,                       -- sha1(query)[:12]; never the query itself
    embed_ms       integer,
    search_ms      integer,
    total_ms       integer,
    results        integer
);
CREATE INDEX IF NOT EXISTS search_metrics_ts_idx ON search_metrics (ts);
-- degradations are the debugging needle; keep them findable at any table size
CREATE INDEX IF NOT EXISTS search_metrics_degraded_ts_idx
    ON search_metrics (ts) WHERE degraded;
