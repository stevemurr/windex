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
    status         text NOT NULL DEFAULT 'extracted',  -- extracted | deduped | embedded | duplicate | deleted | empty (blank text, never embedded) | failed (embed server permanently rejected the text)
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

-- Recently-embedded feed (/v1/recent, /v1/recent/embedded): docs by when their
-- vectors landed in Qdrant.
CREATE INDEX IF NOT EXISTS documents_indexed_at_idx
    ON documents (indexed_at DESC) WHERE indexed_at IS NOT NULL;

-- Recently-indexed feed (/v1/recent/indexed): newest-harvested docs by created_at.
CREATE INDEX IF NOT EXISTS documents_created_at_idx ON documents (created_at DESC);

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

-- Editable job scheduler. The `windex scheduler` timer loop reads this table
-- every ~60s and fires the entries that are enabled + due. init_db seeds
-- sensible defaults when the table is empty (one daily ingest per source at a
-- staggered time, plus the daily-freshness + store-maintenance command jobs);
-- fully editable thereafter via /v1/schedule. kind='ingest' targets a source
-- name (fired as `windex refresh --source <target>`, gated on that source's
-- ingest_enabled flag); kind='command' targets a command key (daily|maintain).
CREATE TABLE IF NOT EXISTS schedule (
    name     text PRIMARY KEY,           -- stable id, e.g. ingest-hf | daily | maintain
    kind     text NOT NULL,              -- ingest | command
    target   text NOT NULL,              -- source name (ingest) | daily|maintain (command)
    hour     integer NOT NULL,           -- 0-23 (local time)
    minute   integer NOT NULL,           -- 0-59
    weekday  integer,                    -- 0=Sun … 6=Sat; NULL = every day
    enabled  boolean NOT NULL DEFAULT true,
    last_run timestamptz                 -- last time the loop fired this entry
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

-- Registry for user-defined "custom sources": push-based indexes that reuse the
-- documents ledger (documents.source = <name>, ids <name>:<suffix>) and the
-- shared embed driver, generalizing the single-purpose `memory` source into an
-- API that creates any number of them. One row per source records its
-- title/description and an optional stored refresh recipe (jsonb) — the recipe
-- is what lets a scheduled refresh prompt be a stateless one-liner. `name` obeys
-- ^[a-z][a-z0-9_]{1,31}$ and is never a reserved/built-in source (enforced in
-- custom_source.registry, not the DB). Additive, idempotent like every table here.
CREATE TABLE IF NOT EXISTS custom_sources (
    name        text PRIMARY KEY,           -- ^[a-z][a-z0-9_]{1,31}$, not reserved
    title       text NOT NULL DEFAULT '',
    description text NOT NULL DEFAULT '',
    recipe      jsonb,                       -- optional stored refresh recipe
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Search QUALITY (relevance), distinct from search_metrics (performance). One
-- row per `windex eval` run; the Prometheus collector reads the latest row and
-- the Grafana panel trends them. git_sha makes runs comparable across changes.
CREATE TABLE IF NOT EXISTS search_quality (
    ts              timestamptz NOT NULL DEFAULT now(),
    mode            text NOT NULL,        -- hybrid | dense | lexical
    k               integer NOT NULL,
    known_item_ndcg real,                 -- label-free title-as-query proxy
    known_item_mrr  real,
    golden_ndcg     real,                 -- curated regression anchors
    golden_mrr      real,
    judge_ndcg      real,                 -- LLM-as-judge graded (null if disabled)
    git_sha         text,
    detail          jsonb                 -- full by-source / by-leg breakdown
);
CREATE INDEX IF NOT EXISTS search_quality_ts_idx ON search_quality (ts);

-- Web-cluster crawls. One row per requested crawl of a seed URL into a custom
-- source. `recipe` is a FROZEN copy of the settings actually executed, not a
-- reference to custom_sources.recipe: editing a source's recipe must not rewrite
-- the history of what past runs did. The worker (`windex crawl-loop`) claims
-- pending rows FOR UPDATE SKIP LOCKED and heartbeats while running, so a run
-- whose worker died is reclaimable exactly like a stale warc/arxiv unit.
CREATE TABLE IF NOT EXISTS crawl_runs (
    id           bigserial PRIMARY KEY,
    source       text NOT NULL,          -- custom_sources.name (the crawl's destination)
    recipe       jsonb NOT NULL,         -- frozen copy of what this run executed
    status       text NOT NULL DEFAULT 'pending',  -- pending|running|done|failed|cancelled
    requested_at timestamptz NOT NULL DEFAULT now(),
    started_at   timestamptz,
    finished_at  timestamptz,
    heartbeat_at timestamptz,            -- liveness; cold ⇒ reclaimable to 'pending'
    stats        jsonb,                  -- {found,fetched,staged,skipped,failed,truncated,…}
    error        text
);
CREATE INDEX IF NOT EXISTS crawl_runs_status_idx ON crawl_runs (status);
CREATE INDEX IF NOT EXISTS crawl_runs_source_idx ON crawl_runs (source, requested_at DESC);

-- The crawl frontier, persisted rather than held in memory. Two reasons: a
-- killed run RESUMES instead of restarting from the seed (the same watermark
-- discipline every other source follows), and the /crawl control page gets
-- per-URL visibility — including *why* a URL was skipped — for free.
CREATE TABLE IF NOT EXISTS crawl_urls (
    run_id  bigint  NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
    url     text    NOT NULL,
    depth   integer NOT NULL,
    status  text    NOT NULL DEFAULT 'pending',  -- pending|staged|skipped|failed
    reason  text,                                -- robots | scope | boilerplate | no_text | http | …
    seq     bigserial,                           -- monotonic: the SSE cursor for tailing transitions
    PRIMARY KEY (run_id, url)
);
CREATE INDEX IF NOT EXISTS crawl_urls_run_status_idx ON crawl_urls (run_id, status);
CREATE INDEX IF NOT EXISTS crawl_urls_seq_idx ON crawl_urls (run_id, seq);
-- The documents id this URL produced. Recorded on the frontier rather than held
-- in memory so `prune` still knows what a RESUMED run covered: an in-memory set
-- would forget everything staged before the restart and prune would then delete
-- it. Null for rows that produced no document (skipped/failed/pending).
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS doc_id text;

-- Runtime-editable source settings. Every built-in source's config otherwise
-- lives in Settings (pydantic ← .env), which is baked into the container
-- environment at create time — so changing `wiki_dump` or `hf_request_interval`
-- meant editing .env, rebuilding the image and recreating containers. A row here
-- overrides that at run time; see settings_schema.py for WHICH keys may be set
-- (a declared allowlist — secrets and DSNs are absent by construction) and
-- config.effective_settings for the precedence.
--
-- SPARSE ON PURPOSE: `settings` holds only the keys explicitly changed, so every
-- untouched key still falls through to .env and then the code default. An
-- install that never opens the console behaves exactly as it did before.
CREATE TABLE IF NOT EXISTS source_config (
    scope      text PRIMARY KEY,          -- source name (wiki|hf|…) or '_global'
    settings   jsonb NOT NULL DEFAULT '{}',
    updated_at timestamptz NOT NULL DEFAULT now()
);


-- ============================================================================
-- SOURCE RECIPES — additive groundwork. NOTHING READS THESE YET.
--
-- The plan (~/.claude/plans/i-want-to-look-cheeky-muffin.md) unifies "source"
-- and "crawl" into one concept: a recipe is a typed-port DAG of nodes drawn from
-- a fixed vocabulary, referencing modules that ship with windex. Every one of the
-- eleven sources is already the same program — enumerate work into a state table,
-- fetch it, turn bytes into documents, write a text_hash-guarded delta to parquet
-- and the documents ledger, advance a watermark only once the text is durably
-- staged — written eleven times, which is how we ended up with twelve watermark
-- tables and six copies of the same ledger upsert.
--
-- This block is Phase 1: create the tables, read none of them. The legacy tables
-- stay authoritative until a per-source cutover flips reads, so rollback at this
-- stage is DROP TABLE and nothing else.
-- ============================================================================

-- One row per installed recipe. Supersedes custom_sources (which becomes a view)
-- and the eight hardcoded pull-source pipelines.
CREATE TABLE IF NOT EXISTS recipes (
    name        text PRIMARY KEY,       -- ^[a-z][a-z0-9_]{1,31}$, custom_source.registry rules
    source      text NOT NULL,          -- documents.source this recipe feeds
    kind        text NOT NULL DEFAULT 'ingest',   -- ingest | maintenance | system
    spec        jsonb NOT NULL,         -- the compiled DAG
    spec_hash   text NOT NULL,          -- sha1 over canonical JSON; cheap change detection
    -- The marketplace's three-way diff needs the as-installed copy: without it,
    -- "did upstream change or did I edit it" is unanswerable and an update either
    -- clobbers local edits or refuses them all.
    base_spec   jsonb,
    origin      jsonb,                  -- {catalog,url,ref,commit_sha,path,blob_sha256,installed_at}
    version     integer NOT NULL DEFAULT 1,
    enabled     boolean NOT NULL DEFAULT true,
    builtin     boolean NOT NULL DEFAULT false,   -- shipped; editable but restorable
    title       text NOT NULL DEFAULT '',
    description text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Edit history. The frozen-copy-per-run discipline (runs.spec) records what a run
-- DID; this records what the recipe WAS, so "why did last week behave differently"
-- is answerable without reading every run's spec blob.
CREATE TABLE IF NOT EXISTS recipe_revisions (
    name       text    NOT NULL REFERENCES recipes(name) ON DELETE CASCADE,
    version    integer NOT NULL,
    spec       jsonb   NOT NULL,
    spec_hash  text    NOT NULL,
    note       text    NOT NULL DEFAULT '',
    author     text    NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, version)
);

-- Per-recipe runtime settings. The successor to source_config: same sparse-row
-- discipline and the same DB > env > default precedence, but keyed by recipe.
-- '_global' stays reserved and keeps living in source_config/settings_schema —
-- those knobs (embed budget, throttle, crawl ceilings) are the OPERATOR's, and a
-- recipe asking for a bigger share of a fleet-wide budget is exactly what the
-- allowlist exists to refuse.
CREATE TABLE IF NOT EXISTS recipe_config (
    recipe     text PRIMARY KEY,
    values     jsonb NOT NULL DEFAULT '{}',
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- source_units: the PERMANENT watermark, replacing all twelve legacy tables.
--
-- Kept strictly separate from the per-run work list (task_units) because their
-- lifetimes are opposite, and conflating them is the one genuinely dangerous
-- mistake available here: a year-old crawl frontier can be pruned with no
-- consequence, whereas pruning warc_files means re-downloading three years of
-- WARCs — or worse, silently re-ingesting the corpus.
--
-- The freshness gate is `upstream IS DISTINCT FROM ingested`, which generalizes
-- every per-source signal windex invented: an etag/last_modified pair, a docset
-- mtime, hf's llms_hash, a sitemap lastmod, a dump date, or an empty string for
-- the sources whose only signal is "seen at all".
--
-- Two properties fall out that are per-source accidents today: re-arming a
-- FAILED unit is free (ingested only advances on clean completion, so a failed
-- unit is still pending by definition), and fail_count -> dead is one generic
-- attempts column instead of smallweb's bespoke one.
--
-- LIST-partitioned by source so per-source indexes stay small, a source teardown
-- is DROP PARTITION, and windex_watermark_rows stays one cheap GROUP BY.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_units (
    source     text NOT NULL,           -- recipe name
    store      text NOT NULL,           -- store declared in recipe.state: warc|shard|window|…
    unit_key   text NOT NULL,           -- path | name | "2019-01-01..2019-12-31" | slug | url
    ord        text,                    -- sortable ordering key (path, date, lastmod)
    status     text NOT NULL DEFAULT 'pending',
    upstream   jsonb NOT NULL DEFAULT '{}',   -- freshness signal, whatever shape it takes
    ingested   jsonb NOT NULL DEFAULT '{}',   -- what we have actually ingested
    stage      text,                    -- lifecycle stage (repos: candidate|hydrated|staged|…)
    attempts   smallint NOT NULL DEFAULT 0,
    counts     jsonb NOT NULL DEFAULT '{}',
    bytes      bigint,
    attrs      jsonb NOT NULL DEFAULT '{}',   -- attribution HTML, license, host, kind
    last_run_id   bigint,
    claimed_at    timestamptz,
    processed_at  timestamptz,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, store, unit_key)     -- must include the partition key
) PARTITION BY LIST (source);

CREATE TABLE IF NOT EXISTS source_units_default PARTITION OF source_units DEFAULT;
CREATE INDEX IF NOT EXISTS source_units_status_idx  ON source_units (source, store, status);
-- The pending claim: exactly the predicate every discover node runs.
CREATE INDEX IF NOT EXISTS source_units_pending_idx ON source_units (source, store, ord)
    WHERE upstream IS DISTINCT FROM ingested;
-- Rotation order for the sources with no freshness signal at all (smallweb's
-- 38k feeds): least-recently-processed first.
CREATE INDEX IF NOT EXISTS source_units_rotate_idx  ON source_units (source, store, processed_at NULLS FIRST);

-- ---------------------------------------------------------------------------
-- runs / run_tasks / task_units: the execution model, generalizing the
-- crawl_runs + crawl_urls pair — today the ONLY per-run observability in windex
-- (every other source has watermarks and a log tail, which is why only crawl can
-- show a progress bar).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runs (
    id             bigserial PRIMARY KEY,
    -- Deliberately NOT a FK to recipes: run history must outlive a deleted recipe,
    -- or "what did this source do last month" dies with an uninstall.
    recipe         text NOT NULL,
    recipe_version integer NOT NULL DEFAULT 1,
    source         text NOT NULL,
    -- FROZEN copy of the compiled recipe, the crawl_runs.recipe discipline
    -- generalized: editing a recipe must never rewrite what past runs did.
    spec           jsonb NOT NULL,
    spec_hash      text NOT NULL DEFAULT '',
    trigger        text NOT NULL DEFAULT 'manual',  -- manual|schedule|event|chain|system
    trigger_by     text NOT NULL DEFAULT '',
    params         jsonb NOT NULL DEFAULT '{}',
    mode           text NOT NULL DEFAULT 'run',     -- run | dry_run
    priority       smallint NOT NULL DEFAULT 50,
    dedupe_key     text NOT NULL,                   -- defaults to the recipe name
    state          text NOT NULL DEFAULT 'queued',
                   -- queued | running | succeeded | failed | cancelled | blocked
    cancel_requested boolean NOT NULL DEFAULT false,
    queued_at      timestamptz NOT NULL DEFAULT now(),
    started_at     timestamptz,
    finished_at    timestamptz,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    progress       jsonb NOT NULL DEFAULT '{}',
    stats          jsonb NOT NULL DEFAULT '{}',
    error          text
);
CREATE INDEX IF NOT EXISTS runs_live_idx    ON runs (state) WHERE state IN ('queued','running','blocked');
CREATE INDEX IF NOT EXISTS runs_recipe_idx  ON runs (recipe, id DESC);
CREATE INDEX IF NOT EXISTS runs_source_idx  ON runs (source, id DESC);
CREATE INDEX IF NOT EXISTS runs_updated_idx ON runs (updated_at DESC);
-- At most one live run per key. This is what turns a double-fire (a human
-- clicking Run while the timer fires; two schedulers during migration; a week of
-- paused nightly runs all releasing at once) into a harmless ON CONFLICT instead
-- of duplicate ingest. Replaces jobs._spawn_lock's flock, which cannot work
-- across container boundaries anyway.
CREATE UNIQUE INDEX IF NOT EXISTS runs_dedupe_live_uniq ON runs (dedupe_key)
    WHERE state IN ('queued','running','blocked');

CREATE TABLE IF NOT EXISTS run_tasks (
    id            bigserial PRIMARY KEY,
    run_id        bigint NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    source        text NOT NULL,          -- denormalized: the claim query's hot predicate
    node          text NOT NULL,          -- node id within the DAG
    kind          text NOT NULL,          -- discover|receive|fetch|catalog|extract|transform|collect|load
    module        text NOT NULL,          -- registered module key, e.g. "http.get"
    lane          text NOT NULL DEFAULT 'io',   -- gpu | net | cpu_heavy | io | maint
    config        jsonb NOT NULL DEFAULT '{}',  -- frozen node config
    depends_on    text[] NOT NULL DEFAULT '{}',
    preconditions text[] NOT NULL DEFAULT '{}', -- 'storage:staging','gateway','gh_token'
    state         text NOT NULL DEFAULT 'pending',
                  -- pending | ready | running | succeeded | failed | skipped | cancelled
    priority      smallint NOT NULL DEFAULT 50,
    attempts      smallint NOT NULL DEFAULT 0,
    max_attempts  smallint NOT NULL DEFAULT 3,
    -- Lease, split from the liveness heartbeat so reclaim is a bare timestamp
    -- comparison and a polite 1-req/3s fetch can hold a 10-minute lease while an
    -- embed task holds two.
    lease_worker     text,
    lease_seconds    integer NOT NULL DEFAULT 300,
    lease_expires_at timestamptz,
    heartbeat_at     timestamptz,
    yield_requested  boolean NOT NULL DEFAULT false,
    -- Resume point INSIDE a unit (wiki shard line offset, OAI resumption token,
    -- HN page). Without it a 333MB shard is not preemptible and monopolizes a
    -- lane for its whole duration.
    cursor        jsonb NOT NULL DEFAULT '{}',
    units_total   integer NOT NULL DEFAULT -1,   -- -1 = not yet known / indeterminate
    units_done    integer NOT NULL DEFAULT 0,
    units_failed  integer NOT NULL DEFAULT 0,
    weight        real NOT NULL DEFAULT 1.0,     -- share of the run's progress bar
    stats         jsonb NOT NULL DEFAULT '{}',
    started_at    timestamptz,
    finished_at   timestamptz,
    error         text,
    UNIQUE (run_id, node)
);
CREATE INDEX IF NOT EXISTS run_tasks_claim_idx   ON run_tasks (lane, priority DESC, id) WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS run_tasks_running_idx ON run_tasks (lane, source) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS run_tasks_lease_idx   ON run_tasks (lease_expires_at) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS run_tasks_run_idx     ON run_tasks (run_id);

-- Fair-share bookkeeping. Weighted-fair-queueing over accumulated service time,
-- so a 20,000-page crawl cannot FIFO-block every other source for eleven hours
-- the way windex-loop-crawl does today.
CREATE TABLE IF NOT EXISTS source_sched (
    source     text PRIMARY KEY,
    weight     real NOT NULL DEFAULT 1.0,
    vtime      double precision NOT NULL DEFAULT 0,   -- accumulated service / weight
    in_flight  integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One sequence shared by task_units.id and .seq: `seq` is a TRANSITION sequence,
-- not an insertion one, and that distinction is a bug fix. crawl_urls.seq is
-- assigned at insert and never updated, but the BFS claim orders by (depth, seq),
-- so terminal transitions do NOT happen in insertion order — once an SSE cursor
-- passes seq 900, a depth-2 row transitioning at seq 500 is never streamed and
-- the client silently misses it. Bumping seq on every state write makes
-- "everything that changed since S" exactly correct.
CREATE SEQUENCE IF NOT EXISTS task_unit_seq;
CREATE SEQUENCE IF NOT EXISTS run_event_seq;

CREATE TABLE IF NOT EXISTS task_units (
    id         bigint NOT NULL DEFAULT nextval('task_unit_seq'),
    run_id     bigint NOT NULL,
    task_id    bigint NOT NULL,
    unit_key   text   NOT NULL,     -- joins source_units on (source, store, unit_key)
    parent     text,                -- crawl: the referring URL. Hierarchical work.
    depth      integer NOT NULL DEFAULT 0,
    state      text NOT NULL DEFAULT 'pending',   -- pending|running|done|skipped|failed
    reason     text,                -- robots|scope|boilerplate|no_text|http_502|…
    -- The documents id this unit produced. On the row rather than in memory so a
    -- RESUMED run still knows what it covered — an in-memory set forgets
    -- everything staged before the restart, and a prune would then delete it.
    doc_id     text,
    attempts   smallint NOT NULL DEFAULT 0,
    bytes      bigint,
    counts     jsonb NOT NULL DEFAULT '{}',
    seq        bigint NOT NULL DEFAULT nextval('task_unit_seq'),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    PRIMARY KEY (created_at, id)      -- must include the partition key
) PARTITION BY RANGE (created_at);

CREATE UNIQUE INDEX IF NOT EXISTS task_units_key_uniq  ON task_units (task_id, unit_key, created_at);
CREATE INDEX IF NOT EXISTS task_units_claim_idx ON task_units (task_id, depth, seq) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS task_units_seq_idx   ON task_units (run_id, seq);
CREATE INDEX IF NOT EXISTS task_units_state_idx ON task_units (task_id, state);

-- Lifecycle and diagnostics — roughly 50-500 rows per run, NOT one per unit
-- (per-item detail is task_units). This is also what replaces tailing
-- ~/.windex/logs/<job>.log for job output: those files live in whichever
-- container wrote them, so with the console gone and a native client the only
-- way "everything is reachable over HTTP" becomes true is to put job output in
-- Postgres.
CREATE TABLE IF NOT EXISTS run_events (
    seq     bigint NOT NULL DEFAULT nextval('run_event_seq'),
    run_id  bigint,
    task_id bigint,
    ts      timestamptz NOT NULL DEFAULT now(),
    level   text NOT NULL DEFAULT 'info',   -- debug|info|warn|error
    event   text NOT NULL,                  -- run.queued|task.leased|task.yielded|…
    message text NOT NULL DEFAULT '',
    data    jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (ts, seq)                   -- must include the partition key
) PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS run_events_run_idx   ON run_events (run_id, seq);
CREATE INDEX IF NOT EXISTS run_events_error_idx ON run_events (seq) WHERE level IN ('warn','error');

-- Monthly partitions, rolled forward on every deploy. DROP PARTITION rather than
-- DELETE is deliberate and load-bearing: schema.sql already records that rolling
-- deletes on minhash_bands never reached autovacuum's threshold and needed
-- hand-tuned settings. task_units has the same shape (smallweb alone is ~1k
-- units/night; one 20k-page crawl is 20k rows), so retention must be O(1).
--
-- Three months ahead, so a box that misses deploys for a while still has
-- somewhere to write. A DEFAULT partition is deliberately NOT created: a row
-- landing in DEFAULT makes the later CREATE for that month fail, which converts
-- a retention problem into an outage. Better to keep the window generous.
DO $$
DECLARE
    tbl  text;
    m    date;
    part text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['task_units', 'run_events'] LOOP
        FOR i IN 0..3 LOOP
            m    := (date_trunc('month', now()) + (i || ' month')::interval)::date;
            part := format('%s_%s', tbl, to_char(m, 'YYYYMM'));
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part) THEN
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                    part, tbl, m, (m + interval '1 month')::date);
            END IF;
        END LOOP;
    END LOOP;
END $$;

-- Scoped pause, replacing control.indexing + control.loop_<src> + control.ingest_<src>.
-- Paused work is simply not claimable, so one mechanism stops ingest, embed,
-- crawl and maintenance alike — and because the scheduler will create ROWS rather
-- than spawn processes, "pause doesn't stop the scheduler" stops being possible.
-- `lane:gpu` is a genuinely new capability: free the GPU for interactive queries
-- while ingest keeps staging parquet.
CREATE TABLE IF NOT EXISTS pauses (
    scope      text PRIMARY KEY,   -- 'global' | 'source:wiki' | 'lane:gpu' | 'recipe:hn_backfill'
    reason     text NOT NULL DEFAULT '',
    paused_by  text NOT NULL DEFAULT '',
    paused_at  timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz         -- optional auto-resume; NULL = until cleared
);

-- Triggers, replacing `schedule`. Any recipe becomes schedulable — today
-- REFRESH_CHAINS hardcodes the eight built-ins and _schedule_sources() subtracts
-- push sources, so a custom source or a crawl can NEVER be scheduled.
--
-- `timezone` fixes a standing lie: schedule.hour's comment says local time, but
-- _is_due compares datetime.now(), which in a container with no TZ is UTC. Every
-- migrated trigger gets 'UTC' so behaviour is preserved exactly, and converting
-- is then an explicit, visible action rather than a silent shift.
CREATE TABLE IF NOT EXISTS triggers (
    name             text PRIMARY KEY,
    recipe           text NOT NULL,
    type             text NOT NULL DEFAULT 'cron',   -- cron | interval | event | manual
    cron             text,
    interval_seconds integer,
    timezone         text NOT NULL DEFAULT 'UTC',    -- IANA
    event            text,
    params           jsonb NOT NULL DEFAULT '{}',
    priority         smallint NOT NULL DEFAULT 50,
    jitter_seconds   integer NOT NULL DEFAULT 0,
    catch_up         boolean NOT NULL DEFAULT false, -- fire ONCE on resume, never N times
    enabled          boolean NOT NULL DEFAULT true,
    last_fired_at    timestamptz,
    next_fire_at     timestamptz,      -- computed in `timezone`, stored absolute
    last_run_id      bigint,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS triggers_due_idx   ON triggers (next_fire_at) WHERE enabled;
CREATE INDEX IF NOT EXISTS triggers_event_idx ON triggers (event) WHERE enabled AND type = 'event';
