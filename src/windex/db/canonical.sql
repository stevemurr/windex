-- Windex contract epoch 2.  Fresh-schema bootstrap only.
--
-- Normal startup applies this file only to an empty database or one already
-- carrying the matching windex_meta row.  Destructive cutover is a separate
-- reviewed command; there is intentionally no DROP statement here.

CREATE TABLE IF NOT EXISTS windex_meta (
    singleton          boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    schema_generation  integer NOT NULL,
    contract_epoch     integer NOT NULL,
    seed_hash          text NOT NULL DEFAULT '',
    bootstrap_id       text NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipelines (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    title       text NOT NULL DEFAULT '',
    description text NOT NULL DEFAULT '',
    builtin     boolean NOT NULL DEFAULT false,
    archived_at timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_revisions (
    id                 bigserial PRIMARY KEY,
    pipeline_id        bigint NOT NULL REFERENCES pipelines(id) ON DELETE RESTRICT,
    version            integer NOT NULL CHECK (version > 0),
    parent_revision_id bigint REFERENCES pipeline_revisions(id) ON DELETE RESTRICT,
    spec               jsonb NOT NULL,
    spec_hash          text NOT NULL,
    registry_version   text NOT NULL,
    registry_digest    text NOT NULL,
    module_locks       jsonb NOT NULL,
    author             text NOT NULL DEFAULT '',
    note               text NOT NULL DEFAULT '',
    created_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, version),
    UNIQUE (pipeline_id, spec_hash)
);
CREATE OR REPLACE FUNCTION windex_immutable_pipeline_revision()
RETURNS trigger LANGUAGE plpgsql AS $immutable_pipeline_revision$
BEGIN
    RAISE EXCEPTION 'Pipeline revisions are immutable';
END
$immutable_pipeline_revision$;
DROP TRIGGER IF EXISTS pipeline_revisions_immutable ON pipeline_revisions;
CREATE TRIGGER pipeline_revisions_immutable
BEFORE UPDATE OR DELETE ON pipeline_revisions
FOR EACH ROW EXECUTE FUNCTION windex_immutable_pipeline_revision();

ALTER TABLE pipelines
    ADD COLUMN IF NOT EXISTS head_revision_id bigint
    REFERENCES pipeline_revisions(id) ON DELETE RESTRICT;

CREATE TABLE IF NOT EXISTS pipeline_layouts (
    pipeline_revision_id bigint NOT NULL
        REFERENCES pipeline_revisions(id) ON DELETE CASCADE,
    flow_name   text NOT NULL,
    layout      jsonb NOT NULL DEFAULT '{}',
    layout_etag text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_revision_id, flow_name)
);

CREATE TABLE IF NOT EXISTS sources (
    id                      bigserial PRIMARY KEY,
    name                    text NOT NULL UNIQUE,
    title                   text NOT NULL DEFAULT '',
    description             text NOT NULL DEFAULT '',
    origin                  jsonb NOT NULL DEFAULT '{}',
    pipeline_revision_id    bigint NOT NULL
        REFERENCES pipeline_revisions(id) ON DELETE RESTRICT,
    search_contract_version text NOT NULL,
    search_name             text NOT NULL UNIQUE,
    id_prefix               text NOT NULL UNIQUE,
    collection_key          text NOT NULL UNIQUE,
    search_profile          text NOT NULL,
    include_in_all          boolean NOT NULL DEFAULT true,
    state_namespace         text NOT NULL UNIQUE,
    enabled                 boolean NOT NULL DEFAULT true,
    generation              bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
    archived_at             timestamptz,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_config (
    source_id   bigint PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    values      jsonb NOT NULL DEFAULT '{}',
    values_hash text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_triggers (
    id            bigserial PRIMARY KEY,
    source_id     bigint NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    flow_name     text NOT NULL,
    trigger_type  text NOT NULL CHECK (
        trigger_type IN ('cron', 'interval', 'event', 'manual')),
    trigger_spec  jsonb NOT NULL DEFAULT '{}',
    enabled       boolean NOT NULL DEFAULT true,
    next_fire_at  timestamptz,
    last_fired_at timestamptz,
    last_run_id   bigint,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS source_triggers_due_idx
    ON source_triggers (next_fire_at) WHERE enabled;

CREATE TABLE IF NOT EXISTS source_control (
    source_id    bigint PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    paused       boolean NOT NULL DEFAULT false,
    pause_reason text NOT NULL DEFAULT '',
    paused_at    timestamptz,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS operator_settings (
    scope       text PRIMARY KEY,
    values      jsonb NOT NULL DEFAULT '{}',
    values_hash text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS secret_references (
    name       text PRIMARY KEY,
    provider   text NOT NULL,
    configured boolean NOT NULL DEFAULT false,
    metadata   jsonb NOT NULL DEFAULT '{}',
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Canonical document ledger.  Public search identity remains denormalized for
-- efficient query filters, while source_id retains the deployment owner.
CREATE TABLE IF NOT EXISTS documents (
    id             text PRIMARY KEY,
    source_id      bigint NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    owner_run_id   bigint,
    source         text NOT NULL,
    url            text NOT NULL,
    canonical_url  text,
    title          text,
    published_at   timestamptz,
    lang           text,
    text_hash      text,
    metadata_hash  text,
    indexed_metadata_hash text,
    status         text NOT NULL DEFAULT 'staged',
    duplicate_of   text,
    embedded_model text,
    text_ref       text,
    indexed_at     timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE documents
    ADD COLUMN IF NOT EXISTS metadata_hash text,
    ADD COLUMN IF NOT EXISTS indexed_metadata_hash text;
CREATE INDEX IF NOT EXISTS documents_source_published_idx
    ON documents (source_id, published_at);
CREATE INDEX IF NOT EXISTS documents_status_idx ON documents (source_id, status);
CREATE INDEX IF NOT EXISTS documents_run_status_idx
    ON documents (owner_run_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS documents_canonical_url_idx ON documents (canonical_url);
CREATE INDEX IF NOT EXISTS documents_text_hash_idx ON documents (text_hash);
CREATE INDEX IF NOT EXISTS documents_embed_backlog_idx
    ON documents (source_id, created_at) WHERE status = 'staged';
CREATE INDEX IF NOT EXISTS documents_indexed_at_idx
    ON documents (indexed_at DESC) WHERE indexed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS documents_metadata_backlog_idx
    ON documents (source_id, updated_at)
    WHERE status = 'searchable'
      AND metadata_hash IS DISTINCT FROM indexed_metadata_hash;

-- The one retained specialized Source store: GitHub repository ranking and
-- hydration needs a wide relational shape.  All ordinary watermarks use
-- source_units.
CREATE TABLE IF NOT EXISTS repos (
    source_id         bigint NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    repo_id           bigint NOT NULL,
    full_name         text NOT NULL,
    stars             integer,
    star_events       integer NOT NULL DEFAULT 0,
    description       text,
    topics            text[],
    primary_language  text,
    default_branch    text,
    pushed_at         timestamptz,
    discovered_at     timestamptz,
    readme_fetched_at timestamptz,
    status            text NOT NULL DEFAULT 'candidate',
    PRIMARY KEY (source_id, repo_id),
    UNIQUE (source_id, full_name)
);
CREATE INDEX IF NOT EXISTS repos_status_idx ON repos (source_id, status);
CREATE INDEX IF NOT EXISTS repos_stars_idx ON repos (source_id, stars);

CREATE TABLE IF NOT EXISTS minhash_bands (
    source_id bigint NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    band_idx  smallint NOT NULL,
    band_hash bigint NOT NULL,
    doc_id    text NOT NULL,
    day       date NOT NULL,
    PRIMARY KEY (source_id, band_idx, band_hash, doc_id)
);
CREATE INDEX IF NOT EXISTS minhash_bands_day_idx ON minhash_bands (day);

CREATE TABLE IF NOT EXISTS source_units (
    source_id      bigint REFERENCES sources(id) ON DELETE CASCADE,
    state_namespace text NOT NULL,
    store          text NOT NULL,
    unit_key       text NOT NULL,
    ord            text,
    upstream       jsonb,
    ingested       jsonb,
    stage          text NOT NULL DEFAULT 'pending',
    status         text NOT NULL DEFAULT 'pending',
    attrs          jsonb NOT NULL DEFAULT '{}',
    owner_run_id   bigint,
    last_run_id    bigint,
    attempts       smallint NOT NULL DEFAULT 0,
    claimed_at     timestamptz,
    lease_until    timestamptz,
    processed_at   timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (state_namespace, store, unit_key)
);
CREATE OR REPLACE FUNCTION windex_bind_source_unit()
RETURNS trigger LANGUAGE plpgsql AS $bind_source_unit$
BEGIN
    IF NEW.source_id IS NULL THEN
        SELECT id INTO NEW.source_id FROM sources
         WHERE state_namespace = NEW.state_namespace;
    END IF;
    IF NEW.source_id IS NULL THEN
        RAISE EXCEPTION 'unknown Source state namespace %', NEW.state_namespace;
    END IF;
    RETURN NEW;
END
$bind_source_unit$;
DROP TRIGGER IF EXISTS source_units_bind_source ON source_units;
CREATE TRIGGER source_units_bind_source
BEFORE INSERT OR UPDATE OF state_namespace ON source_units
FOR EACH ROW EXECUTE FUNCTION windex_bind_source_unit();
CREATE INDEX IF NOT EXISTS source_units_pending_idx
    ON source_units (source_id, store, ord)
    WHERE status IN ('pending', 'failed');
CREATE INDEX IF NOT EXISTS source_units_rotate_idx
    ON source_units (source_id, store, processed_at NULLS FIRST);

CREATE TABLE IF NOT EXISTS runs (
    id                    bigserial PRIMARY KEY,
    source_id             bigint REFERENCES sources(id) ON DELETE RESTRICT,
    source_name           text,
    pipeline_name         text NOT NULL,
    pipeline_revision_id  bigint NOT NULL
        REFERENCES pipeline_revisions(id) ON DELETE RESTRICT,
    pipeline_version      integer NOT NULL,
    pipeline_hash         text NOT NULL,
    flow_name             text NOT NULL,
    source_snapshot       jsonb,
    effective_config      jsonb NOT NULL DEFAULT '{}',
    explicit_inputs       jsonb NOT NULL DEFAULT '{}',
    frozen_spec           jsonb NOT NULL,
    module_locks          jsonb NOT NULL,
    trigger_type          text NOT NULL DEFAULT 'manual',
    trigger_by            text NOT NULL DEFAULT '',
    mode                  text NOT NULL DEFAULT 'run',
    priority              smallint NOT NULL DEFAULT 50,
    dedupe_key            text,
    idempotency_key       text,
    state                 text NOT NULL DEFAULT 'queued',
    cancel_requested      boolean NOT NULL DEFAULT false,
    queued_at             timestamptz NOT NULL DEFAULT now(),
    started_at            timestamptz,
    finished_at           timestamptz,
    updated_at            timestamptz NOT NULL DEFAULT now(),
    progress              jsonb NOT NULL DEFAULT '{}',
    stats                 jsonb NOT NULL DEFAULT '{}',
    error                 text
);
CREATE UNIQUE INDEX IF NOT EXISTS runs_dedupe_live_uniq ON runs (dedupe_key)
    WHERE dedupe_key IS NOT NULL AND state IN ('queued', 'running', 'blocked');
CREATE UNIQUE INDEX IF NOT EXISTS runs_source_idempotency_uniq
    ON runs (source_id, idempotency_key)
    WHERE source_id IS NOT NULL AND idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS runs_live_idx ON runs (state, priority DESC, id)
    WHERE state IN ('queued', 'running', 'blocked');
CREATE INDEX IF NOT EXISTS runs_source_idx ON runs (source_id, id DESC);
CREATE INDEX IF NOT EXISTS runs_pipeline_idx
    ON runs (pipeline_revision_id, id DESC);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'documents_owner_run_fk'
    ) THEN
        ALTER TABLE documents
            ADD CONSTRAINT documents_owner_run_fk
            FOREIGN KEY (owner_run_id) REFERENCES runs(id) ON DELETE SET NULL;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS run_tasks (
    id               bigserial PRIMARY KEY,
    run_id           bigint NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    source_id        bigint REFERENCES sources(id) ON DELETE RESTRICT,
    source_name      text,
    node             text NOT NULL,
    kind             text NOT NULL,
    module           text NOT NULL,
    module_version   text NOT NULL,
    module_digest    text NOT NULL,
    executor         text NOT NULL,
    lane             text NOT NULL DEFAULT 'io',
    config           jsonb NOT NULL DEFAULT '{}',
    depends_on       text[] NOT NULL DEFAULT '{}',
    preconditions    text[] NOT NULL DEFAULT '{}',
    captures         text[] NOT NULL DEFAULT '{}',
    state            text NOT NULL DEFAULT 'pending',
    priority         smallint NOT NULL DEFAULT 50,
    attempts         smallint NOT NULL DEFAULT 0,
    max_attempts     smallint NOT NULL DEFAULT 3,
    lease_worker     text,
    lease_seconds    integer NOT NULL DEFAULT 300,
    lease_expires_at timestamptz,
    heartbeat_at     timestamptz,
    yield_requested  boolean NOT NULL DEFAULT false,
    cursor           jsonb NOT NULL DEFAULT '{}',
    units_total      integer NOT NULL DEFAULT -1,
    units_done       integer NOT NULL DEFAULT 0,
    units_failed     integer NOT NULL DEFAULT 0,
    weight           real NOT NULL DEFAULT 1.0,
    stats            jsonb NOT NULL DEFAULT '{}',
    started_at       timestamptz,
    finished_at      timestamptz,
    error            text,
    UNIQUE (run_id, node)
);
CREATE INDEX IF NOT EXISTS run_tasks_claim_idx
    ON run_tasks (lane, priority DESC, id) WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS run_tasks_running_idx
    ON run_tasks (lane, source_id) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS run_tasks_lease_idx
    ON run_tasks (lease_expires_at) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS run_tasks_run_idx ON run_tasks (run_id);

CREATE TABLE IF NOT EXISTS source_sched (
    source_id  bigint PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    weight     real NOT NULL DEFAULT 1.0,
    vtime      double precision NOT NULL DEFAULT 0,
    in_flight  integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS task_unit_seq;
CREATE SEQUENCE IF NOT EXISTS operational_event_seq;

CREATE TABLE IF NOT EXISTS task_units (
    id          bigint NOT NULL DEFAULT nextval('task_unit_seq'),
    run_id      bigint NOT NULL,
    task_id     bigint NOT NULL,
    unit_key    text NOT NULL,
    parent      text,
    depth       integer NOT NULL DEFAULT 0,
    state       text NOT NULL DEFAULT 'pending',
    reason      text,
    doc_id      text,
    attempts    smallint NOT NULL DEFAULT 0,
    bytes       bigint,
    counts      jsonb NOT NULL DEFAULT '{}',
    outputs     jsonb NOT NULL DEFAULT '[]',
    seq         bigint NOT NULL DEFAULT nextval('task_unit_seq'),
    created_at  timestamptz NOT NULL DEFAULT now(),
    started_at  timestamptz,
    finished_at timestamptz,
    PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);
CREATE INDEX IF NOT EXISTS task_units_claim_idx
    ON task_units (task_id, depth, seq) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS task_units_seq_idx ON task_units (run_id, seq);
CREATE INDEX IF NOT EXISTS task_units_state_idx ON task_units (task_id, state);

CREATE TABLE IF NOT EXISTS run_outputs (
    run_id     bigint NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    boundary  text NOT NULL,
    value_type text NOT NULL,
    value      jsonb,
    size_bytes bigint NOT NULL,
    checksum   text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, boundary)
);

CREATE TABLE IF NOT EXISTS run_artifacts (
    id          text PRIMARY KEY,
    run_id      bigint NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    boundary    text,
    media_type  text NOT NULL,
    relative_path text NOT NULL,
    size_bytes  bigint NOT NULL,
    checksum    text NOT NULL,
    expires_at  timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS run_artifacts_run_idx ON run_artifacts (run_id);

CREATE TABLE IF NOT EXISTS operational_events (
    seq              bigint NOT NULL DEFAULT nextval('operational_event_seq'),
    ts               timestamptz NOT NULL DEFAULT now(),
    level            text NOT NULL DEFAULT 'info',
    component        text NOT NULL,
    source_name      text,
    pipeline_name    text,
    pipeline_version integer,
    run_id           bigint,
    task_id          bigint,
    node             text,
    module           text,
    event            text NOT NULL,
    message          text NOT NULL DEFAULT '',
    data             jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (ts, seq)
) PARTITION BY RANGE (ts);
CREATE INDEX IF NOT EXISTS operational_events_seq_idx ON operational_events (seq);
CREATE INDEX IF NOT EXISTS operational_events_run_idx
    ON operational_events (run_id, seq) WHERE run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS operational_events_source_idx
    ON operational_events (source_name, seq) WHERE source_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS operational_events_error_idx
    ON operational_events (seq) WHERE level IN ('warn', 'error');

CREATE TABLE IF NOT EXISTS search_metrics (
    ts             timestamptz NOT NULL DEFAULT now(),
    source         text NOT NULL,
    mode_requested text NOT NULL,
    degraded       boolean NOT NULL DEFAULT false,
    q_hash         text,
    embed_ms       integer,
    search_ms      integer,
    total_ms       integer,
    results        integer
);
CREATE INDEX IF NOT EXISTS search_metrics_ts_idx ON search_metrics (ts);

CREATE TABLE IF NOT EXISTS search_quality (
    ts              timestamptz NOT NULL DEFAULT now(),
    mode            text NOT NULL,
    k               integer NOT NULL,
    known_item_ndcg real,
    known_item_mrr  real,
    golden_ndcg     real,
    golden_mrr      real,
    judge_ndcg      real,
    git_sha         text,
    detail          jsonb
);
CREATE INDEX IF NOT EXISTS search_quality_ts_idx ON search_quality (ts);

CREATE TABLE IF NOT EXISTS operation_confirmations (
    token_hash   text PRIMARY KEY,
    operation    text NOT NULL,
    subject      text NOT NULL,
    payload_hash text NOT NULL,
    expires_at   timestamptz NOT NULL,
    consumed_at  timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS storage_ownership (
    generation     bigint NOT NULL,
    resource_type text NOT NULL,
    resource_name text NOT NULL,
    metadata      jsonb NOT NULL DEFAULT '{}',
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (generation, resource_type, resource_name)
);

-- BE-8 storage is canonical even when custom execution is not enabled.
CREATE TABLE IF NOT EXISTS module_definitions (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    title       text NOT NULL DEFAULT '',
    description text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS module_versions (
    id                     bigserial PRIMARY KEY,
    module_id              bigint NOT NULL
        REFERENCES module_definitions(id) ON DELETE RESTRICT,
    version                integer NOT NULL CHECK (version > 0),
    runtime                text NOT NULL,
    kind                   text NOT NULL,
    port_spec              jsonb NOT NULL,
    parameter_schema       jsonb NOT NULL DEFAULT '[]',
    requested_capabilities text[] NOT NULL DEFAULT '{}',
    allowed_hosts          text[] NOT NULL DEFAULT '{}',
    source                 text NOT NULL,
    source_digest          text NOT NULL,
    approval_state         text NOT NULL DEFAULT 'draft',
    resource_limits        jsonb NOT NULL,
    approved_by            text,
    approved_at            timestamptz,
    revoked_at             timestamptz,
    created_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (module_id, version),
    UNIQUE (module_id, source_digest)
);
CREATE OR REPLACE FUNCTION windex_immutable_approved_module()
RETURNS trigger LANGUAGE plpgsql AS $immutable_approved_module$
BEGIN
    IF TG_OP = 'DELETE' AND OLD.approval_state IN ('available', 'revoked') THEN
        RAISE EXCEPTION 'approved Module versions are immutable';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.approval_state IN ('available', 'revoked') THEN
        IF OLD.approval_state = 'available'
           AND NEW.approval_state = 'revoked'
           AND NEW.source = OLD.source
           AND NEW.source_digest = OLD.source_digest
           AND NEW.version = OLD.version
           AND NEW.module_id = OLD.module_id THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'approved Module versions are immutable';
    END IF;
    RETURN COALESCE(NEW, OLD);
END
$immutable_approved_module$;
DROP TRIGGER IF EXISTS module_versions_immutable ON module_versions;
CREATE TRIGGER module_versions_immutable
BEFORE UPDATE OR DELETE ON module_versions
FOR EACH ROW EXECUTE FUNCTION windex_immutable_approved_module();

CREATE OR REPLACE FUNCTION windex_roll_canonical_partitions(
    months_ahead integer DEFAULT 3,
    keep_months integer DEFAULT 0
) RETURNS TABLE (action text, part text) LANGUAGE plpgsql AS $windex$
DECLARE
    tbl text;
    month_start date;
    part_name text;
    i integer;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['task_units', 'operational_events'] LOOP
        FOR i IN 0..months_ahead LOOP
            month_start := (
                date_trunc('month', now()) + (i || ' month')::interval)::date;
            part_name := format('%s_%s', tbl, to_char(month_start, 'YYYYMM'));
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                    part_name, tbl, month_start,
                    (month_start + interval '1 month')::date);
                action := 'created';
                part := part_name;
                RETURN NEXT;
            END IF;
        END LOOP;
        IF keep_months > 0 THEN
            FOR part_name IN
                SELECT child.relname
                FROM pg_inherits inheritance
                JOIN pg_class child ON child.oid = inheritance.inhrelid
                WHERE inheritance.inhparent = tbl::regclass
                  AND child.relname ~ ('^' || tbl || '_[0-9]{6}$')
                  AND to_date(right(child.relname, 6), 'YYYYMM')
                      < (date_trunc('month', now())
                         - (keep_months || ' month')::interval)::date
            LOOP
                EXECUTE format('DROP TABLE %I', part_name);
                action := 'dropped';
                part := part_name;
                RETURN NEXT;
            END LOOP;
        END IF;
    END LOOP;
END
$windex$;

SELECT windex_roll_canonical_partitions(3, 0);
