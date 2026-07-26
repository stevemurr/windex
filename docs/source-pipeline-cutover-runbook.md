# Source/Pipeline Contract-Epoch 2 Cutover

This runbook is for the one-time destructive bootstrap from the Recipe-era
database to the canonical Pipeline/Source backend. It intentionally preserves no
Postgres rows, Qdrant vectors, custom Sources, settings overrides, or Run history.
Normal `windex init-db` is non-destructive and refuses a legacy schema.

## Preconditions

1. Build and tag one immutable backend image from the reviewed commit.
2. Generate both OpenAPI documents and build the matching contract-epoch 2
   client before maintenance.
3. Record the configured Postgres DSN, Qdrant URL, `WINDEX_DATA_ROOT`, operator
   settings, required secret references, embedding model/dimension, admin token,
   and module-admin token.
4. Stop new writes and disable Source triggers.
5. Stop, in order: API writers, Source scheduler, workers, then any legacy
   embedding/indexing processes. Verify no Windex writer session remains.
6. Start only Postgres and Qdrant.

Do not use this procedure against a shared Postgres database, a shared Qdrant
collection, a filesystem root, a home directory, or an unresolved/wildcard
target.

## Dry run and review

Choose a unique, immutable bootstrap ID such as the release tag plus UTC time:

```console
uv run windex source-pipeline-cutover \
  --bootstrap-id epoch2-20260725T220000Z \
  --dedicated-qdrant-reset
```

Review every resolved value in the JSON:

- exact Postgres host, database, and `public` schema;
- exact Qdrant endpoint and each manifest-owned alias/collection;
- new `WINDEX_DATA_ROOT/generations/<bootstrap-id>` path;
- prior generation path and exact quarantine destination;
- exact flat legacy `downloads`/`staging` directories, when upgrading the
  pre-generation filesystem layout;
- contract epoch and deterministic seed hash.

The command rejects blank values, wildcards, filesystem roots, an escaped
generation path, and malformed service URLs. Save the emitted `confirmation`
and `quarantine_confirmation` strings with the change record.

## Execute or resume

With writers still stopped, provide the exact reviewed reset confirmation:

```console
uv run windex source-pipeline-cutover \
  --bootstrap-id epoch2-20260725T220000Z \
  --dedicated-qdrant-reset \
  --execute \
  --confirm 'RESET epoch2-20260725T220000Z <manifest-hash>'
```

The durable marker is
`WINDEX_DATA_ROOT/cutover/<bootstrap-id>.json`. The command advances through:

```text
preflight -> postgres_reset -> qdrant_reset -> filesystem_generation
          -> schema_bootstrap -> seed -> verified
```

`--dedicated-qdrant-reset` is an explicit assertion that the resolved Qdrant
service is dedicated to Windex. Its manifest enumerates every existing alias
and collection by exact name; execution never discovers broader targets after
confirmation.

Re-running the exact command resumes completed phases. A manifest-hash mismatch
is refused. The reset drops only the resolved Postgres `public` schema, deletes
only manifest-owned Qdrant resources, creates the generation-scoped filesystem,
bootstraps the canonical schema, seeds the checked-in matrix, verifies contract
epoch/seed hash, and atomically switches the `generations/current` symlink.

If a phase fails, leave the marker and targets in place, correct the external
failure, and rerun the exact command. Do not manually connect a partially reset
old/new combination.

## Controlled restart and acceptance

1. Start the API with all Source triggers disabled.
2. Verify `/admin/v1/health` reports contract epoch `2` and schema generation
   `2`.
3. Verify `/admin/v1/registry`, seeded Pipelines/Sources,
   `/admin/v1/overview`, `/admin/v1/events/stream`, and
   `/admin/v1/log-events/stream`.
4. Confirm event redaction with a token-shaped test value.
5. Start workers, then the Source scheduler.
6. Queue one bounded Source Run. Observe graph tasks, the visible
   `platform.index` continuation, and terminal `succeeded`.
7. Query its document through `/v1/search`.
8. Enable Sources and triggers individually while watching queue pressure,
   failures, and searchable counts.

Do not quarantine the old generation until the bounded Source reaches searchable
output and the query succeeds.

## Quarantine the prior generation

After acceptance, move the prior generation using the reviewed confirmation:

```console
uv run windex source-pipeline-quarantine \
  --bootstrap-id epoch2-20260725T220000Z \
  --confirm 'QUARANTINE epoch2-20260725T220000Z <manifest-hash>'
```

This refuses the active generation, paths outside `generations/`, unknown flat
legacy entries, paths outside the exact quarantine root, and an unverified
marker. It renames rather than deletes the old generation or the exact reviewed
flat legacy `downloads`/`staging` entries. Pruning quarantined data is a separate
exact-target operation and is not part of this cutover.

## Rollback boundary

There is no data rollback: old database rows and vectors are intentionally
disposable. Before acceptance, fix forward by resuming the phase marker. After
the current symlink changes, an application rollback requires a matching
contract-epoch 2 image; a Recipe-era image must not be started against the new
schema. The quarantined filesystem generation is retained only for deliberate
file import/reindex, never for reconnecting stale ledger references.
