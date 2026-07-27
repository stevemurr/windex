# Custom push Sources

Contract epoch 2 represents a custom corpus with two canonical objects:

- a reusable immutable Pipeline revision; and
- a concrete Source pinned to that revision with unique search/storage
  identities.

The built-in `custom` Pipeline is the ordinary choice. Its `receive` Flow uses
`push.docs` in delta mode followed by `ledger.stage`; the worker then adds the
`platform.index` continuation. There is no separate custom-source registry,
document CRUD service, or embedding process.

Admin routes are under `/admin/v1` and use the operator token. Document writes
use the canonical `/v1/sources/{name}/ingest` data route and the same write
token.

## Create a Source

Choose each identity once:

- `name`: control-plane identifier;
- `search_name`: public `source=` value;
- `id_prefix`: prefix added to every submitted document suffix;
- `collection_key`: Qdrant collection/alias namespace;
- `state_namespace`: Pipeline state namespace.

Those five values must not collide with another active Source and cannot be
patched later. Private corpora should normally set `include_in_all: false`.

```sh
B=http://127.0.0.1:8100
TOKEN="${WINDEX_WRITE_TOKEN:?export WINDEX_WRITE_TOKEN first}"
AUTH="Authorization: Bearer $TOKEN"

source_body='{
  "name": "team_docs",
  "title": "Team documents",
  "description": "Private notes pushed by the team agent",
  "origin": {"ingress": "push", "producer": "team-agent"},
  "metadata": {
    "team-agent": {
      "refresh": {"tool": "http.get", "cursor": "$.next"}
    }
  },
  "pipeline_name": "custom",
  "search_name": "team_docs",
  "id_prefix": "team_docs:",
  "collection_key": "team_docs",
  "search_profile": "documents",
  "include_in_all": false,
  "state_namespace": "team_docs",
  "enabled": true,
  "values": {"max_docs": 500}
}'

curl -fsS -X POST "$B/admin/v1/sources/validate" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d "$source_body" | jq .

curl -fsS -X POST "$B/admin/v1/sources" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d "$source_body" | jq .
```

Omitting `pipeline_version` binds to the current `custom` head at creation.
`POST /admin/v1/sources/validate` is side-effect free; require `valid: true`
before creating. Creation returns HTTP 201 and a canonical Source model.

Inspect the active ingest contract:

```sh
curl -fsS "$B/admin/v1/sources/team_docs" -H "$AUTH" | jq \
  '{pipeline_name,pipeline_version,ready,ingress,metadata,values}'
```

The current template defaults to 500 documents per request and permits the
Source setting to raise that to 10,000. The API also enforces a 64 MiB aggregate
text limit and one million characters per document. The current `push.docs`
Module default is stricter—16,000 characters per document—so clients should
honor the pinned revision's Module configuration, not only the outer HTTP
schema.

## Client-owned Source metadata

`Source.metadata` is an opaque JSON object for client-owned persistent state.
The server stores and returns it but never reads it to select ingress, compile a
Pipeline, or execute a Run. It can be supplied to Source creation and replaced
with `PATCH /admin/v1/sources/{name}`. The encoded object is limited to 64 KiB.

Clients should own one namespaced top-level key and keep their data beneath it;
for example, Talkie stores refresh programs under
`metadata.talkie.recipe`. Do not put client state in `origin`: `origin` is
server-interpreted provenance and its keys may affect Source behavior.

## Delta ingest

The built-in custom Pipeline accepts `mode: "delta"` only. Present documents
are upserted; IDs absent from a request are left alone.

```sh
batch='{
  "schema_version": "windex.ingest/1",
  "mode": "delta",
  "documents": [
    {
      "id": "flight-001",
      "url": "teamdocs://travel/flight-001",
      "canonical_url": "teamdocs://travel/flight-001",
      "title": "Tokyo flight",
      "text": "Flight confirmation for Tokyo on 5 August.",
      "published_at": "2026-07-26T20:00:00Z",
      "lang": "en",
      "fields": {
        "workspace": "travel",
        "sender": "airline"
      }
    },
    {
      "id": "hotel-001",
      "url": "teamdocs://travel/hotel-001",
      "title": "Shibuya hotel",
      "text": "Hotel booking in Shibuya.",
      "fields": {"workspace": "travel"}
    }
  ]
}'

response=$(
  curl -fsS -X POST "$B/v1/sources/team_docs/ingest" \
    -H "$AUTH" \
    -H 'Idempotency-Key: team-docs-import-0001' \
    -H 'Content-Type: application/json' \
    -d "$batch"
)
run=$(jq -r .run_id <<<"$response")
curl -fsS "$B/admin/v1/runs/$run" -H "$AUTH" | jq .
```

Document `id` is the suffix; the stored public ID above is
`team_docs:flight-001`. Do not include the Source prefix in the submitted
suffix.

`fields` is the only public extension object. Non-underscore keys are preserved
in the search hit's `extra` object. Keys beginning with `_` are Pipeline
internal and are not exposed. Unknown top-level properties such as `extra` or
`payload` are rejected by the strict epoch-2 request model.

HTTP 202 means the Run was queued. It does not mean staging, embedding, or the
Qdrant write succeeded. Poll `GET /admin/v1/runs/{run_id}` to a terminal state
when delivery matters. An unchanged replay may do no document work, but it is
still represented by a Run.

## Idempotency and retry

`Idempotency-Key` is required, 8–128 characters, and scoped to the Source.
Retry the identical payload with the identical key. Use a new key for any
changed payload:

- same key, same Source: returns the previously created Run;
- new key: creates a new Run;
- one key reused for a changed revision: the changed revision is not ingested.

HTTP 413 means the request exceeds the pinned document-count or aggregate-text
limit; split it into smaller batches with distinct idempotency keys. HTTP 422
means the request was rejected at the HTTP boundary. Module-level document
validation can still fail after HTTP 202, so poll the Run. HTTP 409 commonly
means the Source is paused/disabled, is not push-rooted, or the requested mode
does not match its pinned Pipeline.

## Delete documents

Deletion is an explicit delta tombstone, not a separate route:

```sh
curl -fsS -X POST "$B/v1/sources/team_docs/ingest" \
  -H "$AUTH" \
  -H 'Idempotency-Key: team-docs-delete-flight-0001' \
  -H 'Content-Type: application/json' \
  -d '{
    "schema_version":"windex.ingest/1",
    "mode":"delta",
    "documents":[{
      "id":"flight-001",
      "url":"teamdocs://travel/flight-001",
      "text":"",
      "deleted":true
    }]
  }' | jq .
```

The tombstone removes the vector and marks the ledger document deleted when the
Run executes. Omitting `flight-001` from a later delta batch does nothing.

To erase and rebuild an entire Source, use the confirmation-gated Source reset:

```sh
preview=$(
  curl -fsS -X POST "$B/admin/v1/sources/team_docs/reset/preview" -H "$AUTH"
)
jq . <<<"$preview"
token=$(jq -r .confirmation_token <<<"$preview")
curl -fsS -X POST "$B/admin/v1/sources/team_docs/reset" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg token "$token" '{confirmation_token:$token}')" | jq .
```

Reset advances the Source generation and queues a reset Run. Wait for that Run
to succeed before repushing the corpus. Archiving
`POST /admin/v1/sources/{name}/archive` disables discovery and new Runs; it is
not a document-deletion operation.

## Search and document detail

```sh
curl -fsS \
  "$B/v1/search?source=team_docs&q=flight+confirmation&mode=hybrid&limit=10" |
  jq .
curl -fsS "$B/v1/docs/team_docs:flight-001" | jq .
```

A custom Source becomes a valid `source=` value only while enabled and
unarchived. `include_in_all: false` keeps it out of `source=all`. Public search
may return the submitted non-internal `fields` under `extra`; document detail
returns canonical fields and full staged text.

If the selected custom Source's index is unavailable, search returns HTTP 503.
Consumers must treat that as retryable unavailability, not as an empty result.

## Change the Pipeline safely

Editing a Pipeline publishes a new immutable revision; it does not silently
move Sources. Existing Pipeline publication requires an expected-head
precondition, and each Source upgrade requires a server preview plus the exact
candidate and confirmation token.

After deploying a change to `push.docs`, `ledger.stage`, or another locked
Module:

1. run the new image's `windex init-db`;
2. inspect `GET /admin/v1/sources/team_docs/status` (or the dedicated
   `module-status` projection);
3. preview the reported `latest_pipeline_version`;
4. submit the preview's exact `candidate` and `confirmation_token`; and
5. start new workers only after affected Sources are upgraded.

A restart alone cannot repair a Source pinned to a revoked implementation
digest. See [the production deployment procedure](operations.md#rebuild-and-deploy).

## Client contract summary

Custom-source producers need only these stable surfaces:

| Purpose | Route |
|---|---|
| inspect Source and advertised ingress | `GET /admin/v1/sources/{name}` |
| persist client-owned Source metadata | `POST /admin/v1/sources`; `PATCH /admin/v1/sources/{name}` |
| queue delta documents/tombstones | `POST /v1/sources/{name}/ingest` |
| poll accepted work | `GET /admin/v1/runs/{run_id}` |
| query | `GET /v1/search?source={search_name}&q=...` |
| retrieve staged text | `GET /v1/docs/{document_id}` |

The producer does not create Qdrant collections, invoke an embedding command,
or write parquet directly.
