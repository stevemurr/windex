# Memory ingestion contract

**Status: changed 2026-07-26.** Every memory-consuming application must be
updated. Clients written against the pre-cutover contract no longer work — the
`/v1/memory/conversations/*` endpoints were removed by the contract epoch 2
cutover, and the compatibility path that silently accepted unattributed batches
has been closed.

## Endpoint

```
POST /v1/sources/memory/ingest
```

Headers:

```
Authorization: Bearer <agent-token>
Content-Type: application/json
Idempotency-Key: <stable unique key, 8-128 characters>
```

## Replacing a conversation

```json
{
  "schema_version": "windex.ingest/1",
  "mode": "full",
  "partition": "<conversation-id>",
  "documents": [
    {
      "id": "<conversation-id>/00000",
      "url": "llmchat://chat/<conversation-id>?chunk=0",
      "title": "Conversation title",
      "text": "Chunk text",
      "published_at": "2026-07-26T20:00:00Z",
      "fields": {
        "conversation_id": "<conversation-id>",
        "chunk_index": 0,
        "message_range": [0, 12]
      }
    }
  ]
}
```

## Rules

- **One conversation per request.** Never mix conversations in one batch.
- **Each request is the complete current snapshot** of that conversation.
  `mode: "full"` is scoped to the request's partition, not to the whole source,
  so anything the request omits is tombstoned for that conversation only.
- **Every document id must sit under the `<conversation-id>/` prefix.**
  Replacement works by id scope; a document filed outside its own conversation
  can never be replaced or deleted afterwards.
- **`fields.message_range` is optional, but its shape is fixed.** When present,
  send the inclusive message indexes as exactly two non-negative integers,
  `[start, end]`, with `start <= end`. The same array is returned as
  `message_range` in search hits and document detail.
- **Use a new idempotency key for each conversation revision.** Reuse a key only
  when retrying the identical payload — a repeated
  `(source, Idempotency-Key)` returns the existing run and ingests nothing.
  This is the trap worth naming: a client that keys only on the conversation id
  will find that re-indexing silently does nothing.
- **HTTP 202 means queued, not completed.** Embedding happens afterwards and can
  still fail. Use the returned `run_id` to monitor completion if you need
  certainty; a client that accepts on 202 must treat "believed synced but never
  embedded" as a real state and offer a re-index path.
- **Invalid, mixed, or unattributed batches are rejected synchronously with
  HTTP 422.** The worker repeats the same validation defensively, but the API
  does not return 202 for a deterministic conversation-identity error.

## Deleting a conversation

```json
{
  "schema_version": "windex.ingest/1",
  "mode": "full",
  "partition": "<conversation-id>",
  "documents": []
}
```

An empty full push is a complete census of a conversation with no chunks, so the
partition is tombstoned. `partition` is required here: with no documents there
is nothing else to attribute the request to.

## Reading message ranges

New and re-pushed chunks retain `message_range` in parquet and Qdrant. Both
`GET /v1/search` hits and `GET /v1/docs/{doc_id}` detail return the optional
two-integer array. Chunks written before this retention fix do not have enough
stored information for the backend to reconstruct the range; re-push each
conversation's complete snapshot to backfill it.

## Historical repair

Only start this once a consumer is ready to perform the complete backfill —
step 1 is destructive and step 2 is what restores the corpus.

1. Delete the corrupt legacy partition: send an empty full push with
   `partition: "push"`. Everything written by a pre-cutover client collapsed
   into that single partition.
2. Re-export and full-push every real conversation separately, each under its
   own partition.

## Why the contract changed

`IngestRequest` is strict and carries no batch-level conversation id, but
`push.docs` still looked for one. Every memory push therefore resolved to the
literal partition `"push"`, so all conversations collided in one id space — and
because the memory source replaces by id scope, each push tombstoned every
conversation pushed before it. Only the last chat written survived, and
`conversation_id` in the search payload was the string `"push"`, which broke
conversation filtering entirely.

Identity now comes from the documents themselves, with `partition` available for
the empty-batch delete case. See `src/windex/modules/receive.py` and
`tests/test_memory_push.py`.
