from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from windex.ccnews.dedup import text_hash
from windex.modules.load import (
    _Existing,
    _ledger_rows,
    _metadata_hash,
    _parquet_row,
    _stage_action,
    _upsert_ledger,
)
from windex.modules.receive import custom_metadata
from windex.pipeline.indexing import _refresh_metadata
from windex.pipeline.ports import ExtractedDoc, PartitionRef


def _ctx(search_name: str, *, conn=None):
    return SimpleNamespace(
        conn=conn,
        source_id=1,
        run_id=None,
        search_name=search_name,
        id_prefix=f"{search_name}:",
    )


def _doc(**overrides) -> ExtractedDoc:
    values = {
        "ref": PartitionRef(store="", key="batch"),
        "suffix": "one",
        "url": "https://example.test/one",
        "canonical_url": "https://example.test/one",
        "title": "One",
        "text": "unchanged body",
        "published_at": datetime(2026, 7, 26, tzinfo=UTC),
        "lang": "en",
    }
    values.update(overrides)
    return ExtractedDoc(**values)


@pytest.mark.parametrize(
    ("source", "before", "after"),
    [
        (
            "news",
            _doc(),
            _doc(
                url="https://example.test/moved",
                published_at=datetime(2026, 7, 27, tzinfo=UTC),
                lang="fr",
            ),
        ),
        (
            "github",
            _doc(
                suffix="org/repo",
                fields={"full_name": "org/repo"},
                payload={"stars": 10, "language": "Python"},
            ),
            _doc(
                suffix="org/repo",
                fields={"full_name": "org/repo"},
                payload={"stars": 11, "language": "Rust"},
            ),
        ),
        (
            "hn",
            _doc(fields={"points": 10, "num_comments": 2}),
            _doc(fields={"points": 11, "num_comments": 3}),
        ),
        (
            "custom-search",
            _doc(payload={"workspace": "/one", "kind": "decision"}),
            _doc(payload={"workspace": "/two", "kind": "decision"}),
        ),
        (
            "memory",
            _doc(
                suffix="chat/00000",
                fields={
                    "conversation_id": "chat",
                    "chunk_index": 0,
                },
                payload={"workspace_root": "/repo/one"},
            ),
            _doc(
                suffix="chat/00000",
                fields={
                    "conversation_id": "chat",
                    "chunk_index": 0,
                },
                payload={"workspace_root": "/repo/two"},
            ),
        ),
    ],
)
def test_source_metadata_changes_have_distinct_fingerprints(
    source,
    before,
    after,
):
    ctx = _ctx(source)
    assert before.title == after.title
    assert before.text == after.text
    assert _metadata_hash(ctx, before) != _metadata_hash(ctx, after)


def test_metadata_fingerprint_is_deterministic_and_excludes_body_text():
    ctx = _ctx("custom-search")
    first = _doc(payload={"z": [3, 2, 1], "a": {"b": True}})
    reordered = _doc(payload={"a": {"b": True}, "z": [3, 2, 1]})
    different_text = _doc(
        text="new body", payload={"a": {"b": True}, "z": [3, 2, 1]})

    assert _metadata_hash(ctx, first) == _metadata_hash(ctx, reordered)
    assert _metadata_hash(ctx, first) == _metadata_hash(ctx, different_text)


def test_memory_and_custom_metadata_reach_staged_payload():
    memory = _doc(
        suffix="chat/00000",
        fields={
            "conversation_id": "chat",
            "chunk_index": 0,
        },
        payload={"workspace_root": "/repo", "kind": "decision"},
    )
    memory_row = _parquet_row(_ctx("memory"), memory)
    assert memory_row["published_at"] == datetime(2026, 7, 26, tzinfo=UTC)
    assert memory_row["extra"] == \
        '{"kind":"decision","workspace_root":"/repo"}'

    custom_row = _parquet_row(
        _ctx("custom-search"),
        _doc(
            canonical_url="https://example.test/canonical",
            lang="de",
            payload={"workspace_root": "/repo"},
        ),
    )
    assert custom_row["canonical_url"] == "https://example.test/canonical"
    assert custom_row["lang"] == "de"
    assert custom_row["extra"] == '{"workspace_root":"/repo"}'


def test_pushed_custom_fields_and_payload_share_the_refresh_fingerprint():
    fields, payload = custom_metadata({
        "fields": {
            "workspace_root": "/repo",
            "kind": "decision",
        },
        "payload": {
            "producer": "agent",
            "kind": "legacy-value",
        },
    }, 0)
    document = _doc(fields=fields, payload=payload)
    ctx = _ctx("custom-search")

    assert _parquet_row(ctx, document)["extra"] == (
        '{"kind":"decision","producer":"agent","workspace_root":"/repo"}'
    )
    changed_fields, changed_payload = custom_metadata({
        "fields": {
            "workspace_root": "/other-repo",
            "kind": "decision",
        },
        "payload": {"producer": "agent"},
    }, 0)
    changed = _doc(fields=changed_fields, payload=changed_payload)
    assert _metadata_hash(ctx, document) != _metadata_hash(ctx, changed)


def test_stage_action_refreshes_metadata_but_skips_an_unchanged_replay():
    ctx = _ctx("memory")
    before = _doc(
        suffix="chat/00000",
        fields={
            "conversation_id": "chat",
            "chunk_index": 0,
        },
        payload={"workspace_root": "/repo/one"},
    )
    digest = text_hash(before.title + "\n\n" + before.text)
    metadata_digest = _metadata_hash(ctx, before)
    existing = _Existing(
        digest, metadata_digest, "searchable", "model")

    assert _stage_action(
        before, existing, digest, metadata_digest) == (False, False)

    changed = _doc(
        suffix="chat/00000",
        fields={
            "conversation_id": "chat",
            "chunk_index": 0,
        },
        payload={"workspace_root": "/repo/two"},
    )
    assert _stage_action(
        changed,
        existing,
        digest,
        _metadata_hash(ctx, changed),
    ) == (True, True)

    # Rows created before the additive metadata-hash migration refresh once on
    # their next ordinary ingest, without a bulk reindex.
    legacy = _Existing(digest, None, "searchable", "model")
    assert _stage_action(
        before, legacy, digest, metadata_digest) == (True, True)


class _Cursor:
    def __init__(self, connection):
        self.connection = connection

    def executemany(self, query, rows):
        self.connection.statements.append((" ".join(query.split()), rows))


class _Connection:
    def __init__(self):
        self.statements = []
        self.commits = 0

    @contextmanager
    def cursor(self):
        yield _Cursor(self)

    def commit(self):
        self.commits += 1


class _Qdrant:
    def __init__(self, failure=None):
        self.calls = []
        self.failure = failure

    def batch_update_points(self, **kwargs):
        self.calls.append(kwargs)
        if self.failure is not None:
            raise self.failure


def test_metadata_refresh_overwrites_payload_without_vector_or_embedding(
    monkeypatch,
):
    from windex.pipeline import indexing

    connection = _Connection()
    ctx = SimpleNamespace(
        conn=connection,
        search_name="memory",
    )
    row = {
        "id": "memory:chat/00000",
        "url": "llmchat://chat/chat?chunk=0",
        "title": "Chat",
        "text": "same body",
        "conversation_id": "chat",
        "chunk_index": 0,
        "extra": '{"workspace_root":"/repo"}',
    }
    monkeypatch.setattr(indexing, "_rows", lambda *_args: {row["id"]: row})
    client = _Qdrant()

    refreshed = _refresh_metadata(
        ctx,
        client,
        "memory__model",
        [(row["id"], "memory/new.parquet", "new-metadata-hash")],
        SimpleNamespace(embed_max_tokens=100),
    )

    assert refreshed == 1
    assert connection.commits == 1
    operation = client.calls[0]["update_operations"][0].overwrite_payload
    assert operation.points is not None
    assert len(operation.points) == 1
    assert operation.payload == {
        "id": row["id"],
        "url": row["url"],
        "title": "Chat",
        "conversation_id": "chat",
        "chunk_index": 0,
        "extra": '{"workspace_root":"/repo"}',
        "doc_id": row["id"],
        "source": "memory",
        "snippet": "Chat\n\nsame body",
    }
    assert "vector" not in operation.payload
    query, args = connection.statements[0]
    assert "indexed_metadata_hash = %s" in query
    assert "metadata_hash = %s" in query
    assert args == [
        ("new-metadata-hash", row["id"], "new-metadata-hash"),
    ]


def test_failed_payload_refresh_leaves_the_acknowledgement_pending(monkeypatch):
    from windex.pipeline import indexing

    connection = _Connection()
    ctx = SimpleNamespace(conn=connection, search_name="custom-search")
    row = {
        "id": "custom:one",
        "url": "https://example.test/one",
        "title": "One",
        "text": "same body",
        "extra": '{"revision":2}',
    }
    monkeypatch.setattr(indexing, "_rows", lambda *_args: {row["id"]: row})

    with pytest.raises(TimeoutError, match="qdrant unavailable"):
        _refresh_metadata(
            ctx,
            _Qdrant(failure=TimeoutError("qdrant unavailable")),
            "custom__model",
            [(row["id"], "custom/new.parquet", "pending-hash")],
            SimpleNamespace(embed_max_tokens=100),
        )

    assert connection.statements == []
    assert connection.commits == 0


def test_metadata_only_ledger_update_preserves_searchable_vector(pg):
    with pg.cursor() as cur:
        cur.execute(
            """SELECT id, id_prefix
                 FROM sources
                WHERE name = 'memory'""")
        source_id, id_prefix = cur.fetchone()
    ctx = SimpleNamespace(
        conn=pg,
        source_id=source_id,
        run_id=None,
        search_name="memory",
        id_prefix=id_prefix,
    )
    original = _doc(
        suffix="chat/00000",
        fields={
            "conversation_id": "chat",
            "chunk_index": 0,
        },
        payload={"workspace_root": "/old"},
    )
    _upsert_ledger(
        ctx, _ledger_rows(ctx, [original], "memory/original.parquet"))
    with pg.cursor() as cur:
        cur.execute(
            """UPDATE documents
                  SET status = 'searchable',
                      embedded_model = 'model',
                      indexed_metadata_hash = metadata_hash
                WHERE id = 'memory:chat/00000'""")
    pg.commit()

    changed = _doc(
        suffix="chat/00000",
        url="llmchat://chat/chat?chunk=0&revision=2",
        fields={
            "conversation_id": "chat",
            "chunk_index": 0,
        },
        payload={"workspace_root": "/new"},
    )
    _upsert_ledger(
        ctx, _ledger_rows(ctx, [changed], "memory/changed.parquet"))
    pg.commit()

    with pg.cursor() as cur:
        cur.execute(
            """SELECT status, embedded_model,
                      metadata_hash = indexed_metadata_hash,
                      url, text_ref
                 FROM documents
                WHERE id = 'memory:chat/00000'""")
        row = cur.fetchone()
    assert row == (
        "searchable",
        "model",
        False,
        "llmchat://chat/chat?chunk=0&revision=2",
        "memory/changed.parquet",
    )

    text_changed = ExtractedDoc(
        **{
            **changed.__dict__,
            "text": "changed body",
        },
    )
    _upsert_ledger(
        ctx, _ledger_rows(ctx, [text_changed], "memory/text-changed.parquet"))
    pg.commit()
    with pg.cursor() as cur:
        cur.execute(
            """SELECT status, embedded_model
                 FROM documents
                WHERE id = 'memory:chat/00000'""")
        # H04's marker stays durable until platform.index confirms deletion.
        assert cur.fetchone() == ("staged", "model")


def test_canonical_schema_has_metadata_acknowledgement_columns(pg):
    with pg.cursor() as cur:
        cur.execute(
            """SELECT column_name
                 FROM information_schema.columns
                WHERE table_name = 'documents'
                  AND column_name IN (
                      'metadata_hash', 'indexed_metadata_hash')
                ORDER BY column_name""")
        assert [row[0] for row in cur.fetchall()] == [
            "indexed_metadata_hash",
            "metadata_hash",
        ]
