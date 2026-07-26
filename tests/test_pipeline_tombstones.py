from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import qdrant_client

from windex.embed.pipeline import point_id
from windex.modules.load import (
    _delete_vectors,
    _tombstone_missing,
    _upsert_ledger,
)


class _Cursor:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, query, args):
        self.connection.statements.append((" ".join(query.split()), args))

    def executemany(self, query, rows):
        self.connection.many.append((" ".join(query.split()), rows))

    def fetchall(self):
        return self.connection.results.pop(0)


class _Connection:
    def __init__(self, *, results=None):
        self.statements = []
        self.many = []
        self.results = list(results or [])
        self.commits = 0

    @contextmanager
    def cursor(self):
        yield _Cursor(self)

    def commit(self):
        self.commits += 1


class _Qdrant:
    def __init__(self, *, failure=None):
        self.failure = failure
        self.calls = []
        self.closed = False

    def delete(self, **kwargs):
        self.calls.append(kwargs)
        if self.failure is not None:
            raise self.failure

    def close(self):
        self.closed = True


def _ctx(connection):
    return SimpleNamespace(
        conn=connection,
        collection_key="memory-test",
        search_name="memory",
        module="ledger.stage",
    )


def test_explicit_tombstone_retains_vector_marker_until_delete_confirmation():
    connection = _Connection()
    rows = [(
        "memory:chat/00000",
        1,
        2,
        "memory",
        "llmchat://chat/chat?chunk=0",
        None,
        "Chat",
        None,
        None,
        "text-hash",
        "deleted",
        None,
        None,
    )]

    _upsert_ledger(_ctx(connection), rows)

    update_clause = connection.many[0][0].split("ON CONFLICT", 1)[1]
    assert "embedded_model =" not in update_clause
    assert "indexed_at =" not in update_clause


def test_replacement_tombstone_retains_vector_marker():
    connection = _Connection(results=[
        [("memory:chat/00000",)],
        [("memory:chat/00000",)],
    ])

    removed, vectors = _tombstone_missing(
        _ctx(connection),
        scope="memory:chat/",
        current={"memory:chat/00001"},
        guard="census",
    )

    assert removed == ["memory:chat/00000"]
    assert vectors == ["memory:chat/00000"]
    update = connection.statements[1][0]
    assert "SET status = 'deleted'" in update
    assert "embedded_model" not in update
    assert "indexed_at" not in update


def test_qdrant_delete_failure_leaves_durable_marker(monkeypatch):
    connection = _Connection()
    client = _Qdrant(failure=TimeoutError("qdrant unavailable"))
    monkeypatch.setattr(qdrant_client, "QdrantClient", lambda **_kwargs: client)

    removed = _delete_vectors(_ctx(connection), {"memory:chat/00000"})

    assert removed == 0
    assert client.closed is True
    assert connection.statements == []
    assert connection.commits == 0


def test_qdrant_delete_retries_and_clears_marker_only_after_success(monkeypatch):
    connection = _Connection()
    failed = _Qdrant(failure=TimeoutError("qdrant unavailable"))
    succeeded = _Qdrant()
    clients = iter([failed, succeeded])
    monkeypatch.setattr(
        qdrant_client,
        "QdrantClient",
        lambda **_kwargs: next(clients),
    )
    doc_id = "memory:chat/00000"

    assert _delete_vectors(_ctx(connection), {doc_id}) == 0
    assert connection.statements == []

    assert _delete_vectors(_ctx(connection), {doc_id}) == 1
    assert succeeded.calls[0]["wait"] is True
    assert succeeded.calls[0]["points_selector"].points == [point_id(doc_id)]
    assert "SET embedded_model = NULL, indexed_at = NULL" in (
        connection.statements[0][0]
    )
    assert connection.statements[0][1] == ([doc_id],)
    assert connection.commits == 1
