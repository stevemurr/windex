from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from windex.embed.pipeline import point_id
from windex.pipeline.indexing import _remove_stale_vectors


class _Cursor:
    def __init__(self, statements):
        self.statements = statements

    def execute(self, query, args):
        self.statements.append((" ".join(query.split()), args))


class _Connection:
    def __init__(self):
        self.statements = []
        self.commits = 0

    @contextmanager
    def cursor(self):
        yield _Cursor(self.statements)

    def commit(self):
        self.commits += 1


class _Qdrant:
    def __init__(self):
        self.calls = []

    def delete(self, **kwargs):
        self.calls.append(kwargs)


def test_nonsearchable_vectors_are_removed_and_metadata_cleared():
    connection = _Connection()
    client = _Qdrant()
    ctx = SimpleNamespace(conn=connection)
    stale = ["news:one", "news:two"]

    removed = _remove_stale_vectors(
        ctx,
        client,
        "news__model",
        stale,
    )

    assert removed == 2
    assert client.calls[0]["collection_name"] == "news__model"
    assert client.calls[0]["wait"] is True
    assert client.calls[0]["points_selector"].points == [
        point_id("news:one"),
        point_id("news:two"),
    ]
    assert "status <> 'searchable'" in connection.statements[0][0]
    assert connection.statements[0][1] == (stale,)
    assert connection.commits == 1
