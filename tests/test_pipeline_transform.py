from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from windex.modules import minhash, transform
from windex.modules.common import InputBatch
from windex.pipeline.ports import ExtractedDoc, PartitionRef
from windex.worker.protocol import PermanentTaskError


class _Cursor:
    def __init__(self, statements, batches, collision):
        self.statements = statements
        self.batches = batches
        self.collision = collision

    def execute(self, query, args):
        self.statements.append((" ".join(query.split()), args))

    def executemany(self, query, args):
        self.batches.append((" ".join(query.split()), args))

    def fetchone(self):
        return self.collision


class _Connection:
    def __init__(self, collision=None):
        self.statements = []
        self.batches = []
        self.commits = 0
        self.collision = collision

    @contextmanager
    def cursor(self):
        yield _Cursor(self.statements, self.batches, self.collision)

    def commit(self):
        self.commits += 1


def _context(source_id, collision=None):
    return SimpleNamespace(
        source_id=source_id,
        config={"window_days": 30},
        spec={"corpus": {"id_prefix": "news:"}},
        search_name="news",
        module="dedup.minhash",
        conn=_Connection(collision),
        should_yield=lambda: False,
        heartbeat=lambda *_args: None,
    )


def test_minhash_dedup_scopes_reads_and_writes_to_source(
    monkeypatch,
):
    doc = ExtractedDoc(
        ref=PartitionRef(store="warc", key="one.warc.gz"),
        suffix="story",
        url="https://example.test/story",
        canonical_url="https://example.test/story",
        title="Story",
        text="alpha beta gamma delta epsilon",
        published_at=datetime(2026, 7, 26, tzinfo=UTC),
        lang="en",
    )
    batch = InputBatch(key="extract:1", values=(doc,))
    finished = []
    monkeypatch.setattr(transform, "pending_batches", lambda _ctx, *, limit: ([batch], False))
    monkeypatch.setattr(
        transform,
        "finish_batch",
        lambda _ctx, item, *, outputs: finished.append((item, outputs)),
    )
    monkeypatch.setattr(minhash, "signature", lambda _text: (1,))
    monkeypatch.setattr(minhash, "band_hashes", lambda _signature: [101, 202])
    ctx = _context(42)

    result = minhash.dedup_minhash(ctx)

    assert result.exhausted is True
    assert len(finished) == 1
    assert ctx.conn.statements[0][1] == (42, 30)
    assert "WHERE source_id = %s" in ctx.conn.statements[1][0]
    assert ctx.conn.statements[1][1] == (
        42,
        "news:story",
        [0, 1],
        [101, 202],
    )
    assert ctx.conn.batches[0][1] == [
        (42, 0, 101, "news:story", datetime(2026, 7, 26).date()),
        (42, 1, 202, "news:story", datetime(2026, 7, 26).date()),
    ]


def test_minhash_dedup_rejects_unbound_pipeline_run():
    with pytest.raises(
        PermanentTaskError,
        match="requires a source-bound Pipeline Run",
    ):
        minhash.dedup_minhash(_context(None))


def test_minhash_dedup_ignores_existing_bands_from_same_document(
    monkeypatch,
):
    doc = ExtractedDoc(
        ref=PartitionRef(store="warc", key="one.warc.gz"),
        suffix="story",
        url="https://example.test/story",
        canonical_url="https://example.test/story",
        title="Story",
        text="alpha beta gamma delta epsilon",
        published_at=datetime(2026, 7, 26, tzinfo=UTC),
        lang="en",
    )
    batch = InputBatch(key="extract:1", values=(doc,))
    finished = []
    monkeypatch.setattr(
        transform,
        "pending_batches",
        lambda _ctx, *, limit: ([batch], False),
    )
    monkeypatch.setattr(
        transform,
        "finish_batch",
        lambda _ctx, item, *, outputs: finished.extend(outputs),
    )
    monkeypatch.setattr(minhash, "signature", lambda _text: (1,))
    monkeypatch.setattr(
        minhash,
        "band_hashes",
        lambda _signature: [101, 202],
    )

    minhash.dedup_minhash(_context(42, collision=("news:story",)))

    assert len(finished) == 1
    assert "_duplicate_of" not in finished[0].fields
