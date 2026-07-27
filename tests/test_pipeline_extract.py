from __future__ import annotations

import bz2
import json
from contextlib import contextmanager
from types import SimpleNamespace

from windex.modules import extract, warc
from windex.modules.common import InputBatch
from windex.pipeline.ports import PartitionRef, RawBlob


class _ProgressCursor:
    def __init__(self, completed):
        self.completed = completed
        self.row = None

    def execute(self, _query, _args):
        self.row = (sum(
            item["counts"]["warc_input_documents"]
            for item in self.completed
            if item["key"] != "download:1"
        ),)

    def fetchone(self):
        return self.row


class _ProgressConnection:
    def __init__(self, completed):
        self.completed = completed
        self.commits = 0

    @contextmanager
    def cursor(self):
        yield _ProgressCursor(self.completed)

    def commit(self):
        self.commits += 1


def test_warc_extraction_resumes_in_durable_record_chunks(
    monkeypatch, tmp_path,
):
    completed = []
    calls = []
    total_records = 7_250
    source = InputBatch(
        key="download:1",
        values=(RawBlob(
            ref=PartitionRef(store="warc", key="one.warc.gz"),
            uri="https://example.test/one.warc.gz",
            path=tmp_path / "one.warc.gz",
            epoch=17,
        ),),
    )

    def pending(_ctx, *, limit):
        assert limit == 1
        if any(item["key"] == source.key for item in completed):
            return [], False
        return [source], False

    def process(
        _warc_dir, _local_names, _output, _logs, _language, workers=0,
        *, skip=0, limit=-1,
    ):
        assert workers == 1
        calls.append((skip, limit))
        return min(limit, max(total_records - skip, 0))

    def finish(_ctx, batch, *, outputs=None, counts=None):
        completed.append({
            "key": batch.key,
            "outputs": outputs or [],
            "counts": counts or {},
        })

    monkeypatch.setattr(warc, "pending_batches", pending)
    monkeypatch.setattr(warc, "finish_batch", finish)
    monkeypatch.setattr(warc, "process_batch", process)
    monkeypatch.setattr(
        warc,
        "Settings",
        lambda: SimpleNamespace(staging_dir=tmp_path / "staging"),
    )
    (tmp_path / "one.warc.gz").write_bytes(b"fixture")
    heartbeats = []
    ctx = SimpleNamespace(
        config={
            "language": "en",
            "workers": 1,
            "records_per_slice": 1_500,
        },
        run_id=17,
        task_id=76,
        module="warc.datatrove",
        conn=_ProgressConnection(completed),
        heartbeat=lambda *args: heartbeats.append(args),
    )

    first = warc.warc_datatrove(ctx)
    second = warc.warc_datatrove(ctx)
    third = warc.warc_datatrove(ctx)
    fourth = warc.warc_datatrove(ctx)
    fifth = warc.warc_datatrove(ctx)
    terminal = warc.warc_datatrove(ctx)

    assert calls == [
        (0, 1500),
        (1500, 1500),
        (3000, 1500),
        (4500, 1500),
        (6000, 1500),
    ]
    assert [item["key"] for item in completed] == [
        "download:1#records=0",
        "download:1#records=1500",
        "download:1#records=3000",
        "download:1#records=4500",
        "download:1",
    ]
    assert [
        item["counts"]["warc_input_documents"] for item in completed
    ] == [1500, 1500, 1500, 1500, 1250]
    assert [
        first.exhausted,
        second.exhausted,
        third.exhausted,
        fourth.exhausted,
        fifth.exhausted,
    ] == [
        False,
        False,
        False,
        False,
        True,
    ]
    assert terminal.exhausted is True
    assert len(heartbeats) == 5


class _WikiProgressCursor:
    def __init__(self, completed):
        self.completed = completed
        self.row = None

    def execute(self, _query, args):
        prefix = args[-1]
        offsets = [
            item["counts"]["wiki_decoded_offset"]
            for item in self.completed
            if item["key"].startswith(prefix) and "wiki_decoded_offset" in item["counts"]
        ]
        self.row = (max(offsets, default=0),)

    def fetchone(self):
        return self.row


class _WikiProgressConnection:
    def __init__(self, completed):
        self.completed = completed
        self.commits = 0

    @contextmanager
    def cursor(self):
        yield _WikiProgressCursor(self.completed)

    def commit(self):
        self.commits += 1


def _wiki_blob(tmp_path, pairs):
    raw = b"".join(
        json.dumps({"index": {"_id": page_id}}).encode()
        + b"\n"
        + json.dumps(
            {
                "page_id": page_id,
                "namespace": namespace,
                "title": f"Page {page_id}",
                "text": f"Body {page_id}",
                "timestamp": "2026-07-26T00:00:00Z",
                "incoming_links": page_id,
                "opening_text": f"Opening {page_id}",
            }
        ).encode()
        + b"\n"
        for page_id, namespace in pairs
    )
    path = tmp_path / "wiki.json.bz2"
    path.write_bytes(bz2.compress(raw))
    return RawBlob(
        ref=PartitionRef(store="shard", key="wiki.json.bz2"),
        uri="https://example.test/wiki.json.bz2",
        path=path,
        epoch=17,
    )


def _wiki_context(
    tmp_path,
    completed,
    *,
    chunk_rows,
    should_yield=lambda: False,
):
    source = InputBatch(
        key="download:1",
        values=(
            _wiki_blob(
                tmp_path,
                [(1, 0), (2, 0), (99, 1), (3, 0), (4, 0)],
            ),
        ),
    )

    def pending(_ctx, *, limit):
        assert limit == 1
        if any(item["key"] == source.key for item in completed):
            return [], False
        return [source], False

    def finish(_ctx, batch, *, outputs=None, counts=None):
        completed.append(
            {
                "key": batch.key,
                "outputs": outputs or [],
                "counts": counts or {},
            }
        )

    heartbeats = []
    context = SimpleNamespace(
        config={"chunk_rows": chunk_rows},
        run_id=17,
        task_id=76,
        module="cirrus.articles",
        conn=_WikiProgressConnection(completed),
        heartbeat=lambda *args: heartbeats.append(args),
        should_yield=should_yield,
    )
    return context, pending, finish, heartbeats


def test_wiki_extraction_commits_bounded_resumable_chunks(
    monkeypatch,
    tmp_path,
):
    completed = []
    ctx, pending, finish, heartbeats = _wiki_context(
        tmp_path,
        completed,
        chunk_rows=2,
    )
    monkeypatch.setattr(extract, "pending_batches", pending)
    monkeypatch.setattr(extract, "finish_batch", finish)
    monkeypatch.setattr(
        extract,
        "Settings",
        lambda: SimpleNamespace(staging_dir=tmp_path / "staging"),
    )

    first = extract.cirrus_articles(ctx)
    second = extract.cirrus_articles(ctx)
    final = extract.cirrus_articles(ctx)

    assert [first.exhausted, second.exhausted, final.exhausted] == [
        False,
        False,
        True,
    ]
    assert [item["key"] for item in completed] == [
        "download:1#bytes=0",
        f"download:1#bytes={completed[0]['counts']['wiki_decoded_offset']}",
        "download:1",
    ]
    assert all(item["counts"]["wiki_pairs"] <= 2 for item in completed)
    assert all(len(item["outputs"]) <= 2 for item in completed)
    assert [document.suffix for item in completed for document in item["outputs"]] == [
        "1",
        "2",
        "3",
        "4",
    ]
    offsets = [item["counts"]["wiki_decoded_offset"] for item in completed]
    assert offsets == sorted(offsets)
    assert len(set(offsets)) == len(offsets)
    assert completed[-1]["counts"]["wiki_complete"] is True
    assert len(heartbeats) == 3
    assert not list((tmp_path / "staging").rglob("*.json"))


def test_wiki_extraction_yields_after_durable_partial_progress_and_resumes(
    monkeypatch,
    tmp_path,
):
    completed = []
    yield_checks = 0

    def yield_after_first_pair():
        nonlocal yield_checks
        yield_checks += 1
        return yield_checks == 3

    ctx, pending, finish, _heartbeats = _wiki_context(
        tmp_path,
        completed,
        chunk_rows=5,
        should_yield=yield_after_first_pair,
    )
    monkeypatch.setattr(extract, "pending_batches", pending)
    monkeypatch.setattr(extract, "finish_batch", finish)
    monkeypatch.setattr(
        extract,
        "Settings",
        lambda: SimpleNamespace(staging_dir=tmp_path / "staging"),
    )

    yielded = extract.cirrus_articles(ctx)
    assert yielded.exhausted is False
    assert completed[0]["counts"]["wiki_pairs"] == 1
    assert [doc.suffix for doc in completed[0]["outputs"]] == ["1"]
    committed_offset = completed[0]["counts"]["wiki_decoded_offset"]
    assert committed_offset > 0

    ctx.should_yield = lambda: False
    resumed = extract.cirrus_articles(ctx)

    assert resumed.exhausted is True
    assert completed[1]["counts"]["wiki_start_offset"] == committed_offset
    assert [document.suffix for item in completed for document in item["outputs"]] == [
        "1",
        "2",
        "3",
        "4",
    ]
