from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from windex.modules import warc
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
    monkeypatch.setattr("windex.ccnews.pipeline.process_batch", process)
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
