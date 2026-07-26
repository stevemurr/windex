"""The memory message range is one optional value from ingest to retrieval."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from qdrant_client import models as qm

from windex.api import service
from windex.api.app import DocumentResponse, SearchResponse, app
from windex.index import qdrant as qidx
from windex.modules.load import _parquet_row
from windex.modules.receive import memory_identity
from windex.pipeline.indexing import _point_payload
from windex.pipeline.ports import ExtractedDoc, PartitionRef
from windex.worker.protocol import PermanentTaskError

CONVERSATION = "7a32c09b-73d0-4e4d-b4e1-9c4e35de77c3"


def _raw(message_range=...):
    fields = {
        "conversation_id": CONVERSATION,
        "chunk_index": 3,
    }
    if message_range is not ...:
        fields["message_range"] = message_range
    return {
        "id": f"{CONVERSATION}/00003",
        "url": f"llmchat://chat/{CONVERSATION}?chunk=3",
        "title": "Indexing review",
        "text": "We decided to preserve the inclusive message indexes.",
        "published_at": "2026-07-26T20:15:00Z",
        "fields": fields,
    }


def _staged(message_range=...):
    raw = _raw(message_range)
    identity = memory_identity(raw, 0, CONVERSATION)
    document = ExtractedDoc(
        ref=PartitionRef(store="", key=CONVERSATION),
        suffix=identity.suffix,
        url=identity.url,
        canonical_url=identity.url,
        title=identity.title,
        text=raw["text"],
        published_at=identity.published_at,
        fields=identity.fields,
    )
    ctx = SimpleNamespace(
        search_name="memory",
        id_prefix="memory:",
    )
    row = _parquet_row(ctx, document)
    return row, _point_payload(ctx, row, document.text)


@pytest.mark.parametrize(
    ("wire_value", "expected"),
    [
        ([4, 11], [4, 11]),
        (..., None),
    ],
)
def test_range_survives_receive_parquet_point_and_search(
    monkeypatch, wire_value, expected,
):
    row, point = _staged(wire_value)
    assert row["message_range"] == expected
    if expected is None:
        assert "message_range" not in point
    else:
        assert point["message_range"] == expected

    monkeypatch.setattr(
        service,
        "index_search",
        lambda *_args, **_kwargs: {
            "results": [{"score": 0.75, **point}],
            "degraded": False,
            "timings": {"embed_query_ms": 0, "search_ms": 1},
        },
    )
    monkeypatch.setattr(service, "_record_search_metric", lambda *_args: None)
    response = service.run_search(
        SimpleNamespace(),
        "indexing decision",
        source="memory",
        mode="lexical",
    )
    encoded = SearchResponse.model_validate(response).model_dump(mode="json")
    hit = encoded["results"][0]
    assert hit["message_range"] == expected
    if expected is None:
        # The transport model keeps the field optional; service output itself
        # does not manufacture it for historical points.
        assert "message_range" not in response["results"][0]


@pytest.mark.parametrize(
    "value",
    [
        [],
        [1],
        [1, 2, 3],
        [-1, 2],
        [4, 3],
        [True, 2],
        ["1", 2],
    ],
)
def test_invalid_range_is_rejected_at_receive(value):
    with pytest.raises(PermanentTaskError, match="message_range"):
        memory_identity(_raw(value), 0, CONVERSATION)


class _Cursor:
    def __init__(self, ledger_row):
        self.ledger_row = ledger_row

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, *_args):
        return None

    def fetchone(self):
        return self.ledger_row


class _Connection:
    def __init__(self, ledger_row):
        self.ledger_row = ledger_row

    def cursor(self):
        return _Cursor(self.ledger_row)


@pytest.mark.parametrize(
    ("wire_value", "expected"),
    [
        ([4, 11], [4, 11]),
        (..., None),
    ],
)
def test_document_detail_reads_optional_range_from_parquet(
    monkeypatch, tmp_path, wire_value, expected,
):
    row, _point = _staged(wire_value)
    relative = "memory/pipeline/test/range.parquet"
    artifact = tmp_path / relative
    artifact.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([row]), artifact)
    ledger_row = (
        row["id"],
        "memory",
        row["url"],
        row["title"],
        datetime(2026, 7, 26, 20, 15, tzinfo=UTC),
        None,
        "searchable",
        None,
        relative,
    )

    @contextmanager
    def pooled(_dsn):
        yield _Connection(ledger_row)

    monkeypatch.setattr(service.db, "pooled", pooled)
    result = service.get_document(
        SimpleNamespace(pg_dsn="unused", staging_dir=tmp_path),
        row["id"],
    )
    assert result is not None
    assert result["message_range"] == expected
    encoded = DocumentResponse.model_validate(result).model_dump(mode="json")
    assert encoded["message_range"] == expected


def test_range_is_declared_in_payload_and_public_response_contracts():
    assert (
        qidx.PAYLOAD_INDEXES["memory"]["message_range"]
        == qm.PayloadSchemaType.INTEGER
    )
    schemas = app.openapi()["components"]["schemas"]
    for model in ("SearchResult", "DocumentResponse"):
        variants = schemas[model]["properties"]["message_range"]["anyOf"]
        array = next(item for item in variants if item.get("type") == "array")
        assert array["minItems"] == 2
        assert array["maxItems"] == 2
        assert array["items"]["type"] == "integer"
        assert array["items"]["minimum"] == 0
