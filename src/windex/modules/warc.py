"""Cooperatively sliced WARC extraction for CC-News."""

from __future__ import annotations

import hashlib
import shutil

import pyarrow.parquet as pq

from windex.config import Settings
from windex.dateparse import parse_and_clamp
from windex.modules.common import (
    InputBatch,
    finish_batch,
    pending_batches,
    require_type,
)
from windex.pipeline.ports import ExtractedDoc, RawBlob
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_RECORDS_PER_SLICE = 1_500
_INPUT_COUNT = "warc_input_documents"


def _published(value):
    return parse_and_clamp(value) if value else None


def warc_datatrove(ctx: TaskContext) -> SliceResult:
    from windex.ccnews.pipeline import process_batch

    language = str(ctx.config.get("language", "en"))
    workers = int(ctx.config.get("workers", 4))
    records_per_slice = int(
        ctx.config.get("records_per_slice", _RECORDS_PER_SLICE))
    batches, more = pending_batches(ctx, limit=1)
    if not batches:
        ctx.conn.commit()
        return SliceResult(exhausted=True)

    batch = batches[0]
    if len(batch.values) != 1:
        raise PermanentTaskError(
            "warc.datatrove requires one RawBlob per upstream task unit")
    blob = require_type(batch.values[0], RawBlob, ctx.module)
    if blob.path is None:
        raise PermanentTaskError("warc.datatrove requires a spooled RawBlob")

    chunk_prefix = f"{batch.key}#records="
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT coalesce(sum((counts->>%s)::bigint), 0)
              FROM task_units
             WHERE task_id = %s
               AND counts ? %s
               AND left(unit_key, length(%s)) = %s
            """,
            (_INPUT_COUNT, ctx.task_id, _INPUT_COUNT,
             chunk_prefix, chunk_prefix),
        )
        offset = int(cur.fetchone()[0])

    digest = hashlib.sha256(
        f"{ctx.run_id}:{ctx.task_id}:{blob.ref.key}:{offset}".encode()
    ).hexdigest()[:24]
    base = Settings().staging_dir / "_pipeline_extract" / str(ctx.run_id) / digest
    output = base / "parquet"
    logs = base / "logs"
    shutil.rmtree(base, ignore_errors=True)
    input_documents = process_batch(
        blob.path.parent,
        [blob.path.name],
        output,
        logs,
        language,
        workers=workers,
        skip=offset,
        limit=records_per_slice,
    )

    documents = []
    for path in sorted(output.rglob("*.parquet")):
        for row in pq.read_table(path).to_pylist():
            metadata = row.get("metadata") or {}
            url = str(row.get("url") or metadata.get("url") or "")
            text = str(row.get("text") or "")
            if not url or not text:
                continue
            canonical = __import__(
                "windex.ccnews.dedup", fromlist=["canonical_url"]
            ).canonical_url(url)
            documents.append(ExtractedDoc(
                ref=blob.ref,
                suffix=hashlib.sha1(canonical.encode()).hexdigest()[:20],
                url=url,
                canonical_url=canonical,
                title=str(row.get("title") or metadata.get("title") or ""),
                text=text,
                published_at=_published(
                    row.get("date") or metadata.get("date")),
                lang=str(row.get("language") or metadata.get("language")
                         or language),
                fields=dict(metadata),
                epoch=blob.epoch,
            ))

    complete = input_documents < records_per_slice
    output_batch = (
        batch if complete
        else InputBatch(
            key=f"{batch.key}#records={offset}",
            values=batch.values,
        )
    )
    finish_batch(
        ctx,
        output_batch,
        outputs=documents,
        counts={
            _INPUT_COUNT: input_documents,
            "warc_offset": offset,
            "warc_complete": complete,
        },
    )
    shutil.rmtree(base, ignore_errors=True)
    ctx.conn.commit()
    ctx.heartbeat(1, 0, {
        "last": batch.key,
        "documents": len(documents),
        "record_offset": offset,
        "input_documents": input_documents,
        "warc_complete": complete,
    })
    return SliceResult(
        units_done=1,
        exhausted=complete and not more,
        stats={
            "inputs": 1,
            "documents": len(documents),
            "record_offset": offset,
            "input_documents": input_documents,
            "warc_complete": complete,
        },
    )
