"""Receive roots: validated pushed JSON -> ExtractedDoc."""

from __future__ import annotations

from datetime import datetime

from psycopg.types.json import Jsonb

from windex.modules.common import _store_outputs
from windex.pipeline.ports import ExtractedDoc, PartitionRef
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext


def _datetime(value):
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PermanentTaskError(
            f"push.docs received invalid timestamp {value!r}") from exc


def push_docs(ctx: TaskContext) -> SliceResult:
    """Consume one immutable run payload as a pushed document batch."""
    with ctx.conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM task_units WHERE task_id = %s AND unit_key = 'push'",
            (ctx.task_id,),
        )
        if cur.fetchone():
            ctx.conn.commit()
            return SliceResult(exhausted=True, units_total=1)

    payload = dict(ctx.inputs.get("documents") or {})
    raw_docs = payload.get("documents")
    if raw_docs is None:
        raw_docs = payload.get("chunks", [])
    if not isinstance(raw_docs, list):
        raise PermanentTaskError("push.docs documents must be an array")
    maximum = int(ctx.config.get("max_docs", 500))
    max_chars = int(ctx.config.get("max_text_chars", 16_000))
    if len(raw_docs) > maximum:
        raise PermanentTaskError(
            f"push.docs received {len(raw_docs)} documents, max is {maximum}")

    mode = str(ctx.config.get("mode", "delta"))
    partition = str(
        payload.get("partition") or payload.get("conversation_id") or "push")
    source = ctx.search_name
    outputs = []
    for index, raw in enumerate(raw_docs):
        if not isinstance(raw, dict):
            raise PermanentTaskError(
                f"push.docs document {index} must be an object")
        if source == "memory":
            suffix = f"{partition}/{int(raw.get('index', index)):05d}"
            url = str(
                raw.get("url")
                or f"llmchat://chat/{partition}?chunk={int(raw.get('index', index))}")
            fields = {
                "conversation_id": partition,
                "chunk_index": int(raw.get("index", index)),
                "message_range": raw.get("message_range"),
            }
            title = str(raw.get("title") or payload.get("title") or "")
            published = _datetime(raw.get("ended_at") or raw.get("published_at"))
        else:
            suffix = str(raw.get("id") or raw.get("suffix") or "")
            if not suffix:
                raise PermanentTaskError(
                    f"push.docs document {index} requires id")
            url = str(raw.get("url") or f"custom://{source}/{suffix}")
            fields = dict(raw.get("fields") or {})
            title = str(raw.get("title") or "")
            published = _datetime(raw.get("published_at"))
        text = str(raw.get("text") or "")
        if len(text) > max_chars:
            raise PermanentTaskError(
                f"push.docs document {index} exceeds {max_chars} characters")
        ref = PartitionRef(
            store="",
            key=partition,
            id_scope=(
                f"{ctx.id_prefix}"
                f"{partition}/"
                if mode == "full_set" else None
            ),
        )
        outputs.append(ExtractedDoc(
            ref=ref,
            suffix=suffix,
            url=url,
            canonical_url=str(raw.get("canonical_url") or url),
            title=title,
            text=text,
            published_at=published,
            lang=raw.get("lang"),
            fields=fields,
            payload=dict(raw.get("extra") or raw.get("payload") or {}),
            deleted=bool(raw.get("deleted", False)),
            epoch=ctx.run_id,
        ))
    if not outputs:
        prefix = str(
            ctx.id_prefix or f"{source}:")
        outputs.append(ExtractedDoc(
            ref=PartitionRef(
                store="", key=partition,
                id_scope=f"{prefix}{partition}/" if mode == "full_set" else None,
            ),
            suffix="",
            url=f"push://{source}/{partition}",
            text="",
            fields={"_coverage_only": True},
            epoch=ctx.run_id,
        ))
    stored = _store_outputs(ctx, "push", outputs)
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, finished_at)
            VALUES (%s, %s, 'push', 'done', %s, now())
            """,
            (ctx.run_id, ctx.task_id, Jsonb(stored)),
        )
    ctx.conn.commit()
    ctx.heartbeat(1, 0, {"documents": len(raw_docs), "partition": partition})
    return SliceResult(
        units_done=1,
        exhausted=True,
        units_total=1,
        stats={"documents": len(raw_docs), "partition": partition},
    )
