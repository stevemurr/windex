"""Receive roots: validated pushed JSON -> ExtractedDoc."""

from __future__ import annotations

from datetime import datetime
from typing import NamedTuple

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


def _doc_conversation(raw: dict) -> str:
    """The conversation a memory document belongs to.

    Preferred source is `fields.conversation_id`; the leading segment of the
    document id (`"<conversation>/00003"`) is the fallback, because that is what
    the id scope used for partition replacement is built from.
    """
    fields = raw.get("fields")
    if isinstance(fields, dict) and fields.get("conversation_id"):
        return str(fields["conversation_id"])
    doc_id = str(raw.get("id") or "")
    head, sep, _ = doc_id.partition("/")
    return head if sep else ""


def memory_partition(payload: dict, raw_docs: list) -> str:
    """Resolve the one conversation a memory push replaces.

    `IngestRequest` forbids unknown top-level keys, so a memory push carries its
    conversation id per document. An explicit batch `partition` still wins: an
    empty full push is the delete path and has no document to read an id from.

    A batch that mixes conversations is rejected rather than silently collapsed.
    Partition replacement tombstones everything under `memory:<partition>/`, so
    guessing wrong here does not lose one document — it empties a whole chat.
    """
    found = {
        conversation for raw in raw_docs
        if isinstance(raw, dict) and (conversation := _doc_conversation(raw))
    }
    if len(found) > 1:
        raise PermanentTaskError(
            "push.docs memory batch mixes conversations ("
            + ", ".join(sorted(found))
            + "); one ingest run replaces exactly one conversation")
    explicit = payload.get("partition") or payload.get("conversation_id")
    if explicit:
        partition = str(explicit)
        if found and partition not in found:
            raise PermanentTaskError(
                "push.docs memory batch conversation "
                f"{next(iter(found))!r} does not match partition "
                f"{partition!r}")
        return partition
    if not found:
        raise PermanentTaskError(
            "push.docs memory batch names no conversation; send "
            "fields.conversation_id on each document or a batch partition")
    return found.pop()


class MemoryIdentity(NamedTuple):
    suffix: str
    url: str
    fields: dict
    title: str
    published_at: datetime | None


def custom_metadata(raw: dict, index: int) -> tuple[dict, dict]:
    """Keep public custom fields both for pipeline logic and search results."""
    raw_fields = raw.get("fields") or {}
    if not isinstance(raw_fields, dict):
        raise PermanentTaskError(
            f"push.docs document {index} fields must be an object")
    fields = dict(raw_fields)
    metadata = {
        **dict(raw.get("extra") or raw.get("payload") or {}),
        **{
            key: value for key, value in fields.items()
            if not str(key).startswith("_")
        },
    }
    return fields, metadata


def memory_identity(
    raw: dict, index: int, partition: str, batch_title=None,
) -> MemoryIdentity:
    """Derive one memory chunk's identity from an epoch-2 ingest document.

    Identity comes from the document (`id`, `url`, `fields`, `published_at`).
    The positional fallbacks accept the pre-epoch-2 shape (`index`, `ended_at`,
    top-level `message_range`) so an older client is not silently mis-filed.
    """
    doc_fields = raw.get("fields") or {}
    if not isinstance(doc_fields, dict):
        raise PermanentTaskError(
            f"push.docs document {index} fields must be an object")
    chunk_index = int(doc_fields.get("chunk_index", raw.get("index", index)))
    suffix = str(raw.get("id") or f"{partition}/{chunk_index:05d}")
    # Partition replacement tombstones by id scope. A document filed outside
    # its own conversation's scope would survive every later push of that
    # conversation and never be reachable to delete.
    if not suffix.startswith(f"{partition}/"):
        raise PermanentTaskError(
            f"push.docs document {index} id {suffix!r} lies outside "
            f"conversation {partition!r}")
    return MemoryIdentity(
        suffix=suffix,
        url=str(
            raw.get("url") or f"llmchat://chat/{partition}?chunk={chunk_index}"),
        fields={
            "conversation_id": partition,
            "chunk_index": chunk_index,
            "message_range": doc_fields.get(
                "message_range", raw.get("message_range")),
        },
        title=str(raw.get("title") or batch_title or ""),
        published_at=_datetime(
            raw.get("published_at") or raw.get("ended_at")),
    )


def validate_memory_batch(payload: dict, raw_docs: list) -> str:
    """Validate memory replacement semantics before accepting or executing it."""
    if not isinstance(raw_docs, list):
        raise PermanentTaskError("push.docs documents must be an array")
    partition = memory_partition(payload, raw_docs)
    for index, raw in enumerate(raw_docs):
        if not isinstance(raw, dict):
            raise PermanentTaskError(
                f"push.docs document {index} must be an object")
        memory_identity(
            raw, index, partition, batch_title=payload.get("title"))
    return partition


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
    source = ctx.search_name
    if source == "memory":
        partition = validate_memory_batch(payload, raw_docs)
    else:
        partition = str(
            payload.get("partition") or payload.get("conversation_id") or "push")
    outputs = []
    for index, raw in enumerate(raw_docs):
        if not isinstance(raw, dict):
            raise PermanentTaskError(
                f"push.docs document {index} must be an object")
        if source == "memory":
            suffix, url, fields, title, published = memory_identity(
                raw, index, partition, batch_title=payload.get("title"))
            metadata = dict(raw.get("extra") or raw.get("payload") or {})
        else:
            suffix = str(raw.get("id") or raw.get("suffix") or "")
            if not suffix:
                raise PermanentTaskError(
                    f"push.docs document {index} requires id")
            url = str(raw.get("url") or f"custom://{source}/{suffix}")
            fields, metadata = custom_metadata(raw, index)
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
            payload=metadata,
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
