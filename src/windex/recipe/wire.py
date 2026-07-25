"""Durable serialization for the four values that may cross a recipe edge.

The worker DAG is task-level: a downstream node starts only after every upstream
task succeeds. The values crossing those edges still need their own durable
home, however. Keeping them in ``task_units.outputs`` gives each input transition
and its emitted values one atomic commit, so a slot can die after that commit
without losing or duplicating the edge stream.

This is deliberately a closed codec over ``ports.py``'s closed vocabulary. A
stored value naming an unknown type is corrupt run state, not an invitation to
import a class named by data.
"""

from __future__ import annotations

import base64
from datetime import datetime
from pathlib import Path
from typing import Any

from windex.recipe.ports import (
    ExtractedDoc,
    PartitionRecord,
    PartitionRef,
    RawBlob,
    WorkUnit,
)

WireValue = WorkUnit | RawBlob | PartitionRecord | ExtractedDoc


def _ref(ref: PartitionRef) -> dict[str, Any]:
    return {"store": ref.store, "key": ref.key, "id_scope": ref.id_scope}


def _load_ref(raw: Any) -> PartitionRef:
    if not isinstance(raw, dict):
        raise ValueError("wire ref must be an object")
    return PartitionRef(
        store=str(raw.get("store", "")),
        key=str(raw.get("key", "")),
        id_scope=raw.get("id_scope"),
    )


def encode(value: WireValue) -> dict[str, Any]:
    """Encode one port value into JSON-safe, explicitly tagged data."""
    if isinstance(value, WorkUnit):
        return {
            "type": "WorkUnit",
            "ref": _ref(value.ref),
            "payload": value.payload,
            "upstream": value.upstream,
            "attempt": value.attempt,
            "epoch": value.epoch,
        }
    if isinstance(value, RawBlob):
        return {
            "type": "RawBlob",
            "ref": _ref(value.ref),
            "uri": value.uri,
            "media_type": value.media_type,
            "path": str(value.path) if value.path is not None else None,
            # Network runners should spool bodies before persistence. Supporting
            # bytes here keeps the codec total for small synthetic/local modules.
            "body_b64": (
                base64.b64encode(value.body).decode("ascii")
                if value.body is not None else None
            ),
            "meta": value.meta,
            "epoch": value.epoch,
        }
    if isinstance(value, PartitionRecord):
        return {
            "type": "PartitionRecord",
            "store": value.store,
            "key": value.key,
            "ref": _ref(value.ref) if value.ref is not None else None,
            "upstream": value.upstream,
            "stage": value.stage,
            "payload": value.payload,
            "delta": value.delta,
            "absent_ok": value.absent_ok,
        }
    if isinstance(value, ExtractedDoc):
        return {
            "type": "ExtractedDoc",
            "ref": _ref(value.ref),
            "suffix": value.suffix,
            "url": value.url,
            "text": value.text,
            "title": value.title,
            "canonical_url": value.canonical_url,
            "published_at": (
                value.published_at.isoformat()
                if value.published_at is not None else None
            ),
            "lang": value.lang,
            "fields": value.fields,
            "payload": value.payload,
            "deleted": value.deleted,
            "epoch": value.epoch,
        }
    raise TypeError(f"cannot encode recipe wire value {type(value).__name__}")


def decode(raw: Any) -> WireValue:
    """Decode one stored value, rejecting anything outside the port vocabulary."""
    if not isinstance(raw, dict):
        raise ValueError("wire value must be an object")
    kind = raw.get("type")
    if kind == "WorkUnit":
        return WorkUnit(
            ref=_load_ref(raw.get("ref")),
            payload=dict(raw.get("payload") or {}),
            upstream=dict(raw.get("upstream") or {}),
            attempt=int(raw.get("attempt", 0)),
            epoch=int(raw.get("epoch", 0)),
        )
    if kind == "RawBlob":
        encoded = raw.get("body_b64")
        return RawBlob(
            ref=_load_ref(raw.get("ref")),
            uri=str(raw.get("uri", "")),
            media_type=str(raw.get("media_type", "")),
            path=Path(raw["path"]) if raw.get("path") is not None else None,
            body=base64.b64decode(encoded) if encoded is not None else None,
            meta=dict(raw.get("meta") or {}),
            epoch=int(raw.get("epoch", 0)),
        )
    if kind == "PartitionRecord":
        return PartitionRecord(
            store=str(raw.get("store", "")),
            key=str(raw.get("key", "")),
            ref=(
                _load_ref(raw.get("ref"))
                if raw.get("ref") is not None else None
            ),
            upstream=dict(raw.get("upstream") or {}),
            stage=raw.get("stage"),
            payload=dict(raw.get("payload") or {}),
            delta=dict(raw.get("delta") or {}),
            absent_ok=bool(raw.get("absent_ok", True)),
        )
    if kind == "ExtractedDoc":
        published = raw.get("published_at")
        return ExtractedDoc(
            ref=_load_ref(raw.get("ref")),
            suffix=str(raw.get("suffix", "")),
            url=str(raw.get("url", "")),
            text=str(raw.get("text", "")),
            title=str(raw.get("title", "")),
            canonical_url=raw.get("canonical_url"),
            published_at=datetime.fromisoformat(published) if published else None,
            lang=raw.get("lang"),
            fields=dict(raw.get("fields") or {}),
            payload=dict(raw.get("payload") or {}),
            deleted=bool(raw.get("deleted", False)),
            epoch=int(raw.get("epoch", 0)),
        )
    raise ValueError(f"unknown recipe wire type {kind!r}")


def encode_many(values: list[WireValue]) -> list[dict[str, Any]]:
    return [encode(value) for value in values]


def decode_many(values: Any) -> list[WireValue]:
    if not isinstance(values, list):
        raise ValueError("task unit outputs must be an array")
    return [decode(value) for value in values]
