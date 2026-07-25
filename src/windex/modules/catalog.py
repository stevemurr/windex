"""Pure listing parsers: RawBlob -> PartitionRecord."""

from __future__ import annotations

import gzip
import json
import re
from datetime import date, timedelta
from urllib.parse import urlsplit

from windex.modules.common import (
    blob_bytes,
    downstream_store,
    finish_input,
    pending_inputs,
    require_type,
)
from windex.recipe.ports import PartitionRecord, RawBlob
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_INPUT_BATCH = 20


def _patterns(raw) -> list[re.Pattern]:
    if raw in (None, "", []):
        return []
    values = raw if isinstance(raw, list) else [raw]
    try:
        return [re.compile(str(value)) for value in values]
    except re.error as exc:
        raise PermanentTaskError(f"invalid catalog key pattern: {exc}") from exc


def _run(ctx: TaskContext, parse) -> SliceResult:
    items, more = pending_inputs(ctx, limit=_INPUT_BATCH)
    store = downstream_store(ctx)
    records = 0
    processed = []
    for item in items:
        blob = require_type(item, RawBlob, ctx.module)
        outputs = parse(blob, store)
        finish_input(ctx, item, outputs=outputs)
        records += len(outputs)
        processed.append(item)
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {"records": records, "last": processed[-1].key})
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(items),
        stats={"inputs": done, "records": records},
    )


def list_lines(ctx: TaskContext) -> SliceResult:
    schemes = {
        value.strip().lower()
        for value in str(ctx.config.get("scheme_allow", "http,https")).split(",")
        if value.strip()
    }
    floor = int(ctx.config.get("shrink_floor", 200))

    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        text = blob_bytes(blob).decode("utf-8")
        seen: set[str] = set()
        records = []
        for raw in text.splitlines():
            value = raw.strip()
            if not value or value.startswith("#") or value in seen:
                continue
            parsed = urlsplit(value)
            if parsed.scheme.lower() not in schemes or not parsed.netloc:
                continue
            seen.add(value)
            records.append(PartitionRecord(
                store=store,
                key=value,
                payload={"url": value, "host": parsed.netloc.lower()},
            ))
        if len(records) < floor:
            raise RuntimeError(
                f"{ctx.module} parsed {len(records)} records, below shrink_floor={floor}")
        return records

    return _run(ctx, parse)


def list_json_manifest(ctx: TaskContext) -> SliceResult:
    key_field = str(ctx.config.get("key_field", ""))
    upstream_field = str(ctx.config.get("upstream_field", ""))
    if not key_field:
        raise PermanentTaskError("list.json_manifest requires key_field")

    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        try:
            raw = json.loads(blob_bytes(blob))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"invalid JSON manifest: {exc}") from exc
        if not isinstance(raw, list):
            raise PermanentTaskError("JSON manifest root must be an array")
        records = []
        for entry in raw:
            if not isinstance(entry, dict) or not entry.get(key_field):
                continue
            key = str(entry[key_field])
            upstream = (
                {upstream_field: entry[upstream_field]}
                if upstream_field and entry.get(upstream_field) is not None else {}
            )
            records.append(PartitionRecord(
                store=store,
                key=key,
                upstream=upstream,
                payload=dict(entry),
            ))
        return records

    return _run(ctx, parse)


def list_path_manifest_gz(ctx: TaskContext) -> SliceResult:
    patterns = _patterns(ctx.config.get("key_pattern", []))
    min_age = int(ctx.config.get("min_age_days", 0))
    cutoff = date.today() - timedelta(days=min_age)

    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        data = blob_bytes(blob)
        try:
            text = gzip.decompress(data).decode("utf-8")
        except (gzip.BadGzipFile, UnicodeDecodeError) as exc:
            raise RuntimeError(f"invalid gzipped path manifest: {exc}") from exc
        records = []
        seen = set()
        for raw in text.splitlines():
            key = raw.strip()
            if not key or key in seen:
                continue
            if patterns and not any(pattern.search(key) for pattern in patterns):
                continue
            if min_age:
                match = re.search(r"(20\d{2})(?:/)?([01]\d)(?:/)?([0-3]\d)", key)
                if match is None:
                    continue
                try:
                    published = date(*(int(part) for part in match.groups()))
                except ValueError:
                    continue
                if published > cutoff:
                    continue
            seen.add(key)
            records.append(PartitionRecord(store=store, key=key))
        return records

    return _run(ctx, parse)
