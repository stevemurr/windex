"""Cooperatively sliced HTTP fetching for large Hugging Face doc roots."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from tempfile import NamedTemporaryFile

import httpx

from windex.config import Settings
from windex.crawl.fetch import check_url
from windex.crawl.links import extract_links
from windex.crawl.scope import canonicalize
from windex.hf.fetch import PagesRateLimiter
from windex.hf.formats import parse_llms
from windex.hf import license_for
from windex.modules import fetch as legacy
from windex.modules.common import (
    InputBatch,
    finish_batch,
    pending_batches,
    require_type,
)
from windex.pipeline import wire
from windex.pipeline.ports import PartitionRef, RawBlob, WorkUnit
from windex.smallweb.feed import newest_entries
from windex.smallweb.http import HostRateLimiter, RobotsCache
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_PAGES_PER_SLICE = 20
_PAGE_COUNT = "hf_pages_fetched"


def _anchor_ids(ctx: TaskContext) -> set[str]:
    raw = ctx.effective_config.get("anchor_ids")
    values = raw.split(",") if isinstance(raw, str) else raw or []
    return {str(value).strip() for value in values if str(value).strip()}


def _root_dir(ctx: TaskContext, lineage: str) -> Path:
    digest = hashlib.sha256(f"{ctx.run_id}:{ctx.task_id}:{lineage}".encode()).hexdigest()[:24]
    return (
        Settings().staging_dir / "_pipeline_hf_fetch" / str(ctx.run_id) / str(ctx.task_id) / digest
    )


def _atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, prefix=".hf-", delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(value)
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, value: object) -> None:
    _atomic_bytes(
        path,
        json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode(),
    )


def _load_json(path: Path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid Hugging Face slice artifact {path}: {exc}") from exc


def _page_plan(
    ctx: TaskContext,
    unit: WorkUnit,
    root: Path,
    client: httpx.Client,
    robots: RobotsCache,
    limiter: HostRateLimiter,
) -> dict:
    plan_path = root / "plan.json"
    if plan_path.is_file():
        plan = _load_json(plan_path)
        if not isinstance(plan, dict) or not isinstance(plan.get("pages"), list):
            raise RuntimeError(f"invalid Hugging Face page plan {plan_path}")
        return plan

    listing = legacy._page(ctx, unit, client, robots, limiter)
    pages = []
    if listing.body:
        root_key = unit.ref.key.strip("/")
        pages = parse_llms(
            listing.body.decode("utf-8", errors="replace"),
            root_key,
        )
        anchors = _anchor_ids(ctx)
        if anchors:
            pages = [page for page in pages if f"hf:{root_key}/{page['path']}" in anchors]
    plan = {
        "pages": pages,
        "listing_status": int(listing.meta.get("status") or 0),
        "listing_uri": listing.uri,
    }
    _atomic_json(plan_path, plan)
    return plan


def _page_unit(ctx: TaskContext, unit: WorkUnit, page: dict) -> WorkUnit:
    root = unit.ref.key.strip("/")
    version = str(page.get("version") or "")
    path = str(page["path"])
    doc_id = f"hf:{root}/{path}"
    anchors = _anchor_ids(ctx)
    version_part = f"{version}/" if version else ""
    return WorkUnit(
        ref=PartitionRef(
            store=unit.ref.store,
            key=unit.ref.key,
            id_scope=(doc_id if anchors else unit.ref.id_scope or f"hf:{root}/"),
        ),
        payload={
            **unit.payload,
            "url": f"https://huggingface.co/{root}/{version_part}{path}.md",
            "root": root,
            "path": path,
            "title": str(page.get("title") or ""),
            "version": version,
        },
        upstream=unit.upstream,
        epoch=unit.epoch,
    )


def _persist_page(root: Path, index: int, blob: RawBlob) -> None:
    manifest = root / f"{index:06d}.json"
    if blob.body:
        body = root / f"{index:06d}.body"
        _atomic_bytes(body, blob.body)
        stored = replace(blob, body=None, path=body)
        payload = {"include": True, "blob": wire.encode(stored)}
    else:
        payload = {"include": False}
    _atomic_json(manifest, payload)


def _load_pages(root: Path, total: int) -> list[RawBlob]:
    outputs = []
    for index in range(total):
        payload = _load_json(root / f"{index:06d}.json")
        if not isinstance(payload, dict) or "include" not in payload:
            raise RuntimeError(f"invalid Hugging Face page manifest at offset {index}")
        if not payload["include"]:
            continue
        value = wire.decode(payload.get("blob"))
        if not isinstance(value, RawBlob):
            raise RuntimeError(f"Hugging Face page manifest {index} did not contain a RawBlob")
        outputs.append(value)
    return outputs


def _offset(ctx: TaskContext, lineage: str) -> int:
    prefix = f"{lineage}#hf-pages="
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT coalesce(sum((counts->>%s)::bigint), 0)
              FROM task_units
             WHERE task_id = %s
               AND counts ? %s
               AND left(unit_key, length(%s)) = %s
            """,
            (_PAGE_COUNT, ctx.task_id, _PAGE_COUNT, prefix, prefix),
        )
        return int(cur.fetchone()[0])


def _hf_root_slice(
    ctx: TaskContext,
    batch: InputBatch,
    client: httpx.Client,
    robots: RobotsCache,
    limiter: HostRateLimiter,
) -> tuple[int, bool]:
    if len(batch.values) != 1:
        raise PermanentTaskError(
            "Hugging Face root fetching requires one WorkUnit per upstream batch"
        )
    unit = require_type(batch.values[0], WorkUnit, ctx.module)
    if unit.ref.store != "root":
        raise PermanentTaskError("Hugging Face sliced fetching requires a root WorkUnit")

    root = _root_dir(ctx, batch.key)
    plan = _page_plan(ctx, unit, root, client, robots, limiter)
    pages = plan["pages"]
    offset = _offset(ctx, batch.key)
    if offset > len(pages):
        raise RuntimeError(f"Hugging Face page offset {offset} exceeds plan size {len(pages)}")

    maximum = int(ctx.config.get("root_pages_per_slice", _PAGES_PER_SLICE))
    processed = 0
    for index, page in enumerate(
        pages[offset : offset + maximum],
        start=offset,
    ):
        blob = legacy._page(
            ctx,
            _page_unit(ctx, unit, page),
            client,
            robots,
            limiter,
        )
        _persist_page(root, index, blob)
        processed += 1
        if ctx.should_yield():
            break

    complete = offset + processed >= len(pages)
    if complete:
        outputs = _load_pages(root, len(pages))
        if not pages and int(plan.get("listing_status") or 0) == 200:
            outputs = [
                RawBlob(
                    ref=unit.ref,
                    uri=str(plan.get("listing_uri") or unit.payload.get("url") or ""),
                    body=b"",
                    meta={
                        "status": 200,
                        "payload": unit.payload,
                        "upstream": unit.upstream,
                    },
                    epoch=unit.epoch,
                )
            ]
        finish_batch(
            ctx,
            batch,
            outputs=outputs,
            counts={
                _PAGE_COUNT: processed,
                "hf_page_offset": offset,
                "hf_pages_total": len(pages),
                "hf_root_complete": True,
            },
        )
    elif processed:
        finish_batch(
            ctx,
            InputBatch(
                key=f"{batch.key}#hf-pages={offset}",
                values=batch.values,
            ),
            outputs=[],
            counts={
                _PAGE_COUNT: processed,
                "hf_page_offset": offset,
                "hf_pages_total": len(pages),
                "hf_root_complete": False,
            },
        )
    return processed, complete


def _hf_root_get(ctx: TaskContext) -> SliceResult:
    timeout = float(ctx.config.get("request_timeout", 15))
    interval = float(ctx.config.get("host_interval", 2))
    with httpx.Client(
        timeout=httpx.Timeout(timeout, read=timeout),
        follow_redirects=False,
        headers={"User-Agent": legacy._USER_AGENT},
    ) as client:
        robots = RobotsCache(
            client,
            getattr(Settings(), "crawl_robots_ttl", 86_400),
            user_agent=legacy._USER_AGENT,
        )
        limiter = legacy._page_limiter(ctx.search_name, interval)
        batches, more = pending_batches(ctx, limit=1)
        if not batches:
            ctx.conn.commit()
            return SliceResult(exhausted=True)
        batch = batches[0]
        processed, complete = _hf_root_slice(
            ctx,
            batch,
            client,
            robots,
            limiter,
        )
    ctx.conn.commit()
    if processed or complete:
        ctx.heartbeat(
            1 if complete else 0,
            0,
            {
                "last": batch.key,
                "pages": processed,
                "root_complete": complete,
            },
        )
    return SliceResult(
        units_done=1 if complete else 0,
        exhausted=complete and not more,
        stats={
            "inputs": 1,
            "pages": processed,
            "root_complete": complete,
        },
    )


def http_get(ctx: TaskContext) -> SliceResult:
    if ctx.search_name == "hf" and bool(ctx.config.get("hf_root_census", False)):
        return _hf_root_get(ctx)
    return legacy.http_get(ctx)


# The wrapper delegates every non-root request to the legacy runner. Including
# that runner as a digest dependency keeps frozen Run locks honest when its
# implementation changes.
http_get.__windex_digest_dependencies__ = (
    legacy.http_get,
    check_url,
    extract_links,
    canonicalize,
    PagesRateLimiter,
    parse_llms,
    newest_entries,
    RobotsCache,
    legacy._unit_url,
    pending_batches,
    wire.encode,
    license_for,
)


__all__ = ["http_get"]
