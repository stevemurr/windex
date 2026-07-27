"""Operational event query and streaming routes for the epoch-2 admin API."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import orjson
from fastapi import APIRouter, Header, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict
from starlette.concurrency import run_in_threadpool

from windex import db
from windex.config import get_settings
from windex.pipeline.events import facets, list_events

router = APIRouter()


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OperationalEventModel(Strict):
    seq: int
    ts: str
    level: str
    component: str
    source_name: str | None
    pipeline_name: str | None
    pipeline_version: int | None
    run_id: int | None
    task_id: int | None
    node: str | None
    module: str | None
    event: str
    message: str
    data: dict[str, Any]


class EventsResponse(Strict):
    events: list[OperationalEventModel]
    next_cursor: int


class FacetsResponse(Strict):
    levels: list[str]
    components: list[str]
    sources: list[str]
    pipelines: list[str]
    nodes: list[str]
    modules: list[str]


@router.get("/log-events", response_model=EventsResponse)
def log_events(
    after: int = 0, before: int | None = None,
    started_at: datetime | None = None, ended_at: datetime | None = None,
    limit: int = Query(200, ge=1, le=1000), level: str | None = None,
    component: str | None = None, source: str | None = None,
    pipeline: str | None = None, run_id: int | None = None,
    node: str | None = None, module: str | None = None, text: str | None = None,
) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        events = list_events(
            conn, after=after, before=before, started_at=started_at,
            ended_at=ended_at, limit=limit, level=level,
            component=component, source=source, pipeline=pipeline, run_id=run_id,
            node=node, module=module, text=text)
    return {"events": events, "next_cursor": events[-1]["seq"] if events else after}


@router.get("/log-events/facets", response_model=FacetsResponse)
def log_event_facets() -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return facets(conn)


def _event_stream(
    after: int,
    ticks: int | None,
    logs: bool,
    filters: Mapping[str, Any] | None = None,
) -> StreamingResponse:
    settings = get_settings()

    async def generate():
        cursor, count = after, 0
        while True:
            def read():
                with db.pooled(settings.pg_dsn) as conn:
                    return list_events(
                        conn, after=cursor, limit=500, **dict(filters or {}))

            events = await run_in_threadpool(read)
            if events:
                cursor = events[-1]["seq"]
                for event in events:
                    yield (
                        f"id: {event['seq']}\n"
                        f"event: {'log' if logs else event['event']}\n"
                        f"data: {orjson.dumps(event).decode()}\n\n")
            else:
                yield ": keepalive\n\n"
            count += 1
            if ticks is not None and count >= ticks:
                return
            await asyncio.sleep(1)

    return StreamingResponse(
        generate(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/events/stream")
def events_stream(
    after: int = 0, ticks: int | None = Query(None, ge=1, le=10_000),
    last_event_id: str | None = Header(None),
) -> StreamingResponse:
    cursor = int(last_event_id) if last_event_id and last_event_id.isdigit() else after
    return _event_stream(cursor, ticks, False)


@router.get("/log-events/stream")
def log_events_stream(
    after: int = 0, ticks: int | None = Query(None, ge=1, le=10_000),
    last_event_id: str | None = Header(None),
    level: str | None = None, component: str | None = None,
    source: str | None = None, pipeline: str | None = None,
    run_id: int | None = None, node: str | None = None,
    module: str | None = None, text: str | None = None,
    started_at: datetime | None = None, ended_at: datetime | None = None,
) -> StreamingResponse:
    cursor = int(last_event_id) if last_event_id and last_event_id.isdigit() else after
    return _event_stream(cursor, ticks, True, {
        "level": level, "component": component, "source": source,
        "pipeline": pipeline, "run_id": run_id, "node": node,
        "module": module, "text": text, "started_at": started_at,
        "ended_at": ended_at,
    })


__all__ = ["EventsResponse", "router"]
