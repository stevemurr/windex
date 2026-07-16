import asyncio
import json
from datetime import datetime
from importlib.resources import files
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from fastapi import Body

from windex.api import jobs, service
from windex.config import get_settings

app = FastAPI(title="windex", version="0.1.0",
              description="Self-hosted web index for search agents")


@app.get("/", include_in_schema=False)
def dashboard() -> HTMLResponse:
    return HTMLResponse(files("windex.api").joinpath("dashboard.html").read_text())


@app.get("/v1/search")
def search(
    q: str = Query(min_length=1),
    source: Literal["news", "github", "all"] = "all",
    limit: int = Query(10, ge=1, le=50),
    mode: Literal["hybrid", "dense", "lexical"] = "hybrid",
    published_after: datetime | None = None,
    published_before: datetime | None = None,
    min_stars: int | None = None,
    language: str | None = None,
) -> dict:
    return service.run_search(
        get_settings(), q, source=source, limit=limit, mode=mode,
        published_after=published_after, published_before=published_before,
        min_stars=min_stars, language=language,
    )


@app.get("/v1/docs/{doc_id:path}")
def get_doc(doc_id: str) -> dict:
    doc = service.get_document(get_settings(), doc_id)
    if doc is None:
        raise HTTPException(404, f"unknown document id: {doc_id}")
    return doc


@app.get("/v1/stats")
def stats() -> dict:
    return service.get_stats(get_settings())


@app.get("/v1/recent")
def recent(limit: int = Query(30, ge=1, le=100)) -> list[dict]:
    return service.get_recent(get_settings(), limit=limit)


@app.get("/v1/timeseries")
def timeseries(minutes: int = Query(60, ge=5, le=1440)) -> list[dict]:
    return service.get_timeseries(get_settings(), minutes=minutes)


@app.post("/v1/control/{action}")
def control(action: Literal["start", "pause"]) -> dict:
    value = "running" if action == "start" else "paused"
    return {"indexing": service.set_control(get_settings(), value)}


@app.get("/v1/workers")
def workers() -> dict:
    return service.get_worker_activity(get_settings())


@app.get("/v1/jobs")
def jobs_list() -> list[dict]:
    return jobs.list_jobs()


@app.post("/v1/jobs/{name}/start")
def jobs_start(name: str, params: dict = Body(default={})) -> dict:
    try:
        return jobs.start(name, params)
    except KeyError:
        raise HTTPException(404, f"unknown job: {name}")
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))


@app.post("/v1/jobs/{name}/stop")
def jobs_stop(name: str) -> dict:
    try:
        return jobs.stop(name)
    except KeyError:
        raise HTTPException(404, f"unknown job: {name}")


@app.get("/v1/events")
async def events(ticks: int | None = Query(None, ge=1, le=100)) -> StreamingResponse:
    """SSE stream for the dashboard: `stats` every ~2s, `recent` only when it
    changes, `timeseries` every ~16s. REST endpoints remain the poll/agent API;
    `ticks` bounds the stream for tests."""
    settings = get_settings()

    async def gen():
        last_recent_key = None
        n = 0
        while True:
            stats = await run_in_threadpool(service.get_stats, settings)
            yield f"event: stats\ndata: {json.dumps(stats)}\n\n"
            recent = await run_in_threadpool(service.get_recent, settings, 25)
            key = (recent[0]["id"], recent[0]["indexed_at"]) if recent else ()
            if key != last_recent_key:
                last_recent_key = key
                yield f"event: recent\ndata: {json.dumps(recent)}\n\n"
            if n % 8 == 0:
                series = await run_in_threadpool(service.get_timeseries, settings, 60)
                yield f"event: timeseries\ndata: {json.dumps(series)}\n\n"
            if n % 3 == 0:
                job_state = await run_in_threadpool(jobs.list_jobs)
                yield f"event: jobs\ndata: {json.dumps(job_state)}\n\n"
            worker_state = await run_in_threadpool(service.get_worker_activity, settings)
            yield f"event: workers\ndata: {json.dumps(worker_state)}\n\n"
            n += 1
            if ticks is not None and n >= ticks:
                return
            await asyncio.sleep(2)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
