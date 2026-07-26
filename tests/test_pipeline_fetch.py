from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import httpx

from windex.modules import fetch, http_get
from windex.modules.common import InputBatch
from windex.pipeline.ports import PartitionRef, RawBlob, WorkUnit


def _unit(day: str) -> WorkUnit:
    return WorkUnit(
        ref=PartitionRef(store="window", key=day),
        payload={"from": day, "until": day},
        epoch=3,
    )


def test_oai_paces_across_terminal_window_boundaries(monkeypatch):
    events: list[tuple[str, object]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        events.append(("request", request.url.params.get("from")))
        return httpx.Response(
            200,
            content=b"<OAI-PMH/>",
            headers={"content-type": "application/xml"},
        )

    monkeypatch.setattr(
        "windex.arxiv.harvest.parse_records",
        lambda _body: ([], None),
    )
    monkeypatch.setattr(
        fetch.time,
        "sleep",
        lambda seconds: events.append(("sleep", seconds)),
    )
    ctx = SimpleNamespace(config={"request_interval": 3.0})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        fetch._oai(ctx, _unit("2026-07-24"), client)
        fetch._oai(ctx, _unit("2026-07-25"), client)

    assert events == [
        ("request", "2026-07-24"),
        ("sleep", 3.0),
        ("request", "2026-07-25"),
        ("sleep", 3.0),
    ]


def test_oai_paces_every_resumption_page_including_last(monkeypatch):
    events: list[tuple[str, object]] = []
    tokens = iter(["next-page", None])

    def respond(request: httpx.Request) -> httpx.Response:
        events.append(("request", request.url.params.get("resumptionToken", "first-page")))
        return httpx.Response(
            200,
            content=b"<OAI-PMH/>",
            headers={"content-type": "application/xml"},
        )

    monkeypatch.setattr(
        "windex.arxiv.harvest.parse_records",
        lambda _body: ([], next(tokens)),
    )
    monkeypatch.setattr(
        fetch.time,
        "sleep",
        lambda seconds: events.append(("sleep", seconds)),
    )
    ctx = SimpleNamespace(config={"request_interval": 3.0})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        outputs = fetch._oai(ctx, _unit("2026-07-25"), client)

    assert events == [
        ("request", "first-page"),
        ("sleep", 3.0),
        ("request", "next-page"),
        ("sleep", 3.0),
    ]
    assert [output.meta["page"] for output in outputs] == [1, 2]


class _ProgressCursor:
    def __init__(self, completed):
        self.completed = completed
        self.row = None

    def execute(self, _query, args):
        prefix = args[-1]
        self.row = (
            sum(
                item["counts"].get("hf_pages_fetched", 0)
                for item in self.completed
                if item["key"].startswith(prefix)
            ),
        )

    def fetchone(self):
        return self.row


class _ProgressConnection:
    def __init__(self, completed):
        self.completed = completed

    @contextmanager
    def cursor(self):
        yield _ProgressCursor(self.completed)


def test_hf_root_fetch_persists_page_slices_before_atomic_census(
    monkeypatch,
    tmp_path,
):
    completed = []
    calls = []
    batch = InputBatch(
        key="roots:1",
        values=(
            WorkUnit(
                ref=PartitionRef(
                    store="root",
                    key="docs/example",
                    id_scope="hf:docs/example/",
                ),
                payload={"kind": "docs"},
                epoch=24,
            ),
        ),
    )
    pages = [
        {"path": f"page-{index}", "title": f"Page {index}", "version": ""} for index in range(5)
    ]

    def page(_ctx, unit, _client, _robots, _limiter):
        path = unit.payload.get("path")
        calls.append(path or "listing")
        if path is None:
            return RawBlob(
                ref=unit.ref,
                uri="https://huggingface.co/docs/example/llms.txt",
                body=b"listing",
                meta={"status": 200},
                epoch=unit.epoch,
            )
        return RawBlob(
            ref=unit.ref,
            uri=str(unit.payload["url"]),
            body=f"body:{path}".encode(),
            meta={"status": 200, "payload": unit.payload},
            epoch=unit.epoch,
        )

    def finish(_ctx, item, *, outputs=None, counts=None):
        completed.append(
            {
                "key": item.key,
                "outputs": outputs or [],
                "counts": counts or {},
            }
        )

    monkeypatch.setattr(fetch, "_page", page)
    monkeypatch.setattr("windex.hf.sync.parse_llms", lambda _text, _root: pages)
    monkeypatch.setattr(http_get, "finish_batch", finish)
    monkeypatch.setattr(
        http_get,
        "Settings",
        lambda: SimpleNamespace(staging_dir=tmp_path),
    )
    ctx = SimpleNamespace(
        config={"root_pages_per_slice": 2},
        effective_config={},
        run_id=24,
        task_id=127,
        module="http.get",
        conn=_ProgressConnection(completed),
        should_yield=lambda: False,
    )

    first = http_get._hf_root_slice(ctx, batch, None, None, None)
    second = http_get._hf_root_slice(ctx, batch, None, None, None)
    third = http_get._hf_root_slice(ctx, batch, None, None, None)

    assert first == (2, False)
    assert second == (2, False)
    assert third == (1, True)
    assert calls == [
        "listing",
        "page-0",
        "page-1",
        "page-2",
        "page-3",
        "page-4",
    ]
    assert [item["key"] for item in completed] == [
        "roots:1#hf-pages=0",
        "roots:1#hf-pages=2",
        "roots:1",
    ]
    assert completed[0]["outputs"] == []
    assert completed[1]["outputs"] == []
    assert [blob.path.read_bytes().decode() for blob in completed[2]["outputs"]] == [
        f"body:page-{index}" for index in range(5)
    ]
    assert completed[2]["counts"] == {
        "hf_pages_fetched": 1,
        "hf_page_offset": 4,
        "hf_pages_total": 5,
        "hf_root_complete": True,
    }


def test_http_get_only_routes_explicit_hf_root_census(monkeypatch):
    legacy_result = object()
    census_result = object()
    monkeypatch.setattr(http_get.legacy, "http_get", lambda _ctx: legacy_result)
    monkeypatch.setattr(http_get, "_hf_root_get", lambda _ctx: census_result)

    ordinary = SimpleNamespace(
        search_name="hf",
        config={"root_pages_per_slice": 20},
    )
    census = SimpleNamespace(
        search_name="hf",
        config={"hf_root_census": True, "root_pages_per_slice": 20},
    )

    assert http_get.http_get(ordinary) is legacy_result
    assert http_get.http_get(census) is census_result
