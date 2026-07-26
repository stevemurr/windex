from __future__ import annotations

from types import SimpleNamespace

import httpx

from windex.modules import fetch
from windex.pipeline.ports import PartitionRef, WorkUnit


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
        events.append(
            ("request", request.url.params.get("resumptionToken", "first-page"))
        )
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
