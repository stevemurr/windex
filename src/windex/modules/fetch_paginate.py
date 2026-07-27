"""Protocol handlers used by the ``http.paginate`` runner."""

from __future__ import annotations

import json
import time
from collections import deque
from datetime import date, timedelta

import httpx

from windex.arxiv.oai import parse_records
from windex.config import Settings
from windex.github.api import search_get
from windex.hn.algolia import fetch_window_stories
from windex.modules.fetch_urls import assert_host, hosts, unit_url
from windex.pipeline.ports import RawBlob, WorkUnit
from windex.worker.protocol import PermanentTaskError, TaskContext


def _oai(
    ctx: TaskContext,
    unit: WorkUnit,
    client: httpx.Client,
) -> list[RawBlob]:
    endpoint = Settings().arxiv_oai_endpoint
    interval = float(ctx.config.get("request_interval", 3))
    token = None
    outputs = []
    while True:
        params = (
            {"verb": "ListRecords", "resumptionToken": token}
            if token else
            {"verb": "ListRecords",
             "metadataPrefix": Settings().arxiv_metadata_prefix,
             "from": unit.payload["from"], "until": unit.payload["until"]}
        )
        response = client.get(endpoint, params=params)
        response.raise_for_status()
        outputs.append(RawBlob(
            ref=unit.ref,
            uri=str(response.url),
            media_type=response.headers.get("content-type", "application/xml"),
            body=response.content,
            meta={"status": response.status_code, "page": len(outputs) + 1,
                  "payload": unit.payload, "upstream": unit.upstream},
            epoch=unit.epoch,
        ))
        _, token = parse_records(response.content)
        # The next request may be either another resumption page in this call
        # or the first page of a new window in a later worker slice. Cool down
        # after terminal pages too, otherwise the slice boundary bypasses the
        # configured arXiv request interval.
        time.sleep(interval)
        if not token:
            return outputs
        # A date window is the replace/resume boundary. Finish it even when the
        # supervisor requests a yield; the heartbeat thread keeps the lease
        # alive and _run_batches yields cleanly before claiming another window.


def _algolia(
    ctx: TaskContext,
    unit: WorkUnit,
    client: httpx.Client,
) -> list[RawBlob]:
    interval = float(ctx.config.get("request_interval", 1))
    last = [0.0]

    def pace():
        delay = interval - (time.monotonic() - last[0])
        if delay > 0:
            time.sleep(delay)
        last[0] = time.monotonic()

    hits, queries = fetch_window_stories(
        client,
        Settings().hn_algolia_url,
        int(unit.payload["from_ts"]),
        int(unit.payload["until_ts"]),
        on_request=pace,
        max_hits=int(ctx.config.get("result_cap", 1000)),
    )
    return [RawBlob(
        ref=unit.ref,
        uri=Settings().hn_algolia_url,
        media_type="application/json",
        body=json.dumps({"hits": hits}).encode(),
        meta={"status": 200, "queries": queries, "payload": unit.payload,
              "upstream": unit.upstream},
        epoch=unit.epoch,
    )]


def _github_search(
    ctx: TaskContext,
    unit: WorkUnit,
    client: httpx.Client,
) -> list[RawBlob]:
    tokens = Settings().github_token_list()
    if not tokens:
        raise PermanentTaskError("github_search_pages requires a GitHub token")
    threshold = int(unit.payload.get(
        "star_threshold", ctx.effective_config.get("star_threshold", 10)))
    start = date.fromisoformat(str(unit.payload.get("from", "2008-01-01")))
    end = date.fromisoformat(str(unit.payload.get("to", date.today().isoformat())))
    page_size = int(ctx.config.get("page_size", 100))
    cap = int(ctx.config.get("result_cap", 1000))
    split = bool(ctx.config.get("split_on_cap", True))
    interval = float(ctx.config.get("request_interval", 2.1))
    queue = deque([(start, end)])
    items = []
    leaves = []
    token_index = 0
    while queue:
        a, b = queue.popleft()
        query = f"stars:>={threshold} created:{a}..{b}"
        token = tokens[token_index % len(tokens)]
        token_index += 1
        first = search_get(
            client, token,
            {"q": query, "per_page": page_size, "page": 1},
        )
        total = int(first.get("total_count", 0))
        if split and total > cap and (b - a).days >= 1:
            midpoint = a + (b - a) / 2
            queue.extend(((a, midpoint), (midpoint + timedelta(days=1), b)))
            time.sleep(interval / len(tokens))
            continue
        shard_items = list(first.get("items") or [])
        pages = min((min(total, cap) + page_size - 1) // page_size,
                    max(1, cap // page_size))
        for page in range(2, pages + 1):
            time.sleep(interval / len(tokens))
            token = tokens[token_index % len(tokens)]
            token_index += 1
            shard_items.extend(search_get(
                client, token,
                {"q": query, "per_page": page_size, "page": page},
            ).get("items") or [])
        items.extend(shard_items)
        leaves.append({
            "from": a.isoformat(), "to": b.isoformat(),
            "star_threshold": threshold, "repos": len(shard_items),
            "capped": total > cap,
        })
        # state.pending emits one creation day per input unit. Complete that
        # bounded unit atomically, then _run_batches observes the yield before
        # claiming another day.
        time.sleep(interval / len(tokens))
    return [RawBlob(
        ref=unit.ref,
        uri="https://api.github.com/search/repositories",
        media_type="application/json",
        body=json.dumps({"items": items, "shards": leaves}).encode(),
        meta={"status": 200, "items": len(items), "payload": unit.payload,
              "upstream": unit.upstream},
        epoch=unit.epoch,
    )]


def _link_header(
    ctx: TaskContext,
    unit: WorkUnit,
    client: httpx.Client,
) -> list[RawBlob]:
    url = unit_url(ctx, unit)
    allowed = hosts(ctx.config.get("allowed_hosts"))
    outputs = []
    while url:
        assert_host(url, allowed)
        response = client.get(url)
        response.raise_for_status()
        outputs.append(RawBlob(
            ref=unit.ref, uri=str(response.url),
            media_type=response.headers.get("content-type", ""),
            body=response.content,
            meta={"status": response.status_code, "payload": unit.payload,
                  "upstream": unit.upstream},
            epoch=unit.epoch,
        ))
        url = response.links.get("next", {}).get("url")
    return outputs


# implementation_digest reads the complete source file containing each
# dependency. One local protocol plus each delegated focused helper therefore
# captures the entire behavior-bearing pagination graph without relying on the
# retired compatibility wrappers.
DIGEST_DEPENDENCIES = (
    _oai,
    parse_records,
    fetch_window_stories,
    search_get,
    unit_url,
)


__all__ = [
    "DIGEST_DEPENDENCIES",
    "_algolia",
    "_github_search",
    "_link_header",
    "_oai",
]
