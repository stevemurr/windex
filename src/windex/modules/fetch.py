"""Fetch-node implementations: WorkUnit -> RawBlob.

Network bodies are either bounded in memory (ordinary pages/API responses) or
atomically spooled to the downloads tier. Caller-selected URLs go through the
crawl SSRF guard at every redirect; fixed upstreams are also constrained by the
host allowlist frozen into the node config.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx
import pyarrow.dataset as ds
from psycopg.types.json import Jsonb

from windex.config import Settings
from windex.crawl.fetch import BlockedTarget, check_url
from windex.crawl.links import extract_links
from windex.crawl.scope import canonicalize, in_scope
from windex.github.api import (
    TokenPool as _GitHubTokenPool,
    build_graphql_query as _build_graphql_query,
    graphql_post as _graphql_post,
)
from windex.modules.common import (
    _store_outputs,
    finish_batch,
    pending_batches,
    require_type,
)
from windex.modules.fetch_paginate import (
    DIGEST_DEPENDENCIES as _PAGINATION_DIGEST_DEPENDENCIES,
    _algolia,
    _github_search,
    _link_header,
    _oai,
)
from windex.modules.fetch_urls import (
    assert_host as _assert_host,
    hosts as _hosts,
    unit_url as _unit_url,
)
from windex.pipeline.ports import PartitionRef, RawBlob, WorkUnit
from windex.smallweb.http import HostRateLimiter, RobotsCache
from windex.wiki.snapshots import latest_complete as _wiki_latest_complete
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_INPUT_BATCH = 40
_USER_AGENT = "windex/1.0 (+local knowledge index)"
_REDIRECTS = frozenset({301, 302, 303, 307, 308})
_CRAWL_UNIT_PREFIX = "crawl-url:"
_CRAWL_COVERAGE_KEY = "crawl-coverage"


def _template_url(ctx: TaskContext, unit: WorkUnit) -> str:
    template = str(ctx.config.get("url_template", ""))
    if not template:
        raise PermanentTaskError("http.download requires url_template")
    # Effective Pipeline configuration is frozen in the Run. Templates need both that
    # partition-invariant context (for example wiki's ``dump``) and the unit's
    # catalog payload (``dump_date``).
    values = {**ctx.effective_config, "key": unit.ref.key, **unit.payload}
    try:
        return template.format_map(values)
    except (KeyError, ValueError) as exc:
        raise PermanentTaskError(
            f"http.download template cannot be expanded: {exc}") from exc


def _download_path(ctx: TaskContext, unit: WorkUnit, url: str) -> Path:
    suffixes = "".join(Path(urlsplit(url).path).suffixes[-2:]) or ".bin"
    digest = hashlib.sha256(
        f"{ctx.task_id}:{unit.ref.store}:{unit.ref.key}:{url}".encode()
    ).hexdigest()
    return (
        Settings().downloads_dir / "_pipeline_runs" / str(ctx.run_id)
        / str(ctx.task_id) / f"{digest}{suffixes}"
    )


def _write_stream(resp: httpx.Response, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, prefix=".download-", delete=False) as tmp:
        temp = Path(tmp.name)
        size = 0
        try:
            for chunk in resp.iter_bytes(1 << 20):
                tmp.write(chunk)
                size += len(chunk)
            tmp.flush()
            os.fsync(tmp.fileno())
        except Exception:
            temp.unlink(missing_ok=True)
            raise
    os.replace(temp, path)
    return size


def _request_download(ctx: TaskContext, unit: WorkUnit,
                      client: httpx.Client) -> RawBlob:
    url = _template_url(ctx, unit)
    allowed = _hosts(ctx.config.get("allowed_hosts"))
    _assert_host(url, allowed)
    if ctx.search_name == "wiki" and unit.ref.key == "dump-index":
        wiki = str(ctx.effective_config.get("dump", "enwiki"))
        dump_date, files = _wiki_latest_complete(client, wiki)
        listing = ""
        if dump_date:
            listing = '<a href="_SUCCESS">_SUCCESS</a>\n' + "\n".join(
                f'<a href="{name}">{name}</a> now x {size}'
                for name, size in files
            )
        return RawBlob(
            ref=unit.ref,
            uri=url,
            media_type="application/json",
            body=json.dumps({"date": dump_date, "listing": listing}).encode(),
            meta={"status": 200, "files": len(files),
                  "payload": unit.payload, "upstream": unit.upstream},
            epoch=unit.epoch,
        )
    path = _download_path(ctx, unit, url)
    if path.is_file() and path.stat().st_size:
        return RawBlob(
            ref=unit.ref,
            uri=url,
            path=path,
            meta={"status": 200, "bytes": path.stat().st_size, "cached": True,
                  "upstream": unit.upstream, "payload": unit.payload},
            epoch=unit.epoch,
        )
    retries = int(ctx.config.get("retries", 3))
    for attempt in range(retries + 1):
        try:
            current = url
            for _ in range(6):
                _assert_host(current, allowed)
                with client.stream("GET", current) as response:
                    if response.status_code in _REDIRECTS:
                        location = response.headers.get("location")
                        if not location:
                            response.raise_for_status()
                        current = str(httpx.URL(current).join(location))
                        continue
                    if response.status_code == 404 and ctx.config.get("missing_ok"):
                        return RawBlob(
                            ref=unit.ref,
                            uri=current,
                            body=b"",
                            meta={"status": 404, "missing": True,
                                  "upstream": unit.upstream,
                                  "payload": unit.payload},
                            epoch=unit.epoch,
                        )
                    response.raise_for_status()
                    size = _write_stream(response, path)
                    return RawBlob(
                        ref=unit.ref,
                        uri=current,
                        media_type=response.headers.get("content-type", ""),
                        path=path,
                        meta={"status": response.status_code, "bytes": size,
                              "etag": response.headers.get("etag"),
                              "last_modified": response.headers.get("last-modified"),
                              "upstream": unit.upstream, "payload": unit.payload},
                        epoch=unit.epoch,
                    )
            raise RuntimeError(f"too many redirects downloading {url}")
        except httpx.HTTPError:
            if attempt >= retries:
                raise
            time.sleep(min(2 ** attempt, 30))
    raise AssertionError("unreachable")


def _run_batches(ctx: TaskContext, process, *, limit: int = _INPUT_BATCH) -> SliceResult:
    batches, more = pending_batches(ctx, limit=limit)
    processed = []
    outputs = 0
    for batch in batches:
        emitted = []
        for value in batch.values:
            emitted.extend(process(require_type(value, WorkUnit, ctx.module)))
        finish_batch(ctx, batch, outputs=emitted)
        processed.append(batch)
        outputs += len(emitted)
        # A fetched/downloaded upstream unit is already the resume boundary.
        # Commit it before starting another potentially long network unit so a
        # later crash cannot replay work that finished minutes earlier.
        ctx.conn.commit()
        ctx.heartbeat(
            len(processed),
            0,
            {"last": batch.key, "outputs": outputs},
        )
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(processed)
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(batches),
        stats={"inputs": done, "outputs": outputs},
    )


def http_download(ctx: TaskContext) -> SliceResult:
    timeout = httpx.Timeout(30, read=300)
    with httpx.Client(
        timeout=timeout, follow_redirects=False,
        headers={"User-Agent": _USER_AGENT},
    ) as client:
        return _run_batches(
            ctx, lambda unit: [_request_download(ctx, unit, client)])


http_download.__windex_digest_dependencies__ = (
    _wiki_latest_complete,
    _assert_host,
    pending_batches,
)


def _allowed_types(raw) -> tuple[str, ...]:
    aliases = {
        "html": "html",
        "xhtml": "xhtml",
        "markdown": "markdown",
        "xml": "xml",
        "rss": "rss",
        "atom": "atom",
    }
    values = raw if isinstance(raw, list) else str(raw or "").split(",")
    result = []
    for value in values:
        token = str(value).strip().lower()
        if token:
            result.append(aliases.get(token, token))
    return tuple(result)


def _page(ctx: TaskContext, unit: WorkUnit, client: httpx.Client,
          robots: RobotsCache, limiter: HostRateLimiter) -> RawBlob:
    url = _unit_url(ctx, unit)
    current = url
    max_bytes = int(ctx.config.get("max_bytes", 4_000_000))
    accepted = _allowed_types(ctx.config.get(
        "allowed_types", "html,xhtml,text/plain,markdown,xml,rss,atom"))
    conditional = bool(ctx.config.get("conditional", True))
    headers = {"User-Agent": _USER_AGENT}
    if conditional:
        etag = unit.payload.get("etag")
        modified = unit.payload.get("last_modified")
        if etag:
            headers["If-None-Match"] = str(etag)
        if modified:
            headers["If-Modified-Since"] = str(modified)

    redirects = 0
    retries = int(ctx.config.get("retries", 3))
    transient_attempt = 0
    while redirects < 6:
        check_url(current)
        if bool(ctx.config.get("robots", True)) and not robots.allowed(current):
            return RawBlob(
                ref=unit.ref, uri=current, body=b"",
                meta={"status": 0, "reason": "robots",
                      "payload": unit.payload, "upstream": unit.upstream},
                epoch=unit.epoch,
            )
        limiter.wait((urlsplit(current).hostname or "").lower())
        with client.stream("GET", current, headers=headers) as response:
            observe = getattr(limiter, "observe", None)
            if observe is not None:
                # The HF pages bucket is shared by every process on this IP.
                # Its response header is the only honest view of the remaining
                # budget, and a 429 is the response where observing it matters
                # most. Do this before raise_for_status().
                observe(response)
            if response.status_code in _REDIRECTS:
                location = response.headers.get("location")
                if not location:
                    break
                current = str(httpx.URL(current).join(location))
                redirects += 1
                continue
            if response.status_code == 304:
                return RawBlob(
                    ref=unit.ref, uri=current, body=b"",
                    meta={"status": 304, "not_modified": True,
                          "payload": unit.payload, "upstream": unit.upstream},
                    epoch=unit.epoch,
                )
            if response.status_code == 429 or response.status_code >= 500:
                if transient_attempt >= retries:
                    response.raise_for_status()
                transient_attempt += 1
                delay = min(2 ** (transient_attempt - 1), 30)
                if response.status_code == 429:
                    delay = max(
                        delay,
                        _retry_after_seconds(response),
                        float(getattr(limiter, "interval", 0)),
                    )
                time.sleep(delay)
                continue
            response.raise_for_status()
            media = response.headers.get("content-type", "").lower()
            if accepted and not any(kind in media for kind in accepted):
                return RawBlob(
                    ref=unit.ref, uri=current, body=b"",
                    media_type=media,
                    meta={"status": response.status_code, "reason": "content_type",
                          "payload": unit.payload, "upstream": unit.upstream},
                    epoch=unit.epoch,
                )
            length = response.headers.get("content-length")
            if length and length.isdigit() and int(length) > max_bytes:
                return RawBlob(
                    ref=unit.ref, uri=current, body=b"",
                    media_type=media,
                    meta={"status": response.status_code, "reason": "oversize",
                          "payload": unit.payload, "upstream": unit.upstream},
                    epoch=unit.epoch,
                )
            body = bytearray()
            for chunk in response.iter_bytes(1 << 16):
                body.extend(chunk)
                if len(body) > max_bytes:
                    return RawBlob(
                        ref=unit.ref, uri=current, body=b"",
                        media_type=media,
                        meta={"status": response.status_code, "reason": "oversize",
                              "payload": unit.payload, "upstream": unit.upstream},
                        epoch=unit.epoch,
                    )
            return RawBlob(
                ref=unit.ref,
                uri=current,
                media_type=media,
                body=bytes(body),
                meta={"status": response.status_code, "bytes": len(body),
                      "final_url": current, "etag": response.headers.get("etag"),
                      "last_modified": response.headers.get("last-modified"),
                      "payload": unit.payload, "upstream": unit.upstream},
                epoch=unit.epoch,
            )
    raise RuntimeError(f"too many redirects fetching {url}")


def _retry_after_seconds(response: httpx.Response) -> float:
    """Retry-After as seconds, accepting both legal wire formats."""
    raw = response.headers.get("retry-after")
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        try:
            when = parsedate_to_datetime(raw)
            if when.tzinfo is None:
                when = when.replace(tzinfo=UTC)
            return max(0.0, (when - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def _page_limiter(source: str, interval: float) -> HostRateLimiter:
    if source == "hf":
        from windex.hf.fetch import PagesRateLimiter

        return PagesRateLimiter(interval)
    return HostRateLimiter(interval)


def _node_config(ctx: TaskContext, module: str) -> dict:
    flow_name = ctx.effective_config.get("flow")
    flows = (ctx.spec.get("flows") or {})
    choices = [flows.get(flow_name)] if flow_name else list(flows.values())
    for flow in choices:
        for node in ((flow or {}).get("nodes") or {}).values():
            if node.get("uses") == module:
                return dict(node.get("with") or {})
    return {}


def _crawl_policy(ctx: TaskContext, seed: str):
    links = _node_config(ctx, "crawl.links")
    prefix = links.get("path_prefix")
    if prefix is None:
        path = urlsplit(seed).path
        prefix = path if path.endswith("/") else path.rsplit("/", 1)[0] + "/"
    include = tuple(re.compile(value) for value in (links.get("include") or []))
    exclude = tuple(re.compile(value) for value in (links.get("exclude") or []))
    scope = SimpleNamespace(
        same_host=bool(links.get("same_host", True)),
        path_prefix=str(prefix),
        include_re=include,
        exclude_re=exclude,
    )
    return SimpleNamespace(scope=scope), int(links.get("max_depth", 2))


def _crawl_unit_key(url: str) -> str:
    return _CRAWL_UNIT_PREFIX + hashlib.sha256(url.encode()).hexdigest()


def _crawl_insert_url(
    ctx: TaskContext,
    *,
    url: str,
    seed: str,
    depth: int,
    max_pages: int,
    store: str,
    id_scope: str | None,
    parent: str | None = None,
) -> str:
    """Add one URL if neither the seen set nor the run budget contains it."""
    key = _crawl_unit_key(url)
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                count(*) FILTER (WHERE unit_key LIKE %s),
                bool_or(unit_key = %s)
              FROM task_units
             WHERE task_id = %s
            """,
            (f"{_CRAWL_UNIT_PREFIX}%", key, ctx.task_id),
        )
        count, exists = cur.fetchone()
        if exists:
            return "seen"
        if int(count or 0) >= max_pages:
            return "budget"
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, parent, depth, state, counts)
            VALUES (%s, %s, %s, %s, %s, 'pending', %s)
            """,
            (
                ctx.run_id,
                ctx.task_id,
                key,
                parent,
                depth,
                Jsonb({
                    "url": url,
                    "seed": seed,
                    "max_pages": max_pages,
                    "store": store,
                    "id_scope": id_scope,
                }),
            ),
        )
    return "inserted"


def _crawl_mark_incomplete(ctx: TaskContext, reason: str) -> None:
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            UPDATE task_units
               SET counts = counts || %s, reason = %s
             WHERE task_id = %s AND unit_key = %s
            """,
            (
                Jsonb({"truncated": True}),
                reason,
                ctx.task_id,
                _CRAWL_COVERAGE_KEY,
            ),
        )


def _crawl_initialize(ctx: TaskContext) -> None:
    """Consume seed batches and persist their URLs before any network work."""
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, counts)
            SELECT %s, %s, %s, 'pending', %s
             WHERE NOT EXISTS (
                   SELECT 1 FROM task_units
                    WHERE task_id = %s AND unit_key = %s)
            """,
            (
                ctx.run_id,
                ctx.task_id,
                _CRAWL_COVERAGE_KEY,
                Jsonb({"truncated": False}),
                ctx.task_id,
                _CRAWL_COVERAGE_KEY,
            ),
        )
    batches, _ = pending_batches(ctx, limit=_INPUT_BATCH)
    for batch in batches:
        for value in batch.values:
            unit = require_type(value, WorkUnit, ctx.module)
            seed = canonicalize(
                str(unit.payload.get("seed") or _unit_url(ctx, unit)))
            inserted = _crawl_insert_url(
                ctx,
                url=seed,
                seed=seed,
                depth=0,
                max_pages=int(unit.payload.get("max_pages", 500)),
                store=unit.ref.store,
                id_scope=unit.ref.id_scope,
            )
            if inserted == "budget":
                _crawl_mark_incomplete(ctx, "page_budget")
        # This empty row is the durable edge-consumption marker. The URL rows
        # below carry the actual RawBlob outputs once fetched.
        finish_batch(ctx, batch)
    ctx.conn.commit()


def _crawl_pending(ctx: TaskContext):
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT unit_key, depth, counts
              FROM task_units
             WHERE task_id = %s AND state = 'pending'
               AND unit_key LIKE %s
             ORDER BY depth, seq
             LIMIT 1
            """,
            (ctx.task_id, f"{_CRAWL_UNIT_PREFIX}%"),
        )
        return cur.fetchone()


def _crawl_finish_url(
    ctx: TaskContext,
    key: str,
    *,
    state: str,
    outputs: list[RawBlob] | None = None,
    reason: str | None = None,
    size: int | None = None,
) -> None:
    stored = _store_outputs(ctx, key, outputs or [])
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            UPDATE task_units
               SET state = %s, reason = %s, bytes = %s, outputs = %s,
                   seq = nextval('task_unit_seq'), finished_at = now()
             WHERE task_id = %s AND unit_key = %s AND state = 'pending'
            """,
            (state, reason, size, Jsonb(stored), ctx.task_id, key),
        )


def _crawl_finish_coverage(ctx: TaskContext) -> bool:
    """Emit the census marker consumed by the prune guard."""
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT counts, reason
              FROM task_units
             WHERE task_id = %s AND unit_key = %s
            """,
            (ctx.task_id, _CRAWL_COVERAGE_KEY),
        )
        row = cur.fetchone()
        if row is None:
            return False
        counts, reason = row
        truncated = bool(counts.get("truncated"))
        cur.execute(
            """
            SELECT counts
              FROM task_units
             WHERE task_id = %s AND unit_key LIKE %s
             ORDER BY depth, id
             LIMIT 1
            """,
            (ctx.task_id, f"{_CRAWL_UNIT_PREFIX}%"),
        )
        seed_row = cur.fetchone()
    seed_counts = seed_row[0] if seed_row else {}
    seed = str(seed_counts.get("seed") or "")
    marker = RawBlob(
        ref=PartitionRef(
            store=str(seed_counts.get("store") or "frontier"),
            key=seed,
            id_scope=seed_counts.get("id_scope"),
        ),
        uri=seed,
        body=b"",
        meta={
            "missing": True,
            "_coverage_truncated": truncated,
            "reason": reason or "",
        },
        epoch=ctx.run_id,
    )
    stored = _store_outputs(ctx, _CRAWL_COVERAGE_KEY, [marker])
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            UPDATE task_units
               SET state = 'done', outputs = %s,
                   seq = nextval('task_unit_seq'), finished_at = now()
             WHERE task_id = %s AND unit_key = %s AND state = 'pending'
            """,
            (Jsonb(stored), ctx.task_id, _CRAWL_COVERAGE_KEY),
        )
    return truncated


def _crawl_http_get(
    ctx: TaskContext,
    client: httpx.Client,
    robots: RobotsCache,
    limiter: HostRateLimiter,
) -> SliceResult:
    """Fetch a durable BFS frontier, committing one URL at a time."""
    _crawl_initialize(ctx)
    processed = outputs = skipped = 0
    last = ""
    while row := _crawl_pending(ctx):
        key, depth, counts = row
        url = str(counts["url"])
        seed = str(counts["seed"])
        max_pages = int(counts["max_pages"])
        unit = WorkUnit(
            ref=PartitionRef(
                store=str(counts["store"]),
                key=url,
                id_scope=counts.get("id_scope"),
            ),
            payload={"url": url, "seed": seed, "depth": depth},
            epoch=ctx.run_id,
        )
        try:
            blob = _page(ctx, unit, client, robots, limiter)
        except (httpx.HTTPError, BlockedTarget) as exc:
            response = getattr(exc, "response", None)
            reason = (
                f"http_{response.status_code}"
                if response is not None
                else type(exc).__name__.lower()
            )
            _crawl_finish_url(ctx, key, state="skipped", reason=reason)
            _crawl_mark_incomplete(ctx, reason)
            skipped += 1
        else:
            if blob.body:
                policy, max_depth = _crawl_policy(ctx, seed)
                if depth < max_depth and "html" in blob.media_type:
                    for found in extract_links(
                            blob.body.decode("utf-8", errors="replace"),
                            blob.uri):
                        candidate = canonicalize(found)
                        allowed, _ = in_scope(candidate, policy, seed)
                        if allowed:
                            inserted = _crawl_insert_url(
                                ctx,
                                url=candidate,
                                seed=seed,
                                depth=depth + 1,
                                max_pages=max_pages,
                                store=unit.ref.store,
                                id_scope=unit.ref.id_scope,
                                parent=url,
                            )
                            if inserted == "budget":
                                _crawl_mark_incomplete(ctx, "page_budget")
                _crawl_finish_url(
                    ctx,
                    key,
                    state="done",
                    outputs=[blob],
                    size=len(blob.body),
                )
                outputs += 1
            else:
                _crawl_finish_url(
                    ctx,
                    key,
                    state="skipped",
                    reason=str(blob.meta.get("reason") or "empty"),
                )
                _crawl_mark_incomplete(
                    ctx, str(blob.meta.get("reason") or "empty"))
                skipped += 1
        ctx.conn.commit()
        processed += 1
        last = url
        ctx.heartbeat(
            processed,
            0,
            {"last": last, "outputs": outputs, "skipped": skipped},
        )
        if ctx.should_yield():
            break

    pending = _crawl_pending(ctx) is not None
    truncated = False
    if not pending:
        truncated = _crawl_finish_coverage(ctx)
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM task_units
             WHERE task_id = %s AND unit_key LIKE %s
            """,
            (ctx.task_id, f"{_CRAWL_UNIT_PREFIX}%"),
        )
        total = int(cur.fetchone()[0])
    ctx.conn.commit()
    return SliceResult(
        units_done=processed,
        exhausted=not pending,
        units_total=total,
        stats={
            "inputs": processed,
            "outputs": outputs,
            "skipped": skipped,
            "truncated": truncated,
            "last": last,
        },
    )


def _hf_sync_blob(ctx: TaskContext, unit: WorkUnit, client: httpx.Client,
                  robots: RobotsCache, limiter: HostRateLimiter) -> RawBlob:
    from windex.hf import license_for
    from windex.hf.formats import (
        WANTED_SHARDS,
        kind_of,
        parse_llms,
        parse_sitemap_index,
        parse_urlset,
        root_key,
        root_version,
        sha1,
    )

    index = _page(ctx, unit, client, robots, limiter)
    if not index.body:
        return index
    try:
        shards = parse_sitemap_index(
            index.body.decode("utf-8", errors="replace"))
    except Exception as exc:
        raise RuntimeError(f"invalid Hugging Face sitemap index: {exc}") from exc
    wanted_roots = {
        value.strip().strip("/")
        for value in str(ctx.effective_config.get("roots") or "").split(",")
        if value.strip()
    }
    entries = []
    for shard_url in shards:
        shard_name = shard_url.rsplit("/", 1)[-1]
        if shard_name not in WANTED_SHARDS:
            continue
        shard_unit = WorkUnit(
            ref=unit.ref,
            payload={"url": shard_url},
            epoch=unit.epoch,
        )
        shard = _page(ctx, shard_unit, client, robots, limiter)
        if not shard.body:
            continue
        for url, lastmod in parse_urlset(
                shard.body.decode("utf-8", errors="replace")):
            entry = {"shard": shard_name, "url": url, "lastmod": lastmod}
            if shard_name == "sitemap-doc.xml":
                root = root_key(url)
                if wanted_roots and root not in wanted_roots:
                    continue
                llms_unit = WorkUnit(
                    ref=unit.ref,
                    payload={"url": f"https://huggingface.co/{root}/llms.txt"},
                    epoch=unit.epoch,
                )
                try:
                    llms = _page(ctx, llms_unit, client, robots, limiter)
                except httpx.HTTPStatusError as exc:
                    # HF advertises a handful of client-rendered roots that
                    # deliberately have no llms.txt. They remain in the catalog
                    # with a NULL hash so discovery can exclude them.
                    if exc.response.status_code != 404:
                        raise
                    llms = None
                text = (
                    llms.body.decode("utf-8", errors="replace")
                    if llms is not None and llms.body else ""
                )
                pages = parse_llms(text, root) if text else []
                entry.update({
                    "kind": kind_of(root),
                    "llms_hash": sha1(text) if text else None,
                    "pages": len(pages),
                    "version": root_version(pages),
                    "license": license_for(root),
                })
            entries.append(entry)
            # The sitemap plus every llms.txt hash is one destructive-census
            # guard. It must finish atomically even when HF's polite pacing
            # exceeds the worker's ordinary slice deadline; the heartbeat
            # thread continues renewing the lease while this loop runs.
    return RawBlob(
        ref=unit.ref,
        uri=index.uri,
        media_type="application/json",
        body=json.dumps({"sitemaps": entries}).encode(),
        meta={"status": 200, "entries": len(entries),
              "payload": unit.payload, "upstream": unit.upstream},
        epoch=unit.epoch,
    )


def _hf_root_pages(ctx: TaskContext, unit: WorkUnit, client: httpx.Client,
                   robots: RobotsCache,
                   limiter: HostRateLimiter) -> list[RawBlob]:
    from windex.hf.formats import parse_llms

    listing = _page(ctx, unit, client, robots, limiter)
    if not listing.body:
        return []
    root = unit.ref.key.strip("/")
    pages = parse_llms(
        listing.body.decode("utf-8", errors="replace"), root)
    raw_anchors = ctx.effective_config.get("anchor_ids")
    anchors = {
        value.strip()
        for value in (
            raw_anchors.split(",")
            if isinstance(raw_anchors, str) else raw_anchors or []
        )
        if value.strip()
    }
    outputs = []
    for page in pages:
        version = page.get("version") or ""
        path = page["path"]
        doc_id = f"hf:{root}/{path}"
        if anchors and doc_id not in anchors:
            continue
        version_part = f"{version}/" if version else ""
        url = f"https://huggingface.co/{root}/{version_part}{path}.md"
        page_unit = WorkUnit(
            ref=unit.ref.__class__(
                store=unit.ref.store,
                key=unit.ref.key,
                # An anchor replay is a deliberately partial root census. Its
                # replace scope owns only that exact document; using the whole
                # root here would tombstone unrelated pages.
                id_scope=(
                    doc_id if anchors
                    else unit.ref.id_scope or f"hf:{root}/"
                ),
            ),
            payload={
                **unit.payload,
                "url": url,
                "root": root,
                "path": path,
                "title": page.get("title") or "",
                "version": version,
            },
            upstream=unit.upstream,
            epoch=unit.epoch,
        )
        blob = _page(ctx, page_unit, client, robots, limiter)
        if blob.body:
            outputs.append(blob)
        # A root is the replace boundary. Returning a partial list would make a
        # truncated fetch look like a complete census and tombstone valid pages,
        # so this atomic unit is allowed to outlive an ordinary worker slice.
    return outputs


def _smallweb_feed(ctx: TaskContext, unit: WorkUnit, client: httpx.Client,
                   robots: RobotsCache, limiter: HostRateLimiter) -> RawBlob:
    import feedparser

    from windex.smallweb.feed import (
        entry_link,
        entry_published,
        entry_title,
        item_body,
        newest_entries,
    )

    try:
        feed = _page(ctx, unit, client, robots, limiter)
    except (httpx.HTTPError, httpx.InvalidURL, BlockedTarget) as exc:
        # A public feed list inevitably contains expired domains, broken TLS,
        # and malformed URLs. That is a property of this unit, not a reason to
        # retry and eventually fail the other ~36k feeds in the task.
        return RawBlob(
            ref=unit.ref,
            uri=str(unit.payload.get("url") or unit.ref.key),
            body=b"",
            meta={
                "status": getattr(getattr(exc, "response", None), "status_code", 0),
                "reason": "fetch_error",
                "error": f"{type(exc).__name__}: {str(exc)[:500]}",
                "payload": unit.payload,
                "upstream": unit.upstream,
            },
            epoch=unit.epoch,
        )
    if not feed.body or feed.meta.get("not_modified"):
        return feed
    parsed = feedparser.parse(feed.body)
    maximum = int(ctx.effective_config.get("max_items", 20))
    minimum = int(getattr(Settings(), "smallweb_inline_summary_min", 600))
    outlet = (urlsplit(feed.uri).hostname or "").lower()
    items = []
    for entry in newest_entries(parsed, maximum):
        url = entry_link(entry)
        if not url:
            continue
        body, inline = item_body(entry, minimum)
        if body is None:
            page_unit = WorkUnit(
                ref=unit.ref,
                payload={"url": url},
                upstream=unit.upstream,
                epoch=unit.epoch,
            )
            try:
                page = _page(ctx, page_unit, client, robots, limiter)
            except (httpx.HTTPError, httpx.InvalidURL, BlockedTarget):
                continue
            body = (
                page.body.decode("utf-8", errors="replace")
                if page.body else None
            )
        if body is None:
            continue
        items.append({
            "url": url,
            "body": body,
            "inline": inline,
            "title": entry_title(entry),
            "published": entry_published(entry),
            "outlet": outlet,
        })
    return RawBlob(
        ref=unit.ref,
        uri=feed.uri,
        media_type="application/json",
        body=json.dumps({"items": items}).encode(),
        meta={**feed.meta, "items": len(items)},
        epoch=unit.epoch,
    )


def http_get(ctx: TaskContext) -> SliceResult:
    timeout = float(ctx.config.get("request_timeout", 15))
    interval = float(ctx.config.get("host_interval", 2))
    settings = Settings()
    with httpx.Client(
        timeout=httpx.Timeout(timeout, read=timeout),
        follow_redirects=False,
        headers={"User-Agent": _USER_AGENT},
    ) as client:
        robots = RobotsCache(
            client,
            getattr(settings, "crawl_robots_ttl", 86_400),
            user_agent=_USER_AGENT,
        )
        limiter = _page_limiter(ctx.search_name, interval)
        if ctx.search_name == "crawl":
            return _crawl_http_get(ctx, client, robots, limiter)
        batches, more = pending_batches(ctx, limit=_INPUT_BATCH)
        processed = []
        count = 0
        for batch in batches:
            emitted = []
            for value in batch.values:
                unit = require_type(value, WorkUnit, ctx.module)
                if ctx.search_name == "hf" and unit.ref.key == "sitemap":
                    emitted.append(
                        _hf_sync_blob(ctx, unit, client, robots, limiter))
                elif ctx.search_name == "hf" and unit.ref.store == "root":
                    emitted.extend(
                        _hf_root_pages(ctx, unit, client, robots, limiter))
                elif ctx.search_name == "smallweb":
                    emitted.append(
                        _smallweb_feed(ctx, unit, client, robots, limiter))
                else:
                    emitted.append(_page(ctx, unit, client, robots, limiter))
            finish_batch(ctx, batch, outputs=emitted)
            processed.append(batch)
            count += len(emitted)
            ctx.conn.commit()
            ctx.heartbeat(
                len(processed),
                0,
                {"last": batch.key, "outputs": count},
            )
            if ctx.should_yield():
                break
    ctx.conn.commit()
    done = len(processed)
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(batches),
        stats={"inputs": done, "outputs": count},
    )


def http_paginate(ctx: TaskContext) -> SliceResult:
    protocol = str(ctx.config.get("protocol", ""))
    with httpx.Client(
        timeout=httpx.Timeout(30, read=120),
        follow_redirects=True,
        headers={"User-Agent": _USER_AGENT},
    ) as client:
        if protocol == "oai_resumption":
            return _run_batches(ctx, lambda unit: _oai(ctx, unit, client), limit=1)
        if protocol == "algolia_numeric":
            return _run_batches(ctx, lambda unit: _algolia(ctx, unit, client), limit=1)
        if protocol == "github_search_pages":
            return _run_batches(
                ctx, lambda unit: _github_search(ctx, unit, client), limit=1)
        if protocol == "link_header":
            return _run_batches(
                ctx, lambda unit: _link_header(ctx, unit, client), limit=1)
    raise PermanentTaskError(f"http.paginate has unknown protocol {protocol!r}")


# implementation_digest hashes the runner's source file. Pagination protocols
# live in a focused helper module now, so include that file explicitly to keep
# frozen Module locks sensitive to every protocol implementation change.
http_paginate.__windex_digest_dependencies__ = (
    *_PAGINATION_DIGEST_DEPENDENCIES,
    pending_batches,
)


def github_graphql_batch(ctx: TaskContext) -> SliceResult:
    limit = int(ctx.config.get("batch", 40))
    batches, more = pending_batches(ctx, limit=limit)
    pairs = [
        (batch, require_type(value, WorkUnit, ctx.module))
        for batch in batches for value in batch.values
    ]
    if not pairs:
        return SliceResult(exhausted=not more)
    tokens = Settings().github_token_list()
    if not tokens:
        raise PermanentTaskError("github.graphql_batch requires a GitHub token")
    names = [str(unit.payload["full_name"]) for _, unit in pairs]
    with httpx.Client(timeout=60) as client:
        body = _graphql_post(
            client,
            _GitHubTokenPool(tokens),
            _build_graphql_query(names),
        )
    data = body.get("data") or {}
    grouped: dict[str, list[RawBlob]] = {batch.key: [] for batch in batches}
    for index, (batch, unit) in enumerate(pairs):
        payload = {
            "repo": data.get(f"r{index}"),
            "candidate": unit.payload,
        }
        grouped[batch.key].append(RawBlob(
            ref=unit.ref,
            uri="https://api.github.com/graphql",
            media_type="application/json",
            body=json.dumps(payload).encode(),
            meta={"status": 200, "payload": unit.payload},
            epoch=unit.epoch,
        ))
    for batch in batches:
        finish_batch(ctx, batch, outputs=grouped[batch.key])
    ctx.conn.commit()
    ctx.heartbeat(len(batches), 0, {"repos": len(pairs)})
    return SliceResult(
        units_done=len(batches),
        exhausted=not more,
        stats={"inputs": len(batches), "repos": len(pairs)},
    )


github_graphql_batch.__windex_digest_dependencies__ = (
    _graphql_post,
    pending_batches,
)


def local_parquet_lookup(ctx: TaskContext) -> SliceResult:
    directory = Path(str(ctx.config.get("dir", "")))
    if not directory.is_absolute():
        directory = Settings().staging_dir / directory
    root = Settings().staging_dir.resolve()
    resolved = directory.resolve()
    if not resolved.is_relative_to(root):
        raise PermanentTaskError("local.parquet_lookup dir escapes staging")
    key_column = str(ctx.config.get("key_column", ""))
    value_column = str(ctx.config.get("value_column", ""))
    if not key_column or not value_column:
        raise PermanentTaskError(
            "local.parquet_lookup requires key_column and value_column")
    batches, more = pending_batches(ctx, limit=512)
    pairs = [
        (batch, require_type(value, WorkUnit, ctx.module))
        for batch in batches for value in batch.values
    ]
    keys = [unit.payload.get(key_column, unit.ref.key) for _, unit in pairs]
    found = {}
    files = sorted(resolved.glob("*.parquet")) if resolved.is_dir() else []
    if files:
        try:
            table = ds.dataset(files, format="parquet").to_table(
                columns=[key_column, value_column],
                filter=ds.field(key_column).isin(keys),
            )
            found = dict(zip(
                table[key_column].to_pylist(),
                table[value_column].to_pylist(),
            ))
        except Exception:
            if not bool(ctx.config.get("skip_unreadable", True)):
                raise
    grouped: dict[str, list[RawBlob]] = {batch.key: [] for batch in batches}
    for batch, unit in pairs:
        key = unit.payload.get(key_column, unit.ref.key)
        value = found.get(key)
        if value is None:
            continue
        grouped[batch.key].append(RawBlob(
            ref=unit.ref,
            uri=f"parquet://{resolved}",
            media_type="application/json",
            body=json.dumps({
                **unit.payload,
                key_column: key,
                value_column: value,
            }).encode(),
            meta={"payload": unit.payload},
            epoch=unit.epoch,
        ))
    for batch in batches:
        finish_batch(ctx, batch, outputs=grouped[batch.key])
    ctx.conn.commit()
    done = len(batches)
    if done:
        ctx.heartbeat(done, 0, {"matches": sum(map(len, grouped.values()))})
    return SliceResult(
        units_done=done,
        exhausted=not more,
        stats={"inputs": done, "matches": sum(map(len, grouped.values()))},
    )


local_parquet_lookup.__windex_digest_dependencies__ = (pending_batches,)
