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
from collections import deque
from datetime import date, datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx
import pyarrow.dataset as ds

from windex.config import Settings
from windex.crawl.fetch import BlockedTarget, check_url
from windex.crawl.links import extract_links
from windex.crawl.scope import canonicalize, in_scope
from windex.modules.common import (
    InputBatch,
    finish_batch,
    pending_batches,
    require_type,
)
from windex.recipe.ports import RawBlob, WorkUnit
from windex.smallweb.poll import HostRateLimiter, RobotsCache
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_INPUT_BATCH = 40
_USER_AGENT = "windex/1.0 (+local knowledge index)"
_REDIRECTS = frozenset({301, 302, 303, 307, 308})


def _hosts(raw) -> set[str]:
    values = raw if isinstance(raw, list) else str(raw or "").split(",")
    return {str(value).strip().lower() for value in values if str(value).strip()}


def _assert_host(url: str, allowed: set[str]) -> None:
    host = (urlsplit(url).hostname or "").lower()
    if not host or (allowed and host not in allowed):
        raise PermanentTaskError(
            f"fetch target host {host or '<missing>'!r} is not in the allowlist")


def _unit_url(ctx: TaskContext, unit: WorkUnit) -> str:
    # A root's stored URL is its human-facing landing page. The enumeration
    # contract is llms.txt; page children carry `path` plus their own URL and
    # must still take the ordinary payload branch below.
    if (ctx.source == "hf" and unit.ref.store == "root"
            and not unit.payload.get("path")):
        key = unit.ref.key.strip("/")
        return f"https://huggingface.co/{key}/llms.txt"
    if unit.payload.get("url"):
        return str(unit.payload["url"])
    if ctx.source == "hf" and unit.ref.key == "sitemap":
        return "https://huggingface.co/sitemap.xml"
    if ctx.source == "hf":
        key = unit.ref.key.strip("/")
        if unit.ref.store == "post":
            return f"https://huggingface.co/blog/{key}"
    if unit.ref.key.startswith(("http://", "https://")):
        return unit.ref.key
    raise PermanentTaskError(
        f"{ctx.module} cannot derive a URL for unit {unit.ref.key!r}")


def _template_url(ctx: TaskContext, unit: WorkUnit) -> str:
    template = str(ctx.config.get("url_template", ""))
    if not template:
        raise PermanentTaskError("http.download requires url_template")
    values = {"key": unit.ref.key, **unit.payload}
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
        Settings().downloads_dir / "_recipe_runs" / str(ctx.run_id)
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
    if ctx.source == "wiki" and unit.ref.key == "dump-index":
        from windex.wiki.sync import latest_complete

        wiki = str(ctx.params.get("dump", "enwiki"))
        dump_date, files = latest_complete(client, wiki)
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
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {"last": processed[-1].key, "outputs": outputs})
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
                when = when.replace(tzinfo=timezone.utc)
            return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def _page_limiter(source: str, interval: float) -> HostRateLimiter:
    if source == "hf":
        from windex.hf.fetch import PagesRateLimiter

        return PagesRateLimiter(interval)
    return HostRateLimiter(interval)


def _node_config(ctx: TaskContext, module: str) -> dict:
    flow_name = ctx.params.get("flow")
    flows = (ctx.spec.get("flows") or {})
    choices = [flows.get(flow_name)] if flow_name else list(flows.values())
    for flow in choices:
        for node in ((flow or {}).get("nodes") or {}).values():
            if node.get("uses") == module:
                return dict(node.get("with") or {})
    return {}


def _crawl_recipe(ctx: TaskContext, seeds: list[str]):
    links = _node_config(ctx, "crawl.links")
    prefix = links.get("path_prefix")
    if prefix is None:
        path = urlsplit(seeds[0]).path
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


def _crawl_pages(ctx: TaskContext, batches: list[InputBatch],
                 client: httpx.Client, robots: RobotsCache,
                 limiter: HostRateLimiter) -> tuple[list[RawBlob], int]:
    units = [
        require_type(value, WorkUnit, ctx.module)
        for batch in batches for value in batch.values
    ]
    seeds = [str(unit.payload.get("seed") or _unit_url(ctx, unit)) for unit in units]
    recipe, max_depth = _crawl_recipe(ctx, seeds)
    max_pages = max((int(unit.payload.get("max_pages", 500)) for unit in units),
                    default=500)
    queue = deque((canonicalize(seed), seed, 0) for seed in seeds)
    seen: set[str] = set()
    outputs = []
    while queue and len(outputs) < max_pages:
        url, seed, depth = queue.popleft()
        if url in seen:
            continue
        seen.add(url)
        unit = WorkUnit(
            ref=units[0].ref.__class__(
                store=units[0].ref.store,
                key=url,
                id_scope=units[0].ref.id_scope,
            ),
            payload={"url": url, "seed": seed, "depth": depth},
            epoch=ctx.run_id,
        )
        try:
            blob = _page(ctx, unit, client, robots, limiter)
        except (httpx.HTTPError, BlockedTarget):
            continue
        if blob.body:
            outputs.append(blob)
            if depth < max_depth and "html" in blob.media_type:
                text = blob.body.decode("utf-8", errors="replace")
                for found in extract_links(text, blob.uri):
                    candidate = canonicalize(found)
                    allowed, _ = in_scope(candidate, recipe, seed)
                    if allowed and candidate not in seen:
                        queue.append((candidate, seed, depth + 1))
        if ctx.should_yield():
            break
    return outputs, len(seen)


def _hf_sync_blob(ctx: TaskContext, unit: WorkUnit, client: httpx.Client,
                  robots: RobotsCache, limiter: HostRateLimiter) -> RawBlob:
    from windex.hf import license_for
    from windex.hf.sync import (
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
        for value in str(ctx.params.get("roots") or "").split(",")
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
                llms = _page(ctx, llms_unit, client, robots, limiter)
                text = (
                    llms.body.decode("utf-8", errors="replace")
                    if llms.body else ""
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
            if ctx.should_yield():
                raise RuntimeError(
                    "HF sitemap refresh reached a slice boundary; "
                    "retrying its atomic census")
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
    from windex.hf.sync import parse_llms

    listing = _page(ctx, unit, client, robots, limiter)
    if not listing.body:
        return []
    root = unit.ref.key.strip("/")
    pages = parse_llms(
        listing.body.decode("utf-8", errors="replace"), root)
    raw_anchors = ctx.params.get("anchor_ids")
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
        if ctx.should_yield():
            raise RuntimeError(
                "HF root crawl reached a slice boundary; retrying its atomic root")
    return outputs


def _smallweb_feed(ctx: TaskContext, unit: WorkUnit, client: httpx.Client,
                   robots: RobotsCache, limiter: HostRateLimiter) -> RawBlob:
    import feedparser

    from windex.smallweb.poll import (
        entry_link,
        entry_published,
        entry_title,
        item_body,
        newest_entries,
    )

    feed = _page(ctx, unit, client, robots, limiter)
    if not feed.body or feed.meta.get("not_modified"):
        return feed
    parsed = feedparser.parse(feed.body)
    maximum = int(ctx.params.get("max_items", 20))
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
            page = _page(ctx, page_unit, client, robots, limiter)
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
    batches, more = pending_batches(ctx, limit=_INPUT_BATCH)
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
        limiter = _page_limiter(ctx.source, interval)
        if ctx.source == "crawl" and batches:
            outputs, visited = _crawl_pages(ctx, batches, client, robots, limiter)
            finish_batch(ctx, batches[0], outputs=outputs)
            for batch in batches[1:]:
                finish_batch(ctx, batch)
            ctx.conn.commit()
            ctx.heartbeat(len(batches), 0, {
                "outputs": len(outputs), "visited": visited,
            })
            return SliceResult(
                units_done=len(batches),
                exhausted=not more,
                stats={"inputs": len(batches), "outputs": len(outputs),
                       "visited": visited},
            )
        processed = []
        count = 0
        for batch in batches:
            emitted = []
            for value in batch.values:
                unit = require_type(value, WorkUnit, ctx.module)
                if ctx.source == "hf" and unit.ref.key == "sitemap":
                    emitted.append(
                        _hf_sync_blob(ctx, unit, client, robots, limiter))
                elif ctx.source == "hf" and unit.ref.store == "root":
                    emitted.extend(
                        _hf_root_pages(ctx, unit, client, robots, limiter))
                elif ctx.source == "smallweb":
                    emitted.append(
                        _smallweb_feed(ctx, unit, client, robots, limiter))
                else:
                    emitted.append(_page(ctx, unit, client, robots, limiter))
            finish_batch(ctx, batch, outputs=emitted)
            processed.append(batch)
            count += len(emitted)
            if ctx.should_yield():
                break
    ctx.conn.commit()
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {"last": processed[-1].key, "outputs": count})
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(batches),
        stats={"inputs": done, "outputs": count},
    )


def _oai(ctx: TaskContext, unit: WorkUnit, client: httpx.Client) -> list[RawBlob]:
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
        from windex.arxiv.harvest import parse_records

        _, token = parse_records(response.content)
        if not token:
            return outputs
        if ctx.should_yield():
            raise RuntimeError(
                "OAI pagination reached a slice boundary; retrying its atomic window")
        time.sleep(interval)


def _algolia(ctx: TaskContext, unit: WorkUnit,
              client: httpx.Client) -> list[RawBlob]:
    from windex.hn.harvest import fetch_window_stories

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


def _github_search(ctx: TaskContext, unit: WorkUnit,
                   client: httpx.Client) -> list[RawBlob]:
    from windex.github.discover import _get

    tokens = Settings().github_token_list()
    if not tokens:
        raise PermanentTaskError("github_search_pages requires a GitHub token")
    threshold = int(unit.payload.get(
        "star_threshold", ctx.params.get("star_threshold", 10)))
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
        first = _get(
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
            shard_items.extend(_get(
                client, token,
                {"q": query, "per_page": page_size, "page": page},
            ).get("items") or [])
        items.extend(shard_items)
        leaves.append({
            "from": a.isoformat(), "to": b.isoformat(),
            "star_threshold": threshold, "repos": len(shard_items),
            "capped": total > cap,
        })
        if ctx.should_yield():
            raise RuntimeError(
                "GitHub pagination reached a slice boundary; retrying its atomic shard")
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


def _link_header(ctx: TaskContext, unit: WorkUnit,
                 client: httpx.Client) -> list[RawBlob]:
    url = _unit_url(ctx, unit)
    allowed = _hosts(ctx.config.get("allowed_hosts"))
    outputs = []
    while url:
        _assert_host(url, allowed)
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


def github_graphql_batch(ctx: TaskContext) -> SliceResult:
    from windex.github.hydrate import TokenPool, _build_query, _post

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
        body = _post(client, TokenPool(tokens), _build_query(names))
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
