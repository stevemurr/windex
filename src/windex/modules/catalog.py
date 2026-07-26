"""Pure listing parsers: RawBlob -> PartitionRecord."""

from __future__ import annotations

import gzip
import json
import re
from dataclasses import replace
from datetime import date, timedelta
from html import unescape
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

import feedparser

from windex.modules.common import (
    blob_bytes,
    downstream_store,
    finish_batch,
    pending_batches,
    require_type,
)
from windex.crawl.links import extract_links
from windex.crawl.scope import canonicalize, in_scope
from windex.hf.sync import blog_slug, kind_of, root_key
from windex.pipeline.ports import PartitionRecord, RawBlob
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
    batches, more = pending_batches(ctx, limit=_INPUT_BATCH)
    store = downstream_store(ctx)
    records = 0
    processed = []
    for batch in batches:
        outputs = []
        for value in batch.values:
            blob = require_type(value, RawBlob, ctx.module)
            if blob.meta.get("missing") or blob.meta.get("not_modified"):
                parsed = []
            else:
                parsed = parse(blob, store)
            outputs.extend(
                record if record.ref is not None else replace(record, ref=blob.ref)
                for record in (parsed or [PartitionRecord(
                    store=store,
                    key="",
                    ref=blob.ref,
                    payload={"_coverage_only": True},
                )])
            )
        finish_batch(ctx, batch, outputs=outputs)
        records += sum(
            not record.payload.get("_coverage_only") for record in outputs)
        processed.append(batch)
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {"records": records, "last": processed[-1].key})
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(batches),
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
    wanted = {
        value.strip()
        for value in str(ctx.effective_config.get("slugs") or "").split(",")
        if value.strip()
    }

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
            if wanted and key not in wanted:
                continue
            upstream = (
                {upstream_field: entry[upstream_field]}
                if upstream_field and entry.get(upstream_field) is not None else {}
            )
            records.append(PartitionRecord(
                store=store,
                key=key,
                upstream=upstream,
                payload={
                    **entry,
                    **({"id_scope": f"docs:{key}/"}
                       if ctx.search_name == "docs" else {}),
                },
            ))
        return records

    return _run(ctx, parse)


def list_path_manifest_gz(ctx: TaskContext) -> SliceResult:
    patterns = _patterns(ctx.config.get("key_pattern", []))
    min_age = int(ctx.config.get("min_age_days", 0))
    max_age = int(ctx.config.get("max_age_days", 0))
    newest = date.today() - timedelta(days=min_age)
    oldest = date.today() - timedelta(days=max_age) if max_age else None

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
            if min_age or max_age:
                match = re.search(r"(20\d{2})(?:/)?([01]\d)(?:/)?([0-3]\d)", key)
                if match is None:
                    continue
                try:
                    published = date(*(int(part) for part in match.groups()))
                except ValueError:
                    continue
                if min_age and published > newest:
                    continue
                if oldest is not None and published < oldest:
                    continue
            seen.add(key)
            records.append(PartitionRecord(store=store, key=key))
        return records

    return _run(ctx, parse)


def list_sitemap(ctx: TaskContext) -> SliceResult:
    """Parse a sitemap URL set or the HF fetcher's enriched sitemap envelope."""
    allowed = {
        value.strip()
        for value in str(ctx.config.get("shard_allow", "")).split(",")
        if value.strip()
    }
    wanted_roots = {
        value.strip().strip("/")
        for value in str(ctx.effective_config.get("roots") or "").split(",")
        if value.strip()
    }

    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        body = blob_bytes(blob)
        try:
            envelope = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            envelope = None
        records = []
        if isinstance(envelope, dict) and isinstance(envelope.get("sitemaps"), list):
            entries = envelope["sitemaps"]
            for entry in entries:
                shard = str(entry.get("shard", ""))
                if allowed and shard not in allowed:
                    continue
                url = str(entry.get("url", ""))
                if not url:
                    continue
                lastmod = str(entry.get("lastmod", ""))
                if shard == "sitemap-blog.xml":
                    key = blog_slug(url)
                    target_store = "post"
                    upstream = {"lastmod": lastmod}
                    payload = {
                        "url": url, "lastmod": lastmod, "kind": "blog",
                        "id_scope": f"hf:blog/{key}",
                    }
                else:
                    key = root_key(url)
                    if wanted_roots and key not in wanted_roots:
                        continue
                    target_store = "root"
                    llms_hash = entry.get("llms_hash")
                    upstream = {"llms_hash": llms_hash}
                    payload = {
                        "url": url, "lastmod": lastmod, "kind": kind_of(key),
                        "llms_hash": llms_hash,
                        "pages": entry.get("pages", 0),
                        "version": entry.get("version", ""),
                        "license": entry.get("license", ""),
                        "id_scope": f"hf:{key}/",
                    }
                if key:
                    records.append(PartitionRecord(
                        store=target_store or store,
                        key=key,
                        upstream=upstream,
                        payload=payload,
                    ))
            return records

        try:
            root = ET.fromstring(body)
        except ET.ParseError as exc:
            raise RuntimeError(f"invalid sitemap XML: {exc}") from exc
        namespace = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
        for element in root.iter(f"{namespace}url"):
            url = (element.findtext(f"{namespace}loc") or "").strip()
            if not url:
                continue
            lastmod = (element.findtext(f"{namespace}lastmod") or "").strip()
            records.append(PartitionRecord(
                store=store, key=url,
                upstream={"lastmod": lastmod},
                payload={"url": url, "lastmod": lastmod},
            ))
        return records

    return _run(ctx, parse)


def list_apache_index(ctx: TaskContext) -> SliceResult:
    patterns = _patterns(ctx.config.get("name_pattern", []))
    marker = str(ctx.config.get("require_marker", ""))
    newest = bool(ctx.config.get("newest_only", True))

    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        text = blob_bytes(blob).decode("utf-8", errors="replace")
        # The root Cirrus listing points at dated directories. A fetch runner may
        # enrich it with the selected directory's listing in one body.
        try:
            envelope = json.loads(text)
        except json.JSONDecodeError:
            envelope = None
        if isinstance(envelope, dict) and "listing" in envelope:
            text = str(envelope["listing"])
            dump_date = str(envelope.get("date", ""))
        else:
            dump_date = ""
        if marker and not re.search(
                rf'href=["\'][^"\']*{re.escape(marker)}["\']', text):
            return []
        names = []
        for match in re.finditer(
                r'href=["\']([^"\']+\.(?:bz2|gz))["\'][^>]*>', text, re.I):
            key = unescape(match.group(1)).rsplit("/", 1)[-1]
            if patterns and not any(pattern.search(key) for pattern in patterns):
                continue
            size_match = re.search(
                rf'href=["\']{re.escape(match.group(1))}["\'][^>]*>.*?</a>'
                r"\s+\S+\s+\S+\s+(\d+)",
                text[match.start():match.start() + 500],
                re.I | re.S,
            )
            names.append((key, int(size_match.group(1)) if size_match else None))
        if newest and dump_date:
            names = [item for item in names if dump_date in item[0]]
        return [
            PartitionRecord(
                store=store,
                key=key,
                upstream={"dump_date": dump_date, "bytes": size},
                payload={"dump_date": dump_date, "bytes": size},
            )
            for key, size in sorted(set(names))
        ]

    return _run(ctx, parse)


def list_llms_txt(ctx: TaskContext) -> SliceResult:
    link = re.compile(r"^\s*-\s*\[([^\]]*)\]\((https?://\S+?)\)\s*$", re.M)

    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        text = blob_bytes(blob).decode("utf-8", errors="replace")
        records = []
        for title, url in link.findall(text):
            key = urlsplit(url).path.strip("/")
            if key.endswith(".md"):
                key = key[:-3]
            records.append(PartitionRecord(
                store=store,
                key=key,
                upstream={"url": url},
                payload={"url": url, "title": title.strip()},
            ))
        return records

    return _run(ctx, parse)


def github_watch_events(ctx: TaskContext) -> SliceResult:
    event_type = str(ctx.config.get("event_type", "WatchEvent"))

    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        data = blob_bytes(blob)
        if not data:
            return []
        try:
            text = gzip.decompress(data).decode("utf-8", errors="replace")
        except gzip.BadGzipFile as exc:
            raise RuntimeError(f"invalid GH Archive gzip: {exc}") from exc
        counts: dict[int, tuple[str, int]] = {}
        for line in text.splitlines():
            if event_type not in line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") != event_type:
                continue
            repo = event.get("repo") or {}
            repo_id, name = repo.get("id"), repo.get("name")
            if repo_id is None or not name:
                continue
            old = counts.get(int(repo_id))
            counts[int(repo_id)] = (str(name), (old[1] if old else 0) + 1)
        return [
            PartitionRecord(
                store=store,
                key=str(repo_id),
                stage="candidate",
                payload={"repo_id": repo_id, "full_name": name},
                delta={"star_events": count},
            )
            for repo_id, (name, count) in counts.items()
        ]

    return _run(ctx, parse)


def github_search_items(ctx: TaskContext) -> SliceResult:
    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        try:
            body = json.loads(blob_bytes(blob))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"invalid GitHub search JSON: {exc}") from exc
        records = []
        for item in body.get("items") or []:
            if item.get("id") is None or not item.get("full_name"):
                continue
            records.append(PartitionRecord(
                store=store,
                key=str(item["id"]),
                stage="candidate",
                payload={
                    "repo_id": int(item["id"]),
                    "full_name": item["full_name"],
                    "stars": item.get("stargazers_count"),
                    "description": item.get("description"),
                    "primary_language": item.get("language"),
                    "pushed_at": item.get("pushed_at"),
                },
            ))
        return records

    return _run(ctx, parse)


def github_hydrated_repos(ctx: TaskContext) -> SliceResult:
    threshold = int(ctx.config.get("stars_gte", 10))

    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        try:
            body = json.loads(blob_bytes(blob))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"invalid GitHub GraphQL JSON: {exc}") from exc
        candidate = body.get("candidate") or {}
        node = body.get("repo")
        repo_id = candidate.get("repo_id")
        if repo_id is None:
            return []
        if not node or not node.get("databaseId"):
            return [PartitionRecord(
                store=store, key=str(repo_id), stage="gone",
                payload={"repo_id": int(repo_id),
                         "full_name": candidate.get("full_name", "")},
            )]
        stars = int(node.get("stargazerCount") or 0)
        status = (
            "hydrated"
            if stars >= threshold and not node.get("isArchived")
            else "below_threshold"
        )
        topics = [
            value["topic"]["name"]
            for value in ((node.get("repositoryTopics") or {}).get("nodes") or [])
            if (value.get("topic") or {}).get("name")
        ]
        readme = None
        for alias in ("readme_md", "readme_lower", "readme_rst", "readme_plain"):
            if (node.get(alias) or {}).get("text"):
                readme = node[alias]["text"]
                break
        return [PartitionRecord(
            store=store,
            key=str(node["databaseId"]),
            stage=status,
            payload={
                "repo_id": int(node["databaseId"]),
                "full_name": node.get("nameWithOwner"),
                "stars": stars,
                "description": node.get("description"),
                "topics": topics,
                "primary_language": (node.get("primaryLanguage") or {}).get("name"),
                "default_branch": (node.get("defaultBranchRef") or {}).get("name"),
                "pushed_at": node.get("pushedAt"),
                "readme": readme,
            },
        )]

    return _run(ctx, parse)


def feed_entries(ctx: TaskContext) -> SliceResult:
    limit = int(ctx.config.get("max_items", 20))

    def parse(blob: RawBlob, store: str) -> list[PartitionRecord]:
        parsed = feedparser.parse(blob_bytes(blob))
        records = []
        for entry in list(parsed.entries or [])[:limit]:
            url = str(entry.get("link") or "").strip()
            if not url:
                continue
            records.append(PartitionRecord(
                store=store,
                key=canonicalize(url),
                upstream={"published": entry.get("published") or ""},
                payload={"url": url, "title": entry.get("title") or "",
                         "entry": dict(entry)},
            ))
        return records

    return _run(ctx, parse)


def crawl_links(ctx: TaskContext) -> SliceResult:
    into = str(ctx.config.get("into", ""))
    if not into:
        raise PermanentTaskError("crawl.links requires into")
    max_depth = int(ctx.config.get("max_depth", 2))
    include = _patterns(ctx.config.get("include", []))
    exclude = _patterns(ctx.config.get("exclude", []))

    def parse(blob: RawBlob, _store: str) -> list[PartitionRecord]:
        payload = blob.meta.get("payload") or {}
        depth = int(payload.get("depth", 0))
        if depth >= max_depth or not blob.body:
            return []
        seed = str(payload.get("seed") or blob.uri)
        prefix = ctx.config.get("path_prefix")
        if prefix is None:
            path = urlsplit(seed).path
            prefix = path if path.endswith("/") else path.rsplit("/", 1)[0] + "/"
        scope = type("CrawlPolicy", (), {
            "scope": type("Scope", (), {
                "same_host": bool(ctx.config.get("same_host", True)),
                "path_prefix": str(prefix),
                "include_re": tuple(include),
                "exclude_re": tuple(exclude),
            })()
        })()
        records = []
        for raw in extract_links(
                blob.body.decode("utf-8", errors="replace"), blob.uri):
            url = canonicalize(raw)
            allowed, _ = in_scope(url, scope, seed)
            if allowed:
                records.append(PartitionRecord(
                    store=into,
                    key=url,
                    upstream={"url": url},
                    payload={"url": url, "seed": seed, "depth": depth + 1},
                ))
        return records

    return _run(ctx, parse)
