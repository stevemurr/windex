"""Extraction modules: RawBlob -> ExtractedDoc."""

from __future__ import annotations

import hashlib
import json
import shutil
from types import SimpleNamespace
from urllib.parse import urlsplit

import feedparser
import pyarrow.parquet as pq

from windex.config import Settings
from windex.dateparse import parse_and_clamp
from windex.modules.common import (
    blob_bytes,
    finish_batch,
    pending_batches,
    require_type,
)
from windex.recipe.ports import ExtractedDoc, PartitionRef, RawBlob
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_INPUT_BATCH = 8


def _run(ctx: TaskContext, parse, *, limit: int = _INPUT_BATCH) -> SliceResult:
    batches, more = pending_batches(ctx, limit=limit)
    processed = []
    documents = 0
    for batch in batches:
        outputs = []
        for value in batch.values:
            blob = require_type(value, RawBlob, ctx.module)
            if blob.meta.get("missing") or blob.meta.get("not_modified"):
                parsed = []
            else:
                parsed = parse(blob)
            outputs.extend(parsed or [ExtractedDoc(
                ref=blob.ref,
                suffix="",
                url=blob.uri,
                text="",
                fields={
                    "_coverage_only": True,
                    "_coverage_truncated": bool(
                        blob.meta.get("_coverage_truncated")),
                    "_source_attrs": {
                        key: blob.meta.get(key)
                        for key in ("etag", "last_modified")
                        if blob.meta.get(key) is not None
                    },
                },
                epoch=blob.epoch,
            )])
        finish_batch(ctx, batch, outputs=outputs)
        documents += len(outputs)
        processed.append(batch)
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {
            "last": processed[-1].key, "documents": documents,
        })
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(batches),
        stats={"inputs": done, "documents": documents},
    )


def _published(value):
    return parse_and_clamp(value) if value else None


def html_trafilatura(ctx: TaskContext) -> SliceResult:
    from windex.crawl.extract import declared_canonical, extract_page
    from windex.crawl.run import doc_suffix
    from windex.crawl.scope import canonicalize, same_host

    minimum = int(ctx.config.get("min_chars", 200))
    recipe = SimpleNamespace(extract=SimpleNamespace(
        min_chars=minimum,
        quality_filters=bool(ctx.config.get("quality_filters", False)),
    ))
    honor = str(ctx.config.get("honor_canonical", "in_scope"))

    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        if not blob.body:
            return []
        html = blob.body.decode("utf-8", errors="replace")
        result = extract_page(html, blob.uri, recipe)
        if result is None:
            return []
        canonical = canonicalize(blob.uri)
        declared = declared_canonical(html)
        if declared and honor != "never":
            candidate = canonicalize(str(
                __import__("httpx").URL(blob.uri).join(declared)))
            if honor == "always" or same_host(candidate, blob.uri):
                canonical = candidate
        payload = blob.meta.get("payload") or {}
        seed = str(payload.get("seed") or blob.uri)
        if ctx.source == "hf" and blob.ref.store == "post":
            suffix = f"blog/{blob.ref.key}"
        elif ctx.source == "smallweb":
            suffix = hashlib.sha1(canonical.encode()).hexdigest()[:20]
        else:
            suffix = doc_suffix(canonical, seed)
        outlet = (urlsplit(canonical).hostname or "").lower()
        fields = {"outlet": outlet}
        payload_out = {"outlet": outlet}
        if ctx.source == "hf" and blob.ref.store == "post":
            fields.update({"kind": "blog", "root": "blog", "version": "",
                           "license": ""})
            payload_out.update({"kind": "blog", "root": "blog"})
        return [ExtractedDoc(
            ref=blob.ref,
            suffix=suffix,
            url=blob.uri,
            canonical_url=canonical,
            title=result.get("title") or "",
            text=result["text"],
            published_at=_published(result.get("published_at")),
            fields=fields,
            payload=payload_out,
            epoch=blob.epoch,
        )]

    return _run(ctx, parse)


def html_devdocs_page(ctx: TaskContext) -> SliceResult:
    from windex.docs_source.canonical import canonical_url
    from windex.docs_source.ingest import (
        _ATTR_DIV,
        framework_of,
        html_to_text,
        page_title,
        strip_html,
        upstream_url,
    )

    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        try:
            pages = json.loads(blob_bytes(blob))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"invalid DevDocs db.json: {exc}") from exc
        if not isinstance(pages, dict):
            raise PermanentTaskError("DevDocs db.json must be an object")
        slug = blob.ref.key
        meta = blob.meta.get("payload") or {}
        framework = framework_of(slug)
        version = str(meta.get("release") or "")
        attribution = strip_html(str(meta.get("attribution") or ""))
        ref = PartitionRef(
            store=blob.ref.store,
            key=slug,
            id_scope=blob.ref.id_scope or f"docs:{slug}/",
        )
        outputs = []
        for path, raw_html in sorted(pages.items()):
            html = str(raw_html)
            upstream = upstream_url(html)
            body = html.split(_ATTR_DIV, 1)[0]
            text = html_to_text(body)
            outputs.append(ExtractedDoc(
                ref=ref,
                suffix=f"{slug}/{path}",
                url=canonical_url(slug, path, upstream),
                canonical_url=canonical_url(slug, path, upstream),
                title=page_title(body),
                text=text,
                fields={
                    "framework": framework,
                    "version": version,
                    "attribution": attribution,
                },
                payload={
                    "framework": framework,
                    "version": version,
                    "attribution": attribution,
                },
                epoch=blob.epoch,
            ))
        return outputs

    return _run(ctx, parse, limit=1)


def markdown_passthrough(ctx: TaskContext) -> SliceResult:
    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        payload = blob.meta.get("payload") or {}
        root = str(payload.get("root") or blob.ref.key).strip("/")
        path = str(payload.get("path") or "").strip("/")
        if not path:
            return []
        text = blob_bytes(blob).decode("utf-8", errors="replace").strip()
        if not text:
            return []
        return [ExtractedDoc(
            ref=PartitionRef(
                store=blob.ref.store,
                key=blob.ref.key,
                id_scope=blob.ref.id_scope or f"hf:{root}/",
            ),
            suffix=f"{root}/{path}",
            url=blob.uri,
            canonical_url=f"https://huggingface.co/{root}/{path}",
            title=str(payload.get("title") or ""),
            text=text,
            fields={
                "root": root,
                "kind": payload.get("kind"),
                "version": payload.get("version"),
                "license": payload.get("license"),
            },
            payload={
                "root": root,
                "kind": payload.get("kind"),
                "version": payload.get("version"),
            },
            epoch=blob.epoch,
        )]

    return _run(ctx, parse)


def feed_inline_docs(ctx: TaskContext) -> SliceResult:
    from windex.smallweb.extract import extract_post
    from windex.smallweb.poll import (
        entry_link,
        entry_published,
        entry_title,
        item_body,
        newest_entries,
    )

    max_items = int(ctx.params.get("max_items", 20))
    min_chars = int(ctx.params.get("min_chars", 200))

    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        raw = blob_bytes(blob)
        try:
            envelope = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            envelope = None
        if isinstance(envelope, dict) and isinstance(envelope.get("items"), list):
            raw_items = envelope["items"]
        else:
            parsed = feedparser.parse(raw)
            outlet = (urlsplit(blob.uri).hostname or "").lower()
            raw_items = []
            for entry in newest_entries(parsed, max_items):
                url = entry_link(entry)
                if not url:
                    continue
                body, inline = item_body(entry, min_chars)
                if inline and body is not None:
                    raw_items.append({
                        "url": url, "body": body, "inline": True,
                        "title": entry_title(entry),
                        "published": entry_published(entry),
                        "outlet": outlet,
                    })
        outputs = []
        for item in raw_items[:max_items]:
            url = str(item.get("url") or "")
            outlet = str(
                item.get("outlet")
                or (urlsplit(blob.uri).hostname or "").lower())
            body = item.get("body")
            if not url or body is None:
                continue
            extracted = extract_post(
                str(body),
                url,
                feed_title=item.get("title"),
                feed_published=item.get("published"),
                filters=[],
                wrap=bool(item.get("inline")),
            )
            if extracted is None or len(extracted["text"]) < min_chars:
                continue
            canonical = __import__(
                "windex.ccnews.dedup", fromlist=["canonical_url"]
            ).canonical_url(url)
            outputs.append(ExtractedDoc(
                ref=blob.ref,
                suffix=hashlib.sha1(canonical.encode()).hexdigest()[:20],
                url=url,
                canonical_url=canonical,
                title=extracted["title"],
                text=extracted["text"],
                published_at=_published(extracted.get("date")),
                lang=extracted.get("lang"),
                fields={
                    "outlet": outlet,
                    "_source_attrs": {
                        key: blob.meta.get(key)
                        for key in ("etag", "last_modified")
                        if blob.meta.get(key) is not None
                    },
                },
                payload={"outlet": outlet},
                epoch=blob.epoch,
            ))
        return outputs

    return _run(ctx, parse)


def oai_arxiv_records(ctx: TaskContext) -> SliceResult:
    from windex.arxiv.harvest import abs_url, parse_records

    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        records, _ = parse_records(blob_bytes(blob))
        outputs = []
        for record in records:
            deleted = bool(record.get("deleted"))
            paper_id = record["id"]
            outputs.append(ExtractedDoc(
                ref=blob.ref,
                suffix=paper_id,
                url=abs_url(paper_id),
                title="" if deleted else record.get("title") or "",
                text="" if deleted else record.get("abstract") or "",
                published_at=(
                    None if deleted else _published(record.get("created"))),
                fields={} if deleted else {
                    "abstract": record.get("abstract") or "",
                    "authors": record.get("authors") or [],
                    "primary_category": record.get("primary_category") or "",
                    "categories": record.get("categories") or [],
                    "created": record.get("created"),
                    "updated": record.get("updated"),
                    "doi": record.get("doi"),
                },
                payload={} if deleted else {
                    "authors": record.get("authors") or [],
                    "primary_category": record.get("primary_category") or "",
                    "categories": record.get("categories") or [],
                    "doi": record.get("doi"),
                },
                deleted=deleted,
                epoch=blob.epoch,
            ))
        return outputs

    return _run(ctx, parse, limit=1)


def algolia_hn_stories(ctx: TaskContext) -> SliceResult:
    from windex.hn.harvest import story_from_hit

    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        try:
            body = json.loads(blob_bytes(blob))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"invalid Algolia JSON: {exc}") from exc
        outputs = []
        for hit in body.get("hits") or []:
            story = story_from_hit(hit)
            outputs.append(ExtractedDoc(
                ref=blob.ref,
                suffix=story["id"].removeprefix("hn:"),
                url=story["url"],
                title=story["title"],
                text=story["story_text"],
                published_at=_published(story["created_at"]),
                fields={
                    "target_url": story["target_url"],
                    "story_text": story["story_text"],
                    "author": story["author"],
                    "points": story["points"],
                    "num_comments": story["num_comments"],
                    "created_at": story["created_at"],
                },
                payload={
                    "target_url": story["target_url"],
                    "author": story["author"],
                    "points": story["points"],
                    "num_comments": story["num_comments"],
                },
                epoch=blob.epoch,
            ))
        return outputs

    return _run(ctx, parse, limit=1)


def parquet_rows(ctx: TaskContext) -> SliceResult:
    from windex.hn.backfill import stories_from_table

    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        if blob.path is None:
            raise PermanentTaskError("parquet.rows requires a spooled RawBlob")
        table = pq.read_table(blob.path)
        payload = blob.meta.get("payload") or {}
        stories = stories_from_table(
            table, int(payload["from_ts"]), int(payload["until_ts"]))
        return [
            ExtractedDoc(
                ref=blob.ref,
                suffix=story["id"].removeprefix("hn:"),
                url=story["url"],
                title=story["title"],
                text=story["story_text"],
                published_at=_published(story["created_at"]),
                fields={
                    "target_url": story["target_url"],
                    "story_text": story["story_text"],
                    "author": story["author"],
                    "points": story["points"],
                    "num_comments": story["num_comments"],
                    "created_at": story["created_at"],
                },
                payload={
                    "target_url": story["target_url"],
                    "author": story["author"],
                    "points": story["points"],
                    "num_comments": story["num_comments"],
                },
                epoch=blob.epoch,
            )
            for story in stories
        ]

    return _run(ctx, parse, limit=1)


def cirrus_articles(ctx: TaskContext) -> SliceResult:
    from windex.wiki.reader import iter_articles_from_bytes

    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        data = blob_bytes(blob)
        return [
            ExtractedDoc(
                ref=PartitionRef(
                    store=blob.ref.store,
                    key=blob.ref.key,
                    id_scope=blob.ref.id_scope or "wiki:",
                ),
                suffix=article["id"].removeprefix("wiki:"),
                url=article["url"],
                title=article["title"],
                text=article["text"],
                published_at=_published(article.get("revision_ts")),
                fields={
                    "revision_ts": article.get("revision_ts"),
                    "incoming_links": article.get("incoming_links", 0),
                    "opening_text": article.get("opening_text", ""),
                },
                payload={
                    "incoming_links": article.get("incoming_links", 0),
                    "opening_text": article.get("opening_text", ""),
                },
                epoch=blob.epoch,
            )
            for article in iter_articles_from_bytes([data])
        ]

    return _run(ctx, parse, limit=1)


def warc_datatrove(ctx: TaskContext) -> SliceResult:
    from windex.ccnews.pipeline import process_batch

    language = str(ctx.config.get("language", "en"))
    workers = int(ctx.config.get("workers", 4))

    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        if blob.path is None:
            raise PermanentTaskError("warc.datatrove requires a spooled RawBlob")
        digest = hashlib.sha256(
            f"{ctx.run_id}:{ctx.task_id}:{blob.ref.key}".encode()
        ).hexdigest()[:24]
        base = Settings().staging_dir / "_recipe_extract" / str(ctx.run_id) / digest
        output = base / "parquet"
        logs = base / "logs"
        shutil.rmtree(base, ignore_errors=True)
        process_batch(
            blob.path.parent,
            [blob.path.name],
            output,
            logs,
            language,
            workers=workers,
        )
        documents = []
        for path in sorted(output.rglob("*.parquet")):
            for row in pq.read_table(path).to_pylist():
                metadata = row.get("metadata") or {}
                url = str(row.get("url") or metadata.get("url") or "")
                text = str(row.get("text") or "")
                if not url or not text:
                    continue
                canonical = __import__(
                    "windex.ccnews.dedup", fromlist=["canonical_url"]
                ).canonical_url(url)
                documents.append(ExtractedDoc(
                    ref=blob.ref,
                    suffix=hashlib.sha1(canonical.encode()).hexdigest()[:20],
                    url=url,
                    canonical_url=canonical,
                    title=str(row.get("title") or metadata.get("title") or ""),
                    text=text,
                    published_at=_published(
                        row.get("date") or metadata.get("date")),
                    lang=str(row.get("language") or metadata.get("language")
                             or language),
                    fields=dict(metadata),
                    epoch=blob.epoch,
                ))
        return documents

    return _run(ctx, parse, limit=1)


def github_compose_doc(ctx: TaskContext) -> SliceResult:
    from windex.github.clean import clean_readme, compose_doc

    max_chars = 100_000

    def parse(blob: RawBlob) -> list[ExtractedDoc]:
        try:
            repo = json.loads(blob_bytes(blob))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"invalid repo sidecar JSON: {exc}") from exc
        full_name = str(repo.get("full_name") or "")
        if not full_name:
            return []
        readme = clean_readme(str(repo.get("readme") or ""))
        text = compose_doc(
            full_name,
            repo.get("description"),
            repo.get("topics") or [],
            readme or None,
            max_chars,
        )
        return [ExtractedDoc(
            ref=blob.ref,
            suffix=full_name,
            url=f"https://github.com/{full_name}",
            title=full_name,
            text=text,
            published_at=_published(repo.get("pushed_at")),
            fields={
                "repo_id": repo.get("repo_id"),
                "full_name": full_name,
            },
            payload={
                "stars": repo.get("stars"),
                "language": repo.get("primary_language"),
                "topics": repo.get("topics") or [],
                "description": repo.get("description"),
                "pushed_at": repo.get("pushed_at"),
            },
            epoch=blob.epoch,
        )]

    return _run(ctx, parse)
