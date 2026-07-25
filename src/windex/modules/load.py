"""Terminal document staging: ExtractedDoc -> parquet + documents ledger."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg.types.json import Jsonb

from windex.ccnews.dedup import text_hash
from windex.config import Settings
from windex.modules.common import finish_batch, pending_batches, require_type
from windex.recipe.ports import ExtractedDoc
from windex.sanitize import strip_smuggled
from windex.textguard import is_empty_text
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_BATCH = 1_000
log = logging.getLogger("windex.recipe.load")


def _prefix(ctx: TaskContext) -> str:
    return str((ctx.spec.get("corpus") or {}).get("id_prefix") or f"{ctx.source}:")


def _sanitize(doc: ExtractedDoc) -> ExtractedDoc:
    return replace(
        doc,
        title=strip_smuggled(doc.title),
        text=strip_smuggled(doc.text),
    )


def _doc_id(ctx: TaskContext, doc: ExtractedDoc) -> str:
    return _prefix(ctx) + doc.suffix


def _iso(value):
    return value.isoformat() if value is not None else None


def _parquet_row(ctx: TaskContext, doc: ExtractedDoc) -> dict:
    doc_id = _doc_id(ctx, doc)
    fields = doc.fields
    source = ctx.source
    base = {
        "id": doc_id,
        "url": doc.url,
        "title": doc.title,
        "text": doc.text,
    }
    if source == "news":
        return {
            **base,
            "canonical_url": doc.canonical_url,
            "published_at": _iso(doc.published_at),
            "lang": doc.lang,
        }
    if source == "wiki":
        return {
            **base,
            "revision_ts": fields.get("revision_ts") or _iso(doc.published_at),
            "incoming_links": int(fields.get("incoming_links") or 0),
            "opening_text": fields.get("opening_text") or "",
        }
    if source == "hn":
        return {
            "id": doc_id,
            "url": doc.url,
            "target_url": fields.get("target_url"),
            "title": doc.title,
            "story_text": fields.get("story_text", doc.text),
            "author": fields.get("author") or "",
            "points": int(fields.get("points") or 0),
            "num_comments": int(fields.get("num_comments") or 0),
            "created_at": fields.get("created_at") or _iso(doc.published_at),
        }
    if source == "arxiv":
        return {
            "id": doc_id,
            "url": doc.url,
            "title": doc.title,
            "abstract": fields.get("abstract", doc.text),
            "authors": fields.get("authors") or [],
            "primary_category": fields.get("primary_category") or "",
            "categories": fields.get("categories") or [],
            "created": fields.get("created") or _iso(doc.published_at),
            "updated": fields.get("updated"),
            "doi": fields.get("doi"),
        }
    if source == "smallweb":
        return {
            **base,
            "published_at": _iso(doc.published_at),
            "outlet": fields.get("outlet") or doc.payload.get("outlet") or "",
        }
    if source == "docs":
        return {
            **base,
            "framework": fields.get("framework") or "",
            "version": fields.get("version") or "",
            "attribution": fields.get("attribution") or "",
        }
    if source == "hf":
        return {
            **base,
            "kind": fields.get("kind") or (
                "blog" if doc.suffix.startswith("blog/") else "docs"),
            "root": fields.get("root") or (
                "blog" if doc.suffix.startswith("blog/") else
                doc.suffix.rsplit("/", 1)[0]
            ),
            "version": fields.get("version") or "",
            "license": fields.get("license") or "",
            "published_at": _iso(doc.published_at),
        }
    if source == "github":
        return {
            **base,
            "full_name": fields.get("full_name") or doc.suffix,
            "repo_id": fields.get("repo_id"),
            "stars": doc.payload.get("stars"),
            "language": doc.payload.get("language"),
            "topics": doc.payload.get("topics") or [],
            "description": doc.payload.get("description"),
            "pushed_at": doc.payload.get("pushed_at") or _iso(doc.published_at),
        }
    if source == "memory":
        return {
            **base,
            "conversation_id": fields.get("conversation_id") or "",
            "chunk_index": int(fields.get("chunk_index") or 0),
            "published_at": doc.published_at,
        }
    return {
        **base,
        "published_at": doc.published_at,
        "extra": json.dumps(doc.payload) if doc.payload else None,
    }


def _text_ref(ctx: TaskContext, key: str) -> tuple[str, Path]:
    digest = hashlib.sha256(
        f"{ctx.run_id}:{ctx.task_id}:{key}".encode()).hexdigest()[:24]
    relative = f"{ctx.source}/recipe/{ctx.run_id}/{digest}.parquet"
    return relative, Settings().staging_dir / relative


def _write_parquet(ctx: TaskContext, key: str,
                   docs: list[ExtractedDoc]) -> str | None:
    if not docs:
        return None
    relative, path = _text_ref(ctx, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".parquet.tmp")
    table = pa.Table.from_pylist([_parquet_row(ctx, doc) for doc in docs])
    pq.write_table(table, temp)
    os.replace(temp, path)
    return relative


def _existing(
        ctx: TaskContext, ids: list[str],
) -> dict[str, tuple[str | None, str, str | None]]:
    if not ids:
        return {}
    with ctx.conn.cursor() as cur:
        cur.execute(
            "SELECT id, text_hash, status, embedded_model "
            "FROM documents WHERE id = ANY(%s)",
            (ids,),
        )
        return {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}


def _ledger_rows(ctx: TaskContext, docs: list[ExtractedDoc],
                 text_ref: str | None) -> list[tuple]:
    rows = []
    for doc in docs:
        doc_id = _doc_id(ctx, doc)
        digest = doc.fields.get(
            "_text_hash", text_hash(doc.title + "\n\n" + doc.text))
        duplicate = doc.fields.get("_duplicate_of")
        if doc.deleted:
            status, ref = "deleted", None
        elif duplicate:
            status, ref = "duplicate", None
        elif is_empty_text(doc.title + "\n\n" + doc.text):
            status, ref = "empty", None
        else:
            status, ref = "deduped", text_ref
        rows.append((
            doc_id, ctx.source, doc.url, doc.canonical_url, doc.title,
            doc.published_at, doc.lang, digest, status, duplicate, ref,
        ))
    return sorted(rows)


def _upsert_ledger(ctx: TaskContext, rows: list[tuple]) -> None:
    if not rows:
        return
    with ctx.conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO documents
                   (id, source, url, canonical_url, title, published_at, lang,
                    text_hash, status, duplicate_of, text_ref)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                source = EXCLUDED.source,
                url = EXCLUDED.url,
                canonical_url = EXCLUDED.canonical_url,
                title = EXCLUDED.title,
                published_at = EXCLUDED.published_at,
                lang = EXCLUDED.lang,
                text_hash = EXCLUDED.text_hash,
                status = EXCLUDED.status,
                duplicate_of = EXCLUDED.duplicate_of,
                text_ref = EXCLUDED.text_ref,
                embedded_model = CASE
                    WHEN documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                      OR documents.status = 'deleted'
                    THEN NULL ELSE documents.embedded_model END,
                indexed_at = CASE
                    WHEN documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                      OR documents.status = 'deleted'
                    THEN NULL ELSE documents.indexed_at END
            WHERE documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
               OR documents.status IS DISTINCT FROM EXCLUDED.status
               OR documents.url IS DISTINCT FROM EXCLUDED.url
               OR documents.title IS DISTINCT FROM EXCLUDED.title
            """,
            rows,
        )


def _coverage_path(ctx: TaskContext, key: str) -> tuple[str, Path]:
    digest = hashlib.sha256(
        f"coverage:{ctx.run_id}:{ctx.task_id}:{key}".encode()
    ).hexdigest()
    relative = f"_recipe_runs/coverage/{ctx.run_id}/{ctx.task_id}/{digest}.txt.gz"
    return relative, Settings().staging_dir / relative


def _write_coverage(ctx: TaskContext, key: str, ids: set[str]) -> str:
    relative, path = _coverage_path(ctx, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    with gzip.open(temp, "wt", encoding="utf-8") as stream:
        for doc_id in sorted(ids):
            stream.write(doc_id + "\n")
    os.replace(temp, path)
    return relative


def _all_coverage(ctx: TaskContext) -> set[str]:
    with ctx.conn.cursor() as cur:
        cur.execute(
            "SELECT counts->>'coverage_path' FROM task_units "
            "WHERE task_id = %s AND counts ? 'coverage_path'",
            (ctx.task_id,),
        )
        relatives = [row[0] for row in cur.fetchall()]
    root = Settings().staging_dir.resolve()
    ids = set()
    for relative in relatives:
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise PermanentTaskError("coverage artifact escaped staging")
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            ids.update(line.rstrip("\n") for line in stream if line.rstrip("\n"))
    return ids


def _tombstone_missing(ctx: TaskContext, *, scope: str,
                       current: set[str],
                       guard: str) -> tuple[list[str], list[str]]:
    if guard == "census" and not current:
        raise RuntimeError(
            f"{ctx.module} refuses an empty {scope} census")
    with ctx.conn.cursor() as cur:
        if scope == "source":
            cur.execute(
                """
                SELECT id
                  FROM documents
                 WHERE source = %s AND status <> 'deleted'
                   AND NOT (id = ANY(%s))
                   AND embedded_model IS NOT NULL
                 ORDER BY id
                 FOR UPDATE
                """,
                (ctx.source, sorted(current)),
            )
            vector_ids = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                UPDATE documents
                   SET status = 'deleted', embedded_model = NULL, indexed_at = NULL
                 WHERE source = %s AND status <> 'deleted'
                   AND NOT (id = ANY(%s))
                RETURNING id
                """,
                (ctx.source, sorted(current)),
            )
        else:
            cur.execute(
                """
                SELECT id
                  FROM documents
                 WHERE starts_with(id, %s) AND status <> 'deleted'
                   AND NOT (id = ANY(%s))
                   AND embedded_model IS NOT NULL
                 ORDER BY id
                 FOR UPDATE
                """,
                (scope, sorted(current)),
            )
            vector_ids = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                UPDATE documents
                   SET status = 'deleted', embedded_model = NULL, indexed_at = NULL
                 WHERE starts_with(id, %s) AND status <> 'deleted'
                   AND NOT (id = ANY(%s))
                RETURNING id
                """,
                (scope, sorted(current)),
            )
        return [row[0] for row in cur.fetchall()], vector_ids


def _delete_vectors(ctx: TaskContext, doc_ids: set[str]) -> None:
    """Best-effort Qdrant half of a committed ledger tombstone."""
    if not doc_ids:
        return
    try:
        from qdrant_client import QdrantClient
        from qdrant_client import models as qm

        from windex.embed.pipeline import point_id
        from windex.index import qdrant as qidx

        collection = str(
            (ctx.spec.get("corpus") or {}).get("collection") or ctx.source)
        client = QdrantClient(url=Settings().qdrant_url, timeout=30)
        try:
            client.delete(
                collection_name=qidx.alias_name(collection),
                points_selector=qm.PointIdsList(
                    points=[point_id(doc_id) for doc_id in sorted(doc_ids)]),
                wait=True,
            )
        finally:
            client.close()
    except Exception as exc:  # absent/unreachable index: ledger remains authoritative
        log.warning("recipe tombstone: qdrant delete skipped (%s)", exc)


def _advance_refs(ctx: TaskContext, docs: list[ExtractedDoc]) -> None:
    source = ctx.recipe or ctx.source
    refs = {(doc.ref.store, doc.ref.key) for doc in docs}
    attrs: dict[tuple[str, str], dict] = defaultdict(dict)
    for doc in docs:
        attrs[(doc.ref.store, doc.ref.key)].update(
            doc.fields.get("_source_attrs") or {})
    with ctx.conn.cursor() as cur:
        for store, key in refs:
            if not store:
                continue
            cur.execute(
                """
                UPDATE source_units
                   SET ingested = upstream, status = 'done',
                       attrs = attrs || %s,
                       processed_at = now(), claimed_at = NULL,
                       last_run_id = %s, updated_at = now()
                 WHERE source = %s AND store = %s AND unit_key = %s
                """,
                (Jsonb(attrs[(store, key)]), ctx.run_id, source, store, key),
            )


def ledger_stage(ctx: TaskContext) -> SliceResult:
    replace_enabled = bool(ctx.config.get("replace", False))
    replace_scope = str(ctx.config.get("replace_scope", "partition"))
    guard = str(ctx.config.get("replace_guard", "census"))
    if replace_scope not in {"partition", "source"}:
        raise PermanentTaskError(
            f"ledger.stage has unknown replace_scope {replace_scope!r}")
    if guard not in {"none", "census"}:
        raise PermanentTaskError(
            f"ledger.stage has unknown replace_guard {guard!r}")

    batches, more = pending_batches(ctx, limit=_BATCH)
    processed = []
    total = changed = skipped = 0
    tombstoned: set[str] = set()
    vector_tombstones: set[str] = set()
    for batch in batches:
        received = [
            _sanitize(require_type(value, ExtractedDoc, ctx.module))
            for value in batch.values
        ]
        coverage_docs = [
            doc for doc in received if doc.fields.get("_coverage_only")]
        docs = [
            doc for doc in received if not doc.fields.get("_coverage_only")]
        tombstoned.update(_doc_id(ctx, doc) for doc in docs if doc.deleted)
        total += len(docs)
        ids = [_doc_id(ctx, doc) for doc in docs]
        existing = _existing(ctx, ids)
        for doc, doc_id in zip(docs, ids):
            prior = existing.get(doc_id)
            if doc.deleted and prior and prior[2] is not None:
                vector_tombstones.add(doc_id)
        stage_docs = []
        ledger_docs = []
        for doc, doc_id in zip(docs, ids):
            digest = doc.fields.get(
                "_text_hash", text_hash(doc.title + "\n\n" + doc.text))
            prior = existing.get(doc_id)
            if (not doc.deleted and not doc.fields.get("_duplicate_of")
                    and not is_empty_text(doc.title + "\n\n" + doc.text)
                    and (prior is None or prior[0] != digest
                         or prior[1] == "deleted")):
                stage_docs.append(doc)
                ledger_docs.append(doc)
            elif doc.deleted:
                ledger_docs.append(doc)
            elif doc.fields.get("_duplicate_of") or is_empty_text(
                    doc.title + "\n\n" + doc.text):
                if prior is None or prior[0] != digest or prior[1] not in {
                        "duplicate", "empty"}:
                    ledger_docs.append(doc)
                else:
                    skipped += 1
            else:
                skipped += 1
        text_ref = None
        if ctx.mode != "dry_run":
            text_ref = _write_parquet(ctx, batch.key, stage_docs)
            _upsert_ledger(ctx, _ledger_rows(ctx, ledger_docs, text_ref))
            live_ids = {
                doc_id for doc, doc_id in zip(docs, ids) if not doc.deleted
            }
            coverage = _write_coverage(ctx, batch.key, live_ids)
            if replace_enabled and replace_scope == "partition":
                grouped: dict[str, set[str]] = defaultdict(set)
                for doc in coverage_docs:
                    scope = doc.ref.id_scope
                    if not scope:
                        raise PermanentTaskError(
                            "partition replace requires PartitionRef.id_scope")
                    grouped[scope]
                for doc, doc_id in zip(docs, ids):
                    if doc.deleted:
                        continue
                    scope = doc.ref.id_scope
                    if not scope:
                        raise PermanentTaskError(
                            "partition replace requires PartitionRef.id_scope")
                    grouped[scope].add(doc_id)
                for scope, current in grouped.items():
                    removed, vectors = _tombstone_missing(
                        ctx, scope=scope, current=current,
                        # An explicit full-set memory push is itself a
                        # complete census even when the set is empty
                        # (emptying a chat).
                        guard=(
                            "none"
                            if ctx.source == "memory" and coverage_docs
                            else guard
                        ))
                    tombstoned.update(removed)
                    vector_tombstones.update(vectors)
            _advance_refs(ctx, docs + coverage_docs)
        else:
            coverage = ""
        finish_batch(
            ctx,
            batch,
            counts={
                "coverage_path": coverage,
                "documents": len(docs),
                "staged": len(stage_docs),
            } if coverage else {
                "documents": len(docs), "staged": len(stage_docs),
            },
        )
        processed.append(batch)
        changed += len(stage_docs)
        if ctx.should_yield():
            break
    final = not more and len(processed) == len(batches)
    if (ctx.mode != "dry_run" and replace_enabled
            and replace_scope == "source" and final):
        current = _all_coverage(ctx)
        removed, vectors = _tombstone_missing(
            ctx, scope="source", current=current, guard=guard)
        tombstoned.update(removed)
        vector_tombstones.update(vectors)
    ctx.conn.commit()
    if ctx.mode != "dry_run":
        _delete_vectors(ctx, vector_tombstones)
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {
            "documents": total, "staged": changed,
            "skipped": skipped, "deleted": len(tombstoned),
        })
    return SliceResult(
        units_done=done,
        exhausted=final,
        stats={
            "documents": total, "staged": changed, "skipped": skipped,
            "deleted": len(tombstoned), "dry_run": ctx.mode == "dry_run",
        },
    )
