"""Upsert ingest for custom sources — the semantic difference from `memory`.

`memory` is FULL-REPLACE: each push carries a conversation's whole chunk set and
absent ids are tombstoned. Custom sources are UPSERT + EXPLICIT DELETE: a push
stages only the changed delta and never tombstones an id merely for being absent
(a caller removes docs via ``delete_docs`` or drops the whole source). Everything
else mirrors ``memory_source.ingest`` — a per-batch parquet written tmp+rename so
``text_ref`` never points at a partial file, a ``text_hash``-guarded ledger delta
(the ON CONFLICT WHERE clause both skips an identical row and resurrects a
byte-identical tombstoned doc), and rows written sorted by id (the documents-table
deadlock rule the embed loop's UPDATE also obeys).

Superseded doc versions linger in their old batch files — harmless: each ledger
row's ``text_ref`` points at its newest batch, and the embed reader filters by id.
Batch files are only removed on a full-source delete (compaction is deferred).
"""

from __future__ import annotations

import re
import shutil
import uuid
from datetime import datetime, timezone

import orjson
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from windex.ccnews.dedup import text_hash
from windex.config import Settings
from windex.index import qdrant as qidx

console = Console()

CLEAN_SCHEMA = pa.schema(
    [
        ("id", pa.string()),                             # <name>:<suffix>
        ("url", pa.string()),                            # custom://<name>/<suffix> by default
        ("title", pa.string()),
        ("published_at", pa.timestamp("us", tz="UTC")),  # nullable
        ("text", pa.string()),
        ("extra", pa.string()),                          # orjson blob | null (opaque per-doc metadata)
    ]
)

# Per-batch push limits (route-enforced → 422). MAX_DOCS_PER_BATCH mirrors
# memory's MAX_CHUNKS; the text/extra/body caps bound one push's staging cost.
MAX_DOCS_PER_BATCH = 500
MAX_TEXT_CHARS = 16_000
MAX_EXTRA_BYTES = 2_048        # serialized `extra` per doc
MAX_BODY_CHARS = 4_000_000     # total doc text in one push (~4 MB)
# Doc-id suffix: printable, no whitespace/control, ≤200 — it becomes the second
# half of the namespaced documents id (<name>:<suffix>) and the default url path.
SUFFIX_RE = re.compile(r"^[^\s\x00-\x1f\x7f]{1,200}$")


def doc_id(name: str, suffix: str) -> str:
    return f"{name}:{suffix}"


def default_url(name: str, suffix: str) -> str:
    return f"custom://{name}/{suffix}"


def _as_utc(value: datetime | None) -> datetime | None:
    """Normalize to a tz-aware UTC datetime so the pyarrow timestamp[us, tz=UTC]
    array accepts it (memory's rule: a naive/aware mix, or a naive value against a
    tz-typed array, raises). A naive value is assumed UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _existing_hashes(cur: psycopg.Cursor, ids: list[str]) -> dict[str, str]:
    """id -> text_hash for LIVE ledger rows. Tombstoned rows are excluded so a
    byte-identical re-push resurrects them (the ON CONFLICT WHERE below fires).
    No ``source =`` predicate: ids are namespaced (<name>:…), so an id list can't
    match another source, and the predicate would trigger the whole-source scan
    the memory/docs ingest comment warns about."""
    if not ids:
        return {}
    cur.execute(
        "SELECT id, text_hash FROM documents "
        "WHERE status <> 'deleted' AND id = ANY(%s)",
        (ids,),
    )
    return dict(cur.fetchall())


def apply_tombstones(conn: psycopg.Connection, settings: Settings, source: str,
                     doc_ids: list[str]) -> int:
    """Mark live ledger rows status='deleted' and drop their Qdrant points.
    Generalized from memory's apply_tombstones by taking the source (for the
    alias). Qdrant removal is best-effort — a down/absent index still leaves the
    ledger tombstoned (the point is dropped on the next reindex). Returns rows
    marked. ids are locked in sorted order (the documents-table deadlock rule)."""
    if not doc_ids:
        return 0
    ordered = sorted(doc_ids)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE documents SET status = 'deleted', embedded_model = NULL, "
            "indexed_at = NULL WHERE id = ANY(%s) AND status <> 'deleted'",
            (ordered,),  # namespaced ids: no source predicate needed
        )
        marked = cur.rowcount or 0
    conn.commit()
    try:
        from qdrant_client import QdrantClient
        from qdrant_client import models as qm

        from windex.embed.pipeline import point_id

        client = QdrantClient(url=settings.qdrant_url, timeout=30)
        client.delete(
            collection_name=qidx.alias_name(source),
            points_selector=qm.PointIdsList(points=[point_id(i) for i in ordered]),
            wait=True,  # tombstones are rare; deletion should be visible on return
        )
    except Exception as exc:  # index absent/unreachable: ledger tombstone stands
        console.print(f"[yellow]custom tombstone: qdrant delete skipped ({exc})[/yellow]")
    return marked


def upsert_docs(conn: psycopg.Connection, settings: Settings, name: str,
                docs: list[dict]) -> dict:
    """Stage a push's changed delta to a new per-batch parquet + ledger delta.

    Each doc dict carries ``id`` (suffix), ``text`` and optional ``title`` /
    ``url`` / ``published_at`` (datetime | None) / ``extra`` (dict | None). Only
    docs whose ``text_hash`` differs from the live ledger are written (to a fresh
    ``custom/<name>/<batch-uuid>.parquet`` via tmp + rename) and upserted; the
    rest are skipped and keep their existing ``text_ref``. Returns
    ``{source, docs, staged, skipped}``. Never tombstones absent ids."""
    text_ref = f"custom/{name}/{uuid.uuid4().hex}.parquet"
    rows = []
    for d in docs:
        suffix = d["id"]
        title = d.get("title") or ""
        text = d["text"]
        extra = d.get("extra")
        rows.append({
            "id": doc_id(name, suffix),
            "url": d.get("url") or default_url(name, suffix),
            "title": title,
            "published_at": _as_utc(d.get("published_at")),
            "text": text,
            "extra": orjson.dumps(extra).decode() if extra is not None else None,
            "thash": text_hash(title + "\n\n" + text),
        })
    stats = {"source": name, "docs": len(rows), "staged": 0, "skipped": 0}
    if not rows:
        return stats

    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")
    try:
        with conn.cursor() as cur:
            existing = _existing_hashes(cur, [r["id"] for r in rows])
            # Sorted by id: unchanged docs are pre-filtered out, and the ledger
            # upsert (like the embed loop's UPDATE) must lock documents rows in id
            # order or the two deadlock.
            delta = sorted((r for r in rows if existing.get(r["id"]) != r["thash"]),
                           key=lambda r: r["id"])
            stats["skipped"] = len(rows) - len(delta)
            stats["staged"] = len(delta)
            if delta:
                # Only the delta goes to parquet (upsert, not full-replace):
                # unchanged docs keep pointing at their original batch file.
                writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)
                writer.write_table(
                    pa.table(
                        {
                            "id": [r["id"] for r in delta],
                            "url": [r["url"] for r in delta],
                            "title": [r["title"] for r in delta],
                            "published_at": pa.array(
                                [r["published_at"] for r in delta],
                                pa.timestamp("us", tz="UTC"),
                            ),
                            "text": [r["text"] for r in delta],
                            "extra": [r["extra"] for r in delta],
                        },
                        schema=CLEAN_SCHEMA,
                    )
                )
                writer.close()
                tmp_path.rename(clean_path)
                cur.executemany(
                    """
                    INSERT INTO documents
                        (id, source, url, canonical_url, title, published_at,
                         text_hash, status, text_ref)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'deduped', %s)
                    ON CONFLICT (id) DO UPDATE SET
                        url = EXCLUDED.url, canonical_url = EXCLUDED.canonical_url,
                        title = EXCLUDED.title, published_at = EXCLUDED.published_at,
                        text_hash = EXCLUDED.text_hash, text_ref = EXCLUDED.text_ref,
                        status = 'deduped', embedded_model = NULL, indexed_at = NULL
                    WHERE documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                       OR documents.status = 'deleted'
                    """,
                    [(r["id"], name, r["url"], r["url"], r["title"],
                      r["published_at"], r["thash"], text_ref) for r in delta],
                )
        conn.commit()
    except Exception:
        tmp_path.unlink(missing_ok=True)
        conn.rollback()
        raise
    return stats


def delete_docs(conn: psycopg.Connection, settings: Settings, name: str,
                ids: list[str]) -> dict:
    """Tombstone specific docs by suffix (best-effort Qdrant delete). Idempotent:
    already-deleted / unknown ids don't count. Returns ``{deleted: N}``."""
    return {"deleted": apply_tombstones(conn, settings, name,
                                        [doc_id(name, s) for s in ids])}


def delete_source(conn: psycopg.Connection, settings: Settings, name: str) -> dict | None:
    """Full teardown: tombstone every live doc, drop the registry row, and remove
    the source's staging dir. Returns ``{deleted: N}``, or None if the source is
    unknown (→ 404). Idempotent at the operation level (rmtree ignores a missing
    dir; a re-delete of an unknown source is the None/404 path)."""
    from windex.custom_source import registry

    if registry.get(conn, name) is None:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM documents WHERE source = %s AND status <> 'deleted'",
            (name,),
        )
        live = [r[0] for r in cur.fetchall()]
    deleted = apply_tombstones(conn, settings, name, live)
    registry.delete_row(conn, name)
    shutil.rmtree(settings.custom_staging_dir / name, ignore_errors=True)
    return {"deleted": deleted}


def status(conn: psycopg.Connection, name: str) -> dict:
    """Per-source rollup (memory's status() shape, filtered by this source):
    doc counts by pipeline status + the most recent embed time."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, count(*) FROM documents WHERE source = %s GROUP BY status",
            (name,),
        )
        by_status = dict(cur.fetchall())
        cur.execute(
            "SELECT max(indexed_at) FROM documents WHERE source = %s", (name,)
        )
        last_indexed = cur.fetchone()[0]
    return {
        "source": name,
        "docs": {
            "embedded": by_status.get("embedded", 0),
            "pending": by_status.get("deduped", 0),
            "deleted": by_status.get("deleted", 0),
        },
        "last_indexed_at": last_indexed.isoformat() if last_indexed else None,
    }
