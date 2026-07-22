"""Full-replace ingest for the push-based chat-memory source.

Per conversation, the app POSTs the FULL ordered chunk list; this module
reconciles it against the ledger with the same semantics as
``docs_source.ingest.stage_docset``:

* The whole chunk set is written to ``memory/clean/<conversation_id>.parquet``
  via a temp file + atomic rename, so ``text_ref`` never points at a partial
  file and unchanged chunks stay readable at a stable ref.
* Only the changed delta reaches the ledger — the ``text_hash`` guard makes the
  append-only common case a single trailing-chunk re-embed, and the ON CONFLICT
  WHERE clause both avoids re-embedding an identical row and resurrects a
  tombstoned chunk that reappeared byte-identically.
* Chunk ids present in the ledger but absent from the new set are tombstoned
  (``status='deleted'`` + best-effort Qdrant point delete).

The server constructs every id and hardcodes ``source='memory'``; the client
never supplies ids, so this endpoint cannot write into any other source.
"""

from __future__ import annotations

from datetime import datetime, timezone

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from windex.ccnews.dedup import text_hash
from windex.config import Settings

console = Console()

CLEAN_SCHEMA = pa.schema(
    [
        ("id", pa.string()),                            # memory:<cid>/<index %05d>
        ("url", pa.string()),                           # llmchat://chat/<cid>?chunk=<index>
        ("title", pa.string()),                         # conversation title (shared across chunks)
        ("conversation_id", pa.string()),
        ("chunk_index", pa.int64()),
        ("published_at", pa.timestamp("us", tz="UTC")),  # = chunk ended_at (nullable)
        ("text", pa.string()),
    ]
)

MAX_CHUNKS = 500
MAX_TEXT_CHARS = 16_000


def doc_id(cid: str, index: int) -> str:
    return f"memory:{cid}/{index:05d}"


def chunk_url(cid: str, index: int) -> str:
    return f"llmchat://chat/{cid}?chunk={index}"


def _as_utc(value: datetime | None) -> datetime | None:
    """Normalize to a tz-aware UTC datetime so the pyarrow timestamp[us, tz=UTC]
    array accepts it (a mix of aware and naive datetimes, or a naive datetime
    against a tz-typed array, raises). A naive value is assumed to be UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# --- ledger reconciliation (mirrors docs_source.ingest) ----------------------

def _existing_hashes(cur: psycopg.Cursor, ids: list[str]) -> dict[str, str]:
    """id -> text_hash for live ledger rows. Tombstoned rows are deliberately
    excluded so a chunk that reappears (even byte-identical) re-stages. No
    `source =` predicate: ids are namespaced (memory:<cid>/…) so an id list can't
    match another source, and adding the predicate makes the planner scan the
    whole source (the 244s-vs-63ms note at docs_source/ingest.py:195)."""
    if not ids:
        return {}
    cur.execute(
        "SELECT id, text_hash FROM documents "
        "WHERE status <> 'deleted' AND id = ANY(%s)",
        (ids,),
    )
    return dict(cur.fetchall())


def _ledger_ids_for_conversation(cur: psycopg.Cursor, cid: str) -> set[str]:
    cur.execute(
        "SELECT id FROM documents WHERE source = 'memory' "
        "AND status <> 'deleted' AND starts_with(id, %s)",
        (f"memory:{cid}/",),
    )
    return {r[0] for r in cur.fetchall()}


def apply_tombstones(conn: psycopg.Connection, settings: Settings, doc_ids: list[str]) -> int:
    """Mark vanished-chunk ledger rows status='deleted' and drop their Qdrant
    points. Qdrant removal is best-effort: a down index still leaves the ledger
    tombstoned (the point is dropped on the next reindex). Returns rows marked."""
    if not doc_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE documents SET status = 'deleted', embedded_model = NULL, "
            "indexed_at = NULL WHERE id = ANY(%s)",  # namespaced ids: no source predicate
            (doc_ids,),
        )
        marked = cur.rowcount or 0
    conn.commit()
    try:
        from qdrant_client import QdrantClient
        from qdrant_client import models as qm

        from windex.embed.pipeline import point_id
        from windex.index import qdrant as qidx

        client = QdrantClient(url=settings.qdrant_url, timeout=30)
        client.delete(
            collection_name=qidx.alias_name("memory"),
            points_selector=qm.PointIdsList(points=[point_id(i) for i in doc_ids]),
            wait=True,  # tombstones are rare; deletion should be visible on return
        )
    except Exception as exc:  # index absent/unreachable: ledger tombstone stands
        console.print(f"[yellow]memory tombstone: qdrant delete skipped ({exc})[/yellow]")
    return marked


# --- ingest ------------------------------------------------------------------

def replace_conversation(
    conn: psycopg.Connection,
    settings: Settings,
    conversation_id: str,
    title: str,
    chunks: list[dict],
) -> dict:
    """Full-replace one conversation's staging partition + ledger delta.

    ``chunks`` is the app's full ordered list; each dict carries ``index``,
    ``text`` and optional ``started_at`` / ``ended_at`` (datetime | None) /
    ``message_range``. The parquet is written to a temp path and renamed into
    place only after the whole set is processed, so ``text_ref`` never points at
    a partial file; the ledger upsert is deferred and committed once, after the
    rename. Returns ``{conversation_id, chunks, staged, skipped, deleted}``.
    """
    cid = conversation_id
    title = title or ""
    text_ref = f"memory/clean/{cid}.parquet"
    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")

    rows = [
        {
            "id": doc_id(cid, c["index"]),
            "url": chunk_url(cid, c["index"]),
            "title": title,
            "conversation_id": cid,
            "chunk_index": int(c["index"]),
            "published_at": _as_utc(c.get("ended_at")),
            "text": c["text"],
            "thash": text_hash(title + "\n\n" + c["text"]),
        }
        for c in chunks
    ]
    current_ids = {r["id"] for r in rows}
    stats = {"conversation_id": cid, "chunks": len(rows), "staged": 0,
             "skipped": 0, "deleted": 0}

    try:
        with conn.cursor() as cur:
            # The FULL chunk set goes to parquet (full-replace semantics —
            # unchanged chunks must stay readable at this text_ref); only the
            # changed delta is queued for the ledger -> re-embed. An empty push
            # writes no parquet (writer stays None): every existing chunk falls
            # into `missing` below and is tombstoned, which is the server-side
            # delete the app leans on when a conversation is emptied.
            if rows:
                writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)
                writer.write_table(
                    pa.table(
                        {
                            "id": [r["id"] for r in rows],
                            "url": [r["url"] for r in rows],
                            "title": [r["title"] for r in rows],
                            "conversation_id": [r["conversation_id"] for r in rows],
                            "chunk_index": pa.array([r["chunk_index"] for r in rows], pa.int64()),
                            "published_at": pa.array(
                                [r["published_at"] for r in rows],
                                pa.timestamp("us", tz="UTC"),
                            ),
                            "text": [r["text"] for r in rows],
                        },
                        schema=CLEAN_SCHEMA,
                    )
                )
                writer.close()

            existing = _existing_hashes(cur, [r["id"] for r in rows])
            delta = [r for r in rows if existing.get(r["id"]) != r["thash"]]
            stats["skipped"] = len(rows) - len(delta)
            stats["staged"] = len(delta)

            missing = sorted(_ledger_ids_for_conversation(cur, cid) - current_ids)

            if rows:
                tmp_path.rename(clean_path)

            # Change-aware ledger upsert: unchanged chunks never reach here
            # (pre-filtered); the WHERE guards a race re-embedding an identical
            # row, while still resurrecting a tombstoned chunk that reappeared.
            # sorted by id: the embed loop UPDATEs these same rows, and locking
            # them in a different order deadlocks — every batch writer to
            # `documents` locks in id order.
            cur.executemany(
                """
                INSERT INTO documents
                    (id, source, url, canonical_url, title, published_at,
                     text_hash, status, text_ref)
                VALUES (%s, 'memory', %s, %s, %s, %s, %s, 'deduped', %s)
                ON CONFLICT (id) DO UPDATE SET
                    url = EXCLUDED.url, canonical_url = EXCLUDED.canonical_url,
                    title = EXCLUDED.title, published_at = EXCLUDED.published_at,
                    text_hash = EXCLUDED.text_hash, text_ref = EXCLUDED.text_ref,
                    status = 'deduped', embedded_model = NULL, indexed_at = NULL
                WHERE documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                   OR documents.status = 'deleted'
                """,
                sorted(
                    (r["id"], r["url"], r["url"], r["title"], r["published_at"],
                     r["thash"], text_ref)
                    for r in delta
                ),
            )
        conn.commit()
    except Exception:
        tmp_path.unlink(missing_ok=True)
        conn.rollback()
        raise
    stats["deleted"] = apply_tombstones(conn, settings, missing)
    if not rows:
        # Emptied conversation (the documented 'conversation emptied' push): the
        # ledger is tombstoned above, but the empty branch never wrote/renamed a
        # parquet, so the OLD one still holds the full chat text. Remove it.
        _unlink_clean(settings, cid)
    return stats


def _unlink_clean(settings: Settings, cid: str) -> None:
    """Remove a conversation's clean parquet (+ its tmp sibling). Called once its
    ledger rows are tombstoned — otherwise deleting/emptying a conversation leaves
    the full chat text on the staging volume indefinitely (an unbounded leak and a
    'delete' that doesn't actually delete the content). Best-effort."""
    clean = settings.staging_dir / f"memory/clean/{cid}.parquet"
    clean.unlink(missing_ok=True)
    clean.with_suffix(".parquet.tmp").unlink(missing_ok=True)


def delete_conversation(conn: psycopg.Connection, settings: Settings,
                        conversation_id: str) -> dict:
    """Tombstone every live ledger row under ``memory:<cid>/`` + best-effort
    Qdrant delete, and remove the conversation's clean parquet. Idempotent:
    deleting a conversation with nothing live returns ``deleted: 0``."""
    with conn.cursor() as cur:
        missing = sorted(_ledger_ids_for_conversation(cur, conversation_id))
    deleted = apply_tombstones(conn, settings, missing)
    _unlink_clean(settings, conversation_id)
    return {"conversation_id": conversation_id, "deleted": deleted}


def status(conn: psycopg.Connection) -> dict:
    """Corpus-wide memory rollup for the app's Settings status row and health
    probe: conversation count (live), chunk counts by pipeline status, and the
    most recent embed time. Cheap at memory scale (thousands of rows)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, count(*) FROM documents WHERE source = 'memory' "
            "GROUP BY status"
        )
        by_status = dict(cur.fetchall())
        cur.execute(
            "SELECT count(DISTINCT split_part(id, '/', 1)) FROM documents "
            "WHERE source = 'memory' AND status <> 'deleted'"
        )
        conversations = cur.fetchone()[0]
        cur.execute(
            "SELECT max(indexed_at) FROM documents WHERE source = 'memory'"
        )
        last_indexed = cur.fetchone()[0]
    return {
        "conversations": conversations,
        "chunks": {
            "embedded": by_status.get("embedded", 0),
            "pending": by_status.get("deduped", 0),
            "deleted": by_status.get("deleted", 0),
        },
        "last_indexed_at": last_indexed.isoformat() if last_indexed else None,
    }
