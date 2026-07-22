"""One-time cleanups for pre-existing hn index-quality defects (2026-07-22).

Run against prod while indexing is PAUSED, AFTER the ingest guards land:
  - ``tombstone_empty_stories``: the ~13.5K fully-empty docs (blank title AND
    body) staged/embedded before the empty guard existed -> 'deleted', drop any
    live vector.
  - ``backfill_exact_duplicates``: the ~429K un-deduped exact text_hash
    collisions -> 'duplicate' of the earliest canonical, drop any embedded dup's
    vector.

``tombstone_empty_stories`` MUST run before ``backfill_exact_duplicates`` — every
empty doc shares one text_hash, so tombstoning them first keeps them out of the
dedup scan. Both are idempotent/resumable (candidates filtered by
``status IN ('deduped','embedded')``, which shrinks monotonically) and mirror the
``apply_tombstones`` shape (mark the ledger, then a best-effort Qdrant delete).
"""

import psycopg
import pyarrow.parquet as pq
from rich.console import Console

from windex.config import Settings
from windex.textguard import is_empty_text

console = Console()


def _drop_points(settings: Settings, doc_ids: list[str], chunk: int = 4000) -> None:
    """Best-effort delete of hn Qdrant points, CHUNKED — a single delete of 100k+
    ids times out (observed: 138,902 ids @ 30s). A down index leaves the ledger
    change standing; the point is dropped on the next reindex."""
    if not doc_ids:
        return
    try:
        from qdrant_client import QdrantClient
        from qdrant_client import models as qm

        from windex.embed.pipeline import point_id
        from windex.index import qdrant as qidx

        client = QdrantClient(url=settings.qdrant_url, timeout=180)
        try:
            for i in range(0, len(doc_ids), chunk):
                client.delete(
                    collection_name=qidx.alias_name("hn"),
                    points_selector=qm.PointIdsList(
                        points=[point_id(x) for x in doc_ids[i:i + chunk]]
                    ),
                    wait=True,
                )
        finally:
            client.close()
    except Exception as exc:  # index absent/unreachable: the ledger change stands
        console.print(f"[yellow]hn cleanup: qdrant delete skipped ({exc})[/yellow]")


def find_empty_story_ids(conn: psycopg.Connection, settings: Settings) -> list[str]:
    """hn docs (still deduped/embedded) whose composed title+story_text is empty.
    Cheap ledger blank-title candidates, confirmed against the parquet body (the
    title lives in the ledger; the body only in the clean parquet)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, text_ref FROM documents
            WHERE source = 'hn' AND status IN ('deduped', 'embedded')
              AND (title IS NULL OR btrim(title) = '')
            """
        )
        candidates = cur.fetchall()
    by_ref: dict[str, set[str]] = {}
    for doc_id, ref in candidates:
        by_ref.setdefault(ref, set()).add(doc_id)

    empty: list[str] = []
    for ref, ids in by_ref.items():
        path = settings.staging_dir / ref
        if not path.exists():
            continue  # parquet gone (drive detached / pruned): can't confirm, skip
        table = pq.read_table(path, columns=["id", "title", "story_text"])
        for row in table.to_pylist():
            if row["id"] in ids and is_empty_text((row["title"] or "") + (row["story_text"] or "")):
                empty.append(row["id"])
    return empty


def tombstone_empty_stories(conn: psycopg.Connection, settings: Settings) -> int:
    """Mark fully-empty hn docs 'deleted' and drop any embedded vectors. Returns
    rows marked. Idempotent: a re-run only picks up what is still deduped/embedded."""
    ids = find_empty_story_ids(conn, settings)
    if not ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM documents WHERE id = ANY(%s) AND status = 'embedded'", (ids,)
        )
        embedded = [r[0] for r in cur.fetchall()]
        cur.execute(
            "UPDATE documents SET status = 'deleted', embedded_model = NULL, "
            "indexed_at = NULL WHERE id = ANY(%s)",
            (sorted(ids),),
        )
        marked = cur.rowcount or 0
    conn.commit()
    _drop_points(settings, embedded)
    return marked


def backfill_exact_duplicates(conn: psycopg.Connection, settings: Settings) -> dict:
    """Mark every non-earliest hn doc sharing a text_hash 'duplicate' of the
    earliest (created_at, id) canonical, and drop any embedded dup's vector. One
    window-function UPDATE — the system is paused during cleanup, so there is no
    lock-order contention with the embed loop. Idempotent: once 'duplicate' a row
    leaves the scan set (`status IN ('deduped','embedded')`)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT id, status,
                       first_value(id) OVER (
                           PARTITION BY text_hash ORDER BY created_at, id
                       ) AS canon
                FROM documents
                WHERE source = 'hn' AND status IN ('deduped', 'embedded')
            )
            UPDATE documents d
            SET status = 'duplicate', duplicate_of = r.canon,
                embedded_model = NULL, indexed_at = NULL
            FROM ranked r
            WHERE d.id = r.id AND r.id <> r.canon
            RETURNING d.id, r.status
            """
        )
        changed = cur.fetchall()
    conn.commit()
    embedded_dups = [i for i, status in changed if status == "embedded"]
    _drop_points(settings, embedded_dups)
    return {"marked_duplicate": len(changed), "vectors_dropped": len(embedded_dups)}
