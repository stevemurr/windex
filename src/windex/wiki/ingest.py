"""Stream-download Wikipedia CirrusSearch shards, stage clean parquet, and
insert documents-ledger rows.

Resumable per shard via the wiki_dumps watermark (one shard = one clean parquet
= one unit of work). Change detection reuses the documents.text_hash ledger the
way ccnews dedup does: on a weekly re-ingest, an article whose normalized text
is unchanged is skipped, so only the delta is re-staged and re-embedded. The
shard is streamed and decompressed on the fly — the multi-hundred-MB dump is
never held in memory.
"""

import time
from datetime import datetime

import httpx

from windex.wiki import USER_AGENT
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from windex import db
from windex.ccnews.dedup import text_hash
from windex.config import Settings
from windex.wiki import reader
from windex.wiki import sync as wsync

console = Console()

CLEAN_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("url", pa.string()),
        ("title", pa.string()),
        ("revision_ts", pa.string()),
        ("incoming_links", pa.int64()),
        ("opening_text", pa.string()),
        ("text", pa.string()),
    ]
)

_SUFFIX = ".json.bz2"


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _chunked(it, n: int):
    chunk = []
    for x in it:
        chunk.append(x)
        if len(chunk) >= n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _existing_hashes(cur: psycopg.Cursor, ids: list[str]) -> dict[str, str]:
    if not ids:
        return {}
    cur.execute(
        # No `source =` predicate: ids are namespaced (hn:, wiki:, …) so an id
        # list can't match another source. Including it makes the planner pick
        # documents_source_published_idx (est. rows=1 — rare sources are absent
        # from the MCV list) and scan every row of the source: 244s vs 63ms.
        "SELECT id, text_hash FROM documents WHERE id = ANY(%s)",
        (ids,),
    )
    return dict(cur.fetchall())


def stage_shard(
    conn: psycopg.Connection,
    settings: Settings,
    name: str,
    date: str,
    wiki: str,
    client: httpx.Client,
    chunk_rows: int,
    pause_poll_seconds: float = 10.0,
) -> dict:
    """Stream one shard into its clean parquet + ledger rows. Returns per-shard
    stats. The clean parquet is written to a temp path and renamed into place
    only after a full pass, so text_ref never points at a partial file; the
    ledger insert is committed once, after the rename."""
    stem = name[: -len(_SUFFIX)]  # e.g. enwiki_content-20260712-00000
    text_ref = f"wiki/clean/{stem}.parquet"
    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")

    stats = {"articles": 0, "staged": 0, "skipped": 0}
    writer: pq.ParquetWriter | None = None
    doc_rows: list[tuple] = []
    url = wsync.shard_url(date, name, wiki)
    try:
        with conn.cursor() as cur, client.stream("GET", url) as resp:
            resp.raise_for_status()
            articles = reader.iter_articles_from_bytes(resp.iter_bytes(1 << 20))
            for chunk in _chunked(articles, chunk_rows):
                # dashboard pause: honor it between chunks, never mid-chunk. No
                # DB writes are pending here (ledger insert is deferred to the
                # end), so waiting is side-effect free.
                while db.get_control(conn, "indexing", "running") == "paused":
                    db.set_control(conn, "wiki_stage", "paused")
                    time.sleep(pause_poll_seconds)

                stats["articles"] += len(chunk)
                for a in chunk:
                    a["thash"] = text_hash(a["text"])
                existing = _existing_hashes(cur, [a["id"] for a in chunk])
                delta = [a for a in chunk if existing.get(a["id"]) != a["thash"]]
                stats["skipped"] += len(chunk) - len(delta)
                if not delta:
                    continue
                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)
                writer.write_batch(
                    pa.record_batch(
                        [
                            pa.array([a["id"] for a in delta]),
                            pa.array([a["url"] for a in delta]),
                            pa.array([a["title"] for a in delta]),
                            pa.array([a["revision_ts"] for a in delta]),
                            pa.array([a["incoming_links"] for a in delta], pa.int64()),
                            pa.array([a["opening_text"] for a in delta]),
                            pa.array([a["text"] for a in delta]),
                        ],
                        schema=CLEAN_SCHEMA,
                    )
                )
                for a in delta:
                    doc_rows.append(
                        (a["id"], a["url"], a["title"], _parse_ts(a["revision_ts"]),
                         a["thash"], text_ref)
                    )
                stats["staged"] += len(delta)

            if writer is not None:
                writer.close()
                writer = None
                tmp_path.rename(clean_path)

            # Change-aware ledger upsert: unchanged articles never reach here
            # (pre-filtered), and the WHERE guards against a race re-embedding an
            # identical revision.
            # Sorted: the embed loop UPDATEs these same rows by id, and taking
            # the locks in a different order deadlocks — shards 00006 and 00052
            # died exactly that way on 2026-07-16. A shard is one transaction of
            # ~112k rows, so the window is wide.
            doc_rows.sort(key=lambda r: r[0])
            cur.executemany(
                """
                INSERT INTO documents
                    (id, source, url, title, published_at, text_hash, status, text_ref)
                VALUES (%s, 'wiki', %s, %s, %s, %s, 'deduped', %s)
                ON CONFLICT (id) DO UPDATE SET
                    url = EXCLUDED.url, title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at, text_hash = EXCLUDED.text_hash,
                    text_ref = EXCLUDED.text_ref, status = 'deduped',
                    embedded_model = NULL, indexed_at = NULL
                WHERE documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                """,
                doc_rows,
            )
        conn.commit()
    except Exception:
        if writer is not None:
            writer.close()
        tmp_path.unlink(missing_ok=True)
        conn.rollback()
        raise
    return stats


def ingest(
    conn: psycopg.Connection,
    settings: Settings,
    max_files: int | None = None,
    chunk_rows: int | None = None,
    max_consecutive_failures: int = 3,
    pause_poll_seconds: float = 10.0,
) -> dict:
    """Process pending shards one at a time. Returns aggregate stats. A single
    bad shard is marked failed and skipped so a long run survives it; repeated
    back-to-back failures still abort."""
    wiki = settings.wiki_dump
    chunk_rows = chunk_rows or settings.wiki_chunk_rows
    totals = {"files": 0, "articles": 0, "staged": 0, "skipped": 0}
    consecutive_failures = 0
    try:
        with httpx.Client(
            timeout=httpx.Timeout(30, read=300), follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            while max_files is None or totals["files"] < max_files:
                pending = wsync.pending_shards(conn, 1)
                if not pending:
                    break
                name, date = pending[0]
                wsync.mark(conn, [name], "processing")
                console.print(f"[bold]shard[/bold] {name}")
                try:
                    db.set_control(conn, "wiki_stage", f"streaming {name}")
                    stats = stage_shard(
                        conn, settings, name, date, wiki, client, chunk_rows,
                        pause_poll_seconds,
                    )
                    wsync.mark(conn, [name], "done", stats)
                    for k in ("articles", "staged", "skipped"):
                        totals[k] += stats[k]
                    totals["files"] += 1
                    console.print(f"  {stats}")
                    consecutive_failures = 0
                except Exception as exc:
                    conn.rollback()
                    wsync.mark(conn, [name], "failed")
                    consecutive_failures += 1
                    console.print(f"[red]shard {name} failed[/red] ({exc}); continuing")
                    if consecutive_failures >= max_consecutive_failures:
                        raise
    finally:
        db.set_control(conn, "wiki_stage", "idle")
    return totals
