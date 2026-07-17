"""Harvest arXiv paper metadata over OAI-PMH, stage clean parquet, and insert
documents-ledger rows.

Source: arXiv's OAI-PMH endpoint (https://oaipmh.arxiv.org/oai), metadataPrefix
``arXiv``. Verified live 2026-07-16: earliestDatestamp 2005-09-16, granularity
day-level, deletedRecord=persistent, ~1,300 records/page, resumption tokens are
skip-offset style and EXPIRE at the next 00:00 UTC. arXiv metadata is CC0 — we
harvest metadata only (title + abstract), never full text.

Because a token chain must complete before the next 00:00 UTC, the backfill is
chunked into per-year date windows (``arxiv_windows`` watermark); each window is
independently restartable. Change detection reuses the documents.text_hash ledger
the way ccnews/wiki do: on a re-harvest, a paper whose title+abstract is unchanged
is skipped, so only the delta is re-staged and re-embedded. deletedRecord
tombstones (header status="deleted") mark the ledger row deleted and drop the
Qdrant point.

Everything OAI/format-specific (XML shape, namespaces, id derivation, resumption
tokens) lives here so a different upstream only touches this module.
"""

import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta

import httpx
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from windex import db
from windex.arxiv import USER_AGENT
from windex.ccnews.dedup import text_hash
from windex.config import Settings

console = Console()

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
ARXIV_NS = "http://arxiv.org/OAI/arXiv/"
_IDENTIFIER_PREFIX = "oai:arxiv.org:"

CLEAN_SCHEMA = pa.schema(
    [
        ("id", pa.string()),                     # stable doc id: arxiv:<paper_id>
        ("url", pa.string()),
        ("title", pa.string()),
        ("abstract", pa.string()),
        ("authors", pa.list_(pa.string())),
        ("primary_category", pa.string()),
        ("categories", pa.list_(pa.string())),
        ("created", pa.string()),
        ("updated", pa.string()),
        ("doi", pa.string()),
    ]
)


class OAIError(RuntimeError):
    """A non-recoverable OAI-PMH protocol error (bad verb/argument/token)."""

    def __init__(self, code: str, message: str | None):
        self.code = code
        super().__init__(f"{code}: {message}")


def _q(ns: str, tag: str) -> str:
    return f"{{{ns}}}{tag}"


def paper_id(identifier: str) -> str:
    """arXiv paper id from an OAI identifier: oai:arXiv.org:0805.3819 -> 0805.3819
    (old-style ids keep their slash, e.g. oai:arXiv.org:hep-th/9901001)."""
    ident = identifier.strip()
    if ident.lower().startswith(_IDENTIFIER_PREFIX):
        return ident[len(_IDENTIFIER_PREFIX):]
    return ident


def abs_url(pid: str) -> str:
    return f"https://arxiv.org/abs/{pid}"


def _text(el, ns: str, tag: str, default: str = "") -> str:
    val = el.findtext(_q(ns, tag))
    return val.strip() if val else default


def _collapse(s: str) -> str:
    return " ".join(s.split())


def _parse_authors(arx) -> list[str]:
    out: list[str] = []
    authors_el = arx.find(_q(ARXIV_NS, "authors"))
    if authors_el is None:
        return out
    for a in authors_el.findall(_q(ARXIV_NS, "author")):
        keyname = _text(a, ARXIV_NS, "keyname")
        forenames = _text(a, ARXIV_NS, "forenames")
        suffix = _text(a, ARXIV_NS, "suffix")
        name = " ".join(p for p in (forenames, keyname, suffix) if p)
        if name:
            out.append(name)
    return out


def _parse_record(rec) -> dict | None:
    header = rec.find(_q(OAI_NS, "header"))
    if header is None:
        return None
    identifier = _text(header, OAI_NS, "identifier")
    pid = paper_id(identifier)
    if not pid:
        return None
    if header.get("status") == "deleted":
        return {"id": pid, "deleted": True}
    meta = rec.find(_q(OAI_NS, "metadata"))
    arx = meta.find(_q(ARXIV_NS, "arXiv")) if meta is not None else None
    if arx is None:
        return None
    pid = _text(arx, ARXIV_NS, "id") or pid  # metadata id is canonical
    categories = _text(arx, ARXIV_NS, "categories").split()
    return {
        "id": pid,
        "deleted": False,
        "created": _text(arx, ARXIV_NS, "created"),
        "updated": _text(arx, ARXIV_NS, "updated") or None,
        "title": _collapse(_text(arx, ARXIV_NS, "title")),
        "abstract": _collapse(_text(arx, ARXIV_NS, "abstract")),
        "authors": _parse_authors(arx),
        "categories": categories,
        "primary_category": categories[0] if categories else "",
        "doi": _text(arx, ARXIV_NS, "doi") or None,
        "journal_ref": _text(arx, ARXIV_NS, "journal-ref") or None,
        "license": _text(arx, ARXIV_NS, "license") or None,
    }


def parse_records(xml_bytes: bytes) -> tuple[list[dict], str | None]:
    """Parse one ListRecords page: (records, resumption_token). An empty or
    absent resumptionToken means the token chain is complete. noRecordsMatch is
    not an error (empty window); other OAI error codes raise OAIError."""
    root = ET.fromstring(xml_bytes)
    err = root.find(_q(OAI_NS, "error"))
    if err is not None:
        if err.get("code") == "noRecordsMatch":
            return [], None
        raise OAIError(err.get("code") or "unknown", (err.text or "").strip())
    lr = root.find(_q(OAI_NS, "ListRecords"))
    if lr is None:
        return [], None
    records = [r for rec in lr.findall(_q(OAI_NS, "record")) if (r := _parse_record(rec))]
    token_el = lr.find(_q(OAI_NS, "resumptionToken"))
    token = None
    if token_el is not None and token_el.text and token_el.text.strip():
        token = token_el.text.strip()
    return records, token


# --- window watermark ------------------------------------------------------

def plan_backfill(conn: psycopg.Connection, from_year: int, to_year: int) -> int:
    """Insert one pending per-year window [YYYY-01-01, YYYY-12-31] for each year
    in [from_year, to_year]. Idempotent: already-recorded years are left as-is."""
    rows = [(f"{y}-01-01", f"{y}-12-31") for y in range(from_year, to_year + 1)]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO arxiv_windows (from_date, until_date) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING",
            rows,
            returning=False,
        )
        inserted = max(cur.rowcount or 0, 0)
    conn.commit()
    return inserted


def plan_incremental(conn: psycopg.Connection, days: int, today: date | None = None) -> tuple[str, str]:
    """Arm a rolling incremental window [today-days, today]. Re-arms a completed
    or failed window of the same span back to pending so freshness re-runs pick up
    updates; leaves an in-flight window untouched. Returns (from_date, until_date)."""
    today = today or date.today()
    frm = (today - timedelta(days=days)).isoformat()
    until = today.isoformat()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO arxiv_windows (from_date, until_date) VALUES (%s, %s) "
            "ON CONFLICT (from_date, until_date) DO UPDATE SET "
            "status = 'pending', processed_at = NULL "
            "WHERE arxiv_windows.status IN ('done', 'failed')",
            (frm, until),
        )
    conn.commit()
    return frm, until


def pending_windows(conn: psycopg.Connection, limit: int) -> list[tuple[str, str]]:
    """Oldest-first (from_date, until_date) pairs still pending."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT from_date, until_date FROM arxiv_windows WHERE status = 'pending' "
            "ORDER BY from_date, until_date LIMIT %s",
            (limit,),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def mark_window(
    conn: psycopg.Connection,
    frm: str,
    until: str,
    status: str,
    stats: dict | None = None,
    token: str | None = None,
) -> None:
    stats = stats or {}
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE arxiv_windows SET status = %s,
               token = coalesce(%s, token),
               pages = coalesce(%s, pages), records = coalesce(%s, records),
               staged = coalesce(%s, staged), deleted = coalesce(%s, deleted),
               processed_at = CASE WHEN %s IN ('done', 'failed') THEN now() ELSE processed_at END
               WHERE from_date = %s AND until_date = %s""",
            (status, token, stats.get("pages"), stats.get("records"),
             stats.get("staged"), stats.get("deleted"), status, frm, until),
        )
    conn.commit()


# --- ingest ----------------------------------------------------------------

def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


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


def _delta_batch(delta: list[dict]) -> pa.RecordBatch:
    return pa.record_batch(
        [
            pa.array([r["doc_id"] for r in delta]),
            pa.array([r["url"] for r in delta]),
            pa.array([r["title"] for r in delta]),
            pa.array([r["abstract"] for r in delta]),
            pa.array([r["authors"] for r in delta], pa.list_(pa.string())),
            pa.array([r["primary_category"] for r in delta]),
            pa.array([r["categories"] for r in delta], pa.list_(pa.string())),
            pa.array([r["created"] for r in delta]),
            pa.array([r["updated"] for r in delta]),
            pa.array([r["doi"] for r in delta]),
        ],
        schema=CLEAN_SCHEMA,
    )


def apply_tombstones(conn: psycopg.Connection, settings: Settings, doc_ids: list[str]) -> int:
    """Mark deleted-record ledger rows status='deleted' and drop their Qdrant
    points. Qdrant removal is best-effort: a down index still leaves the ledger
    tombstoned (the point is dropped on the next reindex). Returns rows marked."""
    if not doc_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE documents SET status = 'deleted', embedded_model = NULL, "
            "indexed_at = NULL WHERE id = ANY(%s)",  # see note above: no source predicate
            (doc_ids,),
        )
        marked = cur.rowcount or 0
    conn.commit()
    try:
        from qdrant_client import QdrantClient
        from qdrant_client import models as qm

        from windex.ccnews.embed_index import point_id
        from windex.index import qdrant as qidx

        client = QdrantClient(url=settings.qdrant_url, timeout=30)
        client.delete(
            collection_name=qidx.alias_name("arxiv"),
            points_selector=qm.PointIdsList(points=[point_id(i) for i in doc_ids]),
            wait=True,  # tombstones are rare; deletion should be visible on return
        )
    except Exception as exc:  # index absent/unreachable: ledger tombstone stands
        console.print(f"[yellow]tombstone: qdrant delete skipped ({exc})[/yellow]")
    return marked


def _oai_client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(30, read=120), follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )


def harvest_window(
    conn: psycopg.Connection,
    settings: Settings,
    frm: str,
    until: str,
    client: httpx.Client,
    request_interval: float | None = None,
    pause_poll_seconds: float = 10.0,
) -> dict:
    """Run one window's full resumption-token chain into its clean parquet +
    ledger rows. The parquet is written to a temp path and renamed into place
    only after the whole chain completes, so text_ref never points at a partial
    file; the ledger insert is deferred and committed once, after the rename."""
    request_interval = settings.arxiv_request_interval if request_interval is None else request_interval
    text_ref = f"arxiv/clean/{frm}_{until}.parquet"
    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")

    stats = {"pages": 0, "records": 0, "staged": 0, "skipped": 0, "deleted": 0}
    writer: pq.ParquetWriter | None = None
    doc_rows: list[tuple] = []
    deleted_ids: list[str] = []
    token: str | None = None
    first = True
    endpoint = settings.arxiv_oai_endpoint
    try:
        with conn.cursor() as cur:
            while True:
                # dashboard pause: honor it between pages, never mid-page. The
                # ledger insert is deferred to the end, so waiting here (which
                # commits the stage flag) is side-effect free.
                while db.get_control(conn, "indexing", "running") == "paused":
                    db.set_control(conn, "arxiv_stage", "paused")
                    time.sleep(pause_poll_seconds)

                if first:
                    params = {"verb": "ListRecords",
                              "metadataPrefix": settings.arxiv_metadata_prefix,
                              "from": frm, "until": until}
                    first = False
                else:
                    params = {"verb": "ListRecords", "resumptionToken": token}
                resp = client.get(endpoint, params=params)
                resp.raise_for_status()
                records, token = parse_records(resp.content)
                stats["pages"] += 1
                stats["records"] += len(records)

                live = [r for r in records if not r.get("deleted")]
                deleted_ids.extend(f"arxiv:{r['id']}" for r in records if r.get("deleted"))
                for r in live:
                    r["doc_id"] = f"arxiv:{r['id']}"
                    r["url"] = abs_url(r["id"])
                    r["thash"] = text_hash((r["title"] or "") + "\n\n" + (r["abstract"] or ""))
                existing = _existing_hashes(cur, [r["doc_id"] for r in live])
                delta = [r for r in live if existing.get(r["doc_id"]) != r["thash"]]
                stats["skipped"] += len(live) - len(delta)
                if delta:
                    if writer is None:
                        writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)
                    writer.write_batch(_delta_batch(delta))
                    for r in delta:
                        doc_rows.append(
                            (r["doc_id"], r["url"], r["title"], _parse_date(r["created"]),
                             r["thash"], text_ref)
                        )
                    stats["staged"] += len(delta)

                db.set_control(conn, "arxiv_stage", f"{frm}..{until} p{stats['pages']}")
                if not token:
                    break
                time.sleep(request_interval)  # arXiv ToU: >= 3s between requests

            if writer is not None:
                writer.close()
                writer = None
                tmp_path.rename(clean_path)

            # Change-aware ledger upsert: unchanged papers never reach here
            # (pre-filtered); the WHERE guards a race re-embedding an identical row.
            cur.executemany(
                """
                INSERT INTO documents
                    (id, source, url, title, published_at, text_hash, status, text_ref)
                VALUES (%s, 'arxiv', %s, %s, %s, %s, 'deduped', %s)
                ON CONFLICT (id) DO UPDATE SET
                    url = EXCLUDED.url, title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at, text_hash = EXCLUDED.text_hash,
                    text_ref = EXCLUDED.text_ref, status = 'deduped',
                    embedded_model = NULL, indexed_at = NULL
                WHERE documents.text_hash IS DISTINCT FROM EXCLUDED.text_hash
                """,
                # sorted by id: the embed loop UPDATEs these same rows, and
                # locking them in a different order deadlocks (killed two wiki
                # shards 2026-07-16). Every batch writer to `documents` locks
                # in id order.
                sorted(doc_rows),
            )
        conn.commit()
    except Exception:
        if writer is not None:
            writer.close()
        tmp_path.unlink(missing_ok=True)
        conn.rollback()
        raise
    stats["deleted"] = apply_tombstones(conn, settings, deleted_ids)
    return stats


def harvest(
    conn: psycopg.Connection,
    settings: Settings,
    max_windows: int | None = None,
    max_consecutive_failures: int = 3,
    client: httpx.Client | None = None,
    request_interval: float | None = None,
    pause_poll_seconds: float = 10.0,
) -> dict:
    """Process pending windows one at a time. Returns aggregate stats. A single
    failed window is marked failed and skipped so a long backfill survives it;
    repeated back-to-back failures still abort."""
    totals = {"windows": 0, "pages": 0, "records": 0, "staged": 0, "skipped": 0, "deleted": 0}
    consecutive_failures = 0
    own = client is None
    client = client or _oai_client()
    try:
        while max_windows is None or totals["windows"] < max_windows:
            pending = pending_windows(conn, 1)
            if not pending:
                break
            frm, until = pending[0]
            mark_window(conn, frm, until, "processing")
            console.print(f"[bold]window[/bold] {frm}..{until}")
            try:
                stats = harvest_window(
                    conn, settings, frm, until, client,
                    request_interval=request_interval, pause_poll_seconds=pause_poll_seconds,
                )
                mark_window(conn, frm, until, "done", stats)
                for k in ("pages", "records", "staged", "skipped", "deleted"):
                    totals[k] += stats[k]
                totals["windows"] += 1
                console.print(f"  {stats}")
                consecutive_failures = 0
            except Exception as exc:
                conn.rollback()
                mark_window(conn, frm, until, "failed")
                consecutive_failures += 1
                console.print(f"[red]window {frm}..{until} failed[/red] ({exc}); continuing")
                if consecutive_failures >= max_consecutive_failures:
                    raise
    finally:
        db.set_control(conn, "arxiv_stage", "idle")
        if own:
            client.close()
    return totals
