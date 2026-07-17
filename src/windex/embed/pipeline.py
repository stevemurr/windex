"""Shared streaming embed driver for the parquet-backed sources.

Six sources (news, wiki, hn, arxiv, smallweb, docs) stage extracted text to
clean parquet and then embed it into a per-source Qdrant collection. They
differ only in *which* columns they stage, *how* the embedded text is composed,
and *what* the payload carries. The driver — stream parquet → embed → upsert →
commit status — was copy-pasted six times; it lives here once instead.

github/ deliberately does NOT use this. Its pass composes documents from the
`repos` table and *writes* the clean parquet rather than reading a text_ref, and
it has no text_ref/pause/profile shape to share. Folding it in would mean
bending the abstraction around a pipeline that is genuinely different.

Shape (and why):

    reader thread ──work_q──▶ embed pool ──up_q──▶ upsert threads
                                                        │
                                  main thread ◀─Future──┘  status='embedded'

* The reader streams row-group batches (column projection + id pushdown) and
  runs *across* text_refs, so the next ref's I/O overlaps the current batch's
  embedding and no per-ref barrier drains the pool. It streams rather than
  materializing whole files: a 333MB wiki ref used to become ~400MB of Python
  dicts via read_table().to_pylist() before a single vector was computed.
* The upsert threads keep Qdrant's round-trip (avg 355ms, worst case 36s) out
  of the embed workers, so a slow PUT /points never occupies a GPU slot.
* Every queue is sized from settings.embed_concurrency, so the runtime profile
  ("polite"/"full") still bounds total in-flight work and memory. Prefetch
  cannot outrun the configured throttle.

ORDERING CONTRACT (the correctness constraint of this module): a document's
status only becomes 'embedded' after its vectors have durably landed. The
upsert threads call client.upsert(wait=True) and only then resolve the Future
that the main thread waits on before its UPDATE. Never pass wait=False here: a
crash between a non-durable upsert and the status commit would strand documents
marked 'embedded' whose vectors do not exist, and nothing would ever retry them.
"""

from __future__ import annotations

import concurrent.futures as cf
import queue
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

import psycopg
import pyarrow.dataset as ds
from qdrant_client import QdrantClient
from qdrant_client import models as qm

from windex import db
from windex.config import Settings
from windex.embed import build_embedder, with_runtime_profile
from windex.index import qdrant as qidx

_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # uuid5 namespace

# Rows pulled per scanner batch. Independent of embed_batch_size (which the
# runtime profile changes): this only governs parquet read granularity.
_SCAN_ROWS = 1024

# Sentinel for "the producer is finished" on both queues.
_DONE = object()


def point_id(doc_id: str) -> str:
    """Qdrant requires uuid/int point ids; the string doc id lives in the
    payload and is the public API id."""
    return str(uuid.uuid5(_NS, doc_id))


@dataclass(frozen=True)
class SourceSpec:
    """What makes one parquet-backed source different from another."""

    source: str  # documents.source, and the payload "source"
    collection: str  # qdrant collection base name (qidx.ensure_collection)
    columns: tuple[str, ...]  # staged columns this pass reads (projection)
    text_field: str  # the body column: "text" / "abstract" / "story_text"
    payload: Callable[[dict], dict]  # row → payload (doc_id/source added here)
    default_limit: int = 100_000


def pending_refs(conn: psycopg.Connection, source: str, limit: int) -> dict[str, list[str]]:
    """text_ref → doc ids, for the oldest pending docs of one source."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT text_ref, array_agg(id)
            FROM (
                SELECT text_ref, id FROM documents
                WHERE source = %s AND status = 'deduped'
                ORDER BY created_at LIMIT %s
            ) t GROUP BY text_ref
            """,
            (source, limit),
        )
        return dict(cur.fetchall())


def compose_text(row: dict, text_field: str, max_chars: int) -> str:
    """title + body, bounded. Every source embeds this shape."""
    title = row.get("title")
    body = row.get(text_field) or ""
    return ((title + "\n\n") if title else "") + body[:max_chars]


def _put(q: queue.Queue, item, stop: threading.Event) -> bool:
    """Bounded put that stays responsive to cancellation (pause)."""
    while not stop.is_set():
        try:
            q.put(item, timeout=0.5)
            return True
        except queue.Full:
            continue
    return False


def _reader(settings: Settings, spec: SourceSpec, refs: dict[str, list[str]],
            batch_size: int, work_q: queue.Queue, stop: threading.Event,
            err: list[BaseException]) -> None:
    """Producer thread: stream pending rows out of clean parquet in embed-sized
    batches, ref after ref with no barrier between them. The bounded work_q is
    both the backpressure and the memory bound.

    A read failure (the external staging drive has detached twice in this
    project's life) is stashed in `err` for the main thread to re-raise. Reading
    used to happen on the caller's thread, where a dead drive raised loudly and
    tripped embed-loop's circuit breaker; swallowing it here would turn an
    outage into a silent no-op pass, which is the exact failure class
    tests/test_outage_guards.py exists to prevent."""
    try:
        for text_ref, ids in refs.items():
            if stop.is_set():
                return
            path = settings.staging_dir / text_ref
            # Column projection + id pushdown, the streaming form of the idiom
            # api.service.get_document uses (filters=[("id","==",doc_id)]):
            # read only the columns this source embeds and let parquet row-group
            # statistics skip groups holding no pending id.
            scanner = ds.dataset(path, format="parquet").scanner(
                columns=list(spec.columns),
                filter=ds.field("id").isin(ids),
                batch_size=_SCAN_ROWS,
            )
            buf: list[dict] = []
            for record_batch in scanner.to_batches():
                if stop.is_set():
                    return
                buf.extend(record_batch.to_pylist())
                while len(buf) >= batch_size:
                    if not _put(work_q, buf[:batch_size], stop):
                        return
                    del buf[:batch_size]
            if buf and not _put(work_q, buf, stop):
                return
    except BaseException as exc:  # noqa: BLE001 — re-raised on the main thread
        err.append(exc)
    finally:
        # Best-effort: on the pause path the main thread has stopped consuming.
        try:
            work_q.put(_DONE, timeout=5)
        except queue.Full:
            pass


def _upserter(up_q: queue.Queue, client: QdrantClient, collection: str) -> None:
    """Upsert thread: drains points the embed workers produced.

    wait=True is mandatory and load-bearing — see the module docstring's
    ordering contract. The Future resolved here is what gates the caller's
    status='embedded' commit, so it must not resolve until Qdrant has the
    vectors durably."""
    while True:
        item = up_q.get()
        try:
            if item is _DONE:
                return
            points, ids, fut = item
            if not fut.set_running_or_notify_cancel():
                continue
            try:
                client.upsert(collection_name=collection, points=points, wait=True)
            except BaseException as exc:  # noqa: BLE001 — surfaced on the main thread
                fut.set_exception(exc)
            else:
                fut.set_result(ids)
        finally:
            up_q.task_done()


def _embed_batch(rows: list[dict], spec: SourceSpec, embedder, bm25, up_q: queue.Queue,
                 max_chars: int, throttle: float, stop: threading.Event) -> cf.Future:
    """Embed worker: dense + sparse, hand the points to an upsert thread, and go
    straight back for more work — the Qdrant round-trip must not hold this slot.
    Returns the Future the upserter resolves once the vectors have landed."""
    texts = [compose_text(r, spec.text_field, max_chars) for r in rows]
    dense = embedder.embed_batch(texts)
    if throttle:
        time.sleep(throttle)  # leave the embedding server a gap for queries
    sparse = list(bm25.embed(texts))
    points = [
        qm.PointStruct(
            id=point_id(r["id"]),
            vector={
                qidx.DENSE: dense[i],
                qidx.SPARSE: qm.SparseVector(
                    indices=sparse[i].indices.tolist(),
                    values=sparse[i].values.tolist(),
                ),
            },
            payload={"doc_id": r["id"], "source": spec.source, **spec.payload(r)},
        )
        for i, r in enumerate(rows)
    ]
    fut: cf.Future = cf.Future()
    if not _put(up_q, (points, [r["id"] for r in rows], fut), stop):
        fut.cancel()
    return fut


def embed_pending(conn: psycopg.Connection, settings: Settings, spec: SourceSpec,
                  limit: int | None = None) -> int:
    """Embed one source's pending docs. Returns the number marked 'embedded'.

    Idempotent: work is selected by status='deduped' and each batch's status
    commit happens only after that batch's vectors are durable, so a crash at
    any point loses at most the in-flight batches — which the next pass simply
    re-selects. Qdrant upserts are keyed by a stable uuid5 point id, so a
    re-embed overwrites rather than duplicates."""
    settings = with_runtime_profile(conn, settings)
    refs = pending_refs(conn, spec.source, spec.default_limit if limit is None else limit)
    if not refs:
        return 0

    embedder = build_embedder(settings)
    from windex.index.sparse import bm25_model

    bm25 = bm25_model()
    client = QdrantClient(url=settings.qdrant_url, timeout=120)
    collection = qidx.ensure_collection(
        client, spec.collection, settings.embed_model, settings.embed_dim
    )

    max_chars = settings.embed_max_tokens * 4  # crude token→char bound
    concurrency = max(settings.embed_concurrency, 1)
    batch_size = max(settings.embed_batch_size, 1)
    # The window bounds prefetch: at most this many batches read-ahead, in the
    # embed pool, or awaiting upsert. Scales with the runtime profile, so
    # "polite" throttles the prefetch too rather than becoming a firehose.
    window = concurrency * 2

    stop = threading.Event()
    work_q: queue.Queue = queue.Queue(maxsize=window)
    up_q: queue.Queue = queue.Queue(maxsize=window)
    reader_err: list[BaseException] = []

    reader = threading.Thread(
        target=_reader, args=(settings, spec, refs, batch_size, work_q, stop, reader_err),
        name=f"{spec.source}-embed-read", daemon=True,
    )
    reader.start()
    # Default (0) matches embed_concurrency: the old inline upsert gave every
    # embed worker its own Qdrant round-trip, so anything narrower would turn
    # Qdrant's latency tail into backpressure the old code never had.
    upsert_workers = settings.embed_upsert_workers or concurrency
    upserters = [
        threading.Thread(target=_upserter, args=(up_q, client, collection),
                         name=f"{spec.source}-embed-upsert-{i}", daemon=True)
        for i in range(max(upsert_workers, 1))
    ]
    for t in upserters:
        t.start()

    total = 0
    try:
        with cf.ThreadPoolExecutor(concurrency) as pool:
            inflight: deque[cf.Future] = deque()
            drained = False
            commits = 0
            while True:
                # Top the window up first: the pool should always have queued
                # work so no GPU slot waits on the reader.
                while not drained and len(inflight) < window:
                    try:
                        item = work_q.get(timeout=0.1)
                    except queue.Empty:
                        break
                    if item is _DONE:
                        drained = True
                        break
                    inflight.append(pool.submit(
                        _embed_batch, item, spec, embedder, bm25, up_q,
                        max_chars, settings.embed_throttle_seconds, stop,
                    ))
                if not inflight:
                    if drained:
                        break
                    continue
                # FIFO: only the *commit* waits in order — the pool and the
                # upsert threads keep running ahead regardless.
                ids = inflight.popleft().result().result()  # embed → vectors durable
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE documents
                        SET status = 'embedded', embedded_model = %s, indexed_at = now()
                        WHERE id = ANY(%s)
                        """,
                        (settings.embed_model, ids),
                    )
                conn.commit()  # only now: the vectors are already in Qdrant
                total += len(ids)
                commits += 1
                # A pass can span 100k docs — honor pause within seconds, not at
                # pass boundaries (checked every few commits).
                if commits % 5 == 0 and db.get_control(conn, "indexing", "running") == "paused":
                    stop.set()
                    for f in inflight:
                        f.cancel()
                    break
        # A dead staging drive must surface as a raise (embed-loop circuit-breaks
        # on it), not as a quietly short pass. Everything already committed stays
        # committed; the rest is still 'deduped' for the next run.
        if reader_err:
            raise reader_err[0]
    finally:
        stop.set()
        # Order matters: the embed pool is already shut down here (the `with`
        # exited), so no worker can still be blocked putting to up_q. Only now
        # is it safe to stop the upsert threads.
        for _ in upserters:
            up_q.put(_DONE)
        for t in upserters:
            t.join(timeout=60)
        reader.join(timeout=5)
        client.close()
    return total
