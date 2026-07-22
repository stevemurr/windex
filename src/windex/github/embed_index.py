"""Compose, embed, and index hydrated repos. Creates the documents ledger rows
(id gh:owner/repo) with text_ref into clean parquet, mirroring the news flow so
/v1/docs works identically for both sources."""

import logging
import time
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from qdrant_client import QdrantClient
from qdrant_client import models as qm

from windex.ccnews.embed_index import point_id
from windex.config import Settings
from windex.embed import build_embedder
from windex.embed.base import embed_isolating
from windex.github.clean import clean_readme, compose_doc
from windex.index import qdrant as qidx

log = logging.getLogger("windex.github.embed_index")

CLEAN_SCHEMA = pa.schema(
    [("id", pa.string()), ("full_name", pa.string()), ("text", pa.string())]
)
MAX_DOC_CHARS = 100_000


def _readmes(readme_dir: Path) -> dict[int, str]:
    """repo_id → raw readme across all hydration parquet files."""
    out: dict[int, str] = {}
    if not readme_dir.exists():
        return out
    for f in sorted(readme_dir.glob("*.parquet")):
        # Belt-and-suspenders beyond hydrate's write-then-rename: a file can
        # still be truncated out from under us (the external volume has detached
        # mid-read on this box). Skip the one bad file loudly rather than let it
        # raise and fail every embed cycle for every repo.
        try:
            for batch in pq.ParquetFile(f).iter_batches(batch_size=1024):
                for row in batch.to_pylist():
                    out[row["repo_id"]] = row["readme"]
        except Exception as e:
            log.warning("skipping unreadable readme parquet %s: %r", f, e)
    return out


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    embedder = build_embedder(settings, bulk=True)
    from windex.index.sparse import bm25_model

    bm25 = bm25_model()
    client = QdrantClient(url=settings.qdrant_url, timeout=120)
    collection = qidx.ensure_collection(client, "repos", settings.embed_model, settings.embed_dim)

    readmes = _readmes(settings.repos_staging_dir / "readme")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT repo_id, full_name, description, topics, stars, primary_language, pushed_at
            FROM repos WHERE status = 'hydrated' ORDER BY stars DESC LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    if not rows:
        return 0

    text_ref = f"repos/clean/{time.strftime('%Y%m%d-%H%M%S')}.parquet"
    clean_path = settings.staging_dir / text_ref
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(clean_path, CLEAN_SCHEMA)

    total = 0
    max_chars = min(settings.embed_max_tokens * 4, MAX_DOC_CHARS)
    try:
        for start in range(0, len(rows), settings.embed_batch_size):
            batch = rows[start : start + settings.embed_batch_size]
            docs = []
            for repo_id, full_name, description, topics, stars, lang, pushed_at in batch:
                readme_text = clean_readme(readmes[repo_id]) if repo_id in readmes else None
                docs.append(
                    {
                        "id": f"gh:{full_name}",
                        "repo_id": repo_id,
                        "full_name": full_name,
                        "stars": stars,
                        "language": lang,
                        "topics": topics or [],
                        "description": description,
                        "pushed_at": pushed_at.isoformat() if pushed_at else None,
                        "text": compose_doc(full_name, description, topics, readme_text, MAX_DOC_CHARS),
                    }
                )
            texts = [d["text"][:max_chars] for d in docs]
            dense, ok = embed_isolating(embedder, texts)
            rejected = [d for d, good in zip(docs, ok) if not good]
            if rejected:
                # A repo the server refuses even on its own (still over the token
                # window, malformed, …): mark it 'failed' so the next pass stops
                # re-selecting it and wedging the loop forever, then carry on with
                # the rest of the batch.
                log.warning("gh embed: skipping %d unembeddable repo(s): %s",
                            len(rejected), ", ".join(d["full_name"] for d in rejected))
                with conn.cursor() as cur:
                    cur.execute("UPDATE repos SET status = 'failed' WHERE repo_id = ANY(%s)",
                                ([d["repo_id"] for d in rejected],))
                conn.commit()
            good = [(i, d) for i, d in enumerate(docs) if ok[i]]
            if not good:
                continue
            sparse = list(bm25.embed([texts[i] for i, _ in good]))
            points = [
                qm.PointStruct(
                    id=point_id(d["id"]),
                    vector={
                        qidx.DENSE: dense[i],
                        qidx.SPARSE: qm.SparseVector(
                            indices=sparse[j].indices.tolist(),
                            values=sparse[j].values.tolist(),
                        ),
                    },
                    payload={
                        "doc_id": d["id"],
                        "source": "github",
                        "url": f"https://github.com/{d['full_name']}",
                        "title": d["full_name"],
                        "snippet": (d["description"] or d["text"][:300])[:400],
                        "stars": d["stars"],
                        "language": d["language"],
                        "topics": d["topics"],
                        "pushed_at": d["pushed_at"],
                    },
                )
                for j, (i, d) in enumerate(good)
            ]
            gdocs = [d for _, d in good]
            client.upsert(collection_name=collection, points=points)
            writer.write_batch(
                pa.record_batch(
                    [
                        pa.array([d["id"] for d in gdocs]),
                        pa.array([d["full_name"] for d in gdocs]),
                        pa.array([d["text"] for d in gdocs]),
                    ],
                    schema=CLEAN_SCHEMA,
                )
            )
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO documents (id, source, url, title, status, text_ref, embedded_model, indexed_at)
                    VALUES (%s, 'github', %s, %s, 'embedded', %s, %s, now())
                    ON CONFLICT (id) DO UPDATE SET
                        text_ref = EXCLUDED.text_ref, embedded_model = EXCLUDED.embedded_model,
                        indexed_at = now(), status = 'embedded'
                    """,
                    [
                        (d["id"], f"https://github.com/{d['full_name']}", d["full_name"],
                         text_ref, settings.embed_model)
                        for d in gdocs
                    ],
                )
                cur.execute(
                    "UPDATE repos SET status = 'embedded' WHERE repo_id = ANY(%s)",
                    ([d["repo_id"] for d in gdocs],),
                )
            conn.commit()
            total += len(gdocs)
    finally:
        writer.close()
    return total
