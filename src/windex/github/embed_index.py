"""Compose, embed, and index hydrated repos. Creates the documents ledger rows
(id gh:owner/repo) with text_ref into clean parquet, mirroring the news flow so
/v1/docs works identically for both sources."""

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
from windex.github.clean import clean_readme, compose_doc
from windex.index import qdrant as qidx

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
        for batch in pq.ParquetFile(f).iter_batches(batch_size=1024):
            for row in batch.to_pylist():
                out[row["repo_id"]] = row["readme"]
    return out


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    embedder = build_embedder(settings)
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
            dense = embedder.embed_batch([d["text"][:max_chars] for d in docs])
            sparse = list(bm25.embed([d["text"][:max_chars] for d in docs]))
            points = [
                qm.PointStruct(
                    id=point_id(d["id"]),
                    vector={
                        qidx.DENSE: dense[i],
                        qidx.SPARSE: qm.SparseVector(
                            indices=sparse[i].indices.tolist(),
                            values=sparse[i].values.tolist(),
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
                for i, d in enumerate(docs)
            ]
            client.upsert(collection_name=collection, points=points)
            writer.write_batch(
                pa.record_batch(
                    [
                        pa.array([d["id"] for d in docs]),
                        pa.array([d["full_name"] for d in docs]),
                        pa.array([d["text"] for d in docs]),
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
                        for d in docs
                    ],
                )
                cur.execute(
                    "UPDATE repos SET status = 'embedded' WHERE repo_id = ANY(%s)",
                    ([d["repo_id"] for d in docs],),
                )
            conn.commit()
            total += len(docs)
    finally:
        writer.close()
    return total
