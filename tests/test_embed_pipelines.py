"""Embed/index steps against live Qdrant (pytest-model collections) with a fake
dense embedder — verifies status transitions, payloads, and point counts."""

import pyarrow as pa
import pyarrow.parquet as pq
from qdrant_client import QdrantClient

from conftest import QDRANT_URL

import windex.ccnews.embed_index as news_embed
import windex.github.embed_index as gh_embed
import windex.wiki.embed_index as wiki_embed


def _qdrant_count(name: str) -> int:
    return QdrantClient(url=QDRANT_URL).get_collection(name).points_count


def test_news_embed_pending(pg, settings, qclient, fake_embedder, monkeypatch):
    monkeypatch.setattr(news_embed, "build_embedder", lambda s: fake_embedder)
    text_ref = "news/clean/b1.parquet"
    path = settings.staging_dir / text_ref
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({
            "id": ["news:aa", "news:bb"],
            "url": ["https://x/a", "https://y/b"],
            "canonical_url": ["https://x/a", "https://y/b"],
            "title": ["Story A", "Story B"],
            "published_at": ["2026-07-13T00:00:00", None],
            "lang": ["en", "en"],
            "text": ["alpha " * 50, "beta " * 50],
        }),
        path,
    )
    with pg.cursor() as cur:
        cur.executemany(
            """INSERT INTO documents (id, source, url, title, status, text_ref)
               VALUES (%s, 'news', %s, %s, 'deduped', %s)""",
            [("news:aa", "https://x/a", "Story A", text_ref),
             ("news:bb", "https://y/b", "Story B", text_ref)],
        )
    pg.commit()

    n = news_embed.embed_pending(pg, settings, limit=10)
    assert n == 2
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE status='embedded' AND embedded_model=%s",
                    (settings.embed_model,))
        assert cur.fetchone()[0] == 2
    assert _qdrant_count("news__pytest-model") >= 2
    # payload carries the public contract fields
    pts = QdrantClient(url=QDRANT_URL).query_points(
        "news__pytest-model", query=fake_embedder.embed_batch(["alpha"])[0],
        using="dense", limit=1, with_payload=True,
    ).points
    assert pts and {"doc_id", "url", "title", "snippet", "outlet"} <= set(pts[0].payload)


def test_gh_embed_pending(pg, settings, qclient, fake_embedder, monkeypatch):
    monkeypatch.setattr(gh_embed, "build_embedder", lambda s: fake_embedder)
    readme_dir = settings.repos_staging_dir / "readme"
    readme_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"repo_id": pa.array([1], pa.int64()), "full_name": ["o/tool"],
                  "readme": ["# Tool\nDoes [things](https://x) fast."]}),
        readme_dir / "h1.parquet",
    )
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO repos (repo_id, full_name, stars, description, topics,
                                  primary_language, status)
               VALUES (1, 'o/tool', 55, 'a fast tool', ARRAY['cli'], 'Rust', 'hydrated')"""
        )
    pg.commit()

    n = gh_embed.embed_pending(pg, settings, limit=10)
    assert n == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM repos WHERE repo_id = 1")
        assert cur.fetchone()[0] == "embedded"
        cur.execute("SELECT text_ref FROM documents WHERE id = 'gh:o/tool'")
        text_ref = cur.fetchone()[0]
    table = pq.read_table(settings.staging_dir / text_ref)
    text = table.column("text")[0].as_py()
    assert text.startswith("o / tool") and "things fast" in text and "https://x" not in text
    assert _qdrant_count("repos__pytest-model") >= 1


def test_wiki_embed_pending(pg, settings, qclient, fake_embedder, monkeypatch):
    monkeypatch.setattr(wiki_embed, "build_embedder", lambda s: fake_embedder)
    text_ref = "wiki/clean/enwiki_content-20260712-00000.parquet"
    path = settings.staging_dir / text_ref
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({
            "id": ["wiki:12", "wiki:39"],
            "url": ["https://en.wikipedia.org/wiki/Anarchism",
                    "https://en.wikipedia.org/wiki/Autism"],
            "title": ["Anarchism", "Autism"],
            "revision_ts": ["2026-07-12T00:00:00Z", "2026-07-11T00:00:00Z"],
            "incoming_links": pa.array([1200, 800], pa.int64()),
            "opening_text": ["Anarchism is a philosophy.", "Autism is a condition."],
            "text": ["anarchism " * 50, "autism " * 50],
        }),
        path,
    )
    with pg.cursor() as cur:
        cur.executemany(
            """INSERT INTO documents (id, source, url, title, published_at, status, text_ref)
               VALUES (%s, 'wiki', %s, %s, %s, 'deduped', %s)""",
            [("wiki:12", "https://en.wikipedia.org/wiki/Anarchism", "Anarchism",
              "2026-07-12T00:00:00Z", text_ref),
             ("wiki:39", "https://en.wikipedia.org/wiki/Autism", "Autism",
              "2026-07-11T00:00:00Z", text_ref)],
        )
    pg.commit()

    n = wiki_embed.embed_pending(pg, settings, limit=10)
    assert n == 2
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE source='wiki' AND status='embedded' "
                    "AND embedded_model=%s", (settings.embed_model,))
        assert cur.fetchone()[0] == 2
    assert _qdrant_count("wiki__pytest-model") >= 2
    pts = QdrantClient(url=QDRANT_URL).query_points(
        "wiki__pytest-model", query=fake_embedder.embed_batch(["anarchism"])[0],
        using="dense", limit=1, with_payload=True,
    ).points
    assert pts and {"doc_id", "url", "title", "snippet", "incoming_links"} <= set(pts[0].payload)
    assert pts[0].payload["source"] == "wiki"
