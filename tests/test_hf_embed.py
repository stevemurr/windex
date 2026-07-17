"""The hf embed pass against live Qdrant (pytest-model collection) with a fake
dense embedder.

ALIAS SAFETY: `qidx.alias_name` is monkeypatched throughout. ensure_collection
points `<source>_current` at the collection it just made whenever that alias
doesn't exist yet — and `hf_current` does NOT exist in production yet, so an
unpatched run here would silently alias production's hf searches at a
pytest-model collection. (A test once deleted through the live `arxiv_current`
alias and hit production; same class of bug, one step earlier.)
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from qdrant_client import QdrantClient

from conftest import QDRANT_URL

import windex.embed.pipeline as embed_pipeline
import windex.hf.embed_index as hf_embed
from windex.index import qdrant as qidx

TEST_ALIAS = "hf_pytest_alias"


@pytest.fixture(autouse=True)
def _isolate_alias(monkeypatch):
    monkeypatch.setattr(qidx, "alias_name", lambda source: f"{source}_pytest_alias")


def _stage(pg, settings, rows, text_ref="hf/clean/docs__transformers.parquet"):
    path = settings.staging_dir / text_ref
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({k: [r[k] for r in rows] for k in rows[0]}), path)
    with pg.cursor() as cur:
        cur.executemany(
            """INSERT INTO documents (id, source, url, canonical_url, title, status, text_ref)
               VALUES (%s, 'hf', %s, %s, %s, 'deduped', %s)""",
            sorted((r["id"], r["url"], r["url"], r["title"], text_ref) for r in rows),
        )
    pg.commit()


DOC_ROW = {
    "id": "hf:docs/transformers/quicktour",
    "url": "https://huggingface.co/docs/transformers/quicktour",
    "title": "Quickstart",
    "kind": "docs",
    "root": "transformers",
    "version": "v5.14.0",
    "license": "Apache-2.0",
    "published_at": "",
    "text": "Transformers is designed to be fast and easy to use. " * 20,
}
BLOG_ROW = {
    "id": "hf:blog/nvidia/nemotron",
    "url": "https://huggingface.co/blog/nvidia/nemotron",
    "title": "Nemotron wins RTEB",
    "kind": "blog",
    "root": "blog",
    "version": "",
    "license": "",
    "published_at": "2026-07-16T16:01:21+00:00",
    "text": "We are pleased to report benchmark results. " * 20,
}


def test_hf_embed_pending(pg, settings, qclient, fake_embedder, monkeypatch):
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    _stage(pg, settings, [DOC_ROW, BLOG_ROW])

    n = hf_embed.embed_pending(pg, settings, limit=10)
    assert n == 2
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE source='hf' AND status='embedded' "
                    "AND embedded_model=%s", (settings.embed_model,))
        assert cur.fetchone()[0] == 2

    client = QdrantClient(url=QDRANT_URL)
    assert client.get_collection("hf__pytest-model").points_count >= 2
    pts = client.scroll("hf__pytest-model", limit=10, with_payload=True)[0]
    payloads = {p.payload["doc_id"]: p.payload for p in pts}

    doc = payloads["hf:docs/transformers/quicktour"]
    assert {"doc_id", "url", "title", "snippet", "kind", "root", "version",
            "attribution", "source"} <= set(doc)
    assert doc["source"] == "hf"
    assert doc["kind"] == "docs" and doc["root"] == "transformers"
    assert doc["version"] == "v5.14.0"
    assert doc["url"] == "https://huggingface.co/docs/transformers/quicktour"
    assert len(doc["snippet"]) <= 400
    assert doc["attribution"] == "Apache-2.0"
    # Reference pages aren't dated: the key is omitted rather than set to null,
    # so the datetime-indexed field only ever sees real timestamps.
    assert "published_at" not in doc

    blog = payloads["hf:blog/nvidia/nemotron"]
    assert blog["kind"] == "blog" and blog["root"] == "blog"
    assert blog["published_at"] == "2026-07-16T16:01:21+00:00"


def test_hf_embed_is_idempotent(pg, settings, qclient, fake_embedder, monkeypatch):
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    _stage(pg, settings, [DOC_ROW])
    assert hf_embed.embed_pending(pg, settings, limit=10) == 1
    # nothing left pending; a re-run is a no-op rather than a duplicate
    assert hf_embed.embed_pending(pg, settings, limit=10) == 0


def test_hf_spec_matches_the_staged_schema():
    """The SourceSpec projects columns out of parquet by name — a drift between
    the crawl's schema and the spec's projection is an unreadable text_ref."""
    from windex.hf.crawl import CLEAN_SCHEMA

    assert set(hf_embed.SPEC.columns) <= set(CLEAN_SCHEMA.names)
    assert hf_embed.SPEC.text_field in CLEAN_SCHEMA.names
    assert hf_embed.SPEC.source == "hf" and hf_embed.SPEC.collection == "hf"


def test_long_pages_are_truncated_not_chunked(pg, settings, qclient, fake_embedder, monkeypatch):
    """The chunking decision, asserted: a huge page (main_classes/pipelines
    extracts to 141k chars) stays ONE document with ONE stable id. The shared
    driver bounds the embedded text at embed_max_tokens, exactly as it does for
    wiki and DevDocs — and the FULL text stays in parquet, so if windex ever
    adopts chunking it is a re-embed across all sources, never a re-crawl."""
    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    huge = dict(DOC_ROW, id="hf:docs/transformers/main_classes/pipelines",
                url="https://huggingface.co/docs/transformers/main_classes/pipelines",
                title="Pipelines", text="The pipeline abstraction. " * 8000)
    _stage(pg, settings, [huge])

    seen = []
    real = embed_pipeline.compose_text
    monkeypatch.setattr(embed_pipeline, "compose_text",
                        lambda row, tf, mc: seen.append(real(row, tf, mc)) or seen[-1])

    assert hf_embed.embed_pending(pg, settings, limit=10) == 1
    assert len(seen) == 1, "a page must embed as exactly one document"
    assert len(seen[0]) <= settings.embed_max_tokens * 4 + len("Pipelines\n\n")
    assert seen[0].startswith("Pipelines\n\n")  # title leads: findable by what it IS

    # The collection is shared across this session's tests, so look only at the
    # points this page produced: exactly one, id unsuffixed.
    pts = QdrantClient(url=QDRANT_URL).scroll("hf__pytest-model", limit=100,
                                              with_payload=True)[0]
    mine = [p.payload["doc_id"] for p in pts
            if p.payload["doc_id"].startswith("hf:docs/transformers/main_classes/pipelines")]
    assert mine == ["hf:docs/transformers/main_classes/pipelines"]
    assert not any("#" in i for i in mine)  # no chunk suffixes anywhere

    # the full text is still on disk, unabridged
    table = pq.read_table(settings.staging_dir / "hf/clean/docs__transformers.parquet")
    assert len(table.to_pylist()[0]["text"]) == len(huge["text"])
