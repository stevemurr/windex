"""Shared fixtures. Integration tests run against the live dev services using
isolated namespaces (windex_test database; *__pytest-model collections) and
skip cleanly when a service isn't running."""

import hashlib
from pathlib import Path

import psycopg
import pytest

from windex import db as windex_db
from windex.config import Settings

ADMIN_DSN = "postgresql://windex:windex@127.0.0.1:5432/windex"
# Per-checkout DB name: concurrent agent worktrees each run their own suite,
# and a shared name means one suite force-drops the DB out from under another
# (observed 2026-07-16 by two builders simultaneously).
_checkout_key = hashlib.sha1(str(Path(__file__).resolve().parent).encode()).hexdigest()[:8]
TEST_DB = f"windex_test_{_checkout_key}"
TEST_DSN = f"postgresql://windex:windex@127.0.0.1:5432/{TEST_DB}"
QDRANT_URL = "http://127.0.0.1:6333"
TEST_MODEL = "pytest-model"


@pytest.fixture(scope="session")
def pg_dsn():
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("postgres not running (scripts/dev.sh up)")
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    with psycopg.connect(TEST_DSN) as conn:
        windex_db.init_db(conn)
        windex_db.init_db(conn)  # idempotency is part of the contract
    yield TEST_DSN
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
    admin.close()


@pytest.fixture()
def pg(pg_dsn):
    conn = psycopg.connect(pg_dsn)
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE documents, warc_files, repos, gharchive_files, gh_shards, minhash_bands, "
            "control, wiki_dumps, arxiv_windows, feeds, docsets, hn_windows, "
            "hf_roots, hf_posts, search_metrics, custom_sources"
        )
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def qclient():
    from qdrant_client import QdrantClient

    client = QdrantClient(url=QDRANT_URL, timeout=30)
    try:
        client.get_collections()
    except Exception:
        pytest.skip("qdrant not running (scripts/dev.sh up)")
    yield client
    for c in client.get_collections().collections:
        if TEST_MODEL in c.name:
            client.delete_collection(c.name)


@pytest.fixture()
def settings(pg_dsn, tmp_path):
    return Settings(
        _env_file=None,
        data_root=tmp_path,
        pg_dsn=pg_dsn,
        qdrant_url=QDRANT_URL,
        embed_model=TEST_MODEL,
        embed_dim=8,
        embed_batch_size=4,
    )


class FakeEmbedder:
    """Deterministic 8-dim embedder: hash-seeded, no network."""

    model_id = TEST_MODEL
    dim = 8

    def embed_batch(self, texts):
        out = []
        for t in texts:
            h = hash(t) & 0xFFFF
            out.append([((h >> i) & 1) * 1.0 + 0.1 for i in range(8)])
        return out

    def ping(self):
        return True

    def close(self):
        pass  # honors the Embedder.close() contract (a one-off pass closes it)


@pytest.fixture()
def fake_embedder():
    return FakeEmbedder()


@pytest.fixture(autouse=True)
def _reset_query_embed_breaker():
    """The query-embed breaker is process-global by design (see
    index/embed_breaker.py). Tests that embed against a dead endpoint leave real
    failures on it, so a cold breaker per test keeps ordering from deciding
    whether a later search embeds or short-circuits."""
    from windex.index.embed_breaker import breaker

    breaker.reset()
    yield
    breaker.reset()
