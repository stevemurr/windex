"""Guards for the 2026-07-16 overnight outage class: services dying mid-run
must fail fast and trip breakers, never wedge silently."""

import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

import windex.cli as cli_mod
from windex.cli import app
from windex.config import Settings

runner = CliRunner()


def test_pooled_connections_reuse_and_work(pg_dsn):
    from windex import db

    with db.pooled(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
    # same pool object per dsn; connections are recycled, not re-dialed
    assert db.pool(pg_dsn) is db.pool(pg_dsn)
    before = db.pool(pg_dsn).get_stats().get("connections_num", 0)
    for _ in range(5):
        with db.pooled(pg_dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
    after = db.pool(pg_dsn).get_stats().get("connections_num", 0)
    assert after <= max(before, 2)  # churn does not create new backends


def test_db_connect_fails_fast_on_dead_port():
    from windex import db

    t0 = time.monotonic()
    with pytest.raises(Exception):
        db.connect("postgresql://windex:windex@127.0.0.1:15432/windex")
    assert time.monotonic() - t0 < 15  # connect_timeout, not a TCP-stack hang


def test_embed_pending_fails_fast_when_qdrant_down(pg, settings, fake_embedder, monkeypatch, tmp_path):
    import windex.ccnews.embed_index as news_embed
    import windex.embed.pipeline as embed_pipeline

    monkeypatch.setattr(embed_pipeline, "build_embedder", lambda s, **kw: fake_embedder)
    dead = Settings(
        _env_file=None, data_root=settings.data_root, pg_dsn=settings.pg_dsn,
        qdrant_url="http://127.0.0.1:16333",  # nothing listens here
        embed_model=settings.embed_model, embed_dim=8,
    )
    text_ref = "news/clean/x.parquet"
    path = dead.staging_dir / text_ref
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"id": ["news:x"], "url": ["u"], "canonical_url": ["u"], "title": ["t"],
                  "published_at": [None], "lang": ["en"], "text": ["hello " * 40]}),
        path,
    )
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO documents (id, source, url, status, text_ref)
               VALUES ('news:x', 'news', 'u', 'deduped', %s)""",
            (text_ref,),
        )
    pg.commit()
    t0 = time.monotonic()
    with pytest.raises(Exception):
        news_embed.embed_pending(pg, dead, limit=5)
    assert time.monotonic() - t0 < 30  # raise, don't spin


def test_embed_loop_circuit_breaker_exits(settings, monkeypatch):
    import windex.ccnews.embed_index as news_embed

    monkeypatch.setattr(cli_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(
        news_embed, "embed_pending",
        lambda conn, s, limit=50_000: (_ for _ in ()).throw(RuntimeError("qdrant down")),
    )
    monkeypatch.setattr("time.sleep", lambda s: None)
    result = runner.invoke(app, ["ccnews", "embed-loop", "--max-consecutive-failures", "3"])
    assert result.exit_code == 2
    assert "circuit breaker tripped" in result.output


def test_embed_loop_exits_when_drained_and_no_processor(pg, settings, monkeypatch):
    import windex.ccnews.embed_index as news_embed

    monkeypatch.setattr(cli_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(news_embed, "embed_pending", lambda conn, s, limit=50_000: 0)
    monkeypatch.setattr(cli_mod, "_processor_alive", lambda: False)
    result = runner.invoke(app, ["ccnews", "embed-loop"])
    assert result.exit_code == 0
    assert "drained" in result.output


def test_new_collections_keep_memory_flat(qclient):
    """Regression: the outage config (always_ram quantization, in-RAM payload)
    must never come back for newly created collections."""
    from windex.index import qdrant as qidx

    name = qidx.ensure_collection(qclient, "news", "pytest-model-guard", dim=8)
    info = qclient.get_collection(name)
    assert info.config.params.on_disk_payload is True
    assert info.config.quantization_config.scalar.always_ram is False
    qclient.delete_collection(name)