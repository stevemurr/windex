"""`windex reset` — the clean-slate path.

Two things are being asserted, and the second matters more than the first: that it
destroys what a fresh ingest must forget, and that it does NOT destroy how the box
is configured. A reset that also wiped settings, the schedule and registered
sources would be a footgun dressed as a clean slate — you would reset once and
then spend an hour rediscovering your own tuning.
"""

import pytest
from typer.testing import CliRunner

from windex.cli import app as cli

runner = CliRunner()


@pytest.fixture(autouse=True)
def _never_touch_production_qdrant(monkeypatch):
    """`reset` DELETES collections, and the `settings` fixture points at the real
    Qdrant on 127.0.0.1:6333. This test file once deleted all 13 production
    collections — the corpus survived (extracted text is the source of truth) but
    every vector had to be rebuilt.

    Two belts, because one was not enough:
      * `reset` now only deletes collections it owns, for its configured model
        (see the comment in cli.reset). That is the structural fix.
      * This fixture pins the model to a name production never uses, so even a
        regression in that scoping cannot reach a real collection from here.
    """
    monkeypatch.setattr("windex.index.qdrant.alias_name",
                        lambda source: f"{source}_pytest_alias")


@pytest.fixture()
def seeded(pg, settings, monkeypatch):
    """A database that looks lived-in: corpus, watermarks, run history, settings,
    a registered source, and a staged parquet file."""
    monkeypatch.setattr(settings, "embed_model", "pytest-reset-model")
    monkeypatch.setattr("windex.cli.get_settings", lambda: settings)
    with pg.cursor() as cur:
        # source_config is deliberately absent from conftest's truncate list (it is
        # config, not data — the same reason `reset` leaves it alone), so this file
        # clears its own.
        cur.execute("DELETE FROM source_config")
        cur.execute("INSERT INTO documents (id, source, url, status) "
                    "VALUES ('news:a', 'news', 'https://e.dev/a', 'embedded')")
        cur.execute("INSERT INTO warc_files (path, status) VALUES ('w.gz', 'done')")
        cur.execute("INSERT INTO docsets (slug, mtime, ingested_mtime, status) "
                    "VALUES ('rust', 5, 5, 'done')")
        cur.execute("INSERT INTO source_units (source, store, unit_key) "
                    "VALUES ('ccnews', 'warc', 'w.gz')")
        cur.execute("INSERT INTO runs (recipe, source, spec, dedupe_key) "
                    "VALUES ('r', 's', '{}', 'k')")
        cur.execute("INSERT INTO control (key, value) VALUES ('ingest_ts_wiki', '123')")
        cur.execute("INSERT INTO control (key, value) VALUES ('indexing', 'running')")
        cur.execute("INSERT INTO source_config (scope, settings) "
                    "VALUES ('hf', '{\"hf_blog_batch\": 7}')")
        cur.execute("INSERT INTO custom_sources (name) VALUES ('notes')")
        # Owns its own schedule row rather than relying on init_db's seed: the
        # --drop-settings test truncates the table, and a later test must not
        # inherit that.
        cur.execute("INSERT INTO schedule (name, kind, target, hour, minute) "
                    "VALUES ('reset-probe', 'ingest', 'hf', 3, 0) "
                    "ON CONFLICT (name) DO NOTHING")
    pg.commit()
    ref = settings.staging_dir / "news" / "x.parquet"
    ref.parent.mkdir(parents=True, exist_ok=True)
    ref.write_bytes(b"not really parquet")
    return pg


def _count(pg, table, where="TRUE"):
    with pg.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table} WHERE {where}")
        return cur.fetchone()[0]


def test_reset_clears_corpus_watermarks_runs_and_parquet(seeded, settings, qclient):
    r = runner.invoke(cli, ["reset", "--yes"])
    assert r.exit_code == 0, r.output

    assert _count(seeded, "documents") == 0
    assert _count(seeded, "warc_files") == 0
    assert _count(seeded, "docsets") == 0
    assert _count(seeded, "source_units") == 0
    assert _count(seeded, "runs") == 0
    # The staging tree is recreated empty, not left containing stale batches.
    assert settings.staging_dir.exists()
    assert list(settings.staging_dir.rglob("*.parquet")) == []


def test_reset_preserves_configuration_by_default(seeded, settings, qclient):
    """The distinction the whole command is built around: data goes, config stays."""
    assert runner.invoke(cli, ["reset", "--yes"]).exit_code == 0

    assert _count(seeded, "source_config") == 1, "settings overrides must survive"
    assert _count(seeded, "custom_sources") == 1, "registered sources must survive"
    assert _count(seeded, "schedule", "name = 'reset-probe'") == 1, \
        "the schedule must survive"


def test_reset_clears_stale_ingest_watermarks_but_not_control_flags(seeded, qclient):
    """`ingest_ts_*` says "this source is up to date as of X". Left behind, a fresh
    index would claim to be current with nothing in it — and the scheduler would
    happily skip the first run."""
    assert runner.invoke(cli, ["reset", "--yes"]).exit_code == 0
    assert _count(seeded, "control", "key LIKE 'ingest_ts_%'") == 0
    assert _count(seeded, "control", "key = 'indexing'") == 1


def test_drop_flags_opt_into_the_wider_blast_radius(seeded, qclient):
    assert runner.invoke(
        cli, ["reset", "--yes", "--drop-sources", "--drop-settings"]).exit_code == 0
    assert _count(seeded, "custom_sources") == 0
    assert _count(seeded, "source_config") == 0
    assert _count(seeded, "schedule") == 0


def test_keep_staging_retains_extracted_text(seeded, settings, qclient):
    """The `reindex` middle ground: drop vectors, keep the text that derives them,
    so a model swap does not force a re-crawl."""
    assert runner.invoke(cli, ["reset", "--yes", "--keep-staging"]).exit_code == 0
    assert list(settings.staging_dir.rglob("*.parquet")) != []
    assert _count(seeded, "documents") == 0


def test_reset_without_yes_aborts_and_changes_nothing(seeded, qclient):
    r = runner.invoke(cli, ["reset"], input="n\n")
    assert r.exit_code != 0
    assert _count(seeded, "documents") == 1


def test_reset_only_deletes_collections_it_owns(seeded, settings, qclient, capsys):
    """The guard that matters. `reset` must never enumerate-and-delete the whole
    cluster: Qdrant may be shared, and a destructive command that can only reach
    its own named objects cannot be aimed elsewhere by accident.

    Plants a collection belonging to a different model and a foreign one, and
    asserts both survive.
    """
    from qdrant_client import models as qm

    from conftest import QDRANT_URL
    from qdrant_client import QdrantClient

    c = QdrantClient(url=QDRANT_URL)
    mine = f"news__{settings.embed_model}"          # this windex, this model
    other_model = "news__some-other-model"          # this source, a DIFFERENT model
    foreign = "someone_elses_app"                   # not windex at all
    for name in (mine, other_model, foreign):
        if c.collection_exists(name):
            c.delete_collection(name)
        c.create_collection(name, vectors_config={
            "dense": qm.VectorParams(size=4, distance=qm.Distance.COSINE)})
    try:
        assert runner.invoke(cli, ["reset", "--yes"]).exit_code == 0
        assert not c.collection_exists(mine), "should have dropped its own"
        assert c.collection_exists(other_model), "another model's is not ours to drop"
        assert c.collection_exists(foreign), "a foreign collection must be untouched"
    finally:
        for name in (mine, other_model, foreign):
            if c.collection_exists(name):
                c.delete_collection(name)
