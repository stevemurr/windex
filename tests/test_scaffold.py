from windex.config import Settings
from windex.index.qdrant import alias_name, collection_name, slug


def test_settings_paths_derive_from_data_root(tmp_path):
    s = Settings(data_root=tmp_path, _env_file=None)
    assert s.ccnews_downloads_dir == tmp_path / "downloads" / "ccnews"
    assert s.news_staging_dir == tmp_path / "staging" / "news"


def test_github_token_list_parses_csv():
    s = Settings(github_tokens=" tok1, tok2 ,", _env_file=None)
    assert s.github_token_list() == ["tok1", "tok2"]


def test_collection_naming_is_slugged_and_aliased():
    assert slug("BAAI/bge-m3") == "baai-bge-m3"
    assert collection_name("news", "BAAI/bge-m3") == "news__baai-bge-m3"
    assert alias_name("news") == "news_current"


def test_ledger_probes_carry_no_source_predicate():
    """Regression guard (2026-07-17): `source = 'x' AND id = ANY(...)` makes the
    planner pick documents_source_published_idx with a rows=1 estimate — rare
    sources are absent from the MCV list — and scan every row of that source
    (measured 244s vs 63ms for the pkey plan). Ids are namespaced, so the
    predicate is redundant. Don't let it back in."""
    import pathlib
    import re

    pat = re.compile(r"source\s*=\s*'[a-z_]+'[^\"']*\bid\s*=\s*ANY", re.I)
    offenders = [
        f"{p}:{i}"
        for p in pathlib.Path("src/windex").rglob("*.py")
        for i, line in enumerate(p.read_text().splitlines(), 1)
        if pat.search(line)
    ]
    assert not offenders, f"source predicate alongside an id list: {offenders}"


def test_every_documents_batch_writer_locks_in_id_order():
    """Regression (2026-07-16): wiki ingest upserted `documents` in dump order
    while the embed loop UPDATEd the same rows in parquet order. Two
    transactions, same row locks, different order — deadlock. Postgres killed
    one and the whole shard failed; two shards died that way, leaving 5% of the
    wiki corpus unindexed. Sorting only one side does NOT help: a cycle needs
    only one writer out of order, so every batch writer must sort."""
    import pathlib
    import re

    writers = {
        "src/windex/wiki/ingest.py", "src/windex/docs_source/ingest.py",
        "src/windex/hn/harvest.py", "src/windex/arxiv/harvest.py",
        "src/windex/smallweb/poll.py", "src/windex/embed/pipeline.py",
    }
    for path in sorted(writers):
        src = pathlib.Path(path).read_text()
        assert re.search(r"sort\(|sorted\(", src), f"{path}: no sort before a documents batch write"

    # and the shared embed driver must sort the ids it locks
    driver = pathlib.Path("src/windex/embed/pipeline.py").read_text()
    assert "ids = sorted(ids)" in driver, "embed driver UPDATEs documents without ordering its locks"
