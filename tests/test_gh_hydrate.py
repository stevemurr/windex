import json
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from windex.github import embed_index, hydrate


def _seed_candidates(pg, names):
    with pg.cursor() as cur:
        for i, name in enumerate(names, start=1):
            cur.execute(
                "INSERT INTO repos (repo_id, full_name, star_events) VALUES (%s, %s, 5)",
                (i, name),
            )
    pg.commit()


def _node(rid, name, stars, readme="# Hello\nA useful tool.", archived=False):
    return {
        "databaseId": rid,
        "nameWithOwner": name,
        "description": f"desc of {name}",
        "stargazerCount": stars,
        "pushedAt": "2026-07-01T00:00:00Z",
        "isArchived": archived,
        "primaryLanguage": {"name": "Go"},
        "defaultBranchRef": {"name": "main"},
        "repositoryTopics": {"nodes": [{"topic": {"name": "cli"}}]},
        "readme_md": {"text": readme} if readme else None,
        "readme_lower": None,
        "readme_rst": None,
        "readme_plain": None,
    }


def test_build_query_aliases_and_escapes():
    q = hydrate._build_query(['o1/r1', 'we"ird/na"me'])
    assert "r0: repository(owner: \"o1\", name: \"r1\")" in q
    assert '\\"' in q  # json escaping applied
    assert q.count("...repoFields") == 2


def test_extract_readme_fallback_order():
    node = {"readme_md": None, "readme_lower": None,
            "readme_rst": {"text": "rst wins"}, "readme_plain": {"text": "plain"}}
    assert hydrate._extract_readme(node) == "rst wins"
    assert hydrate._extract_readme({k: None for k in hydrate.README_EXPRESSIONS}) is None


def test_hydrate_statuses_and_readme_parquet(pg, settings, monkeypatch):
    _seed_candidates(pg, ["a/keeper", "b/small", "c/deleted"])
    responses = {
        "r0": _node(1, "a/keeper", stars=50),
        "r1": _node(2, "b/small", stars=3, readme=None),
        "r2": None,  # repo gone
    }
    monkeypatch.setattr(hydrate, "_post", lambda client, pool, q: {"data": responses})
    readme_dir = settings.repos_staging_dir / "readme"
    stats = hydrate.hydrate(
        pg, tokens=["t1"], readme_dir=readme_dir, star_threshold=10, limit=40
    )
    assert stats["hydrated"] == 1 and stats["below_threshold"] == 1 and stats["gone"] == 1
    assert stats["readmes"] == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status, stars, topics FROM repos WHERE full_name = 'a/keeper'")
        status, stars, topics = cur.fetchone()
        assert (status, stars, topics) == ("hydrated", 50, ["cli"])
        cur.execute("SELECT status FROM repos WHERE full_name = 'c/deleted'")
        assert cur.fetchone()[0] == "gone"
    table = pq.read_table(readme_dir / stats["readme_file"])
    assert table.num_rows == 1 and table.column("full_name")[0].as_py() == "a/keeper"


def test_hydrate_handles_full_name_collision(pg, settings, monkeypatch):
    """A candidate that resolves (via GitHub's redirect) to a full_name another
    repo_id already holds must not raise UniqueViolation out of hydrate() and
    wedge the batch forever (same batch re-selected + re-crashed every run). The
    incumbent is #stale-suffixed and the newer repo wins the name."""
    with pg.cursor() as cur:
        cur.execute("INSERT INTO repos (repo_id, full_name, star_events, status) "
                    "VALUES (1, 'o/candidate', 5, 'candidate')")
        cur.execute("INSERT INTO repos (repo_id, full_name, stars, status) "
                    "VALUES (2, 'o/taken', 99, 'hydrated')")
    pg.commit()
    # candidate 1 now resolves to 'o/taken', which repo_id 2 already owns
    responses = {"r0": _node(1, "o/taken", stars=50)}
    monkeypatch.setattr(hydrate, "_post", lambda client, pool, q: {"data": responses})

    stats = hydrate.hydrate(pg, tokens=["t1"], readme_dir=settings.repos_staging_dir / "readme",
                            star_threshold=10, limit=40)
    assert stats["hydrated"] == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status, full_name FROM repos WHERE repo_id = 1")
        assert cur.fetchone() == ("hydrated", "o/taken")  # newer repo won the name
        cur.execute("SELECT full_name FROM repos WHERE repo_id = 2")
        assert cur.fetchone()[0] == "o/taken#stale:2"  # incumbent suffixed


def test_hydrate_requires_tokens(pg, settings):
    with pytest.raises(ValueError, match="tokens"):
        hydrate.hydrate(pg, tokens=[], readme_dir=settings.repos_staging_dir, star_threshold=10)


def test_hydrate_hides_in_progress_file_then_publishes(pg, settings, monkeypatch):
    """The embed loop's *.parquet glob must never see the live writer; the final
    named file appears only once complete. Forces two batches so a mid-write
    observation happens after the first batch is written."""
    _seed_candidates(pg, ["a/one", "b/two", "c/three", "d/four"])
    ids = {"a/one": 1, "b/two": 2, "c/three": 3, "d/four": 4}
    readme_dir = settings.repos_staging_dir / "readme"
    monkeypatch.setattr(hydrate, "BATCH", 2)
    seen_parquet, seen_tmp = [], []

    def fake_post(client, pool, q):
        # snapshot what a concurrent embed cycle would glob at this moment
        seen_parquet.append(sorted(p.name for p in readme_dir.glob("*.parquet")))
        seen_tmp.append(sorted(p.name for p in readme_dir.glob("*.parquet.tmp")))
        data = {}
        for i, (owner_j, name_j) in enumerate(
            re.findall(r'repository\(owner: (".*?"), name: (".*?")\)', q)
        ):
            full = f"{json.loads(owner_j)}/{json.loads(name_j)}"
            data[f"r{i}"] = _node(ids[full], full, stars=50)
        return {"data": data}

    monkeypatch.setattr(hydrate, "_post", fake_post)
    stats = hydrate.hydrate(
        pg, tokens=["t1"], readme_dir=readme_dir, star_threshold=10, limit=40
    )

    assert all(not sp for sp in seen_parquet), seen_parquet  # glob never saw the live file
    assert any(st for st in seen_tmp), seen_tmp  # ...yet a write really was in progress
    final = readme_dir / stats["readme_file"]
    assert final.exists() and final.name.endswith(".parquet")
    assert not list(readme_dir.glob("*.parquet.tmp"))  # tmp renamed away on completion
    assert pq.read_table(final).num_rows == 4


def test_hydrate_removes_stale_tmp_on_start(pg, settings, monkeypatch, caplog):
    """A prior run SIGKILLed mid-close leaves a footerless *.parquet.tmp; the
    next hydrate sweeps it and says so."""
    readme_dir = settings.repos_staging_dir / "readme"
    readme_dir.mkdir(parents=True, exist_ok=True)
    stale = readme_dir / "20200101-000000.parquet.tmp"
    stale.write_bytes(b"garbage-no-footer")
    monkeypatch.setattr(hydrate, "_post", lambda client, pool, q: {"data": {}})

    with caplog.at_level("WARNING"):
        hydrate.hydrate(pg, tokens=["t1"], readme_dir=readme_dir, star_threshold=10, limit=40)

    assert not stale.exists()
    assert any("20200101-000000.parquet.tmp" in r.getMessage() for r in caplog.records)


def test_readmes_skips_unreadable_parquet(tmp_path, caplog):
    """One corrupt *.parquet must not fail the whole embed cycle: it is skipped
    with a loud warning and the valid rows still come back."""
    readme_dir = tmp_path / "readme"
    readme_dir.mkdir()
    pq.write_table(
        pa.table({"repo_id": [7], "full_name": ["a/b"], "readme": ["hello"]}),
        readme_dir / "0-good.parquet",
    )
    (readme_dir / "1-bad.parquet").write_bytes(b"not a parquet file")

    with caplog.at_level("WARNING"):
        out = embed_index._readmes(readme_dir)

    assert out == {7: "hello"}
    assert any("1-bad.parquet" in r.getMessage() for r in caplog.records)
