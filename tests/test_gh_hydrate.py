import pyarrow.parquet as pq
import pytest

from windex.github import hydrate


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


def test_hydrate_requires_tokens(pg, settings):
    with pytest.raises(ValueError, match="tokens"):
        hydrate.hydrate(pg, tokens=[], readme_dir=settings.repos_staging_dir, star_threshold=10)
