from datetime import date

from windex.github import discover


def _item(rid, name, stars):
    return {"id": rid, "full_name": name, "stargazers_count": stars,
            "description": "d", "language": "Rust", "pushed_at": "2026-01-01T00:00:00Z"}


def test_sweep_splits_shards_over_cap_and_upserts(pg, monkeypatch):
    calls = []

    def fake_get(client, token, params, retries=5):
        calls.append(params["q"])
        window = params["q"].split("created:")[1]
        a, b = window.split("..")
        span = (date.fromisoformat(b) - date.fromisoformat(a)).days
        if span > 40:  # wide shard reports over-cap → must split
            return {"total_count": 1500, "items": []}
        return {"total_count": 2, "items": [_item(hash(window) % 10_000, f"o/{window[:10]}-{a}", 12)]}

    monkeypatch.setattr(discover, "_get", fake_get)
    monkeypatch.setattr(discover.time, "sleep", lambda s: None)
    stats = discover.sweep(pg, tokens=["t"], star_threshold=10,
                           created_from=date(2025, 10, 1), created_to=date(2026, 1, 31))
    assert stats["shards"] >= 2                      # split happened
    assert stats["repos_new"] == stats["repos_seen"] >= 2
    assert any("created:2025-10-01" in c for c in calls)


def test_sweep_updates_existing_without_resetting_status(pg, monkeypatch):
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO repos (repo_id, full_name, stars, status) VALUES (7, 'o/r', 11, 'hydrated')"
        )
    pg.commit()
    monkeypatch.setattr(
        discover, "_get",
        lambda client, token, params, retries=5: {"total_count": 1, "items": [_item(7, "o/r", 99)]},
    )
    monkeypatch.setattr(discover.time, "sleep", lambda s: None)
    stats = discover.sweep(pg, tokens=["t"], star_threshold=10,
                           created_from=date(2026, 1, 1), created_to=date(2026, 1, 2))
    assert stats["repos_new"] == 0
    with pg.cursor() as cur:
        cur.execute("SELECT stars, status FROM repos WHERE repo_id = 7")
        assert cur.fetchone() == (99, "hydrated")
