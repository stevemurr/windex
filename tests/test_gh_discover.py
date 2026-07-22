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


class _Resp:
    def __init__(self, status, headers=None, body=None):
        self.status_code = status
        self.headers = headers or {}
        self.text = "limited" if status >= 400 else ""
        self._body = body or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        pass


class _Client:
    """Scripted httpx.Client stand-in: pops one response per get()."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, url, params=None, headers=None):
        self.calls += 1
        return self.responses.pop(0)


def test_get_retries_on_transport_error(monkeypatch):
    """A connection-level httpx error (dropped/half-closed socket, read timeout)
    must be retried with backoff, not propagate out of _get and crash the whole
    sweep — hydrate._post catches httpx.HTTPError for exactly this; _get didn't."""
    import httpx

    sleeps = []
    monkeypatch.setattr(discover.time, "sleep", lambda s: sleeps.append(s))

    class _FlakyClient:
        def __init__(self):
            self.calls = 0

        def get(self, url, params=None, headers=None):
            self.calls += 1
            if self.calls == 1:
                raise httpx.ReadTimeout("connection dropped mid-sweep")
            return _Resp(200, body={"total_count": 0, "items": []})

    client = _FlakyClient()
    out = discover._get(client, "t", {"q": "x", "page": 1})
    assert out["total_count"] == 0 and client.calls == 2
    assert len(sleeps) == 1  # backed off once, then succeeded


def test_get_secondary_limit_honors_retry_after(monkeypatch):
    sleeps = []
    monkeypatch.setattr(discover.time, "sleep", lambda s: sleeps.append(s))
    # secondary limit: 403 with remaining > 0 and a retry-after header —
    # the pre-fix handler read only x-ratelimit-reset and retried instantly
    client = _Client([
        _Resp(403, {"retry-after": "90", "x-ratelimit-remaining": "28"}),
        _Resp(200, body={"total_count": 0, "items": []}),
    ])
    out = discover._get(client, "t", {"q": "x", "page": 1})
    assert out["total_count"] == 0 and client.calls == 2
    assert sleeps == [90]  # waited exactly retry-after, not ~0s


def test_get_secondary_limit_escalates_and_respects_budget(monkeypatch):
    sleeps = []
    monkeypatch.setattr(discover.time, "sleep", lambda s: sleeps.append(s))
    client = _Client([_Resp(403, {"x-ratelimit-remaining": "28"})] * 50)
    try:
        discover._get(client, "t", {"q": "x", "page": 1}, budget=300)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "retry waiting" in str(e)
    assert sleeps == [60, 120]  # 60 → 120 doubled; next (240) would break 300s budget
    assert client.calls == 3


def test_get_primary_limit_waits_to_reset(monkeypatch):
    import time as _time
    sleeps = []
    monkeypatch.setattr(discover.time, "sleep", lambda s: sleeps.append(s))
    reset = str(int(_time.time()) + 30)
    client = _Client([
        _Resp(403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": reset}),
        _Resp(200, body={"total_count": 0, "items": []}),
    ])
    discover._get(client, "t", {"q": "x", "page": 1})
    assert len(sleeps) == 1 and 25 < sleeps[0] < 35  # to reset + 1, no escalation


def test_sweep_resumes_from_shard_ledger(pg, monkeypatch):
    calls = []

    def fake_get(client, token, params, budget=None):
        calls.append(params["q"])
        return {"total_count": 1, "items": [_item(41, "o/ledger", 12)]}

    monkeypatch.setattr(discover, "_get", fake_get)
    monkeypatch.setattr(discover.time, "sleep", lambda s: None)
    span = dict(created_from=date(2026, 2, 1), created_to=date(2026, 2, 10))
    s1 = discover.sweep(pg, tokens=["t"], star_threshold=10, **span)
    assert s1["shards"] == 1 and s1["shards_skipped"] == 0 and len(calls) == 1
    # second run: leaf is in the ledger → skipped before any request
    s2 = discover.sweep(pg, tokens=["t"], star_threshold=10, **span)
    assert s2["shards_skipped"] == 1 and s2["repos_seen"] == 0 and len(calls) == 1
    # fresh=True clears the ledger for the range and re-sweeps
    s3 = discover.sweep(pg, tokens=["t"], star_threshold=10, fresh=True, **span)
    assert s3["shards"] == 1 and len(calls) == 2
    # a different threshold is a different sweep — ledger must not match
    s4 = discover.sweep(pg, tokens=["t"], star_threshold=5, **span)
    assert s4["shards"] == 1 and len(calls) == 3
    with pg.cursor() as cur:
        cur.execute("SELECT discovered_at IS NOT NULL FROM repos WHERE repo_id = 41")
        assert cur.fetchone()[0]
