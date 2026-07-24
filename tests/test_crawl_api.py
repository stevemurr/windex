"""API surface: auth gating, validation mapping, run lifecycle, SSE shape.

The network is stubbed — these test the contract, not the crawler.
"""

import pytest
from fastapi.testclient import TestClient

import windex.api.app as app_mod
import windex.api.service as service_mod
from windex.api.app import app
from windex.crawl import recipe as R
from windex.crawl import run as crun
from windex.custom_source import registry

TOKEN = "test-write-token"


@pytest.fixture()
def client(settings, monkeypatch, pg):
    monkeypatch.setattr(settings, "write_token", TOKEN)
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(service_mod, "get_settings", lambda: settings, raising=False)
    return TestClient(app)


def auth(extra=None):
    h = {"Authorization": f"Bearer {TOKEN}"}
    h.update(extra or {})
    return h


# --- auth gating ------------------------------------------------------------

@pytest.mark.parametrize("path,body", [
    ("/v1/crawl", {"source": "x", "seed": "https://e.dev/"}),
    ("/v1/crawl/preview", {"seed": "https://e.dev/"}),
    ("/v1/crawl/runs/1/cancel", None),
])
def test_writes_require_token(client, path, body):
    assert client.post(path, json=body).status_code == 401
    assert client.post(path, json=body, headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_reads_are_open(client):
    assert client.get("/v1/crawl/runs").status_code == 200


def test_manage_page_is_served(client):
    r = client.get("/manage")
    assert r.status_code == 200
    assert "/static/manage.js" in r.text


def test_crawl_url_redirects_to_the_manage_tab(client):
    # /crawl is documented in the README and already bookmarked, so the rename to
    # a /manage tab has to keep it working. 308 (not 302) so the method is
    # preserved and clients cache the move.
    r = client.get("/crawl", follow_redirects=False)
    assert r.status_code == 308
    assert r.headers["location"] == "/manage#crawl"


# --- validation → 422 -------------------------------------------------------

@pytest.mark.parametrize("body,fragment", [
    ({"source": "ok", "seed": "ftp://e.dev/x"}, "http"),
    ({"source": "ok"}, "seed"),
    ({"source": "ok", "seed": "https://e.dev/", "scope": {"include": ["[bad"]}}, "invalid regex"),
    # A reserved source name must be refused: a custom source may not shadow a
    # built-in corpus source.
    ({"source": "news", "seed": "https://e.dev/"}, "reserved"),
    ({"source": "Bad Name", "seed": "https://e.dev/"}, "invalid source name"),
])
def test_bad_requests_are_422(client, body, fragment):
    r = client.post("/v1/crawl", json=body, headers=auth())
    assert r.status_code == 422
    assert fragment in r.json()["detail"]


@pytest.mark.parametrize("path,body", [
    ("/v1/crawl", {"source": "ok", "seed": "https://e.dev/d/",
                   "scope": {"path_prfix": "/d/"}}),
    ("/v1/crawl", {"source": "ok", "seed": "https://e.dev/", "limits": {"max_page": 5}}),
    ("/v1/crawl", {"source": "ok", "seed": "https://e.dev/", "dedup": {"prunee": True}}),
    ("/v1/crawl/preview", {"seed": "https://e.dev/d/", "scope": {"same_hostt": True}}),
])
def test_misspelled_recipe_key_is_refused_not_dropped(client, path, body):
    """The sections were `dict`, so an unknown key was silently discarded — and
    `{"scope": {"path_prfix": "/d/"}}` therefore crawled the WHOLE HOST rather
    than the intended subtree. It must name the offending key instead."""
    r = client.post(path, json=body, headers=auth())
    assert r.status_code == 422
    bad = next(iter(k for k in ("path_prfix", "max_page", "prunee", "same_hostt")
                    if k in str(r.json())), None)
    assert bad is not None, r.json()


def test_stored_recipe_round_trips_through_start(client, settings):
    """The console's Re-run posts a past run's frozen recipe back verbatim, and
    `Recipe.to_dict()` includes `version` plus every section key. `extra=forbid`
    must accept its own output or Re-run breaks."""
    stored = R.parse({"seed": "https://e.dev/d/"}, settings).to_dict()
    r = client.post("/v1/crawl", json={"source": "rr_t", **stored}, headers=auth())
    assert r.status_code == 202, r.json()
    assert r.json()["recipe"] == stored


def test_unset_keys_keep_their_parse_defaults(client, settings):
    """`exclude_none` is load-bearing: an absent key and a null one differ. A
    missing `drop_boilerplate` must stay True, and a missing `path_prefix` must
    still mean the seed's own directory rather than the whole host."""
    r = client.post("/v1/crawl", json={"source": "def_t", "seed": "https://e.dev/d/x"},
                    headers=auth())
    assert r.status_code == 202, r.json()
    recipe = r.json()["recipe"]
    assert recipe["dedup"]["drop_boilerplate"] is True
    assert recipe["scope"]["path_prefix"] == "/d/"


# --- lifecycle --------------------------------------------------------------

def test_start_queues_a_run_and_persists_recipe(client, pg, settings):
    r = client.post("/v1/crawl", json={
        "source": "cookbook_t", "title": "T",
        "seed": "https://e.dev/docs/", "limits": {"max_depth": 1},
    }, headers=auth())
    assert r.status_code == 202
    run_id = r.json()["run_id"]

    # Queued, not executed: the worker owns execution.
    detail = client.get(f"/v1/crawl/runs/{run_id}").json()
    assert detail["status"] == "pending"
    assert detail["recipe"]["limits"]["max_depth"] == 1

    # The recipe is also stored on the source, so it is re-runnable/schedulable.
    info = registry.get(pg, "cookbook_t")
    assert info["recipe"]["seeds"] == ["https://e.dev/docs/"]


def test_start_is_idempotent_for_an_existing_source(client, pg):
    body = {"source": "cookbook_t", "seed": "https://e.dev/docs/"}
    assert client.post("/v1/crawl", json=body, headers=auth()).status_code == 202
    # Second call must not 409 — re-crawling an existing source is normal.
    assert client.post("/v1/crawl", json=body, headers=auth()).status_code == 202
    assert len(client.get("/v1/crawl/runs").json()["runs"]) == 2


def test_runs_filter_by_source(client):
    client.post("/v1/crawl", json={"source": "src_a", "seed": "https://a.dev/"}, headers=auth())
    client.post("/v1/crawl", json={"source": "src_b", "seed": "https://b.dev/"}, headers=auth())
    only_a = client.get("/v1/crawl/runs?source=src_a").json()["runs"]
    assert [r["source"] for r in only_a] == ["src_a"]
    assert len(client.get("/v1/crawl/runs").json()["runs"]) == 2


def test_unknown_run_is_404(client):
    assert client.get("/v1/crawl/runs/99999").status_code == 404


def test_cancel_marks_pending_run_cancelled(client):
    run_id = client.post("/v1/crawl", json={"source": "c_t", "seed": "https://e.dev/"},
                         headers=auth()).json()["run_id"]
    r = client.post(f"/v1/crawl/runs/{run_id}/cancel", headers=auth())
    assert r.status_code == 200 and r.json()["status"] == "cancelled"
    assert client.get(f"/v1/crawl/runs/{run_id}").json()["status"] == "cancelled"


def test_cancel_of_finished_run_is_409(client):
    run_id = client.post("/v1/crawl", json={"source": "c_t", "seed": "https://e.dev/"},
                         headers=auth()).json()["run_id"]
    client.post(f"/v1/crawl/runs/{run_id}/cancel", headers=auth())
    assert client.post(f"/v1/crawl/runs/{run_id}/cancel", headers=auth()).status_code == 409


# --- SSE --------------------------------------------------------------------

def test_sse_emits_run_and_url_events(client, pg, settings):
    registry.create(pg, "sse_t", "T", "")
    recipe = R.parse({"seeds": ["https://e.dev/docs/"]}, settings)
    run_id = crun.create_run(pg, "sse_t", recipe)
    with pg.cursor() as cur:
        crun.enqueue(cur, run_id, [("https://e.dev/docs/a", 1)])
        crun.mark(cur, run_id, [("https://e.dev/docs/a", "staged", "")])
    pg.commit()

    body = client.get(f"/v1/crawl/runs/{run_id}/events?ticks=1").text
    assert "event: run" in body
    assert "event: urls" in body
    assert "https://e.dev/docs/a" in body


def test_sse_reports_unknown_run(client):
    body = client.get("/v1/crawl/runs/98765/events?ticks=1").text
    assert "event: error" in body
