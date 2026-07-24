"""The /admin/v1 control plane: the auth posture, and the two contracts staying apart.

The operational routes are defined once and served twice — at /admin/v1 (gated)
and at /v1 (deprecated alias, so the running console keeps working until the
native client replaces it). These tests pin the properties that make that split
worth having, and the two regressions it can quietly cause.
"""

import pytest
from fastapi.testclient import TestClient

import windex.api.app as app_mod
import windex.api.service as service_mod
from windex.api import prom
from windex.api.app import app

TOKEN = "admin-test-token"


@pytest.fixture()
def client(settings, monkeypatch):
    monkeypatch.setattr(settings, "write_token", TOKEN)
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    monkeypatch.setattr(service_mod, "get_settings", lambda: settings, raising=False)
    service_mod._pg_stats_cache.clear()
    service_mod._pg_heavy_cache.clear()
    return TestClient(app)


def auth():
    return {"Authorization": f"Bearer {TOKEN}"}


# --- auth posture -----------------------------------------------------------

def test_health_is_the_one_open_admin_route(client):
    """A client must be able to ask 'are you there, do you want a token' BEFORE it
    can pair — but nothing else should answer unauthenticated."""
    r = client.get("/admin/v1/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and body["auth_required"] is True
    # and it leaks nothing about the index
    assert not {"documents", "sources", "pg_dsn"} & set(body)


@pytest.mark.parametrize("path", [
    "/admin/v1/whoami", "/admin/v1/jobs", "/admin/v1/loops", "/admin/v1/settings",
    "/admin/v1/schedule", "/admin/v1/logs", "/admin/v1/freshness",
])
def test_admin_is_gated_by_default(client, path):
    """Gating is at the MOUNT, not per route — per-route opt-in is exactly why ~35
    operational routes ended up ungated on /v1."""
    assert client.get(path).status_code == 401
    assert client.get(path, headers=auth()).status_code == 200


def test_admin_rejects_a_wrong_token_and_accepts_a_lowercase_scheme(client):
    assert client.get("/admin/v1/jobs", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert client.get("/admin/v1/jobs", headers={"Authorization": f"bearer {TOKEN}"}).status_code == 200


def test_loopback_without_a_token_stays_open(client, settings, monkeypatch):
    """Local dev must not need a token; `curl localhost:8100/admin/v1/jobs` on the
    box is a supported workflow."""
    monkeypatch.setattr(settings, "write_token", "")
    monkeypatch.setattr(settings, "serve_host", "127.0.0.1")
    assert client.get("/admin/v1/jobs").status_code == 200


def test_off_loopback_without_a_token_fails_closed(client, settings, monkeypatch):
    """The admin surface can make the server fetch a caller-chosen host. Exposed on
    the LAN with no token that is an open SSRF proxy, so it must refuse to serve
    rather than serve openly — and say how to fix it."""
    monkeypatch.setattr(settings, "write_token", "")
    monkeypatch.setattr(settings, "serve_host", "0.0.0.0")
    r = client.get("/admin/v1/jobs")
    assert r.status_code == 503
    assert "WINDEX_WRITE_TOKEN" in r.json()["detail"]
    # ...while the agent read API is unaffected: open reads are the product.
    assert client.get("/v1/search", params={"q": "x"}).status_code == 200
    # and health still answers, so a client can diagnose the 503
    assert client.get("/admin/v1/health").status_code == 200


# --- the two contracts ------------------------------------------------------

def test_v1_aliases_still_serve_the_console(client):
    """Deleting /v1's operational routes now would break the running dashboard.
    The alias is what makes stopping partway a supported outcome."""
    for path in ("/v1/jobs", "/v1/loops", "/v1/settings", "/v1/freshness"):
        assert client.get(path).status_code == 200, path


def test_agent_contract_is_untouched_and_ungated(client):
    """/v1 search + docs must never require a token — that is the product."""
    assert client.get("/v1/search", params={"q": "x"}).status_code == 200
    assert client.get("/v1/stats").status_code == 200


def test_admin_and_v1_have_separate_schemas(client):
    """Separate OpenAPI documents are half the reason for the split: a generated
    admin client must not drag in the search contract, and regenerating it must
    not touch search types."""
    main = client.get("/openapi.json").json()
    adm = client.get("/admin/openapi.json").json()
    assert "/v1/search" in main["paths"]
    assert "/v1/search" not in adm["paths"]
    assert "/v1/jobs" in adm["paths"]
    assert "/v1/health" in adm["paths"]
    # the alias is advertised as deprecated so a client knows not to build on it
    assert main["paths"]["/v1/jobs"]["get"].get("deprecated") is True
    assert adm["paths"]["/v1/jobs"]["get"].get("deprecated") is not True


# --- the regression the router split can cause ------------------------------

@pytest.mark.parametrize("path,expected", [
    ("/v1/jobs", "/v1/jobs"),
    ("/v1/settings", "/v1/settings"),
    ("/v1/crawl/runs", "/v1/crawl/runs"),
    ("/v1/docs/abc/def", "/v1/docs/{doc_id:path}"),
    ("/nope", "__unmatched__"),
])
def test_red_metrics_still_resolve_a_route_template(path, expected):
    """FastAPI defers include_router: the parent holds ONE _IncludedRouter that
    matches for many routes but carries no path. A naive scan therefore labels
    every included endpoint __unmatched__, silently collapsing the RED metrics for
    the whole operational surface into one series — including the error rate that
    ApiHighErrorRate alerts on."""
    mw = prom.PrometheusMiddleware(app, routes=app.router.routes)
    scope = {"type": "http", "method": "GET", "path": path,
             "headers": [], "root_path": ""}
    assert mw._handler(scope) == expected


def test_admin_traffic_is_counted_once(client):
    """The parent must skip /admin or an admin request is recorded twice: once by
    the sub-app against its real template, and once by the parent against the bare
    Mount path."""
    mw = prom.PrometheusMiddleware(app, routes=app.router.routes,
                                   skip_prefixes=("/admin",))
    assert mw.skip_prefixes == ("/admin",)
    # the sub-app labels with the full path, so the two never collide
    sub = prom.PrometheusMiddleware(app_mod.admin,
                                    routes=app_mod.admin.router.routes,
                                    label_prefix="/admin")
    scope = {"type": "http", "method": "GET", "path": "/v1/health",
             "headers": [], "root_path": ""}
    assert sub._handler(scope) == "/admin/v1/health"


# --- the recipe engine surface ----------------------------------------------

def test_registry_serves_the_whole_palette(client):
    """The graph editor renders nodes, connection rules and every inspector from
    this one document, so a windex that gains a module needs no client release."""
    r = client.get("/admin/v1/registry", headers=auth())
    assert r.status_code == 200
    d = r.json()
    assert d["modules"] and d["kinds"] and d["port_types"]
    assert r.headers.get("ETag"), "clients cache this and revalidate"
    http_get = next(m for m in d["modules"] if m["id"] == "http.get")
    fields = {f["key"]: f for f in http_get["config"]["fields"]}
    # the two attributes a form cannot be built correctly without
    # both ends bounded: lo/hi declared, and `floor` names the operator key
    assert fields["host_interval"]["clamp"] == "both"
    assert fields["host_interval"]["clampNote"]
    assert fields["ssrf_guard"]["lockedReason"]


def test_registry_requires_the_admin_token(client):
    assert client.get("/admin/v1/registry").status_code == 401


def test_recipe_validate_is_pure_and_reports_precisely(client):
    good = {"name": "probe", "corpus": {"source": "probe"},
            "state": {"s": {}},
            "flows": {"f": {"nodes": {
                "d": {"kind": "discover", "uses": "state.pending",
                      "with": {"store": "s"}},
                "g": {"kind": "fetch", "uses": "http.get", "with": {}},
                "x": {"kind": "extract", "uses": "html.trafilatura", "with": {}},
                "l": {"kind": "load", "uses": "ledger.stage", "with": {}}},
                "edges": [["d", "g"], ["g", "x"], ["x", "l"]]}}}
    r = client.post("/admin/v1/recipes/validate", json=good, headers=auth())
    assert r.status_code == 200 and r.json()["valid"] is True

    good["flows"]["f"]["nodes"]["g"]["with"] = {"host_intervall": 2}
    r = client.post("/admin/v1/recipes/validate", json=good, headers=auth())
    assert r.json()["valid"] is False
    assert "host_intervall" in r.json()["errors"][0]["message"]
