"""Recipe writes and the generic manual-run API's database contract."""

from fastapi.testclient import TestClient

import windex.api.app as app_mod
from windex.api.app import app
from windex.recipe import run_store, runners, store
from windex.worker import dag
from windex.worker.protocol import SliceResult


DOC = {
    "schema": "windex.recipe/1",
    "name": "probe",
    "title": "Probe source",
    "corpus": {"source": "probe"},
    "config": [
        {"key": "seeds", "kind": "url_list", "required": True},
    ],
    "state": {"frontier": {"key": "url"}},
    "flows": {
        "crawl": {
            "nodes": {
                "seed": {
                    "kind": "discover",
                    "uses": "crawl.frontier",
                    "with": {
                        "store": "frontier",
                        "seeds": "@config.seeds",
                    },
                },
                "get": {
                    "kind": "fetch",
                    "uses": "http.get",
                    "with": {},
                },
                "text": {
                    "kind": "extract",
                    "uses": "html.trafilatura",
                    "with": {},
                },
                "stage": {
                    "kind": "load",
                    "uses": "ledger.stage",
                    "with": {},
                },
            },
            "edges": [["seed", "get"], ["get", "text"], ["text", "stage"]],
        },
    },
    "refresh": ["crawl"],
}


def test_create_update_banks_normalized_revisions(pg, settings):
    created = store.create_recipe(pg, DOC, settings)
    assert created["version"] == 1
    assert created["spec"]["version"] == 1
    assert created["spec"]["flows"]["crawl"]["nodes"]["seed"]["with"][
        "max_pages"] == 500

    changed = dict(created["spec"])
    changed["title"] = "Edited probe"
    updated = store.update_recipe(pg, "probe", changed, settings, note="rename")
    assert updated is not None
    assert updated["version"] == 2
    assert updated["spec"]["version"] == 2
    assert updated["title"] == "Edited probe"

    with pg.cursor() as cur:
        cur.execute(
            "SELECT version, note FROM recipe_revisions "
            "WHERE name = 'probe' ORDER BY version")
        assert cur.fetchall() == [(1, ""), (2, "rename")]


def test_create_conflicts_and_update_name_must_match(pg, settings):
    store.create_recipe(pg, DOC, settings)
    try:
        store.create_recipe(pg, DOC, settings)
    except KeyError as exc:
        assert exc.args == ("probe",)
    else:
        raise AssertionError("duplicate recipe was accepted")

    changed = dict(DOC)
    changed["name"] = "someone_else"
    try:
        store.update_recipe(pg, "probe", changed, settings)
    except ValueError as exc:
        assert "does not match path" in str(exc)
    else:
        raise AssertionError("path/document name mismatch was accepted")


def test_builtin_edit_survives_an_ordinary_reseed(pg, settings):
    store.seed_builtins(pg, settings)
    original = store.get_recipe(pg, "arxiv")
    assert original is not None
    changed = dict(original["spec"])
    changed["title"] = "Locally tuned arXiv"
    store.update_recipe(pg, "arxiv", changed, settings)

    actions = {row["name"]: row["action"]
               for row in store.seed_builtins(pg, settings)}
    assert actions["arxiv"] == "kept (locally edited)"
    assert store.get_recipe(pg, "arxiv")["title"] == "Locally tuned arXiv"


def _install_probe_runners(monkeypatch):
    def done(_ctx):
        return SliceResult(exhausted=True)

    for module in (
        "crawl.frontier", "http.get", "html.trafilatura", "ledger.stage"):
        monkeypatch.setitem(runners.RUNNERS, module, done)


def test_submit_refuses_a_valid_but_unimplemented_graph(pg, settings):
    store.create_recipe(pg, DOC, settings)
    try:
        run_store.submit(
            pg,
            recipe="probe",
            settings=settings,
            params={"seeds": ["https://example.com/docs/"]},
        )
    except run_store.ModulesUnavailable as exc:
        assert exc.modules == [
            "crawl.frontier", "html.trafilatura", "http.get", "ledger.stage"]
    else:
        raise AssertionError("unimplemented graph was queued")


def test_submit_freezes_values_dedupes_and_cancels(
        pg, settings, monkeypatch):
    _install_probe_runners(monkeypatch)
    store.create_recipe(pg, DOC, settings)
    run_id = run_store.submit(
        pg,
        recipe="probe",
        settings=settings,
        params={"seeds": ["https://example.com/docs/"]},
    )
    assert run_id is not None
    assert run_store.submit(
        pg,
        recipe="probe",
        settings=settings,
        params={"seeds": ["https://example.com/docs/"]},
    ) is None

    run = run_store.get_run(pg, run_id, include_spec=True)
    assert run is not None
    assert run["state"] == "queued"
    assert run["spec"]["version"] == 1
    assert run["tasks"][0]["config"]["seeds"] == [
        "https://example.com/docs/"]
    assert run["tasks"][0]["state"] == "ready"
    assert run["tasks"][1]["state"] == "pending"

    assert dag.request_cancel(pg, run_id, by="test")
    cancelled = run_store.get_run(pg, run_id)
    assert cancelled["state"] == "cancelled"
    events = run_store.list_events(pg, run_id)
    assert [event["event"] for event in events] == [
        "run.queued", "run.cancel_requested", "task.cancelled",
        "task.cancelled", "task.cancelled", "task.cancelled", "run.cancelled",
    ]
    cursor = events[2]["seq"]
    assert all(event["seq"] > cursor for event in
               run_store.list_events(pg, run_id, after=cursor))


def test_admin_recipe_and_run_surface(pg, settings, monkeypatch):
    _install_probe_runners(monkeypatch)
    monkeypatch.setattr(settings, "write_token", "lifecycle-test-token")
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    client = TestClient(app)
    auth = {"Authorization": "Bearer lifecycle-test-token"}

    created = client.post("/admin/v1/recipes", json=DOC, headers=auth)
    assert created.status_code == 201, created.text
    assert created.json()["version"] == 1
    assert client.post(
        "/admin/v1/recipes", json=DOC, headers=auth).status_code == 409

    edited = created.json()["spec"]
    edited["title"] = "Edited through HTTP"
    updated = client.put(
        "/admin/v1/recipes/probe", json=edited, headers=auth)
    assert updated.status_code == 200, updated.text
    assert updated.json()["version"] == 2

    queued = client.post(
        "/admin/v1/runs",
        json={
            "recipe": "probe",
            "params": {"seeds": ["https://example.com/docs/"]},
        },
        headers=auth,
    )
    assert queued.status_code == 202, queued.text
    run_id = queued.json()["run_id"]
    detail = client.get(f"/admin/v1/runs/{run_id}", headers=auth)
    assert detail.status_code == 200
    assert detail.json()["tasks"][0]["config"]["seeds"] == [
        "https://example.com/docs/"]

    events = client.get(
        f"/admin/v1/runs/{run_id}/events", headers=auth).json()
    assert events["events"][0]["event"] == "run.queued"
    stream = client.get(
        f"/admin/v1/runs/{run_id}/events/stream",
        params={"ticks": 1},
        headers=auth,
    )
    assert stream.status_code == 200
    assert "event: run" in stream.text

    cancelled = client.post(
        f"/admin/v1/runs/{run_id}/cancel", headers=auth)
    assert cancelled.status_code == 200
    listed = client.get(
        "/admin/v1/runs", params={"state": "cancelled"}, headers=auth)
    assert [row["id"] for row in listed.json()["runs"]] == [run_id]


def test_admin_tasks_expose_execution_availability(pg, settings, monkeypatch):
    monkeypatch.setattr(settings, "write_token", "lifecycle-test-token")
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    store.create_recipe(pg, DOC, settings)
    client = TestClient(app)
    auth = {"Authorization": "Bearer lifecycle-test-token"}

    placement = client.get(
        "/admin/v1/recipes/probe/tasks", headers=auth)
    assert placement.status_code == 200
    assert placement.json()["executable"] is False
    assert placement.json()["unavailable_modules"] == [
        "crawl.frontier", "html.trafilatura", "http.get", "ledger.stage"]

    queued = client.post(
        "/admin/v1/runs",
        json={
            "recipe": "probe",
            "params": {"seeds": ["https://example.com/docs/"]},
        },
        headers=auth,
    )
    assert queued.status_code == 409
    assert queued.json()["detail"]["unavailable_modules"] == [
        "crawl.frontier", "html.trafilatura", "http.get", "ledger.stage"]
