from __future__ import annotations

from fastapi.testclient import TestClient

from windex import metrics
from windex.api import app as app_module
from windex.api import prom, service
from windex.config import Settings


def _counter_value(*, handler: str, method: str, code: str) -> float:
    for metric in metrics.HTTP_REQUESTS.collect():
        for sample in metric.samples:
            if (
                sample.name == "windex_http_requests_total"
                and sample.labels == {
                    "handler": handler,
                    "method": method,
                    "code": code,
                }
            ):
                return float(sample.value)
    return 0.0


def _http_total() -> float:
    return sum(
        float(sample.value)
        for metric in metrics.HTTP_REQUESTS.collect()
        for sample in metric.samples
        if sample.name == "windex_http_requests_total"
    )


def _sample(family, **labels) -> float | None:
    for item in family.samples:
        if item.name == family.name and item.labels == labels:
            return float(item.value)
    return None


def test_metrics_route_is_unauthed_ops_surface_with_prometheus_content_type(
    monkeypatch, tmp_path,
):
    settings = Settings(
        _env_file=None,
        data_root=tmp_path,
        write_token="required-everywhere-else",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(prom, "render", lambda _settings: b"windex_test_metric 1\n")

    response = TestClient(app_module.app).get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == prom.CONTENT_TYPE_LATEST
    assert response.content == b"windex_test_metric 1\n"
    assert "/metrics" not in app_module.app.openapi()["paths"]


def test_http_red_uses_templates_counts_admin_once_and_excludes_metrics(
    monkeypatch, tmp_path,
):
    settings = Settings(
        _env_file=None,
        data_root=tmp_path,
        write_token="admin-token",
    )
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)
    monkeypatch.setattr(service, "validate_source", lambda *_args: "all")
    monkeypatch.setattr(
        service,
        "run_search",
        lambda *_args, **_kwargs: {
            "query": "metrics",
            "results": [],
            "mode": "lexical",
            "timings": {},
            "took_ms": 1,
        },
    )
    monkeypatch.setattr(prom, "render", lambda _settings: b"# metrics\n")
    client = TestClient(app_module.app)

    search_before = _counter_value(
        handler="/v1/search", method="GET", code="200")
    health_before = _counter_value(
        handler="/admin/v1/health", method="GET", code="200")
    unmatched_before = _counter_value(
        handler="/admin", method="GET", code="200")

    assert client.get("/v1/search", params={"q": "metrics"}).status_code == 200
    assert client.get("/admin/v1/health").status_code == 200
    assert _counter_value(
        handler="/v1/search", method="GET", code="200") == search_before + 1
    assert _counter_value(
        handler="/admin/v1/health", method="GET", code="200") == health_before + 1
    assert _counter_value(
        handler="/admin", method="GET", code="200") == unmatched_before

    before_scrape = _http_total()
    assert client.get("/metrics").status_code == 200
    assert _http_total() == before_scrape
    assert not any(
        sample.labels.get("handler") == "/metrics"
        for metric in metrics.HTTP_REQUESTS.collect()
        for sample in metric.samples
    )


def test_epoch2_database_families_use_canonical_state(pg):
    with pg.cursor() as cur:
        cur.execute(
            """SELECT id, search_name, state_namespace
                 FROM sources
                WHERE archived_at IS NULL
                ORDER BY id LIMIT 1""")
        source_id, source_name, namespace = cur.fetchone()
        cur.execute(
            """INSERT INTO documents
                   (id, source_id, source, url, status, indexed_at)
               VALUES (%s, %s, %s, %s, 'searchable', now())""",
            (f"{source_name}:prometheus", source_id, source_name, "https://example.test"),
        )
        cur.execute(
            """INSERT INTO source_units
                   (source_id, state_namespace, store, unit_key, status)
               VALUES (%s, %s, 'metrics-test', 'unit-1', 'failed')""",
            (source_id, namespace),
        )
    pg.commit()

    families = {
        family.name: family
        for family in prom.WindexCollector._read_database(pg)
    }

    assert _sample(
        families["windex_documents"],
        source=source_name,
        status="searchable",
    ) == 1.0
    assert _sample(
        families["windex_source_units"],
        source=source_name,
        store="metrics-test",
        status="failed",
    ) == 1.0
    assert _sample(
        families["windex_embeds_per_minute"],
        source=source_name,
        window="2m",
    ) == 0.5
    assert _sample(families["windex_worker_expired_leases"]) == 0.0
    assert _sample(families["windex_worker_claim_stalled"]) == 0.0
    assert _sample(families["windex_scheduler_due_triggers"]) == 0.0


def test_database_failure_degrades_to_db_up_zero(monkeypatch, tmp_path):
    settings = Settings(_env_file=None, data_root=tmp_path)
    collector = prom.WindexCollector(settings)

    def unavailable(_dsn):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(prom.db, "pooled", unavailable)
    families = list(collector._database_metrics())

    assert len(families) == 1
    assert families[0].name == "windex_db_up"
    assert _sample(families[0]) == 0.0


def test_render_combines_process_and_state_registries(monkeypatch, tmp_path):
    settings = Settings(_env_file=None, data_root=tmp_path)
    settings.staging_dir.mkdir(parents=True)
    settings.downloads_dir.mkdir(parents=True)

    class StateOnly:
        def __init__(self, _settings):
            pass

        def collect(self):
            from prometheus_client.core import GaugeMetricFamily

            family = GaugeMetricFamily(
                "windex_state_probe", "test state family")
            family.add_metric([], 1.0)
            yield family

    monkeypatch.setattr(prom, "WindexCollector", StateOnly)
    prom._scrape_cache.clear()
    output = prom.render(settings)

    assert b"windex_state_probe 1.0" in output
    assert b"python_info" in output
    assert b"windex_http_requests_total" in output
