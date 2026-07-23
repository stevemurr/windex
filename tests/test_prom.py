"""Prometheus /metrics exposition tests. Same shape as test_api.py: TestClient
over the app with get_settings monkeypatched at the test DB, plus the `pg` /
`qclient` fixtures for seeding. The scrape and its sub-probes are cached, so the
client fixture clears every cache the endpoint reads through.

The golden contract test at the bottom is the load-bearing one: metric names are
a public API (Grafana dashboards/alerts on the user's box query them by name), so
it is asserted EXACTLY in both directions — additive-only, like the /v1 REST
contract. A failure there means a dashboard just broke; extend CONTRACT rather
than rename.
"""

import re

import prometheus_client.parser as prom_parser
import pytest
from fastapi.testclient import TestClient

import windex.api.app as app_mod
import windex.api.prom as prom
import windex.api.service as service_mod
from windex.api.app import app


@pytest.fixture()
def client(settings, monkeypatch):
    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    service_mod._pg_stats_cache.clear()
    service_mod._pg_heavy_cache.clear()  # windex_documents reads through this 600s cache
    prom._scrape_cache.clear()
    prom._gateway_cache.clear()
    return TestClient(app)


# --- exposition parsing helpers ---------------------------------------------

def _samples(text: str) -> dict:
    """Flatten to {(sample_name, frozenset(labels.items())): value}. Note sample
    names carry the suffixes counters/histograms add (_total, _bucket/_count/_sum)."""
    out = {}
    for fam in prom_parser.text_string_to_metric_families(text):
        for s in fam.samples:
            out[(s.name, frozenset(s.labels.items()))] = s.value
    return out


def _value(text: str, name: str, **labels):
    return _samples(text).get((name, frozenset(labels.items())))


def _families(text: str) -> dict:
    """{family_name: (type, frozenset(user_label_names), n_samples)}. `le` (the
    histogram bucket label) is stripped so a histogram's user labels are visible."""
    out = {}
    for fam in prom_parser.text_string_to_metric_families(text):
        labels = {k for s in fam.samples for k in s.labels} - {"le"}
        out[fam.name] = (fam.type, frozenset(labels), len(fam.samples))
    return out


# --- basic exposition -------------------------------------------------------

def test_metrics_exposition_content_type_and_status(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"] == prom.CONTENT_TYPE_LATEST
    assert r.headers["content-type"].startswith("text/plain")
    assert "windex_db_up" in r.text


def test_windex_documents_matches_seeded_rows(client, pg):
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO documents (id, source, url, status) VALUES
               ('news:1', 'news', 'u1', 'embedded'),
               ('news:2', 'news', 'u2', 'embedded'),
               ('news:3', 'news', 'u3', 'deduped'),
               ('gh:o/r', 'github', 'u4', 'embedded')"""
        )
    pg.commit()
    text = client.get("/metrics").text
    assert _value(text, "windex_documents", source="news", status="embedded") == 2.0
    assert _value(text, "windex_documents", source="news", status="deduped") == 1.0
    assert _value(text, "windex_documents", source="github", status="embedded") == 1.0
    assert _value(text, "windex_db_up") == 1.0
    # old abbreviated name is gone
    assert "windex_docs{" not in text and "\nwindex_docs " not in text


def test_embeds_per_minute_reflects_recent_indexed_at(client, pg):
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO documents (id, source, url, status, indexed_at) VALUES
               ('news:e1', 'news', 'u1', 'embedded', now()),
               ('news:e2', 'news', 'u2', 'embedded', now()),
               ('news:e3', 'news', 'u3', 'embedded', now() - interval '90 seconds'),
               ('news:old', 'news', 'u4', 'embedded', now() - interval '30 minutes')"""
        )
    pg.commit()
    # the 2-min count rides service._pg_stats' 10s cache; clear it so the scrape
    # reflects the rows just seeded (mirrors test_metrics_contract).
    service_mod._pg_stats_cache.clear()
    prom._scrape_cache.clear()
    text = client.get("/metrics").text
    # 3 rows landed in the trailing 2 min (the 30-min-old one excluded); /2 = 1.5/min
    assert _value(text, "windex_embeds_per_minute", window="2m") == 1.5


def test_windex_watermark_rows_matches_seeded_rows(client, pg):
    """Per-source ingest-ledger counts by (source, table, status). warc_files
    carries a fixed schema (path, status) so it seeds cleanly; the assertions pin
    the source→table mapping and prove an empty ledger emits no bogus sample."""
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO warc_files (path, status) VALUES "
            "('crawl-data/CC-NEWS/a.warc.gz', 'failed'), "
            "('crawl-data/CC-NEWS/b.warc.gz', 'failed'), "
            "('crawl-data/CC-NEWS/c.warc.gz', 'done')")
    pg.commit()
    prom._scrape_cache.clear()
    text = client.get("/metrics").text
    assert _value(text, "windex_watermark_rows",
                  source="news", table="warc_files", status="failed") == 2.0
    assert _value(text, "windex_watermark_rows",
                  source="news", table="warc_files", status="done") == 1.0
    # an empty ledger contributes no sample (not a spurious zero)
    assert _value(text, "windex_watermark_rows",
                  source="arxiv", table="arxiv_windows", status="failed") is None


def test_windex_repos_matches_seeded_rows(client, pg):
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO repos (repo_id, full_name, status) VALUES
               (1, 'o/a', 'candidate'), (2, 'o/b', 'hydrated'), (3, 'o/c', 'hydrated')"""
        )
    pg.commit()
    text = client.get("/metrics").text
    assert _value(text, "windex_repos", status="hydrated") == 2.0
    assert _value(text, "windex_repos", status="candidate") == 1.0


def test_indexing_paused_flips_with_control_value(client, pg):
    assert _value(client.get("/metrics").text, "windex_indexing_paused") == 0.0
    assert client.post("/v1/control/pause").json() == {"indexing": "paused"}
    prom._scrape_cache.clear()  # 10s scrape cache would otherwise serve the stale page
    assert _value(client.get("/metrics").text, "windex_indexing_paused") == 1.0
    client.post("/v1/control/start")
    prom._scrape_cache.clear()
    assert _value(client.get("/metrics").text, "windex_indexing_paused") == 0.0


def test_stage_busy_reflects_control_stage_keys(client, pg):
    from windex import db as wdb

    wdb.set_control(pg, "news_stage", "extracting + filtering · batch 20260501-abcd1234")
    wdb.set_control(pg, "gh_stage", "idle")
    text = client.get("/metrics").text
    assert _value(text, "windex_stage_busy", key="news_stage") == 1.0
    assert _value(text, "windex_stage_busy", key="gh_stage") == 0.0
    # the unbounded free-text value must NOT leak into a label anywhere
    assert "20260501-abcd1234" not in text


def test_embed_profile_info_reports_current_profile(client, pg):
    assert _value(client.get("/metrics").text, "windex_embed_profile_info", profile="env") == 1.0
    client.post("/v1/throttle/full")
    prom._scrape_cache.clear()
    text = client.get("/metrics").text
    assert _value(text, "windex_embed_profile_info", profile="full") == 1.0
    profiles = [k for k in _samples(text) if k[0] == "windex_embed_profile_info"]
    assert len(profiles) == 1  # info-metric: exactly one profile series at a time


def test_query_breaker_state_is_one_hot(client):
    """StateSet pattern: three series, exactly one == 1. conftest resets the
    breaker cold before each test, so the live state is closed."""
    text = client.get("/metrics").text
    states = {dict(k[1])["state"]: v for k, v in _samples(text).items()
              if k[0] == "windex_query_breaker_state"}
    assert set(states) == {"closed", "open", "half_open"}
    assert sum(states.values()) == 1.0
    assert states["closed"] == 1.0


def test_build_info_present(client):
    import importlib.metadata as im

    text = client.get("/metrics").text
    assert _value(text, "windex_build_info", version=im.version("windex")) == 1.0


def test_dropped_searches_gauge_is_absent(client, pg):
    """windex_searches (table-count gauge) was removed: search_metrics is pruned
    at ~30d so its rowcount decreases and rate() is garbage. Throughput lives on
    windex_search_requests_total instead."""
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO search_metrics (source, mode_requested, degraded, q_hash,
                                           embed_ms, search_ms, total_ms, results)
               VALUES ('all', 'hybrid', false, 'abc', 1, 2, 3, 5)"""
        )
    pg.commit()
    names = {f.name for f in prom_parser.text_string_to_metric_families(client.get("/metrics").text)}
    assert "windex_searches" not in names


# --- DB-independent liveness (must survive a DB outage) ---------------------

# The documents.source corpus vocabulary the dashboard joins per-source rows on.
# windex_loop_up{source} MUST speak this, not the CLI's ccnews/gh names.
# `custom` is the aggregate loop that drains EVERY registered custom source: its
# per-source documents rows use the real source names, so this one loop series
# deliberately has no windex_documents join (the documented custom-freshness gap).
DOCS_VOCAB = {"news", "github", "wiki", "hn", "arxiv", "docs", "smallweb", "hf",
              "memory", "custom"}


def test_liveness_and_probe_series_present(client):
    samples = _samples(client.get("/metrics").text)
    loop_sources = {dict(k[1])["source"] for k in samples if k[0] == "windex_loop_up"}
    # one series per source, labelled in the corpus vocabulary (ccnews→news, gh→github)
    assert loop_sources == DOCS_VOCAB
    assert ("windex_gateway_up", frozenset()) in samples
    assert ("windex_gateway_probe_duration_seconds", frozenset()) in samples


def test_loop_up_source_label_matches_documents_vocabulary(client):
    """The join invariant the Grafana per-source rows depend on: every
    windex_loop_up{source} value must exist in windex_documents' vocabulary, or
    the news/github rows (and their alerts) silently match nothing."""
    loop_sources = {dict(k[1])["source"] for k in _samples(client.get("/metrics").text)
                    if k[0] == "windex_loop_up"}
    assert loop_sources <= DOCS_VOCAB
    # the two that would break without canonicalisation are actually present
    assert {"news", "github"} <= loop_sources
    assert "ccnews" not in loop_sources and "gh" not in loop_sources


def test_loop_up_reflects_heartbeat_not_pgrep(client, pg):
    """The loops run in separate containers, so host pgrep is always 0 here (the
    old LoopDown false-alarm). windex_loop_up must instead read the per-loop
    Postgres heartbeat: fresh -> 1, stale -> 0."""
    import time

    from windex import db as wdb

    now = int(time.time())
    wdb.set_control(pg, "loop_heartbeat_wiki", str(now))            # fresh -> up
    wdb.set_control(pg, "loop_heartbeat_arxiv", str(now - 10_000))  # stale -> down
    pg.commit()

    text = client.get("/metrics").text
    assert _value(text, "windex_loop_up", source="wiki") == 1.0
    assert _value(text, "windex_loop_up", source="arxiv") == 0.0


def test_log_source_mapping():
    """Both historical embed-loop log naming conventions map to the canonical
    corpus source; non-loop logs get no source."""
    assert prom._log_source("ccnews-embed") == "news"
    assert prom._log_source("embed-ccnews") == "news"
    assert prom._log_source("embed-loop") == "news"       # jobs.py's ccnews loop name
    assert prom._log_source("gh-embed") == "github"
    assert prom._log_source("embed-gh") == "github"
    assert prom._log_source("wiki-embed") == "wiki"       # identity sources unchanged
    assert prom._log_source("embed-arxiv") == "arxiv"
    assert prom._log_source("serve") == ""                # non-loop logs: no source
    assert prom._log_source("gh-discover") == ""
    assert prom._log_source("gh-hydrate") == ""
    assert prom._log_source("smallweb-poll") == ""
    # every mapped source lands in the documents vocabulary
    from windex.cli import EMBED_SOURCES

    mapped = {prom._log_source(f"{s}-embed") for s in EMBED_SOURCES}
    assert mapped == DOCS_VOCAB


def test_default_runtime_collectors_present(client):
    names = {f.name for f in prom_parser.text_string_to_metric_families(client.get("/metrics").text)}
    # process_* are Linux-only (no /proc on the Darwin box); the python_* families
    # from the GC/Platform collectors prove the default collectors are wired.
    assert "python_info" in names
    assert any(n.startswith("python_gc") for n in names)


def test_db_down_still_returns_200_with_db_up_zero(client, pg, monkeypatch):
    """A Postgres failure must degrade to windex_db_up 0, never a 500 — the scrape
    is what an outage alert reads, so it has to keep answering."""
    from windex import db as wdb

    def boom(*a, **k):
        raise wdb.psycopg.OperationalError("simulated postgres down")

    monkeypatch.setattr(wdb, "pooled", boom)
    service_mod._pg_heavy_cache.clear()
    prom._scrape_cache.clear()
    r = client.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"] == prom.CONTENT_TYPE_LATEST
    text = r.text
    assert _value(text, "windex_db_up") == 0.0
    # The DB-independent gateway probe still answers; DB-backed families are
    # absent (no half page) — including windex_loop_up, now heartbeat-based, so a
    # DB outage leaves it ABSENT rather than a false 0 that would fire LoopDown.
    assert ("windex_gateway_up", frozenset()) in _samples(text)
    assert not any(k[0] == "windex_documents" for k in _samples(text))
    assert not any(k[0] == "windex_loop_up" for k in _samples(text))


# --- Qdrant -----------------------------------------------------------------

PROBE_COLLECTION = "windex_metrics_probe__pytest-model"  # 'pytest-model' → qclient cleans it up


def test_qdrant_metrics_present(client, qclient):
    from qdrant_client import models as qm

    if not qclient.collection_exists(PROBE_COLLECTION):
        qclient.create_collection(
            PROBE_COLLECTION, vectors_config=qm.VectorParams(size=4, distance=qm.Distance.COSINE))
    prom._scrape_cache.clear()
    text = client.get("/metrics").text
    assert _value(text, "windex_qdrant_up") == 1.0
    assert _value(text, "windex_qdrant_points", collection=PROBE_COLLECTION) is not None


# --- HTTP RED + search-path instrumentation ---------------------------------

def _mock_search(monkeypatch, *, degraded=False):
    monkeypatch.setattr(
        service_mod, "index_search",
        lambda *a, **k: {"results": [], "degraded": degraded,
                         "timings": {"embed_query_ms": 0, "search_ms": 0}})


def test_http_and_search_instruments_recorded(client, monkeypatch):
    _mock_search(monkeypatch)
    client.get("/v1/search", params={"q": "prometheus"})
    prom._scrape_cache.clear()
    text = client.get("/metrics").text
    # search-path counters/histograms
    assert _value(text, "windex_search_requests_total", mode="hybrid", result="ok") >= 1
    assert _value(text, "windex_search_duration_seconds_count") >= 1
    # HTTP RED, handler = route template
    assert _value(text, "windex_http_requests_total",
                  handler="/v1/search", method="GET", code="200") >= 1
    assert _value(text, "windex_http_request_duration_seconds_count", handler="/v1/search") >= 1
    # query-embed instruments exist (unlabeled; the mocked path leaves them at 0)
    s = _samples(text)
    assert ("windex_query_embed_duration_seconds_count", frozenset()) in s
    assert ("windex_query_embed_failures_total", frozenset()) in s


def test_degraded_search_labels_result_degraded(client, monkeypatch):
    _mock_search(monkeypatch, degraded=True)
    client.get("/v1/search", params={"q": "x"})
    prom._scrape_cache.clear()
    assert _value(client.get("/metrics").text,
                  "windex_search_requests_total", mode="hybrid", result="degraded") >= 1


def test_http_handler_label_is_route_template_not_raw_path(client):
    # 404, but the path-param route still matches → template label, bounded cardinality
    client.get("/v1/docs/news:doesnotexist")
    prom._scrape_cache.clear()
    handlers = {dict(k[1]).get("handler") for k in _samples(client.get("/metrics").text)
                if k[0] == "windex_http_requests_total"}
    assert "/v1/docs/{doc_id:path}" in handlers
    assert not any(h and "doesnotexist" in h for h in handlers)


def test_metrics_endpoint_excluded_from_http_metrics(client):
    client.get("/metrics")
    prom._scrape_cache.clear()
    handlers = {dict(k[1]).get("handler") for k in _samples(client.get("/metrics").text)
                if k[0] == "windex_http_requests_total"}
    assert "/metrics" not in handlers


# --- golden contract + naming lint ------------------------------------------

# {parser family name: (type, frozenset of user label names)}. Counters appear
# under their base name (the parser strips the _total sample suffix); histograms
# under the base _seconds name. ADD to this when you add a family — never rename
# an existing one (see module docstring: additive-only, like /v1).
CONTRACT = {
    "windex_documents": ("gauge", frozenset({"source", "status"})),
    "windex_embeds_per_minute": ("gauge", frozenset({"window"})),
    "windex_repos": ("gauge", frozenset({"status"})),
    "windex_watermark_rows": ("gauge", frozenset({"source", "table", "status"})),
    "windex_loop_up": ("gauge", frozenset({"source"})),
    "windex_job_up": ("gauge", frozenset({"job"})),
    "windex_gateway_up": ("gauge", frozenset()),
    "windex_gateway_probe_duration_seconds": ("gauge", frozenset()),
    "windex_query_breaker_state": ("gauge", frozenset({"state"})),
    "windex_log_last_modified_timestamp_seconds": ("gauge", frozenset({"log", "source"})),
    "windex_build_info": ("gauge", frozenset({"version"})),
    "windex_indexing_paused": ("gauge", frozenset()),
    "windex_stage_busy": ("gauge", frozenset({"key"})),
    "windex_embed_profile_info": ("gauge", frozenset({"profile"})),
    "windex_db_up": ("gauge", frozenset()),
    "windex_qdrant_up": ("gauge", frozenset()),
    "windex_qdrant_points": ("gauge", frozenset({"collection"})),
    "windex_http_requests": ("counter", frozenset({"handler", "method", "code"})),
    "windex_http_request_duration_seconds": ("histogram", frozenset({"handler"})),
    "windex_search_requests": ("counter", frozenset({"mode", "result"})),
    "windex_search_duration_seconds": ("histogram", frozenset()),
    "windex_query_embed_duration_seconds": ("histogram", frozenset()),
    "windex_query_embed_failures": ("counter", frozenset()),
}


def test_metrics_contract(client, pg, qclient, monkeypatch):
    """Every windex_* family present, exactly the contracted set (both
    directions), each with the contracted type and — where the family has samples
    in this run — the contracted labels. Seeds enough state + drives one search so
    every labelled family has at least one sample to check labels against."""
    from qdrant_client import models as qm

    from windex import db as wdb

    with pg.cursor() as cur:
        cur.execute("INSERT INTO documents (id, source, url, status) VALUES "
                    "('news:1', 'news', 'u', 'embedded')")
        cur.execute("INSERT INTO repos (repo_id, full_name, status) VALUES (1, 'o/r', 'hydrated')")
    pg.commit()
    wdb.set_control(pg, "news_stage", "busy on something")
    if not qclient.collection_exists(PROBE_COLLECTION):
        qclient.create_collection(
            PROBE_COLLECTION, vectors_config=qm.VectorParams(size=4, distance=qm.Distance.COSINE))
    _mock_search(monkeypatch)
    client.get("/v1/search", params={"q": "contract"})  # samples for http_* + search_*

    prom._scrape_cache.clear()
    service_mod._pg_heavy_cache.clear()
    observed = _families(client.get("/metrics").text)
    windex = {n: v for n, v in observed.items() if n.startswith("windex_")}

    assert set(windex) == set(CONTRACT), (
        f"contract drift — missing={set(CONTRACT) - set(windex)} "
        f"un-contracted={set(windex) - set(CONTRACT)}")
    for name, (ctype, clabels) in CONTRACT.items():
        otype, olabels, nsamples = windex[name]
        assert otype == ctype, f"{name}: type {otype!r} != contract {ctype!r}"
        if nsamples:  # labels are only observable when the family emitted samples
            assert olabels == clabels, f"{name}: labels {set(olabels)} != contract {set(clabels)}"


def test_metric_naming_conventions(client):
    """promtool-lite: exposed names (from # TYPE headers) obey Prometheus naming —
    snake_case, known prefix, counters end _total, durations/timestamps end
    _seconds. Guards against a careless future addition."""
    text = client.get("/metrics").text
    types = re.findall(r"^# TYPE (\S+) (\S+)$", text, re.M)
    assert types
    for name, mtype in types:
        assert re.fullmatch(r"[a-z_][a-z0-9_]*", name), f"non-snake_case metric name: {name}"
        assert name.startswith(("windex_", "process_", "python_")), f"unexpected prefix: {name}"
        if mtype == "counter":
            assert name.endswith("_total"), f"counter without _total suffix: {name}"
        if "duration" in name:
            assert name.endswith("_seconds"), f"duration metric not in _seconds: {name}"
        if name.endswith("_timestamp_seconds"):
            assert mtype == "gauge", f"timestamp metric should be a gauge: {name}"
