"""Programming-docs (DevDocs) source: manifest parse + sync watermark
idempotency, canonical URL construction (attribution link primary, suffix-rule
fallback incl. anchors), HTML -> text extraction, ingest from fabricated
index+db fixtures -> full-replace parquet + change-aware ledger, refresh delta
(text_hash) + tombstones for vanished pages, and the Qdrant tombstone path.
Uses the pg fixture and a fake CDN client so no network is touched."""

import json

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from windex.docs_source import canonical as dcanon
from windex.docs_source import ingest as dingest
from windex.docs_source import sync as dsync

# --- fabricated DevDocs fixtures --------------------------------------------

MANIFEST = [
    {"name": "Flask", "slug": "flask", "type": "sphinx", "release": "3.1.1",
     "mtime": 1739347690, "db_size": 1236115,
     "attribution": "&copy; 2010 Pallets<br>Licensed under the BSD 3-clause License."},
    {"name": "Vue.js", "slug": "vue~3", "type": "vue", "release": "3.5.38",
     "mtime": 1782016732, "db_size": 1522623,
     "attribution": "&copy; 2013&ndash;present Yuxi Evan You"},
    {"name": "Rust", "slug": "rust", "type": "rust", "release": "1.97.0",
     "mtime": 1783889304, "db_size": 69920690, "attribution": "&copy; Rust contributors"},
    {"slug": "broken-no-mtime"},  # dropped: nothing to watermark against
]


def _page(title, body, upstream=None):
    attribution = (
        '<div class="_attribution"><p class="_attribution-p">&copy; Upstream.<br>'
        + (f'<a href="{upstream}" class="_attribution-link">{upstream}</a>' if upstream else "")
        + "</p></div>"
    )
    return f"<h1>{title}</h1>{body}{attribution}"


class _FakeResp:
    def __init__(self, payload: bytes, status_code: int = 200):
        self.content = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return json.loads(self.content)

    def iter_bytes(self, n):
        for start in range(0, len(self.content), n):
            yield self.content[start : start + n]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeClient:
    """Serves canned JSON bodies keyed by URL; records requested URLs."""

    def __init__(self, routes: dict):
        self.routes = routes
        self.calls: list[str] = []

    def get(self, url, **kw):
        self.calls.append(url)
        return _FakeResp(json.dumps(self.routes[url]).encode())

    def stream(self, method, url, **kw):
        self.calls.append(url)
        return _FakeResp(json.dumps(self.routes[url]).encode())

    def close(self):
        pass


CDN = "https://cdn.test"


def _routes(slug, index, db):
    return {f"{CDN}/{slug}/index.json": index, f"{CDN}/{slug}/db.json": db}


def _seed_docsets(pg, manifest=MANIFEST):
    client = _FakeClient({"https://m.test/docs.json": manifest})
    return dsync.sync(pg, client=client, url="https://m.test/docs.json")


# --- manifest parse + sync watermark ----------------------------------------


def test_parse_manifest_normalizes_and_drops_unwatermarkable():
    rows = dsync.parse_manifest(MANIFEST)
    assert [r["slug"] for r in rows] == ["flask", "vue~3", "rust"]
    flask = rows[0]
    assert flask["release"] == "3.1.1" and flask["mtime"] == 1739347690
    assert flask["db_size"] == 1236115 and "Pallets" in flask["attribution"]


def test_sync_is_idempotent_and_tracks_mtime_advance(pg):
    assert _seed_docsets(pg) == {"total": 3, "added": 3, "updated": 0}
    assert _seed_docsets(pg) == {"total": 3, "added": 0, "updated": 0}

    bumped = [dict(d) for d in MANIFEST]
    bumped[0]["mtime"] += 100
    bumped[0]["release"] = "3.2.0"
    assert _seed_docsets(pg, bumped) == {"total": 3, "added": 0, "updated": 1}
    with pg.cursor() as cur:
        cur.execute("SELECT release, mtime FROM docsets WHERE slug = 'flask'")
        assert cur.fetchone() == ("3.2.0", 1739347790)


def test_pending_respects_seed_list_order_and_ingested_mtime(pg):
    _seed_docsets(pg)
    # seed-list order wins, non-seed slugs (rust) and unknown slugs are ignored
    pending = dsync.pending_docsets(pg, ["vue~3", "flask", "nonexistent"])
    assert [d["slug"] for d in pending] == ["vue~3", "flask"]
    assert pending[1]["release"] == "3.1.1" and pending[1]["mtime"] == 1739347690

    # completing a docset at its manifest mtime clears it until mtime advances
    dsync.mark(pg, "flask", "done", {"pages": 2}, ingested_mtime=1739347690)
    assert [d["slug"] for d in dsync.pending_docsets(pg, ["vue~3", "flask"])] == ["vue~3"]
    bumped = [dict(d) for d in MANIFEST]
    bumped[0]["mtime"] += 100
    _seed_docsets(pg, bumped)
    assert [d["slug"] for d in dsync.pending_docsets(pg, ["vue~3", "flask"])] == ["vue~3", "flask"]


# --- canonical URLs ----------------------------------------------------------


def test_canonical_url_suffix_rules_and_anchors():
    # sphinx family: append .html to the path, before any #anchor
    assert dcanon.canonical_url("python~3.14", "library/functions") == \
        "https://docs.python.org/3.14/library/functions.html"
    assert dcanon.canonical_url("python~3.14", "library/functions#print") == \
        "https://docs.python.org/3.14/library/functions.html#print"
    # MDN family: no suffix
    assert dcanon.canonical_url("javascript", "global_objects/array") == \
        "https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/global_objects/array"
    assert dcanon.canonical_url("css", "flexbox#syntax") == \
        "https://developer.mozilla.org/en-US/docs/Web/CSS/flexbox#syntax"
    # dirhtml family: strip the trailing index segment, keep the directory URL
    assert dcanon.canonical_url("flask", "installation/index") == \
        "https://flask.palletsprojects.com/en/stable/installation/"
    assert dcanon.canonical_url("flask", "index") == \
        "https://flask.palletsprojects.com/en/stable/"
    assert dcanon.canonical_url("go", "net/http/index") == "https://pkg.go.dev/net/http/"
    assert dcanon.canonical_url("flask", "api/index#flask.Flask") == \
        "https://flask.palletsprojects.com/en/stable/api/#flask.Flask"


def test_canonical_url_prefers_usable_attribution_link():
    # the per-page scraped-from URL wins (exact case/suffix beats reconstruction)
    assert dcanon.canonical_url(
        "rust", "std/vec/struct.vec",
        upstream="https://doc.rust-lang.org/std/vec/struct.Vec.html",
    ) == "https://doc.rust-lang.org/std/vec/struct.Vec.html"
    # localhost-scraped docsets (go) must fall back to the rule table
    assert dcanon.canonical_url(
        "go", "fmt/index", upstream="http://localhost:6060/pkg/fmt/"
    ) == "https://pkg.go.dev/fmt/"
    assert not dcanon.usable_upstream_url("file:///tmp/x.html")
    assert not dcanon.usable_upstream_url("http://127.0.0.1/pkg/")
    # unknown slug without a usable link: the DevDocs app URL always resolves
    assert dcanon.canonical_url("zig", "std/arraylist") == "https://devdocs.io/zig/std/arraylist"


# --- HTML -> text ------------------------------------------------------------


def test_html_to_text_strips_markup_keeps_code_and_drops_attribution():
    html = (
        "<h1>Greetings</h1><p>Use <code>print()</code> to greet.</p>"
        "<style>.x{color:red}</style><script>alert(1)</script>"
        '<pre data-language="python">def hi():\n    print("hi")</pre>'
        "<table><tr><th>arg</th><td>desc</td></tr></table>"
    )
    text = dingest.html_to_text(html)
    assert "Greetings" in text and "Use print() to greet." in text
    # code TEXT is kept (deliberately lossy: indentation may collapse — the
    # goal is embedding/snippet text, not fidelity)
    assert 'def hi():' in text and 'print("hi")' in text
    assert "alert(1)" not in text and "color:red" not in text
    assert "arg desc" in text                             # table cells spaced

    page = _page("T", "<p>body</p>", upstream="https://x.example/t.html")
    assert dingest.upstream_url(page) == "https://x.example/t.html"
    body = page.split('<div class="_attribution">')[0]
    assert "Upstream" not in dingest.html_to_text(body)   # boilerplate cut


def test_page_title_h1_then_index_entry_fallback():
    assert dingest.page_title("<h1>Welcome to <em>Flask</em></h1><p>x</p>") == "Welcome to Flask"
    assert dingest.page_title("<p>no heading</p>", fallback="Entry Name") == "Entry Name"
    titles = dingest.page_titles_from_index(
        {"entries": [
            {"name": "abort()", "path": "api/index#flask.abort", "type": "flask"},
            {"name": "API", "path": "api/index", "type": "API"},
            {"name": "Dup", "path": "api/index", "type": "API"},
        ]}
    )
    assert titles == {"api/index": "API"}  # anchored entries skipped, first wins


# --- ingest ------------------------------------------------------------------

FLASK_INDEX = {
    "entries": [
        {"name": "API", "path": "api/index", "type": "API"},
        {"name": "abort()", "path": "api/index#flask.abort", "type": "flask"},
        {"name": "Installation", "path": "installation/index", "type": "Guide"},
        {"name": "Templates", "path": "templating/index", "type": "Guide"},
    ],
    "types": [],
}

FLASK_DB = {
    "api/index": _page("API Reference", "<p>All of Flask's <code>abort()</code> docs.</p>",
                       upstream="https://flask.palletsprojects.com/en/stable/api/"),
    "installation/index": _page("Installation", "<p>pip install flask</p>",
                                upstream="https://flask.palletsprojects.com/en/stable/installation/"),
    "templating/index": _page("Templates", "<p>Jinja everywhere.</p>",
                              upstream="https://flask.palletsprojects.com/en/stable/templating/"),
}


def _ingest_flask(pg, settings, db=FLASK_DB, index=FLASK_INDEX, max_docsets=None):
    client = _FakeClient(_routes("flask", index, db))
    return dingest.ingest(pg, settings, max_docsets=max_docsets, client=client, cdn_url=CDN)


@pytest.fixture()
def docs_settings(settings):
    return settings.model_copy(update={"docs_slugs": "flask"})


def test_ingest_stages_parquet_and_ledger(pg, docs_settings):
    _seed_docsets(pg)
    totals = _ingest_flask(pg, docs_settings)
    assert totals == {"docsets": 1, "pages": 3, "staged": 3, "skipped": 0, "deleted": 0}

    text_ref = "docs/clean/flask.parquet"
    table = pq.read_table(docs_settings.staging_dir / text_ref)
    assert table.num_rows == 3
    assert set(table.column_names) == {
        "id", "url", "title", "framework", "version", "attribution", "text"
    }
    row = table.filter(pc.equal(table["id"], "docs:flask/api/index")).to_pylist()[0]
    assert row["url"] == "https://flask.palletsprojects.com/en/stable/api/"  # attribution link
    assert row["title"] == "API Reference"                                   # first <h1>
    assert row["framework"] == "flask" and row["version"] == "3.1.1"
    assert "Pallets" in row["attribution"] and "<" not in row["attribution"]  # plain text
    assert "abort()" in row["text"] and "<p>" not in row["text"]

    with pg.cursor() as cur:
        cur.execute(
            "SELECT id, url, canonical_url, status, text_ref, published_at, text_hash "
            "FROM documents WHERE source='docs' ORDER BY id"
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == [
        "docs:flask/api/index", "docs:flask/installation/index", "docs:flask/templating/index",
    ]
    for r in rows:
        assert r[1] == r[2] and r[3] == "deduped" and r[4] == text_ref
        assert r[5] is None and r[6]  # published_at NULL, text_hash populated

    with pg.cursor() as cur:
        cur.execute("SELECT status, ingested_mtime, doc_counts FROM docsets WHERE slug='flask'")
        status, ingested, counts = cur.fetchone()
    assert status == "done" and ingested == 1739347690
    assert counts["pages"] == 3 and counts["staged"] == 3
    # completed at the manifest mtime: nothing pending until mtime advances
    assert dsync.pending_docsets(pg, ["flask"]) == []


def test_empty_db_json_does_not_wipe_an_existing_docset(pg, docs_settings, monkeypatch):
    """A truncated/glitched db.json (200 with a {} body) over a docset that
    already has ingested pages must NOT tombstone the whole docset and bank the
    new mtime as 'done' (never retried). ingest marks it 'failed' and leaves the
    corpus intact until a plausible db.json arrives."""
    tombstoned = []
    monkeypatch.setattr(dingest, "apply_tombstones",
                        lambda conn, s, ids: tombstoned.extend(ids) or len(ids))
    _seed_docsets(pg)
    _ingest_flask(pg, docs_settings)  # 3 pages ingested, docset marked done

    bumped = [dict(d) for d in MANIFEST]
    bumped[0]["mtime"] += 100  # flask pending again
    _seed_docsets(pg, bumped)
    _ingest_flask(pg, docs_settings, db={})  # db.json comes back empty

    assert tombstoned == [], "empty db.json mass-tombstoned an existing docset"
    with pg.cursor() as cur:
        cur.execute("SELECT status, ingested_mtime FROM docsets WHERE slug='flask'")
        status, ingested = cur.fetchone()
        assert status == "failed"          # not falsely 'done'
        assert ingested == 1739347690      # mtime NOT advanced past the bad fetch
        cur.execute("SELECT count(*) FROM documents WHERE source='docs' AND status <> 'deleted'")
        assert cur.fetchone()[0] == 3      # pages intact


def test_refresh_reembeds_only_changed_pages_and_tombstones_removed(pg, docs_settings, monkeypatch):
    # tombstoning must never resolve the live docs_current alias from a test:
    # point it at a collection that cannot exist (delete becomes a clean skip)
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda s: "docs__pytest-void")
    _seed_docsets(pg)
    _ingest_flask(pg, docs_settings)
    with pg.cursor() as cur:
        cur.execute("UPDATE documents SET status='embedded', embedded_model='m', "
                    "indexed_at=now() WHERE source='docs'")
    pg.commit()

    # upstream refresh: api page rewritten, installation unchanged, templating gone
    bumped = [dict(d) for d in MANIFEST]
    bumped[0]["mtime"] += 100
    _seed_docsets(pg, bumped)
    changed_db = {
        "api/index": _page("API Reference", "<p>REWRITTEN with new signatures.</p>",
                           upstream="https://flask.palletsprojects.com/en/stable/api/"),
        "installation/index": FLASK_DB["installation/index"],
    }
    totals = _ingest_flask(pg, docs_settings, db=changed_db)
    assert totals == {"docsets": 1, "pages": 2, "staged": 1, "skipped": 1, "deleted": 1}

    with pg.cursor() as cur:
        cur.execute("SELECT id, status FROM documents WHERE source='docs' ORDER BY id")
        rows = dict(cur.fetchall())
    assert rows["docs:flask/api/index"] == "deduped"           # re-queued for embed
    assert rows["docs:flask/installation/index"] == "embedded"  # untouched
    assert rows["docs:flask/templating/index"] == "deleted"     # tombstoned

    # full-replace: the parquet holds the whole live page set (unchanged pages
    # must stay readable at this text_ref), not just the delta
    table = pq.read_table(docs_settings.staging_dir / "docs/clean/flask.parquet")
    assert sorted(table.column("id").to_pylist()) == [
        "docs:flask/api/index", "docs:flask/installation/index",
    ]
    assert "REWRITTEN" in table.filter(
        pc.equal(table["id"], "docs:flask/api/index")).to_pylist()[0]["text"]


def test_reingest_resurrects_tombstoned_page_that_reappears(pg, docs_settings, monkeypatch):
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda s: "docs__pytest-void")
    _seed_docsets(pg)
    _ingest_flask(pg, docs_settings)
    bumped = [dict(d) for d in MANIFEST]
    bumped[0]["mtime"] += 100
    _seed_docsets(pg, bumped)
    _ingest_flask(pg, docs_settings, db={"api/index": FLASK_DB["api/index"]})
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM documents WHERE id='docs:flask/installation/index'")
        assert cur.fetchone()[0] == "deleted"

    # the page comes back byte-identical: it must re-stage, not stay deleted
    bumped[0]["mtime"] += 100
    _seed_docsets(pg, bumped)
    totals = _ingest_flask(pg, docs_settings, db=dict(FLASK_DB))
    assert totals["staged"] == 2 and totals["skipped"] == 1  # installation + templating back
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM documents WHERE id='docs:flask/installation/index'")
        assert cur.fetchone()[0] == "deduped"


def test_ingest_marks_failed_docset_and_continues(pg, docs_settings):
    _seed_docsets(pg)
    settings = docs_settings.model_copy(update={"docs_slugs": "vue~3,flask"})
    # vue~3's routes are missing → KeyError inside stage; flask still completes
    client = _FakeClient(_routes("flask", FLASK_INDEX, FLASK_DB))
    totals = dingest.ingest(pg, settings, client=client, cdn_url=CDN)
    assert totals["docsets"] == 1 and totals["pages"] == 3
    with pg.cursor() as cur:
        cur.execute("SELECT slug, status, ingested_mtime FROM docsets ORDER BY slug")
        rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    assert rows["vue~3"] == ("failed", None)   # stays pending (mtime not advanced)
    assert rows["flask"][0] == "done"
    assert [d["slug"] for d in dsync.pending_docsets(pg, ["vue~3", "flask"])] == ["vue~3"]


def test_ingest_honors_max_docsets(pg, docs_settings):
    _seed_docsets(pg)
    settings = docs_settings.model_copy(update={"docs_slugs": "vue~3,flask"})
    vue_db = {"index": _page("Vue.js", "<p>The Progressive Framework.</p>",
                             upstream="https://vuejs.org/")}
    client = _FakeClient({**_routes("flask", FLASK_INDEX, FLASK_DB),
                          **_routes("vue~3", {"entries": [], "types": []}, vue_db)})
    totals = dingest.ingest(pg, settings, max_docsets=1, client=client, cdn_url=CDN)
    assert totals["docsets"] == 1
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE source='docs'")
        assert cur.fetchone()[0] == 1  # only vue~3 (seed-list order)
    table = pq.read_table(settings.staging_dir / "docs/clean/vue~3.parquet")
    assert table.to_pylist()[0]["framework"] == "vue"  # slug base, version-free


def test_tombstone_marks_ledger_and_drops_point(pg, settings, qclient, monkeypatch):
    from qdrant_client import models as qm

    from windex.ccnews.embed_index import point_id
    from windex.index import qdrant as qidx

    coll = qidx.ensure_collection(qclient, "docs", settings.embed_model, settings.embed_dim)
    # never let the delete resolve through the live alias: once production
    # collections exist, docs_current points at them, not the pytest one
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda source: coll)
    doc_id = "docs:flask/gone/index"
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO documents (id, source, url, status, embedded_model, indexed_at) "
            "VALUES (%s, 'docs', 'https://flask.palletsprojects.com/en/stable/gone/', "
            "'embedded', 'pytest-model', now())",
            (doc_id,),
        )
    pg.commit()
    pid = point_id(doc_id)
    qclient.upsert(coll, points=[qm.PointStruct(
        id=pid,
        vector={qidx.DENSE: [0.1] * settings.embed_dim,
                qidx.SPARSE: qm.SparseVector(indices=[1], values=[1.0])},
        payload={"doc_id": doc_id},
    )])
    assert qclient.retrieve(coll, ids=[pid])

    marked = dingest.apply_tombstones(pg, settings, [doc_id])
    assert marked == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM documents WHERE id=%s", (doc_id,))
        assert cur.fetchone()[0] == "deleted"
    assert qclient.retrieve(coll, ids=[pid]) == []
