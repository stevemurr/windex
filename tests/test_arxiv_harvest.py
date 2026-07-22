"""arXiv OAI-PMH harvest: pure XML parsing from fabricated ListRecords pages,
window-watermark idempotency, streamed harvest → parquet + ledger with text_hash
delta detection, and deletedRecord tombstone handling. Uses the pg fixture and a
fake OAI client so no network is touched."""

from datetime import date

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from windex.arxiv import harvest as aharvest

# --- fabricated OAI-PMH fixtures -------------------------------------------


def _record_xml(pid, title, abstract, created="2024-01-02", updated=None,
                cats="cs.LG stat.ML", authors=(("Smith", "John"),), doi=None):
    authors_xml = "".join(
        f"<author><keyname>{k}</keyname><forenames>{f}</forenames></author>"
        for k, f in authors
    )
    updated_xml = f"<updated>{updated}</updated>" if updated else ""
    doi_xml = f"<doi>{doi}</doi>" if doi else ""
    return (
        f'<record><header><identifier>oai:arXiv.org:{pid}</identifier>'
        f'<datestamp>{created}</datestamp></header>'
        f'<metadata><arXiv xmlns="http://arxiv.org/OAI/arXiv/">'
        f'<id>{pid}</id><created>{created}</created>{updated_xml}'
        f'<authors>{authors_xml}</authors>'
        f'<title>{title}</title><categories>{cats}</categories>{doi_xml}'
        f'<abstract>{abstract}</abstract>'
        f'</arXiv></metadata></record>'
    )


def _deleted_xml(pid, datestamp="2024-01-02"):
    return (
        f'<record><header status="deleted"><identifier>oai:arXiv.org:{pid}</identifier>'
        f'<datestamp>{datestamp}</datestamp></header></record>'
    )


def _page_xml(records_xml, token=None):
    token_xml = f"<resumptionToken>{token}</resumptionToken>" if token else ""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
        '<responseDate>2026-07-16T00:00:00Z</responseDate>'
        f'<ListRecords>{records_xml}{token_xml}</ListRecords></OAI-PMH>'
    ).encode()


class _FakeResp:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        pass


class _FakeClient:
    """Returns a canned page keyed by the resumptionToken param (None = first)."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get(self, url, params=None):
        self.calls.append(dict(params or {}))
        return _FakeResp(self.pages[(params or {}).get("resumptionToken")])

    def close(self):
        pass


# --- pure parsing ----------------------------------------------------------


def test_paper_id_and_abs_url():
    assert aharvest.paper_id("oai:arXiv.org:0805.3819") == "0805.3819"
    assert aharvest.paper_id("oai:arXiv.org:hep-th/9901001") == "hep-th/9901001"
    assert aharvest.abs_url("hep-th/9901001") == "https://arxiv.org/abs/hep-th/9901001"


def test_parse_records_extracts_fields_and_token():
    xml = _page_xml(
        _record_xml("0805.3819", "A Title  with   spaces", "An  abstract. ",
                    created="2024-01-01", updated="2024-01-02", cats="physics.gen-ph",
                    authors=[("Pfeifer", "R. N. C.")], doi="10.1/x"),
        token="verb%3DListRecords%26skip%3D82",
    )
    records, token = aharvest.parse_records(xml)
    assert token == "verb%3DListRecords%26skip%3D82"
    assert len(records) == 1
    r = records[0]
    assert r["id"] == "0805.3819" and r["deleted"] is False
    assert r["title"] == "A Title with spaces"  # whitespace collapsed
    assert r["abstract"] == "An abstract."
    assert r["primary_category"] == "physics.gen-ph" and r["categories"] == ["physics.gen-ph"]
    assert r["authors"] == ["R. N. C. Pfeifer"]
    assert r["created"] == "2024-01-01" and r["updated"] == "2024-01-02" and r["doi"] == "10.1/x"


def test_parse_records_multicat_and_deleted_tombstone():
    xml = _page_xml(
        _record_xml("2401.1", "T", "A", cats="cs.DB stat.ML") + _deleted_xml("hep-th/9901001"),
        token=None,
    )
    records, token = aharvest.parse_records(xml)
    assert token is None
    live = [r for r in records if not r.get("deleted")]
    dele = [r for r in records if r.get("deleted")]
    assert live[0]["categories"] == ["cs.DB", "stat.ML"] and live[0]["primary_category"] == "cs.DB"
    assert dele and dele[0]["id"] == "hep-th/9901001"


def test_parse_records_no_records_match_is_empty():
    xml = (b'<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
           b'<error code="noRecordsMatch">nothing</error></OAI-PMH>')
    assert aharvest.parse_records(xml) == ([], None)


def test_parse_records_protocol_error_raises():
    xml = (b'<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
           b'<error code="badArgument">bad</error></OAI-PMH>')
    with pytest.raises(aharvest.OAIError):
        aharvest.parse_records(xml)


# --- window watermark ------------------------------------------------------


def test_plan_backfill_is_idempotent(pg):
    assert aharvest.plan_backfill(pg, 2005, 2007) == 3
    assert aharvest.plan_backfill(pg, 2005, 2007) == 0  # already recorded
    assert aharvest.pending_windows(pg, 10) == [
        ("2005-01-01", "2005-12-31"),
        ("2006-01-01", "2006-12-31"),
        ("2007-01-01", "2007-12-31"),
    ]


def test_plan_incremental_rearms_completed_window(pg):
    frm, until = aharvest.plan_incremental(pg, 7, today=date(2026, 7, 16))
    assert (frm, until) == ("2026-07-09", "2026-07-16")
    aharvest.mark_window(pg, frm, until, "done")
    assert aharvest.pending_windows(pg, 10) == []
    # a later freshness run re-arms the same span so updates are re-harvested
    aharvest.plan_incremental(pg, 7, today=date(2026, 7, 16))
    assert aharvest.pending_windows(pg, 10) == [(frm, until)]


# --- streamed harvest ------------------------------------------------------


def test_harvest_follows_token_chain_and_stages(pg, settings):
    first = _page_xml(
        _record_xml("2401.1", "Deep Nets", "alpha " * 5, cats="cs.LG stat.ML",
                    authors=[("LeCun", "Yann"), ("Bengio", "Yoshua"),
                             ("Hinton", "Geoffrey"), ("Ng", "Andrew")], doi="10.1/x")
        + _record_xml("2401.2", "Kernels", "beta " * 5, cats="math.CO"),
        token="t1",
    )
    second = _page_xml(_record_xml("2401.3", "Transformers", "gamma " * 5, cats="cs.CL"), token=None)
    fake = _FakeClient({None: first, "t1": second})

    aharvest.plan_backfill(pg, 2024, 2024)
    totals = aharvest.harvest(pg, settings, client=fake, request_interval=0)
    assert totals == {"windows": 1, "pages": 2, "records": 3, "staged": 3, "skipped": 0, "deleted": 0}

    # first call carries from/until/prefix; second call carries only the token
    assert fake.calls[0]["from"] == "2024-01-01" and fake.calls[0]["until"] == "2024-12-31"
    assert fake.calls[0]["metadataPrefix"] == "arXiv"
    assert fake.calls[1] == {"verb": "ListRecords", "resumptionToken": "t1"}

    text_ref = "arxiv/clean/2024-01-01_2024-12-31.parquet"
    table = pq.read_table(settings.staging_dir / text_ref)
    assert table.num_rows == 3
    assert set(table.column_names) == {
        "id", "url", "title", "abstract", "authors", "primary_category",
        "categories", "created", "updated", "doi",
    }
    row = table.filter(pc.equal(table["id"], "arxiv:2401.1")).to_pylist()[0]
    assert row["primary_category"] == "cs.LG" and row["categories"] == ["cs.LG", "stat.ML"]
    assert row["authors"] == ["Yann LeCun", "Yoshua Bengio", "Geoffrey Hinton", "Andrew Ng"]
    assert row["url"] == "https://arxiv.org/abs/2401.1" and row["doi"] == "10.1/x"

    with pg.cursor() as cur:
        cur.execute(
            "SELECT id, source, url, status, text_ref, published_at, text_hash "
            "FROM documents WHERE source='arxiv' ORDER BY id"
        )
        rows = cur.fetchall()
    assert [r[0] for r in rows] == ["arxiv:2401.1", "arxiv:2401.2", "arxiv:2401.3"]
    for r in rows:
        assert r[1] == "arxiv" and r[3] == "deduped" and r[4] == text_ref
        assert r[5] is not None and r[6]  # published_at (created) + text_hash populated
    doc1 = next(r for r in rows if r[0] == "arxiv:2401.1")
    assert doc1[2] == "https://arxiv.org/abs/2401.1"

    with pg.cursor() as cur:
        cur.execute(
            "SELECT status, pages, records, staged FROM arxiv_windows WHERE from_date='2024-01-01'"
        )
        assert cur.fetchone() == ("done", 2, 3, 3)


def test_reharvest_skips_unchanged_papers(pg, settings):
    base = [
        _record_xml("2401.1", "P1", "abstract one " * 3),
        _record_xml("2401.2", "P2", "abstract two " * 3),
        _record_xml("2401.3", "P3", "abstract three " * 3),
    ]
    aharvest.plan_backfill(pg, 2024, 2024)
    aharvest.harvest(pg, settings, client=_FakeClient({None: _page_xml("".join(base))}),
                     request_interval=0)
    with pg.cursor() as cur:
        cur.execute("UPDATE documents SET status='embedded', embedded_model='m', "
                    "indexed_at=now() WHERE source='arxiv'")
    pg.commit()

    changed = [
        _record_xml("2401.1", "P1", "abstract one " * 3),                       # unchanged
        _record_xml("2401.2", "P2", "REWRITTEN abstract two with new text " * 3),  # changed
        _record_xml("2401.3", "P3", "abstract three " * 3),                     # unchanged
    ]
    aharvest.plan_incremental(pg, 7, today=date(2024, 1, 15))  # a different window
    totals = aharvest.harvest(pg, settings, client=_FakeClient({None: _page_xml("".join(changed))}),
                              request_interval=0)
    assert totals["staged"] == 1 and totals["skipped"] == 2

    with pg.cursor() as cur:
        cur.execute("SELECT id, status, text_ref FROM documents WHERE source='arxiv' ORDER BY id")
        rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    assert rows["arxiv:2401.1"][0] == "embedded"      # untouched
    assert rows["arxiv:2401.3"][0] == "embedded"
    assert rows["arxiv:2401.2"][0] == "deduped"       # re-queued
    new_ref = rows["arxiv:2401.2"][1]
    assert new_ref == "arxiv/clean/2024-01-08_2024-01-15.parquet"
    assert pq.read_table(settings.staging_dir / new_ref).num_rows == 1  # only the changed paper


def test_tombstone_marks_ledger_and_drops_point(pg, settings, qclient, monkeypatch):
    from qdrant_client import models as qm

    from windex.ccnews.embed_index import point_id
    from windex.index import qdrant as qidx

    coll = qidx.ensure_collection(qclient, "arxiv", settings.embed_model, settings.embed_dim)
    # never let the delete resolve through the live alias: once production
    # collections exist, arxiv_current points at them, not the pytest one
    monkeypatch.setattr("windex.index.qdrant.alias_name", lambda source: coll)
    doc_id = "arxiv:2401.9"
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO documents (id, source, url, status, embedded_model, indexed_at) "
            "VALUES (%s, 'arxiv', 'https://arxiv.org/abs/2401.9', 'embedded', 'pytest-model', now())",
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

    marked = aharvest.apply_tombstones(pg, settings, [doc_id])
    assert marked == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM documents WHERE id=%s", (doc_id,))
        assert cur.fetchone()[0] == "deleted"
    assert qclient.retrieve(coll, ids=[pid]) == []


def test_harvest_applies_tombstone_during_run(pg, settings):
    # seed a prior paper, then re-harvest a page that tombstones it. Qdrant point
    # removal is best-effort (skipped cleanly if the collection/index is absent),
    # so this asserts the ledger side without requiring a seeded vector.
    aharvest.plan_backfill(pg, 2024, 2024)
    aharvest.harvest(pg, settings,
                     client=_FakeClient({None: _page_xml(_record_xml("2401.5", "Gone", "body " * 5))}),
                     request_interval=0)
    aharvest.plan_incremental(pg, 7, today=date(2024, 1, 15))
    totals = aharvest.harvest(
        pg, settings,
        client=_FakeClient({None: _page_xml(_deleted_xml("2401.5"))}),
        request_interval=0,
    )
    assert totals["records"] == 1 and totals["deleted"] == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM documents WHERE id='arxiv:2401.5'")
        assert cur.fetchone()[0] == "deleted"


def test_plan_backfill_clamps_to_earliest_datestamp(pg):
    """arXiv rejects a window starting before its earliestDatestamp outright
    (badArgument: "start date too early"), so the whole year fails and stays
    failed — losing the year's VALID tail with it. 2005 held ~3.5 months of
    papers we never fetched because the window asked from 2005-01-01."""
    from windex.arxiv import harvest as ah

    ah.plan_backfill(pg, 2004, 2007, earliest="2005-09-16")
    with pg.cursor() as cur:
        cur.execute("SELECT from_date, until_date FROM arxiv_windows ORDER BY from_date")
        rows = [(str(a), str(b)) for a, b in cur.fetchall()]
    assert ("2004-01-01", "2004-12-31") not in rows, "year entirely before the repo existed"
    assert ("2005-09-16", "2005-12-31") in rows, "clamped to the earliest datestamp, tail kept"
    assert ("2006-01-01", "2006-12-31") in rows
    assert ("2007-01-01", "2007-12-31") in rows


def test_plan_backfill_unclamped_when_earliest_unknown(pg):
    """Identify unreachable -> plan as before rather than refuse to work."""
    from windex.arxiv import harvest as ah

    ah.plan_backfill(pg, 2006, 2006, earliest=None)
    with pg.cursor() as cur:
        cur.execute("SELECT from_date FROM arxiv_windows")
        assert str(cur.fetchone()[0]) == "2006-01-01"


def test_reclaim_stale_frees_windows_from_a_killed_run(pg):
    """A killed harvest leaves its window 'processing'; pending_windows() only
    selects 'pending', so nothing ever retries it and that year is silently
    absent from the index. 2008/2014/2015 were stranded exactly this way."""
    from windex.arxiv import harvest as ah

    with pg.cursor() as cur:
        # stale: claimed over an hour ago by a worker that is gone
        cur.execute("""INSERT INTO arxiv_windows (from_date, until_date, status, processed_at)
                       VALUES ('2008-01-01','2008-12-31','processing', now() - interval '3 hours')""")
        # live: claimed seconds ago by a running worker — must NOT be stolen
        cur.execute("""INSERT INTO arxiv_windows (from_date, until_date, status, processed_at)
                       VALUES ('2009-01-01','2009-12-31','processing', now())""")
        cur.execute("""INSERT INTO arxiv_windows (from_date, until_date, status)
                       VALUES ('2010-01-01','2010-12-31','done')""")
    pg.commit()

    assert ah.reclaim_stale(pg, older_than_minutes=60) == 1
    pending = ah.pending_windows(pg, limit=10)
    assert ("2008-01-01", "2008-12-31") in [(str(a), str(b)) for a, b in pending]
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM arxiv_windows WHERE from_date='2009-01-01'")
        assert cur.fetchone()[0] == "processing", "stole a window from a live worker"
        cur.execute("SELECT status FROM arxiv_windows WHERE from_date='2010-01-01'")
        assert cur.fetchone()[0] == "done"


def test_reclaim_ignores_a_window_claimed_via_mark_window(pg):
    """The race in reclaim_stale: mark_window('processing') must stamp
    processed_at AT CLAIM TIME, otherwise a freshly-claimed window has
    processed_at NULL, which reclaim_stale treats as stale and steals — a nightly
    harvest firing while a manual backfill is actively working that same window
    (both then race the same tmp parquet). A claim from one moment ago must never
    be reclaimable."""
    from windex.arxiv import harvest as ah

    ah.plan_backfill(pg, 2007, 2007)
    ah.mark_window(pg, "2007-01-01", "2007-12-31", "processing")  # claimed just now
    with pg.cursor() as cur:
        cur.execute("SELECT processed_at FROM arxiv_windows WHERE from_date='2007-01-01'")
        assert cur.fetchone()[0] is not None, "claim did not stamp processed_at"

    assert ah.reclaim_stale(pg, older_than_minutes=60) == 0
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM arxiv_windows WHERE from_date='2007-01-01'")
        assert cur.fetchone()[0] == "processing"
