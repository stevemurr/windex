"""The HF crawl: .md staging, blog extraction, and the politeness that a
one-host crawl needs but smallweb's config cannot give it."""

import pyarrow.parquet as pq
import pytest

from windex.config import Settings
from windex.hf import crawl as hcrawl
from windex.hf import sync as hsync

LLMS = """# Transformers

## Docs

- [Quickstart](https://huggingface.co/docs/transformers/v5.14.0/quicktour.md)
- [Pipelines](https://huggingface.co/docs/transformers/v5.14.0/main_classes/pipelines.md)
"""

QUICKTOUR_MD = "# Quickstart\n\nTransformers is designed to be fast and easy to use.\n"
PIPELINES_MD = "# Pipelines\n\nThe pipeline API is the easiest way to use a model.\n"

BLOG_HTML = """<html><head><title>Ignored</title></head><body><article>
<h1>Fine-tuning with TRL</h1>
<p>This post walks through supervised fine-tuning end to end, covering dataset
preparation, the training loop, evaluation, and pushing the result to the Hub.
It is long enough that trafilatura treats it as a real document rather than a
navigational fragment, which is what we need for a faithful extraction test.</p>
</article></body></html>"""


class FakeFetcher:
    """Records every URL requested and serves canned bodies (None = failure)."""

    def __init__(self, responses: dict):
        self.responses = responses
        self.calls: list[str] = []

    def fetch(self, url: str):
        self.calls.append(url)
        return self.responses.get(url)


@pytest.fixture()
def settings_hf(pg_dsn, tmp_path):
    return Settings(_env_file=None, data_root=tmp_path, pg_dsn=pg_dsn,
                    embed_model="pytest-model", embed_dim=8)


@pytest.fixture()
def root_row():
    return {"root": "docs/transformers", "kind": "docs", "llms_hash": "h1",
            "version": "v5.14.0", "license": "Apache-2.0"}


def _fetcher(**overrides):
    responses = {
        "https://huggingface.co/docs/transformers/llms.txt": LLMS,
        "https://huggingface.co/docs/transformers/quicktour.md": QUICKTOUR_MD,
        "https://huggingface.co/docs/transformers/main_classes/pipelines.md": PIPELINES_MD,
    }
    responses.update(overrides)
    return FakeFetcher(responses)


# --- ids and URLs -----------------------------------------------------------

def test_doc_id_is_hf_plus_the_canonical_path_and_carries_no_version():
    """A version bump must UPSERT the same document, not fork a new one — so the
    version stays out of the id even though llms.txt pins every link."""
    assert hcrawl.doc_id("docs/transformers", "quicktour") == "hf:docs/transformers/quicktour"
    assert hcrawl.doc_id("learn/agents-course", "unit1/intro") == "hf:learn/agents-course/unit1/intro"
    assert hcrawl.blog_doc_id("nvidia/foo") == "hf:blog/nvidia/foo"
    for did in (hcrawl.doc_id("docs/transformers", "quicktour"), hcrawl.blog_doc_id("x")):
        assert did.startswith("hf:")  # probes rely on prefix == source
        assert "v5.14.0" not in did


def test_page_url_is_the_unversioned_canonical():
    """rel=canonical points at the unversioned URL and it serves byte-identical
    content — so we fetch what we link, and link what we fetch."""
    assert hcrawl.page_url("docs/transformers", "quicktour") == (
        "https://huggingface.co/docs/transformers/quicktour"
    )
    assert hcrawl.md_url("docs/transformers", "quicktour") == (
        "https://huggingface.co/docs/transformers/quicktour.md"
    )


def test_md_title_prefers_the_h1_and_falls_back_to_the_llms_link_text():
    assert hcrawl.md_title(QUICKTOUR_MD, "Ignored") == "Quickstart"
    assert hcrawl.md_title("No heading here.\n", "From llms.txt") == "From llms.txt"


# --- docs staging -----------------------------------------------------------

def test_stage_root_writes_markdown_straight_to_parquet_and_the_ledger(pg, settings_hf, root_row):
    fetcher = _fetcher()
    stats = hcrawl.stage_root(pg, settings_hf, root_row, fetcher)

    assert stats["pages"] == 2 and stats["fetched"] == 2 and stats["staged"] == 2
    assert stats["failed"] == 0 and stats["llms_hash"] == hsync.sha1(LLMS)

    table = pq.read_table(settings_hf.staging_dir / "hf/clean/docs__transformers.parquet")
    rows = {r["id"]: r for r in table.to_pylist()}
    assert set(rows) == {
        "hf:docs/transformers/quicktour", "hf:docs/transformers/main_classes/pipelines",
    }
    page = rows["hf:docs/transformers/quicktour"]
    assert page["url"] == "https://huggingface.co/docs/transformers/quicktour"
    assert page["title"] == "Quickstart"
    assert page["kind"] == "docs" and page["root"] == "transformers"
    assert page["version"] == "v5.14.0" and page["license"] == "Apache-2.0"
    assert page["published_at"] == ""  # reference pages aren't dated
    assert page["text"] == QUICKTOUR_MD  # markdown verbatim: no extraction

    with pg.cursor() as cur:
        cur.execute("SELECT id, source, url, canonical_url, text_ref, status FROM documents")
        ledger = {r[0]: r for r in cur.fetchall()}
    assert len(ledger) == 2
    row = ledger["hf:docs/transformers/quicktour"]
    assert row[1] == "hf"
    # url == canonical_url: the unversioned URL the page itself declares
    assert row[2] == row[3] == "https://huggingface.co/docs/transformers/quicktour"
    assert row[4] == "hf/clean/docs__transformers.parquet"
    assert row[5] == "deduped"


def test_stage_root_is_idempotent_and_reembeds_only_changed_pages(pg, settings_hf, root_row):
    hcrawl.stage_root(pg, settings_hf, root_row, _fetcher())
    again = hcrawl.stage_root(pg, settings_hf, root_row, _fetcher())
    assert again["staged"] == 0 and again["skipped"] == 2  # text_hash guard

    changed = _fetcher(**{
        "https://huggingface.co/docs/transformers/quicktour.md":
            "# Quickstart\n\nRewritten for v5.15.\n",
    })
    third = hcrawl.stage_root(pg, settings_hf, root_row, changed)
    assert third["staged"] == 1 and third["skipped"] == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM documents WHERE id = 'hf:docs/transformers/quicktour'")
        assert cur.fetchone()[0] == "deduped"  # queued for re-embed


def test_stage_root_tombstones_pages_that_left_llms_txt(pg, settings_hf, root_row, monkeypatch):
    monkeypatch.setattr(hcrawl, "apply_tombstones", lambda conn, s, ids: len(ids))
    hcrawl.stage_root(pg, settings_hf, root_row, _fetcher())

    shrunk = _fetcher(**{
        "https://huggingface.co/docs/transformers/llms.txt":
            "# Transformers\n\n- [Quickstart](https://huggingface.co/docs/transformers/v5.14.0/quicktour.md)\n",
    })
    stats = hcrawl.stage_root(pg, settings_hf, root_row, shrunk)
    assert stats["deleted"] == 1  # main_classes/pipelines is gone upstream


def test_a_failed_page_does_not_vanish_from_the_parquet(pg, settings_hf, root_row):
    """Staging is full-replace per root. If a page 502s on a later run it must
    NOT drop out of the parquet: its ledger row still points at this text_ref,
    and the embed reader would then find nothing for it — leaving it 'deduped'
    forever, embedded by no one and erroring for no one. The previous text is
    carried forward instead, and the root stays pending so the page is retried."""
    hcrawl.stage_root(pg, settings_hf, root_row, _fetcher())

    flaky = _fetcher(**{"https://huggingface.co/docs/transformers/quicktour.md": None})
    stats = hcrawl.stage_root(pg, settings_hf, root_row, flaky)
    assert stats["failed"] == 1 and stats["fetched"] == 1

    table = pq.read_table(settings_hf.staging_dir / "hf/clean/docs__transformers.parquet")
    rows = {r["id"]: r for r in table.to_pylist()}
    assert "hf:docs/transformers/quicktour" in rows, "carried-forward page went missing"
    assert rows["hf:docs/transformers/quicktour"]["text"] == QUICKTOUR_MD

    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE text_ref IS NOT NULL")
        assert cur.fetchone()[0] == 2  # both ledger rows still resolve


def test_stage_root_returns_early_when_llms_txt_is_gone(pg, settings_hf, root_row):
    fetcher = _fetcher(**{"https://huggingface.co/docs/transformers/llms.txt": None})
    stats = hcrawl.stage_root(pg, settings_hf, root_row, fetcher)
    assert stats["llms_hash"] is None and stats["fetched"] == 0
    assert len(fetcher.calls) == 1  # no page fetches attempted


def test_documents_batch_write_is_sorted_by_id(pg, settings_hf, root_row, monkeypatch):
    """Regression (2026-07-16): ingest and the embed loop lock the same
    `documents` rows; different orders deadlock and cost a chunk of a corpus.
    Every batch writer sorts — assert the ids actually arrive in order."""
    seen = {}

    class SpyCursor:
        def __init__(self, inner):
            self.inner = inner

        def executemany(self, sql, rows, **kw):
            if "INSERT INTO documents" in sql:
                seen["rows"] = list(rows)
            return self.inner.executemany(sql, rows, **kw)

        def __getattr__(self, name):
            return getattr(self.inner, name)

    class SpyConn:
        def __init__(self, inner):
            self.inner = inner

        def cursor(self):
            import contextlib

            @contextlib.contextmanager
            def cm():
                with self.inner.cursor() as cur:
                    yield SpyCursor(cur)

            return cm()

        def __getattr__(self, name):
            return getattr(self.inner, name)

    hcrawl.stage_root(SpyConn(pg), settings_hf, root_row, _fetcher())
    ids = [r[0] for r in seen["rows"]]
    assert ids == sorted(ids), f"documents batch written out of id order: {ids}"
    assert len(ids) == 2


# --- blog -------------------------------------------------------------------

def test_extract_post_uses_trafilatura_without_the_quality_gate():
    """Docs arrive as markdown, but the blog has no .md (404) → HTML +
    trafilatura. The FineWeb/spaCy filters are deliberately NOT applied: they
    over-reject short/idiosyncratic text and this corpus is curated by
    construction — the quality filter here is the scope decision."""
    out = hcrawl.extract_post(BLOG_HTML, "https://huggingface.co/blog/trl-sft")
    assert out is not None
    assert out["title"] == "Fine-tuning with TRL"
    assert "supervised fine-tuning" in out["text"]


def test_stage_posts_stages_extracted_html_and_advances_the_watermark(pg, settings_hf):
    hsync.upsert_posts(pg, [("https://huggingface.co/blog/trl-sft", "2026-07-16T16:01:21.000Z")])
    posts = hsync.pending_posts(pg, 10)
    fetcher = FakeFetcher({"https://huggingface.co/blog/trl-sft": BLOG_HTML})

    stats = hcrawl.stage_posts(pg, settings_hf, posts, fetcher, "hf/clean/blog/t_0000.parquet")
    assert stats["fetched"] == 1 and stats["staged"] == 1

    rows = pq.read_table(settings_hf.staging_dir / "hf/clean/blog/t_0000.parquet").to_pylist()
    assert rows[0]["id"] == "hf:blog/trl-sft"
    assert rows[0]["kind"] == "blog" and rows[0]["root"] == "blog"
    assert rows[0]["published_at"]  # blog posts are dated
    assert hsync.pending_posts(pg, 10) == []  # watermark advanced


def test_stage_posts_keeps_a_failed_post_pending(pg, settings_hf):
    hsync.upsert_posts(pg, [("https://huggingface.co/blog/gone", "2026-07-16T16:01:21.000Z")])
    posts = hsync.pending_posts(pg, 10)
    stats = hcrawl.stage_posts(pg, settings_hf, posts, FakeFetcher({}), "hf/clean/blog/t.parquet")
    assert stats["failed"] == 1 and stats["staged"] == 0
    # a 502 is not a reason to drop a post forever
    assert [p["slug"] for p in hsync.pending_posts(pg, 10)] == ["gone"]


# --- the whole crawl --------------------------------------------------------

def test_crawl_advances_ingested_hash_only_on_a_complete_root(pg, settings_hf, monkeypatch):
    hsync.upsert_roots(pg, [("https://huggingface.co/docs/transformers", "2026-07-15T09:00:00Z")])
    hsync.mark_root_llms(pg, "docs/transformers", hsync.sha1(LLMS), 2, "v5.14.0", "pending")
    flaky = _fetcher(**{"https://huggingface.co/docs/transformers/main_classes/pipelines.md": None})
    monkeypatch.setattr("windex.hf.fetch.build_fetcher", lambda c, s: flaky)

    hcrawl.crawl(pg, settings_hf, client=object())
    with pg.cursor() as cur:
        cur.execute("SELECT status, ingested_hash FROM hf_roots WHERE root='docs/transformers'")
        status, ingested = cur.fetchone()
    assert status == "partial" and ingested is None
    # still pending -> the next run retries the missing page
    assert [r["root"] for r in hsync.pending_roots(pg, [])] == ["docs/transformers"]

    monkeypatch.setattr("windex.hf.fetch.build_fetcher", lambda c, s: _fetcher())
    hcrawl.crawl(pg, settings_hf, client=object())
    with pg.cursor() as cur:
        cur.execute("SELECT status, ingested_hash FROM hf_roots WHERE root='docs/transformers'")
        assert cur.fetchone() == ("done", hsync.sha1(LLMS))
    assert hsync.pending_roots(pg, []) == []


def test_crawl_skips_unchanged_roots_for_one_request_each(pg, settings_hf, monkeypatch):
    """The hash gate is load-bearing, not an optimization: at 1 req/3s a naive
    re-sweep costs 3.3 HOURS every night. An unchanged root must cost nothing."""
    hsync.upsert_roots(pg, [("https://huggingface.co/docs/transformers", "2026-07-15T09:00:00Z")])
    hsync.mark_root_llms(pg, "docs/transformers", hsync.sha1(LLMS), 2, "v5.14.0", "pending")
    monkeypatch.setattr("windex.hf.fetch.build_fetcher", lambda c, s: _fetcher())
    hcrawl.crawl(pg, settings_hf, client=object())

    second = _fetcher()
    monkeypatch.setattr("windex.hf.fetch.build_fetcher", lambda c, s: second)
    totals = hcrawl.crawl(pg, settings_hf, client=object())
    assert totals["roots"] == 0
    assert second.calls == []  # not even the llms.txt: sync owns that check


def test_crawl_blog_loop_terminates_when_every_post_fails(pg, settings_hf, monkeypatch):
    """Failed posts keep their watermark (they must be retried later), so the
    batch loop has to bound the RUN rather than the post — or it would re-serve
    the same failures forever."""
    hsync.upsert_posts(pg, [
        (f"https://huggingface.co/blog/p{i}", "2026-07-16T00:00:00.000Z") for i in range(5)
    ])
    monkeypatch.setattr("windex.hf.fetch.build_fetcher", lambda c, s: FakeFetcher({}))
    totals = hcrawl.crawl(pg, settings_hf, client=object())
    assert totals["posts"] == 5 and totals["posts_failed"] == 5
    assert len(hsync.pending_posts(pg, 10)) == 5  # all still pending for next run
