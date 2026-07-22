"""Sitemap + llms.txt parsing and the hf_roots / hf_posts watermarks.

The fixtures below are trimmed copies of live huggingface.co responses
(verified 2026-07-17), including the details that bite: version-pinned llms.txt
links, org-namespaced blog slugs, and the four sitemap shards that must never
become a frontier.
"""

import pytest

from windex.config import Settings
from windex.hf import license_for, sync as hsync

SITEMAP_INDEX = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://huggingface.co/sitemap-static.xml</loc></sitemap>
  <sitemap><loc>https://huggingface.co/sitemap-doc.xml</loc></sitemap>
  <sitemap><loc>https://huggingface.co/sitemap-blog.xml</loc></sitemap>
  <sitemap><loc>https://huggingface.co/sitemap-models.xml</loc></sitemap>
  <sitemap><loc>https://huggingface.co/sitemap-datasets.xml</loc></sitemap>
  <sitemap><loc>https://huggingface.co/sitemap-spaces.xml</loc></sitemap>
  <sitemap><loc>https://huggingface.co/sitemap-papers.xml</loc></sitemap>
</sitemapindex>"""

SITEMAP_DOC = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://huggingface.co/docs/transformers</loc>
       <lastmod>2026-07-15T09:00:00.000Z</lastmod></url>
  <url><loc>https://huggingface.co/learn/agents-course</loc>
       <lastmod>2026-06-28T17:11:13.000Z</lastmod></url>
  <url><loc>https://huggingface.co/docs/evaluate</loc>
       <lastmod>2024-01-02T00:00:00.000Z</lastmod></url>
</urlset>"""

SITEMAP_BLOG = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://huggingface.co/blog/nvidia/nemotron-3-embed-wins-rteb</loc>
       <lastmod>2026-07-16T16:01:21.000Z</lastmod></url>
  <url><loc>https://huggingface.co/blog/annotated-diffusion</loc>
       <lastmod>2022-06-07T00:00:00.000Z</lastmod></url>
</urlset>"""

LLMS_TRANSFORMERS = """# Transformers

## Docs

- [Quickstart](https://huggingface.co/docs/transformers/v5.14.0/quicktour.md)
- [Philosophy](https://huggingface.co/docs/transformers/v5.14.0/philosophy.md)
- [Pipelines](https://huggingface.co/docs/transformers/v5.14.0/main_classes/pipelines.md)
"""


# --- sitemaps ---------------------------------------------------------------

def test_sitemap_index_takes_only_the_two_complete_shards():
    """THE SITEMAP TRAP. models/datasets/spaces/papers are recency windows —
    datasets spans 8 days, models holds 6,026 of 2.9M sorted by recency, papers
    is capped at exactly 10,000. Using one as a frontier would silently index a
    random recent slice of the Hub while looking like it works. Only doc (52,
    complete) and blog (829, 2020→today, the whole archive) may be read."""
    shards = hsync.parse_sitemap_index(SITEMAP_INDEX)
    assert shards == [
        "https://huggingface.co/sitemap-doc.xml",
        "https://huggingface.co/sitemap-blog.xml",
    ]
    joined = " ".join(shards)
    for trap in ("models", "datasets", "spaces", "papers"):
        assert trap not in joined


def test_root_key_and_kind_come_from_the_url_path():
    assert hsync.root_key("https://huggingface.co/docs/transformers") == "docs/transformers"
    assert hsync.root_key("https://huggingface.co/learn/agents-course") == "learn/agents-course"
    assert hsync.kind_of("docs/transformers") == "docs"
    assert hsync.kind_of("learn/agents-course") == "learn"


def test_blog_slug_keeps_org_namespace():
    """Blog slugs are NOT always flat: org-authored posts are namespaced, and
    the id has to carry the slash verbatim."""
    assert hsync.blog_slug(
        "https://huggingface.co/blog/nvidia/nemotron-3-embed-wins-rteb"
    ) == "nvidia/nemotron-3-embed-wins-rteb"
    assert hsync.blog_slug("https://huggingface.co/blog/annotated-diffusion") == "annotated-diffusion"


# --- llms.txt ---------------------------------------------------------------

def test_parse_llms_splits_the_version_out_of_the_path():
    """llms.txt links are version-pinned; the version is recorded but must never
    reach the doc id — a version bump has to upsert, not fork."""
    pages = hsync.parse_llms(LLMS_TRANSFORMERS, "docs/transformers")
    assert [p["path"] for p in pages] == ["quicktour", "philosophy", "main_classes/pipelines"]
    assert {p["version"] for p in pages} == {"v5.14.0"}
    assert pages[0]["title"] == "Quickstart"
    assert hsync.root_version(pages) == "v5.14.0"


def test_parse_llms_does_not_mistake_a_page_named_main_for_a_version():
    """`main_classes/pipelines` starts with "main" and is a real page path. The
    version regex is anchored on v+digit precisely so it can't swallow it."""
    pages = hsync.parse_llms(LLMS_TRANSFORMERS, "docs/transformers")
    pipelines = next(p for p in pages if "pipelines" in p["path"])
    assert pipelines["path"] == "main_classes/pipelines"
    assert pipelines["version"] == "v5.14.0"


def test_parse_llms_does_not_treat_a_bare_v1_folder_as_a_version():
    """A real content path segment shaped like a bare version (e.g. a folder named
    'v1') must NOT be stripped as a version pin — doing so collapses '/v1/overview'
    and '/overview' onto the same doc id and silently loses one page. A genuine
    version pin carries dots (v5.14.0); a bare vN does not."""
    text = (
        "- [Overview](https://huggingface.co/docs/root/overview.md)\n"
        "- [V1 Overview](https://huggingface.co/docs/root/v1/overview.md)\n"
    )
    pages = hsync.parse_llms(text, "docs/root")
    assert sorted(p["path"] for p in pages) == ["overview", "v1/overview"]  # distinct
    assert all(p["version"] == "" for p in pages)  # neither is a version pin


def test_parse_llms_handles_unversioned_links():
    text = "- [Index](https://huggingface.co/docs/hub/index.md)"
    pages = hsync.parse_llms(text, "docs/hub")
    assert pages == [{"path": "index", "title": "Index", "version": ""}]


def test_parse_llms_drops_foreign_and_non_md_links():
    text = (
        "- [Ours](https://huggingface.co/docs/transformers/v5.14.0/quicktour.md)\n"
        "- [Other root](https://huggingface.co/docs/diffusers/v1.0.0/index.md)\n"
        "- [Not markdown](https://huggingface.co/docs/transformers/v5.14.0/quicktour)\n"
        "- [Offsite](https://example.com/whatever.md)\n"
    )
    assert [p["path"] for p in hsync.parse_llms(text, "docs/transformers")] == ["quicktour"]


def test_parse_llms_first_occurrence_of_a_path_wins():
    text = (
        "- [First](https://huggingface.co/docs/transformers/v5.14.0/quicktour.md)\n"
        "- [Dup](https://huggingface.co/docs/transformers/v5.14.0/quicktour.md)\n"
    )
    pages = hsync.parse_llms(text, "docs/transformers")
    assert len(pages) == 1 and pages[0]["title"] == "First"


def test_license_is_per_root_and_unknown_stays_empty():
    """Per-root licenses genuinely differ, so an unchecked root reports "" —
    never a guess. A wrong license string is worse than an absent one."""
    assert license_for("docs/transformers") == "Apache-2.0"
    assert license_for("docs/huggingface.js") == "MIT"
    assert license_for("learn/some-new-course") == ""


# --- watermarks -------------------------------------------------------------

class FakeFetcher:
    """Stands in for the polite PageFetcher; records what was requested."""

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


def test_sync_upserts_roots_and_posts_and_is_idempotent(pg, settings_hf):
    fetcher = FakeFetcher({
        "https://huggingface.co/sitemap.xml": SITEMAP_INDEX,
        "https://huggingface.co/sitemap-doc.xml": SITEMAP_DOC,
        "https://huggingface.co/sitemap-blog.xml": SITEMAP_BLOG,
    })
    for _ in range(2):  # idempotency is part of the contract
        for shard in hsync.parse_sitemap_index(SITEMAP_INDEX):
            body = fetcher.fetch(shard)
            entries = hsync.parse_urlset(body)
            if shard.endswith("sitemap-doc.xml"):
                hsync.upsert_roots(pg, entries)
            else:
                hsync.upsert_posts(pg, entries)
    with pg.cursor() as cur:
        cur.execute("SELECT root, kind, license FROM hf_roots ORDER BY root")
        assert cur.fetchall() == [
            ("docs/evaluate", "docs", "Apache-2.0"),
            ("docs/transformers", "docs", "Apache-2.0"),
            ("learn/agents-course", "learn", ""),
        ]
        cur.execute("SELECT count(*) FROM hf_posts")
        assert cur.fetchone()[0] == 2


def test_refresh_llms_hashes_present_roots_and_flags_missing_ones(pg, settings_hf):
    """5 of 52 roots 404 on llms.txt. They get status='no_llms' and a NULL hash,
    which keeps them out of the crawl forever — deliberate: the docs nav is
    client-rendered, so without llms.txt there is no enumeration path at all."""
    hsync.upsert_roots(pg, hsync.parse_urlset(SITEMAP_DOC))
    fetcher = FakeFetcher({
        "https://huggingface.co/docs/transformers/llms.txt": LLMS_TRANSFORMERS,
        # learn/agents-course + docs/evaluate: absent -> None (404)
    })
    stats = hsync.refresh_llms(pg, fetcher, ["docs/transformers", "docs/evaluate"])
    assert stats == {"checked": 2, "with_llms": 1, "no_llms": 1, "pages": 3}
    with pg.cursor() as cur:
        cur.execute("SELECT llms_hash, pages, version, status FROM hf_roots "
                    "WHERE root = 'docs/transformers'")
        h, pages, version, status = cur.fetchone()
        assert h == hsync.sha1(LLMS_TRANSFORMERS)
        assert (pages, version, status) == (3, "v5.14.0", "pending")
        cur.execute("SELECT llms_hash, status FROM hf_roots WHERE root = 'docs/evaluate'")
        assert cur.fetchone() == (None, "no_llms")


def test_pending_roots_is_gated_on_the_hash_not_the_status(pg, settings_hf):
    """A job killed mid-root leaves status='processing'. Pending-ness never
    consults status — it compares llms_hash to ingested_hash — so the root is
    still pending and simply re-crawls. That is why there is no stale claim to
    reclaim here: a status-gated queue is what once stranded 3 years of arXiv."""
    hsync.upsert_roots(pg, hsync.parse_urlset(SITEMAP_DOC))
    hsync.mark_root_llms(pg, "docs/transformers", "hash-1", 3, "v5.14.0", "pending")

    assert [r["root"] for r in hsync.pending_roots(pg, [])] == ["docs/transformers"]

    # a crawl claims it and is then SIGKILLed: the row is stranded 'processing'
    hsync.mark_root(pg, "docs/transformers", "processing")
    assert [r["root"] for r in hsync.pending_roots(pg, [])] == ["docs/transformers"]

    # completing it advances ingested_hash -> no longer pending
    hsync.mark_root(pg, "docs/transformers", "done", ingested_hash="hash-1")
    assert hsync.pending_roots(pg, []) == []

    # upstream publishes a new version -> hash moves -> pending again
    hsync.mark_root_llms(pg, "docs/transformers", "hash-2", 4, "v5.15.0", "pending")
    assert [r["root"] for r in hsync.pending_roots(pg, [])] == ["docs/transformers"]


def test_pending_roots_respects_the_configured_seed_list(pg, settings_hf):
    hsync.upsert_roots(pg, hsync.parse_urlset(SITEMAP_DOC))
    for root in ("docs/transformers", "learn/agents-course"):
        hsync.mark_root_llms(pg, root, f"h-{root}", 1, "", "pending")
    assert len(hsync.pending_roots(pg, [])) == 2  # [] = all
    assert [r["root"] for r in hsync.pending_roots(pg, ["learn/agents-course"])] == [
        "learn/agents-course"
    ]


def test_roots_without_llms_never_become_pending(pg, settings_hf):
    hsync.upsert_roots(pg, hsync.parse_urlset(SITEMAP_DOC))
    hsync.mark_root_llms(pg, "docs/evaluate", None, 0, "", "no_llms")
    assert hsync.pending_roots(pg, []) == []


def test_pending_posts_tracks_lastmod_and_prefers_recent(pg, settings_hf):
    hsync.upsert_posts(pg, hsync.parse_urlset(SITEMAP_BLOG))
    pending = hsync.pending_posts(pg, 10)
    assert [p["slug"] for p in pending] == [
        "nvidia/nemotron-3-embed-wins-rteb",  # 2026 first: newest-first
        "annotated-diffusion",
    ]
    for p in pending:
        hsync.mark_post(pg, p["slug"], "done", ingested_lastmod=p["lastmod"])
    assert hsync.pending_posts(pg, 10) == []

    # an edited post: sitemap lastmod advances -> pending again
    hsync.upsert_posts(pg, [("https://huggingface.co/blog/annotated-diffusion",
                             "2026-07-17T10:00:00.000Z")])
    assert [p["slug"] for p in hsync.pending_posts(pg, 10)] == ["annotated-diffusion"]


def test_upsert_posts_reports_added_vs_updated(pg, settings_hf):
    entries = hsync.parse_urlset(SITEMAP_BLOG)
    assert hsync.upsert_posts(pg, entries) == {"posts": 2, "added": 2, "updated": 0}
    # unchanged lastmod: the guarded upsert touches nothing
    assert hsync.upsert_posts(pg, entries) == {"posts": 2, "added": 0, "updated": 0}
    bumped = [(entries[0][0], "2026-07-18T00:00:00.000Z"), entries[1]]
    assert hsync.upsert_posts(pg, bumped) == {"posts": 2, "added": 0, "updated": 1}


def test_sync_end_to_end_uses_only_doc_and_blog_shards(pg, settings_hf, monkeypatch):
    fetcher = FakeFetcher({
        "https://huggingface.co/sitemap.xml": SITEMAP_INDEX,
        "https://huggingface.co/sitemap-doc.xml": SITEMAP_DOC,
        "https://huggingface.co/sitemap-blog.xml": SITEMAP_BLOG,
        "https://huggingface.co/docs/transformers/llms.txt": LLMS_TRANSFORMERS,
    })
    monkeypatch.setattr("windex.hf.fetch.build_fetcher", lambda client, settings: fetcher)
    out = hsync.sync(pg, settings_hf, client=object())

    # per-shard stats stay nested: both upserts report an "added", and flattening
    # them made 52 roots report 829 added (seen in a live run).
    assert out["doc"] == {"roots": 3, "added": 3}
    assert out["blog"] == {"posts": 2, "added": 2, "updated": 0}
    assert out["llms"]["with_llms"] == 1 and out["llms"]["no_llms"] == 2
    # Not one request to a rolling shard.
    for trap in ("sitemap-models", "sitemap-datasets", "sitemap-spaces", "sitemap-papers"):
        assert not any(trap in c for c in fetcher.calls)
    assert [r["root"] for r in hsync.pending_roots(pg, [])] == ["docs/transformers"]
