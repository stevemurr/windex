"""Driver tests: the BFS loop, the boilerplate guard, budgets, and resumability.

These use the real database (the `pg`/`settings` fixtures) but a fake fetcher —
the network is not the thing under test here, the decisions the driver makes
about what it fetched are.
"""

import pytest

from windex.crawl import recipe as R
from windex.crawl import run as crun
from windex.custom_source import registry

SEED = "https://example.dev/docs/"


def page(body: str, links=()) -> str:
    anchors = "".join(f'<a href="{u}">l</a>' for u in links)
    return (f"<html><head><title>T</title></head><body><main><p>{body}</p>"
            f"{anchors}</main></body></html>")


class FakeFetcher:
    """Serves a fixed URL→html map. Anything unmapped 404s (`reason='http'`)."""

    def __init__(self, pages: dict):
        self.pages = pages
        self.calls = []

    def fetch(self, url):
        self.calls.append(url)
        html = self.pages.get(url)
        if html is None:
            return None, url, "http"
        return html, url, ""


@pytest.fixture()
def source(pg, settings):
    registry.create(pg, "testcrawl", "Test", "")
    return "testcrawl"


def make(settings, **body):
    body.setdefault("seeds", [SEED])
    body.setdefault("limits", {"max_depth": 2, "max_pages": 100})
    return R.parse(body, settings)


def run(pg, settings, source, recipe, fetcher):
    run_id = crun.create_run(pg, source, recipe)
    crun.claim_run(pg)
    return run_id, crun.execute(pg, settings, run_id, source, recipe, fetcher=fetcher)


def frontier(pg, run_id):
    with pg.cursor() as cur:
        cur.execute("SELECT url, status, reason FROM crawl_urls WHERE run_id = %s", (run_id,))
        return {r[0]: (r[1], r[2]) for r in cur.fetchall()}


def test_crawls_cluster_and_stages_docs(pg, settings, source):
    pages = {
        SEED: page("index page " * 40, [f"{SEED}a", f"{SEED}b", "https://other.dev/x"]),
        f"{SEED}a": page("alpha content " * 40),
        f"{SEED}b": page("beta content " * 40),
    }
    run_id, stats = run(pg, settings, source, make(settings), FakeFetcher(pages))
    assert stats["fetched"] == 3
    assert stats["staged"] == 3
    assert stats["failed"] == 0
    # The off-host link was never enqueued, so it was never fetched.
    assert "https://other.dev/x" not in frontier(pg, run_id)
    with pg.cursor() as cur:
        cur.execute("SELECT id, url FROM documents WHERE source = %s ORDER BY id", (source,))
        rows = cur.fetchall()
    assert [r[0] for r in rows] == [
        "testcrawl:docs/", "testcrawl:docs/a", "testcrawl:docs/b"]
    # The real page URL is stored, not the custom:// placeholder — search results
    # have to link somewhere useful.
    assert rows[1][1] == f"{SEED}a"


def test_boilerplate_shell_is_dropped(pg, settings, source):
    """The soft-404 hazard: a site that answers 200 with the SAME shell for every
    unknown path would otherwise stage N distinct ids carrying identical text —
    upsert_docs' text_hash guard is per-id and does not catch that."""
    # Byte-identical shells, distinct from the seed's own text (so this exercises
    # the repeat counter, not the seed-hash guard).
    shell = page("nothing here, this is the index shell " * 20)
    pages = {
        SEED: page("real index " * 40, [f"{SEED}real", f"{SEED}ghost1", f"{SEED}ghost2"]),
        f"{SEED}real": page("genuine article " * 40),
        f"{SEED}ghost1": shell,
        f"{SEED}ghost2": shell,
    }
    run_id, stats = run(pg, settings, source, make(settings), FakeFetcher(pages))
    marks = frontier(pg, run_id)
    # First sighting of the shell is indistinguishable from a real page; the
    # repeat is what proves it is chrome.
    assert marks[f"{SEED}ghost2"] == ("skipped", "boilerplate")
    assert stats["boilerplate"] >= 1
    assert marks[f"{SEED}real"][0] == "staged"


def test_page_identical_to_seed_is_dropped_immediately(pg, settings, source):
    """A child page whose text equals the SEED's is the shell, first sighting or
    not — no repeat needed."""
    body = "the index listing " * 40
    # Both carry the same anchor: a link's text is part of the extracted body, so
    # without this the two pages differ by the anchor and are legitimately not
    # duplicates — the earlier version of this test proved nothing.
    ghost = f"{SEED}ghost"
    pages = {SEED: page(body, [ghost]), ghost: page(body, [ghost])}
    run_id, stats = run(pg, settings, source, make(settings), FakeFetcher(pages))
    assert frontier(pg, run_id)[f"{SEED}ghost"] == ("skipped", "boilerplate")
    assert stats["staged"] == 1  # only the seed


def test_boilerplate_guard_can_be_disabled(pg, settings, source):
    body = "same text everywhere " * 40
    pages = {SEED: page("index " * 40, [f"{SEED}a", f"{SEED}b", f"{SEED}c"]),
             f"{SEED}a": page(body), f"{SEED}b": page(body), f"{SEED}c": page(body)}
    recipe = make(settings, dedup={"drop_boilerplate": False})
    _run_id, stats = run(pg, settings, source, recipe, FakeFetcher(pages))
    assert stats["boilerplate"] == 0
    assert stats["fetched"] == 4


def test_max_depth_bounds_traversal(pg, settings, source):
    pages = {
        SEED: page("d0 " * 40, [f"{SEED}one"]),
        f"{SEED}one": page("d1 " * 40, [f"{SEED}two"]),
        f"{SEED}two": page("d2 " * 40),
    }
    recipe = make(settings, limits={"max_depth": 1, "max_pages": 100})
    run_id, stats = run(pg, settings, source, recipe, FakeFetcher(pages))
    assert stats["fetched"] == 2               # seed + depth 1
    assert f"{SEED}two" not in frontier(pg, run_id)


def test_max_pages_marks_truncated(pg, settings, source):
    """A crawl that stopped early must not look complete — that difference is
    the whole point when deciding if search results are missing something."""
    links = [f"{SEED}p{i}" for i in range(10)]
    pages = {SEED: page("index " * 40, links)}
    pages.update({u: page(f"body {u} " * 40) for u in links})
    recipe = make(settings, limits={"max_depth": 1, "max_pages": 4})
    _run_id, stats = run(pg, settings, source, recipe, FakeFetcher(pages))
    assert stats["fetched"] == 4
    assert stats["truncated"] is True


def test_completed_crawl_is_not_truncated(pg, settings, source):
    pages = {SEED: page("index " * 40, [f"{SEED}a"]), f"{SEED}a": page("a " * 40)}
    _run_id, stats = run(pg, settings, source, make(settings), FakeFetcher(pages))
    assert stats["truncated"] is False


def test_fetch_failure_is_isolated(pg, settings, source):
    """One dead page must not sink the cluster."""
    pages = {SEED: page("index " * 40, [f"{SEED}ok", f"{SEED}dead"]),
             f"{SEED}ok": page("fine " * 40)}
    run_id, stats = run(pg, settings, source, make(settings), FakeFetcher(pages))
    assert stats["failed"] == 1
    assert stats["staged"] == 2
    assert frontier(pg, run_id)[f"{SEED}dead"] == ("failed", "http")


def test_rerun_is_a_text_hash_noop(pg, settings, source):
    """Re-crawling unchanged pages must cost no re-embed — the property that
    makes scheduled refresh cheap."""
    pages = {SEED: page("index " * 40, [f"{SEED}a"]), f"{SEED}a": page("alpha " * 40)}
    recipe = make(settings)
    _first, s1 = run(pg, settings, source, recipe, FakeFetcher(pages))
    assert s1["staged"] == 2
    _second, s2 = run(pg, settings, source, recipe, FakeFetcher(pages))
    assert s2["fetched"] == 2      # pages are re-fetched
    assert s2["staged"] == 0       # but nothing is re-staged


def test_changed_page_restages(pg, settings, source):
    recipe = make(settings)
    pages = {SEED: page("index " * 40, [f"{SEED}a"]), f"{SEED}a": page("alpha " * 40)}
    run(pg, settings, source, recipe, FakeFetcher(pages))
    pages[f"{SEED}a"] = page("alpha rewritten " * 40)
    _run_id, stats = run(pg, settings, source, recipe, FakeFetcher(pages))
    assert stats["staged"] == 1


def test_cancel_stops_the_run(pg, settings, source):
    links = [f"{SEED}p{i}" for i in range(30)]
    pages = {SEED: page("index " * 40, links)}
    pages.update({u: page(f"b {u} " * 40) for u in links})
    recipe = make(settings, limits={"max_depth": 1, "max_pages": 100})
    run_id = crun.create_run(pg, source, recipe)
    crun.claim_run(pg)
    calls = {"n": 0}

    def stop_after_first_batch():
        calls["n"] += 1
        return calls["n"] > 1

    stats = crun.execute(pg, settings, run_id, source, recipe,
                         fetcher=FakeFetcher(pages), should_stop=stop_after_first_batch)
    assert stats["fetched"] < 31          # stopped early
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM crawl_urls WHERE run_id = %s AND status = 'pending'",
                    (run_id,))
        assert cur.fetchone()[0] > 0      # frontier survives for a resume


def test_resume_continues_from_persisted_frontier(pg, settings, source):
    """A killed run resumes rather than restarting at the seed — the reason the
    frontier is a table and not an in-memory set."""
    links = [f"{SEED}p{i}" for i in range(6)]
    pages = {SEED: page("index " * 40, links)}
    pages.update({u: page(f"b {u} " * 40) for u in links})
    recipe = make(settings, limits={"max_depth": 1, "max_pages": 100})
    run_id = crun.create_run(pg, source, recipe)
    crun.claim_run(pg)

    calls = {"n": 0}
    crun.execute(pg, settings, run_id, source, recipe, fetcher=FakeFetcher(pages),
                 should_stop=lambda: (calls.__setitem__("n", calls["n"] + 1), calls["n"] > 1)[1])
    first = FakeFetcher(pages)
    # Resume the SAME run id: only the pages still pending should be fetched.
    stats = crun.execute(pg, settings, run_id, source, recipe, fetcher=first)
    assert SEED not in first.calls        # the seed is done; it is not re-fetched
    assert stats["fetched"] > 0
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM crawl_urls WHERE run_id = %s AND status = 'pending'",
                    (run_id,))
        assert cur.fetchone()[0] == 0


def test_reclaim_stale_returns_dead_runs(pg, settings, source):
    run_id = crun.create_run(pg, source, make(settings))
    crun.claim_run(pg)
    with pg.cursor() as cur:
        cur.execute("UPDATE crawl_runs SET heartbeat_at = now() - interval '2 days' "
                    "WHERE id = %s", (run_id,))
    pg.commit()
    assert crun.reclaim_stale(pg, settings) == 1
    with pg.cursor() as cur:
        cur.execute("SELECT status FROM crawl_runs WHERE id = %s", (run_id,))
        assert cur.fetchone()[0] == "pending"


def test_fresh_heartbeat_is_not_reclaimed(pg, settings, source):
    crun.create_run(pg, source, make(settings))
    crun.claim_run(pg)
    assert crun.reclaim_stale(pg, settings) == 0


def test_run_recipe_is_frozen_against_source_edits(pg, settings, source):
    """History must stay truthful: editing the source's recipe cannot rewrite
    what a past run executed."""
    recipe = make(settings, limits={"max_depth": 1, "max_pages": 7})
    run_id = crun.create_run(pg, source, recipe)
    registry.update(pg, source, recipe={"seeds": ["https://changed.dev/"], "version": 1})
    with pg.cursor() as cur:
        cur.execute("SELECT recipe FROM crawl_runs WHERE id = %s", (run_id,))
        stored = cur.fetchone()[0]
    assert stored["seeds"] == [SEED]
    assert stored["limits"]["max_pages"] == 7


# --- canonical URL ----------------------------------------------------------

def canon_page(body: str, canonical: str, links=()) -> str:
    anchors = "".join(f'<a href="{u}">l</a>' for u in links)
    return (f'<html><head><title>T</title>'
            f'<link rel="canonical" href="{canonical}"></head>'
            f"<body><main><p>{body}</p>{anchors}</main></body></html>")


def test_declared_canonical_becomes_the_doc_identity(pg, settings, source):
    """An index page often links articles with a referrer tag
    (`/docs/a?from=index`); indexing that URL forks one article into several
    documents when it is linked from several places."""
    tagged = f"{SEED}a?from=index"
    pages = {
        SEED: page("index " * 40, [tagged]),
        tagged: canon_page("article " * 40, f"{SEED}a"),
    }
    run(pg, settings, source, make(settings), FakeFetcher(pages))
    with pg.cursor() as cur:
        cur.execute("SELECT id, url FROM documents WHERE source = %s AND id <> %s",
                    (source, "testcrawl:docs/"))
        did, url = cur.fetchone()
    assert did == "testcrawl:docs/a"          # no ?from= in the id
    assert url == f"{SEED}a"


def test_og_url_is_used_when_no_rel_canonical(pg, settings, source):
    tagged = f"{SEED}b?from=index"
    og = (f'<html><head><title>T</title>'
          f'<meta property="og:url" content="{SEED}b"></head>'
          f'<body><main><p>{"article " * 40}</p></main></body></html>')
    pages = {SEED: page("index " * 40, [tagged]), tagged: og}
    run(pg, settings, source, make(settings), FakeFetcher(pages))
    with pg.cursor() as cur:
        cur.execute("SELECT url FROM documents WHERE source = %s AND id = %s",
                    (source, "testcrawl:docs/b"))
        assert cur.fetchone()[0] == f"{SEED}b"


def test_offsite_canonical_is_ignored(pg, settings, source):
    """A canonical pointing outside the cluster must NOT redirect what we index —
    otherwise any crawled page could rewrite its own identity to anywhere."""
    pages = {
        SEED: page("index " * 40, [f"{SEED}c"]),
        f"{SEED}c": canon_page("article " * 40, "https://evil.dev/owned"),
    }
    run(pg, settings, source, make(settings), FakeFetcher(pages))
    with pg.cursor() as cur:
        cur.execute("SELECT url FROM documents WHERE source = %s AND id = %s",
                    (source, "testcrawl:docs/c"))
        assert cur.fetchone()[0] == f"{SEED}c"   # the fetched URL, not the claim


# --- prune: self-cleaning when scope narrows --------------------------------
# The dangerous feature. Every test below is really about when prune must
# REFUSE, because the failure mode is silent corpus deletion.

def prune_recipe(settings, **over):
    body = {"seeds": [SEED], "limits": {"max_depth": 2, "max_pages": 100},
            "dedup": {"prune": True}}
    body.update(over)
    return R.parse(body, settings)


def live_ids(pg, source):
    with pg.cursor() as cur:
        cur.execute("SELECT id FROM documents WHERE source = %s AND status <> 'deleted' "
                    "ORDER BY id", (source,))
        return [r[0] for r in cur.fetchall()]


def test_prune_removes_docs_outside_the_new_scope(pg, settings, source):
    """The motivating case: crawl wide, then narrow, and the orphans go."""
    wide = {SEED: page("index " * 40, [f"{SEED}a", f"{SEED}extra"]),
            f"{SEED}a": page("alpha " * 40), f"{SEED}extra": page("extra " * 40)}
    run(pg, settings, source, make(settings), FakeFetcher(wide))
    assert "testcrawl:docs/extra" in live_ids(pg, source)

    # Second run no longer links /extra — with prune on, it should be tombstoned.
    narrow = {SEED: page("index " * 40, [f"{SEED}a"]), f"{SEED}a": page("alpha " * 40)}
    _rid, stats = run(pg, settings, source, prune_recipe(settings), FakeFetcher(narrow))
    assert stats["pruned"] == 1
    assert live_ids(pg, source) == ["testcrawl:docs/", "testcrawl:docs/a"]


def test_prune_is_off_by_default(pg, settings, source):
    wide = {SEED: page("index " * 40, [f"{SEED}a", f"{SEED}extra"]),
            f"{SEED}a": page("alpha " * 40), f"{SEED}extra": page("extra " * 40)}
    run(pg, settings, source, make(settings), FakeFetcher(wide))
    narrow = {SEED: page("index " * 40, [f"{SEED}a"]), f"{SEED}a": page("alpha " * 40)}
    _rid, stats = run(pg, settings, source, make(settings), FakeFetcher(narrow))
    assert "pruned" not in stats
    assert "testcrawl:docs/extra" in live_ids(pg, source)   # orphan survives


def test_prune_refuses_on_a_truncated_run(pg, settings, source):
    """A run that hit its page budget saw a SUBSET. Pruning against a subset
    deletes everything it merely failed to reach."""
    links = [f"{SEED}p{i}" for i in range(8)]
    pages = {SEED: page("index " * 40, links)}
    pages.update({u: page(f"body {u} " * 40) for u in links})
    run(pg, settings, source, make(settings, limits={"max_depth": 1, "max_pages": 100}),
        FakeFetcher(pages))
    before = live_ids(pg, source)
    assert len(before) == 9

    _rid, stats = run(pg, settings, source,
                      prune_recipe(settings, limits={"max_depth": 1, "max_pages": 3}),
                      FakeFetcher(pages))
    assert stats["truncated"] is True
    assert stats["pruned"] == 0
    assert stats["prune_skipped"] == "truncated"
    assert live_ids(pg, source) == before          # nothing deleted


def test_prune_refuses_when_a_page_failed(pg, settings, source):
    """A page that did not come back is not proof the page is gone — a 502 must
    never be read as a deletion."""
    full = {SEED: page("index " * 40, [f"{SEED}a", f"{SEED}b"]),
            f"{SEED}a": page("alpha " * 40), f"{SEED}b": page("beta " * 40)}
    run(pg, settings, source, make(settings), FakeFetcher(full))
    before = live_ids(pg, source)

    broken = dict(full)
    del broken[f"{SEED}b"]          # /b now 404s rather than being unlinked
    _rid, stats = run(pg, settings, source, prune_recipe(settings), FakeFetcher(broken))
    assert stats["failed"] == 1
    assert stats["pruned"] == 0
    assert stats["prune_skipped"] == "failed_pages"
    assert live_ids(pg, source) == before


def test_prune_refuses_when_the_run_produced_nothing(pg, settings, source):
    """A recipe whose scope matches nothing (a typo) must not wipe the source."""
    run(pg, settings, source, make(settings),
        FakeFetcher({SEED: page("index " * 40, [f"{SEED}a"]), f"{SEED}a": page("a " * 40)}))
    before = live_ids(pg, source)
    # The seed FETCHES fine but is too short to extract, so the run completes
    # with no failures and no documents. (A seed that 404s trips the earlier
    # `failed_pages` guard instead — also safe, just a different reason.)
    _rid, stats = run(pg, settings, source, prune_recipe(settings),
                      FakeFetcher({SEED: page("too short")}))
    assert stats["failed"] == 0
    assert stats["pruned"] == 0
    assert stats["prune_skipped"] == "no_pages"
    assert live_ids(pg, source) == before


def test_prune_does_not_run_on_a_cancelled_crawl(pg, settings, source):
    links = [f"{SEED}p{i}" for i in range(20)]
    pages = {SEED: page("index " * 40, links)}
    pages.update({u: page(f"b {u} " * 40) for u in links})
    recipe = prune_recipe(settings, limits={"max_depth": 1, "max_pages": 100})
    run_id = crun.create_run(pg, source, recipe)
    crun.claim_run(pg)
    calls = {"n": 0}

    def stop_after_first_batch():
        calls["n"] += 1
        return calls["n"] > 1

    stats = crun.execute(pg, settings, run_id, source, recipe,
                         fetcher=FakeFetcher(pages), should_stop=stop_after_first_batch)
    assert stats.get("cancelled") is True
    assert "pruned" not in stats


def test_prune_keeps_docs_a_resumed_run_staged_before_the_restart(pg, settings, source):
    """The reason doc_id lives on the frontier and not in memory: a resumed run
    must not prune what its own earlier half already staged."""
    links = [f"{SEED}p{i}" for i in range(6)]
    pages = {SEED: page("index " * 40, links)}
    pages.update({u: page(f"b {u} " * 40) for u in links})
    recipe = prune_recipe(settings, limits={"max_depth": 1, "max_pages": 100})
    run_id = crun.create_run(pg, source, recipe)
    crun.claim_run(pg)

    calls = {"n": 0}
    # Allow TWO batches: the first claims only the seed (it is the sole pending
    # row until the seed's links are discovered), so stopping after one would
    # leave nothing but the seed staged and prove nothing about resume.
    crun.execute(pg, settings, run_id, source, recipe, fetcher=FakeFetcher(pages),
                 should_stop=lambda: (calls.__setitem__("n", calls["n"] + 1), calls["n"] > 2)[1])
    staged_first_half = live_ids(pg, source)
    assert len(staged_first_half) > 1

    stats = crun.execute(pg, settings, run_id, source, recipe, fetcher=FakeFetcher(pages))
    assert stats["pruned"] == 0                       # nothing orphaned
    assert set(staged_first_half) <= set(live_ids(pg, source))
