"""Small Web poller: feedparser accessors, the polite fetcher (robots / per-host
interval / bounded body), conditional-GET 304, feed watermark transitions, the
light extraction gate, and re-poll dedup. Everything is fed fabricated feeds and
httpx MockTransport responses — no network is touched, and the language filter
(a fastText model) is swapped for a hermetic length+repetition gate."""

import httpx
import pytest

from datatrove.data import Document
from datatrove.pipeline.filters import GopherQualityFilter

from windex.smallweb import extract
from windex.smallweb import poll as swpoll

# Two distinct, legitimately short (< 50 words) personal-blog posts. Each clears
# the 200-char light gate but is below what the news Gopher/FineWeb chain wants.
POST_A = ("Rewired the coop latch today using scrap copper wire and one stubbornly "
          "bent nail. It clicks shut properly now, finally. The hens seemed "
          "thoroughly unimpressed by my triumphant engineering. Still: small "
          "victories deserve celebrating quietly, alone, holding a lukewarm mug.")
POST_B = ("Spent the grey afternoon repotting the windowsill basil that nearly gave "
          "up in June. Fresh soil, a cracked terracotta pot, and a splash of rain "
          "water from the barrel. It looks skeptical. I look skeptical. We will "
          "both, I suspect, be fine by the weekend if the light holds.")

# hermetic gate: no fastText language model
LIGHT = extract.build_quality_filters(min_chars=200, include_language=False)


@pytest.fixture()
def sw_settings(settings):
    # a fast per-host interval so any real limiter wait in a test is negligible
    return settings.model_copy(update={"smallweb_host_interval": 0.0})


def _rss(items_xml: str) -> bytes:
    return (
        '<?xml version="1.0"?>'
        '<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">'
        f'<channel><title>Blog</title><link>https://blog.example</link>{items_xml}'
        '</channel></rss>'
    ).encode()


def _rss_item(title, link, body=None, summary="teaser", pubdate="Tue, 14 Jul 2026 08:00:00 GMT"):
    content = f'<content:encoded><![CDATA[<p>{body}</p>]]></content:encoded>' if body else ""
    return (f'<item><title>{title}</title><link>{link}</link>'
            f'<pubDate>{pubdate}</pubDate><description>{summary}</description>{content}</item>')


def _atom(entries_xml: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<feed xmlns="http://www.w3.org/2005/Atom"><title>Atom Blog</title>'
        f'{entries_xml}</feed>'
    ).encode()


def _atom_entry(title, link, body, updated="2026-07-14T10:00:00Z"):
    esc = body.replace("<", "&lt;").replace(">", "&gt;")
    return (f'<entry><title>{title}</title><link href="{link}"/>'
            f'<updated>{updated}</updated><summary>teaser</summary>'
            f'<content type="html">&lt;p&gt;{esc}&lt;/p&gt;</content></entry>')


def _mock_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler),
                        headers={"User-Agent": swpoll.USER_AGENT})


# --- feedparser accessor seam ----------------------------------------------

def test_feed_accessors_and_newest_ordering():
    import feedparser

    raw = _rss(
        _rss_item("Old", "https://blog.example/old", body=POST_A,
                  pubdate="Mon, 13 Jul 2026 08:00:00 GMT")
        + _rss_item("New", "https://blog.example/new", body=POST_B,
                    pubdate="Wed, 15 Jul 2026 08:00:00 GMT")
    )
    parsed = feedparser.parse(raw)
    newest = swpoll.newest_entries(parsed, 15)
    assert [swpoll.entry_title(e) for e in newest] == ["New", "Old"]  # sorted newest-first
    e = newest[0]
    assert swpoll.entry_link(e) == "https://blog.example/new"
    assert swpoll.entry_published(e) == "2026-07-15T08:00:00+00:00"
    body, inline = swpoll.item_body(e, inline_summary_min=600)
    assert inline is True and POST_B in body  # content:encoded → inline


def test_item_body_summary_only_vs_long_description():
    import feedparser

    # short teaser description, no content → not inline (caller fetches)
    e_short = feedparser.parse(_rss(_rss_item("T", "https://x/1", summary="tiny"))).entries[0]
    assert swpoll.item_body(e_short, inline_summary_min=600) == (None, False)
    # a long description IS treated as a full-text feed (inline, no fetch)
    long_summary = "word " * 200
    e_long = feedparser.parse(_rss(_rss_item("T", "https://x/2", summary=long_summary))).entries[0]
    body, inline = swpoll.item_body(e_long, inline_summary_min=600)
    assert inline is True and body.strip().startswith("word")


# --- poll_feed: inline (no fetch) vs summary-only (fetch), RSS + Atom -------

def test_poll_feed_inline_and_summary_paths(sw_settings):
    raw = _rss(
        _rss_item("Inline", "https://blog.example/inline", body=POST_A)
        + _rss_item("Summary", "https://blog.example/summary", summary="teaser only")
    )
    page = f"<html><head><title>Fetched</title></head><body><article><p>{POST_B}</p></article></body></html>"
    fetched = []

    def handler(req):
        if req.url.path == "/robots.txt":
            return httpx.Response(404)
        if req.url.path == "/feed":
            return httpx.Response(200, content=raw, headers={"content-type": "application/rss+xml"})
        fetched.append(req.url.path)  # a post-page fetch
        return httpx.Response(200, headers={"content-type": "text/html"}, text=page)

    client = _mock_client(handler)
    fetcher = swpoll.PageFetcher(client, sw_settings)
    res = swpoll.poll_feed(("https://blog.example/feed", "blog.example", None, None),
                           client, fetcher, sw_settings)
    assert res["outcome"] == "ok"
    items = swpoll.extract_items(res["raw_items"], LIGHT)
    by_url = {it["url"]: it for it in items}
    assert set(by_url) == {"https://blog.example/inline", "https://blog.example/summary"}
    assert by_url["https://blog.example/inline"]["title"] == "Inline"  # feed title wins
    assert by_url["https://blog.example/inline"]["outlet"] == "blog.example"
    assert POST_A in by_url["https://blog.example/inline"]["text"]
    assert POST_B in by_url["https://blog.example/summary"]["text"]
    assert fetched == ["/summary"]  # only the summary-only item triggered a page fetch


def test_poll_feed_atom_inline(sw_settings):
    raw = _atom(_atom_entry("Atom Post", "https://atom.example/p1", POST_A))

    def handler(req):
        return httpx.Response(200, content=raw, headers={"content-type": "application/atom+xml"})

    client = _mock_client(handler)
    fetcher = swpoll.PageFetcher(client, sw_settings)
    res = swpoll.poll_feed(("https://atom.example/feed", "atom.example", None, None),
                           client, fetcher, sw_settings)
    assert res["outcome"] == "ok"
    items = swpoll.extract_items(res["raw_items"], LIGHT)
    assert len(items) == 1
    assert items[0]["title"] == "Atom Post" and POST_A in items[0]["text"]


# --- conditional GET (304) -------------------------------------------------

def test_poll_feed_conditional_get_304(sw_settings):
    seen = {}

    def handler(req):
        seen["inm"] = req.headers.get("if-none-match")
        seen["ims"] = req.headers.get("if-modified-since")
        return httpx.Response(304)

    client = _mock_client(handler)
    fetcher = swpoll.PageFetcher(client, sw_settings)
    res = swpoll.poll_feed(
        ("https://blog.example/feed", "blog.example", '"etag-v1"', "Tue, 14 Jul 2026 08:00:00 GMT"),
        client, fetcher, sw_settings,
    )
    assert res == {"url": "https://blog.example/feed", "outcome": "not_modified", "status_code": 304}
    # the stored validators were sent back
    assert seen["inm"] == '"etag-v1"' and seen["ims"] == "Tue, 14 Jul 2026 08:00:00 GMT"


def test_poll_feed_http_error_is_reported(sw_settings):
    client = _mock_client(lambda req: httpx.Response(500))
    fetcher = swpoll.PageFetcher(client, sw_settings)
    res = swpoll.poll_feed(("https://blog.example/feed", "blog.example", None, None),
                           client, fetcher, sw_settings)
    assert res["outcome"] == "error" and res["status_code"] == 500


# --- the polite fetcher: robots, content-type, size ------------------------

def test_page_fetcher_honors_robots_disallow(sw_settings):
    requested = []

    def handler(req):
        requested.append(req.url.path)
        if req.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nDisallow: /\n")
        return httpx.Response(200, headers={"content-type": "text/html"}, text="<html></html>")

    fetcher = swpoll.PageFetcher(_mock_client(handler), sw_settings)
    assert fetcher.fetch("https://deny.example/post") is None
    assert requested == ["/robots.txt"]  # page itself was never requested


def test_page_fetcher_robots_fetch_failure_defaults_allow(sw_settings):
    logs = []

    def handler(req):
        if req.url.path == "/robots.txt":
            raise httpx.ConnectError("boom")
        return httpx.Response(200, headers={"content-type": "text/html"}, text=f"<article><p>{POST_A}</p></article>")

    fetcher = swpoll.PageFetcher(_mock_client(handler), sw_settings)
    fetcher.robots._log = logs.append  # capture the log line
    html = fetcher.fetch("https://flaky.example/post")
    assert html is not None and POST_A in html
    assert logs and "robots fetch failed" in logs[0]


def test_page_fetcher_rejects_non_html_and_oversize(sw_settings):
    small = sw_settings.model_copy(update={"smallweb_max_page_bytes": 100})

    def handler(req):
        if req.url.path == "/robots.txt":
            return httpx.Response(404)
        if req.url.path == "/json":
            return httpx.Response(200, headers={"content-type": "application/json"}, text="{}")
        return httpx.Response(200, headers={"content-type": "text/html"}, text="x" * 500)

    fetcher = swpoll.PageFetcher(_mock_client(handler), small)
    assert fetcher.fetch("https://x.example/json") is None       # wrong content-type
    assert fetcher.fetch("https://x.example/big") is None        # body exceeds cap


# --- per-host rate limiter (deterministic fake clock) ----------------------

def test_host_rate_limiter_spaces_same_host_only():
    now = {"t": 0.0}
    slept = []

    def clock():
        return now["t"]

    def sleep(d):
        slept.append(round(d, 3))
        now["t"] += d

    lim = swpoll.HostRateLimiter(10.0, clock=clock, sleep=sleep)
    lim.wait("a")            # first hit to a: no wait
    assert slept == []
    lim.wait("a")            # second hit to a: wait the full interval
    assert slept == [10.0]
    lim.wait("b")            # different host: no wait
    assert slept == [10.0]
    now["t"] += 10.0         # let the interval elapse
    lim.wait("a")            # enough time passed: no wait
    assert slept == [10.0]


# --- feed watermark transitions --------------------------------------------

def _seed_feed(conn, url="https://x.example/feed", host="x.example"):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO feeds (url, host) VALUES (%s, %s)", (url, host))
    conn.commit()


def _feed_row(conn, url="https://x.example/feed"):
    with conn.cursor() as cur:
        cur.execute("SELECT fail_count, status, items_seen, last_status FROM feeds WHERE url=%s", (url,))
        return cur.fetchone()


def test_fail_count_flips_to_dead_at_cap_and_resets_on_success(pg):
    _seed_feed(pg)
    for i in range(1, 5):  # failures 1..4 keep it active
        assert swpoll.mark_feed_failure(pg, "https://x.example/feed", 5, status_code=500) is False
        assert _feed_row(pg)[:2] == (i, "active")
    # the 5th consecutive failure crosses the threshold → dead
    assert swpoll.mark_feed_failure(pg, "https://x.example/feed", 5, status_code=None) is True
    assert _feed_row(pg)[:2] == (5, "dead")
    # a success resets the counter, reactivates, and bumps items_seen
    swpoll.mark_feed_ok(pg, "https://x.example/feed", '"e"', "LM", 200, items=3)
    assert _feed_row(pg) == (0, "active", 3, 200)


def test_not_modified_resets_fail_count(pg):
    _seed_feed(pg)
    swpoll.mark_feed_failure(pg, "https://x.example/feed", 5, status_code=500)
    swpoll.mark_feed_not_modified(pg, "https://x.example/feed", 304)
    assert _feed_row(pg)[:2] == (0, "active")
    assert _feed_row(pg)[3] == 304


def test_active_feeds_orders_never_polled_first(pg):
    with pg.cursor() as cur:
        cur.execute(
            "INSERT INTO feeds (url, host, last_polled, status) VALUES "
            "('https://a/f','a', now(), 'active'),"
            "('https://b/f','b', NULL, 'active'),"
            "('https://c/f','c', now() - interval '1 day', 'active'),"
            "('https://d/f','d', NULL, 'dead')"       # dead excluded
        )
    pg.commit()
    got = [u for u, *_ in swpoll.active_feeds(pg, 10)]
    assert got == ["https://b/f", "https://c/f", "https://a/f"]  # never-polled, then oldest


# --- light extraction gate contrast with the news chain --------------------

def test_light_gate_accepts_short_post_news_chain_rejects():
    html = f"<html><head><title>Latch</title></head><body><article><p>{POST_A}</p></article></body></html>"
    out = extract.extract_post(html, "https://blog.example/latch", filters=LIGHT)
    assert out is not None and len(out["text"]) >= 200
    # the news heavy chain (Gopher quality) would drop this legitimate short post
    doc = Document(text=out["text"], id="x", metadata={})
    keep, reason = GopherQualityFilter().filter(doc)
    assert keep is False and reason == "gopher_short_doc"


def test_light_gate_rejects_too_short():
    tiny = "<html><body><article><p>a brief note, nothing more.</p></article></body></html>"
    assert extract.extract_post(tiny, "https://x/tiny", filters=LIGHT) is None


# --- full poll: staging + re-poll dedup (the ledger is the anti-re-serve) ---

def test_poll_stages_then_repoll_adds_no_new_rows(pg, sw_settings):
    _seed_feed(pg, "https://blog.example/feed", "blog.example")
    raw = _rss(
        _rss_item("Post A", "https://blog.example/a", body=POST_A)
        + _rss_item("Post B", "https://blog.example/b", body=POST_B)
    )

    def handler(req):
        # feeds are re-served verbatim every poll (no ETag) — the ledger must
        # keep the second poll from re-staging anything.
        return httpx.Response(200, content=raw, headers={"content-type": "application/rss+xml"})

    client = _mock_client(handler)
    t1 = swpoll.poll(pg, sw_settings, max_feeds=1, filters=LIGHT, client=client)
    assert t1["feeds"] == 1 and t1["staged"] == 2 and t1["dup_ledger"] == 0

    with pg.cursor() as cur:
        cur.execute("SELECT id, source, status, canonical_url FROM documents WHERE source='smallweb' ORDER BY url")
        rows = cur.fetchall()
    assert [r[2] for r in rows] == ["deduped", "deduped"]
    assert {r[3] for r in rows} == {"https://blog.example/a", "https://blog.example/b"}
    n_after_first = len(rows)

    # re-poll: same items → zero new ledger rows, both counted as ledger dups
    t2 = swpoll.poll(pg, sw_settings, max_feeds=1, filters=LIGHT, client=client)
    assert t2["staged"] == 0 and t2["dup_ledger"] == 2
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents WHERE source='smallweb'")
        assert cur.fetchone()[0] == n_after_first
