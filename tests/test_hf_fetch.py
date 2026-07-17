"""Politeness for a one-host crawl: the content-type trap and the `pages` bucket.

These are the two places where reusing smallweb's *configuration* (as opposed to
its machinery) would quietly break this source.
"""

import httpx
import pytest

from windex.config import Settings
from windex.hf.fetch import PagesRateLimiter, build_fetcher, parse_ratelimit
from windex.smallweb.poll import PageFetcher

ROBOTS = "User-agent: *\nAllow: /\n\nSitemap: https://huggingface.co/sitemap.xml\n"


@pytest.fixture()
def settings_hf(tmp_path):
    return Settings(_env_file=None, data_root=tmp_path)


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


# --- the content-type trap --------------------------------------------------

def test_page_fetcher_default_still_rejects_non_html():
    """smallweb's behavior must not change: it fetches blog HTML and nothing else."""
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        return httpx.Response(200, text="# Markdown", headers={"content-type": "text/markdown"})

    s = Settings(_env_file=None, smallweb_host_interval=0)
    fetcher = PageFetcher(_client(handler), s, host_interval=0)
    assert fetcher.fetch("https://example.com/post.md") is None


def test_hf_fetcher_accepts_markdown_and_plain_text(settings_hf):
    """THE TRAP: `.md` serves text/markdown and llms.txt serves text/plain.
    PageFetcher's hardcoded HTML-only check would have SILENTLY DISCARDED every
    doc page — 3,175 of this source's 4,014 — with no error anywhere."""
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        if request.url.path.endswith(".md"):
            return httpx.Response(200, text="# Quickstart\n",
                                  headers={"content-type": "text/markdown; charset=utf-8"})
        if request.url.path.endswith("llms.txt"):
            return httpx.Response(200, text="# Transformers\n",
                                  headers={"content-type": "text/plain; charset=utf-8"})
        return httpx.Response(200, text="<html><body>blog</body></html>",
                              headers={"content-type": "text/html; charset=utf-8"})

    settings_hf.hf_request_interval = 0
    fetcher = build_fetcher(_client(handler), settings_hf)
    assert fetcher.fetch("https://huggingface.co/docs/transformers/quicktour.md") == "# Quickstart\n"
    assert fetcher.fetch("https://huggingface.co/docs/transformers/llms.txt") == "# Transformers\n"
    assert "blog" in fetcher.fetch("https://huggingface.co/blog/some-post")


def test_hf_fetcher_accepts_the_sitemaps_own_content_type(settings_hf):
    """Regression (caught by a live smoke run, not by a mocked test): the
    sitemaps serve `application/xml`. An allowlist of html/markdown/plain
    rejected the FRONTIER itself — same trap as the .md pages, one level up, and
    invisible to any test that fakes the fetcher."""
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        return httpx.Response(200, text="<urlset></urlset>",
                              headers={"content-type": "application/xml"})

    settings_hf.hf_request_interval = 0
    fetcher = build_fetcher(_client(handler), settings_hf)
    assert fetcher.fetch("https://huggingface.co/sitemap.xml") == "<urlset></urlset>"


def test_hf_fetcher_sends_hfs_own_honest_user_agent(settings_hf):
    """PageFetcher sets the UA header explicitly, overriding the client's
    default — so the hf UA has to be threaded through, on the page AND its
    robots.txt. Otherwise hf/__init__.USER_AGENT is dead code that looks live
    and HF silently inherits smallweb's constant."""
    from windex.hf import USER_AGENT

    seen = []

    def handler(request):
        seen.append((request.url.path, request.headers.get("user-agent")))
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        return httpx.Response(200, text="# Doc", headers={"content-type": "text/markdown"})

    settings_hf.hf_request_interval = 0
    build_fetcher(_client(handler), settings_hf).fetch(
        "https://huggingface.co/docs/transformers/quicktour.md"
    )
    assert seen and all(ua == USER_AGENT for _, ua in seen), seen
    assert "/robots.txt" in [p for p, _ in seen]
    assert "+https://github.com/stevemurr/windex" in USER_AGENT  # honest + contactable


def test_hf_fetcher_honors_robots_even_though_hf_allows_everything(settings_hf):
    """HF's robots.txt is `Allow: /` today. We check anyway — a permissive file
    now is not a licence to stop looking."""
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text="User-agent: *\nDisallow: /docs/\n")
        return httpx.Response(200, text="# Nope", headers={"content-type": "text/markdown"})

    settings_hf.hf_request_interval = 0
    fetcher = build_fetcher(_client(handler), settings_hf)
    assert fetcher.fetch("https://huggingface.co/docs/transformers/quicktour.md") is None


def test_hf_fetcher_uses_hf_interval_not_smallwebs(settings_hf):
    """smallweb's 10s stranger-blog interval would cost ELEVEN HOURS for 4,014
    pages. HF publishes its own number (1 req/3s) and that is what we use."""
    settings_hf.hf_request_interval = 3.0
    fetcher = build_fetcher(_client(lambda r: httpx.Response(404)), settings_hf)
    assert fetcher.limiter.interval == 3.0
    assert settings_hf.smallweb_host_interval == 10.0  # untouched, and unused here


# --- the pages bucket -------------------------------------------------------

def test_parse_ratelimit_reads_hfs_live_counter():
    assert parse_ratelimit('"pages";r=98;t=83') == (98, 83)
    assert parse_ratelimit('"fixed window";"pages";q=100;w=300') == (None, None)
    assert parse_ratelimit(None) == (None, None)
    assert parse_ratelimit("garbage") == (None, None)


def test_limiter_stays_at_base_while_the_bucket_is_healthy():
    """r=99 over t=86 is 0.87s of headroom, but HF's nominal rate is 1 req/3s.
    Reading the header must never make us FASTER than the base interval."""
    lim = PagesRateLimiter(3.0, clock=lambda: 0.0, sleep=lambda d: None)
    lim.observe(httpx.Response(200, headers={"ratelimit": '"pages";r=99;t=86'}))
    assert lim.interval == 3.0


def test_limiter_widens_when_someone_else_has_spent_the_bucket():
    """The budget is per-IP, not per-process. If only 5 requests remain in a
    200s window, the sustainable spacing is 40s — a crawler counting only its
    own requests cannot see that; one reading the header can."""
    lim = PagesRateLimiter(3.0, clock=lambda: 0.0, sleep=lambda d: None)
    lim.observe(httpx.Response(200, headers={"ratelimit": '"pages";r=5;t=200'}))
    assert lim.interval == 40.0


def test_limiter_waits_out_an_exhausted_window():
    lim = PagesRateLimiter(3.0, clock=lambda: 0.0, sleep=lambda d: None)
    lim.observe(httpx.Response(429, headers={"ratelimit": '"pages";r=0;t=120'}))
    assert lim.interval == 121.0


def test_limiter_recovers_when_the_window_resets():
    lim = PagesRateLimiter(3.0, clock=lambda: 0.0, sleep=lambda d: None)
    lim.observe(httpx.Response(200, headers={"ratelimit": '"pages";r=1;t=250'}))
    assert lim.interval == 250.0
    lim.observe(httpx.Response(200, headers={"ratelimit": '"pages";r=100;t=300'}))
    assert lim.interval == 3.0  # self-correcting, back to base


def test_limiter_ignores_responses_without_the_header():
    """robots.txt and the sitemaps carry no ratelimit header (verified live)."""
    lim = PagesRateLimiter(3.0, clock=lambda: 0.0, sleep=lambda d: None)
    lim.observe(httpx.Response(200))
    assert lim.interval == 3.0


def test_fetcher_self_throttles_off_the_response_it_just_got(settings_hf):
    """End to end: the limiter observes each response's budget, including a 429
    — which is exactly when the header matters most."""
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        return httpx.Response(200, text="# Doc",
                              headers={"content-type": "text/markdown",
                                       "ratelimit": '"pages";r=4;t=200'})

    settings_hf.hf_request_interval = 3.0
    fetcher = build_fetcher(_client(handler), settings_hf)
    fetcher.limiter._sleep = lambda d: None  # don't actually wait in a test
    fetcher.fetch("https://huggingface.co/docs/transformers/quicktour.md")
    assert fetcher.limiter.interval == 50.0  # 200s / 4 remaining


def test_a_429_still_updates_the_budget(settings_hf):
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        return httpx.Response(429, headers={"ratelimit": '"pages";r=0;t=90'})

    settings_hf.hf_request_interval = 3.0
    fetcher = build_fetcher(_client(handler), settings_hf)
    fetcher.limiter._sleep = lambda d: None
    assert fetcher.fetch("https://huggingface.co/docs/transformers/quicktour.md") is None
    assert fetcher.limiter.interval == 91.0
