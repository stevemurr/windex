"""Unit tests for the crawl core: scope policy, SSRF, and extraction.

No network and no database — every test here is about a decision the crawler
makes locally, which is where the correctness risks actually live.
"""

import pytest

from windex.config import Settings
from windex.crawl import policy
from windex.crawl.extract import fallback_extract
from windex.crawl.fetch import BlockedTarget, check_url
from windex.crawl.links import extract_links
from windex.crawl.scope import canonicalize, in_scope, same_host, suggest_prefix


@pytest.fixture()
def settings():
    return Settings()


def rec(settings, **body):
    body.setdefault("seeds", ["https://example.dev/docs/"])
    return policy.parse(body, settings)


# --- canonicalization: what counts as ONE document --------------------------

@pytest.mark.parametrize("raw,expected", [
    ("https://Example.dev/a", "https://example.dev/a"),          # host lowercased
    ("https://example.dev/a#frag", "https://example.dev/a"),      # fragment dropped
    ("https://example.dev:443/a", "https://example.dev/a"),       # default port dropped
    ("http://example.dev:80/a", "http://example.dev/a"),
    ("https://example.dev", "https://example.dev/"),              # bare host gets /
    ("https://example.dev/A/B", "https://example.dev/A/B"),       # path case PRESERVED
    ("https://example.dev/a?utm_source=x", "https://example.dev/a"),
    ("https://example.dev/a?b=2&a=1", "https://example.dev/a?a=1&b=2"),  # params sorted
])
def test_canonicalize(raw, expected):
    assert canonicalize(raw) == expected


def test_canonicalize_keeps_meaningful_query():
    assert canonicalize("https://e.dev/p?id=7&utm_medium=x") == "https://e.dev/p?id=7"


def test_same_host_ignores_www():
    assert same_host("https://www.e.dev/a", "https://e.dev/b")
    assert not same_host("https://e.dev/a", "https://other.dev/b")


# --- scope rules ------------------------------------------------------------

def test_prefix_defaults_to_seed_directory(settings):
    """A bare seed must not mean 'crawl the whole host' — that is a surprising
    and expensive reading of 'crawl this cluster'."""
    assert rec(settings).scope.path_prefix == "/docs/"
    assert policy.parse(
        {"seeds": ["https://e.dev/a/page"]}, settings).scope.path_prefix == "/a/"


def test_explicit_empty_prefix_means_whole_host(settings):
    r = policy.parse(
        {"seeds": ["https://e.dev/docs/"], "scope": {"path_prefix": ""}}, settings)
    assert r.scope.path_prefix == ""
    assert in_scope("https://e.dev/elsewhere", r, r.seeds[0])[0]


def test_scope_rejects_other_host_and_prefix(settings):
    r = rec(settings)
    seed = r.seeds[0]
    assert in_scope("https://example.dev/docs/a", r, seed) == (True, "")
    assert in_scope("https://other.dev/docs/a", r, seed) == (False, "host")
    assert in_scope("https://example.dev/blog/a", r, seed) == (False, "prefix")
    assert in_scope("mailto:x@y.dev", r, seed) == (False, "scheme")


def test_exclude_beats_include(settings):
    """`exclude` must win: it is the rule reached for when something slipped
    through, and an include that could resurrect it would make that unfixable."""
    r = rec(settings, scope={"include": [r"/docs/"], "exclude": [r"\.png$"]})
    seed = r.seeds[0]
    assert in_scope("https://example.dev/docs/a", r, seed)[0]
    assert in_scope("https://example.dev/docs/a.png", r, seed) == (False, "exclude")


def test_include_allowlist_rejects_unmatched(settings):
    r = rec(settings, scope={"include": [r"^/docs/[a-z-]+$"]})
    seed = r.seeds[0]
    assert in_scope("https://example.dev/docs/guide", r, seed)[0]
    assert in_scope("https://example.dev/docs/deep/nested", r, seed) == (False, "include")


# --- crawl policy validation is a security boundary -------------------------

def test_limits_clamped_to_ceilings(settings):
    r = rec(settings, limits={"max_pages": 10 ** 9, "max_depth": 999, "host_interval": 0.001})
    assert r.limits.max_pages == settings.crawl_max_pages_ceiling
    assert r.limits.max_depth == settings.crawl_max_depth_ceiling
    # A policy may ask to be slower, never faster than the operator's floor.
    assert r.limits.host_interval == settings.crawl_host_interval_min


def test_policy_rejects_bad_input(settings):
    with pytest.raises(ValueError, match="seed"):
        policy.parse({}, settings)
    with pytest.raises(ValueError, match="http"):
        policy.parse({"seeds": ["ftp://e.dev/x"]}, settings)
    with pytest.raises(ValueError, match="invalid regex"):
        policy.parse(
            {"seeds": ["https://e.dev/"], "scope": {"include": ["[bad"]}},
            settings,
        )
    with pytest.raises(ValueError, match="at most"):
        policy.parse({
            "seeds": ["https://e.dev/"],
            "scope": {"exclude": ["x"] * (policy.MAX_PATTERNS + 1)},
        }, settings)


def test_policy_round_trips(settings):
    """A stored policy re-parses identically for reproducible frozen Runs."""
    original = rec(settings, scope={"exclude": [r"\.png$"]}, limits={"max_depth": 3})
    assert policy.parse(
        original.to_dict(), settings).to_dict() == original.to_dict()


def test_seed_alias_accepted(settings):
    assert policy.parse(
        {"seed": "https://e.dev/x/"}, settings).seeds == ("https://e.dev/x/",)


# --- SSRF guard -------------------------------------------------------------

@pytest.mark.parametrize("url", [
    "http://127.0.0.1:6333/collections",   # the live Qdrant on this host
    "http://localhost:5432/",
    "http://169.254.169.254/latest/meta-data/",  # cloud instance metadata
    "http://10.0.0.5/", "http://192.168.1.10/", "http://172.16.0.1/",
    "https://[::1]/", "http://0.0.0.0/",
])
def test_ssrf_guard_blocks_internal(url):
    with pytest.raises(BlockedTarget):
        check_url(url)


def test_ssrf_guard_blocks_non_http():
    with pytest.raises(BlockedTarget) as exc:
        check_url("file:///etc/passwd")
    assert exc.value.reason == "scheme"


def test_ssrf_guard_blocks_ipv4_mapped_ipv6():
    """::ffff:127.0.0.1 is loopback wearing a v6 costume; a naive is_private
    check on the v6 object misses it."""
    def resolver(*_a, **_k):
        return [(None, None, None, None, ("::ffff:127.0.0.1", 0))]
    with pytest.raises(BlockedTarget) as exc:
        check_url("http://sneaky.dev/", resolver=resolver)
    assert exc.value.reason == "private_ip"


def test_ssrf_guard_blocks_when_any_address_is_private():
    """A host resolving to both a public and a private address must be rejected —
    accepting it because the first record looked fine is the DNS-rebinding shape
    of this attack."""
    def resolver(*_a, **_k):
        return [(None, None, None, None, ("93.184.216.34", 0)),
                (None, None, None, None, ("127.0.0.1", 0))]
    with pytest.raises(BlockedTarget):
        check_url("http://mixed.dev/", resolver=resolver)


def test_ssrf_guard_allows_public():
    def resolver(*_a, **_k):
        return [(None, None, None, None, ("93.184.216.34", 0))]
    check_url("https://example.com/", resolver=resolver)  # must not raise


# --- link extraction --------------------------------------------------------

def test_extract_links_absolutizes_and_dedupes():
    html = """<html><body>
      <a href="/docs/a">a</a><a href="/docs/a#frag">same doc</a>
      <a href="b">rel</a><a href="https://other.dev/x">abs</a>
      <a href="#top">anchor</a><a href="mailto:x@y.dev">mail</a>
      <a href="javascript:void(0)">js</a>
    </body></html>"""
    links = extract_links(html, "https://e.dev/docs/index")
    assert "https://e.dev/docs/a" in links
    assert links.count("https://e.dev/docs/a") == 1  # #frag collapsed
    assert "https://e.dev/docs/b" in links
    assert "https://other.dev/x" in links
    assert not any(x.startswith(("mailto:", "javascript:")) for x in links)


def test_extract_links_honours_base_href():
    html = '<html><head><base href="https://cdn.dev/root/"></head><body><a href="p">p</a></body></html>'
    assert extract_links(html, "https://e.dev/x") == ["https://cdn.dev/root/p"]


def test_extract_links_survives_broken_markup():
    assert extract_links("<html><a href=", "https://e.dev/") == []


# --- extraction fallback ----------------------------------------------------

def test_fallback_extract_recovers_prose():
    """The gap this exists for: trafilatura declines some doc pages entirely
    (2 of 84 on the Claude cookbook), and silently losing them is worse than a
    slightly noisier extraction."""
    html = """<html><head><title>Hosting your agent</title></head><body>
      <nav>Home Docs Blog</nav>
      <main><h1>Hosting</h1><p>%s</p></main>
      <footer>copyright</footer></body></html>""" % ("deploy the agent. " * 60)
    got = fallback_extract(html)
    assert got is not None
    text, title = got
    assert title == "Hosting your agent"
    assert "deploy the agent." in text
    assert "copyright" not in text and "Home Docs Blog" not in text


def test_fallback_extract_returns_none_on_empty():
    assert fallback_extract("<html><body></body></html>") is None


def test_fallback_preserves_tail_text():
    """drop_tree() rather than remove(): removing an element also discards its
    tail, gluing the surrounding words together."""
    html = "<html><body><main>before <script>x=1</script> after</main></body></html>"
    text, _ = fallback_extract(html)
    assert "before" in text and "after" in text


# --- prefix suggestion (the "hub page" shape) -------------------------------

def test_suggest_prefix_finds_dominant_section():
    """An index at /research/ listing articles at /posts/ is the shape that
    produces a silent zero-page crawl; the suggestion is what makes it explain
    itself."""
    urls = [f"https://e.dev/posts/a{i}" for i in range(9)] + ["https://e.dev/about"]
    assert suggest_prefix(urls) == ("/posts/", 9)


def test_suggest_prefix_declines_when_scattered():
    """No majority ⇒ no suggestion. A confident wrong guess is worse than none."""
    urls = ["https://e.dev/a/1", "https://e.dev/b/1", "https://e.dev/c/1", "https://e.dev/d/1"]
    assert suggest_prefix(urls) is None


def test_suggest_prefix_ignores_rootless_urls():
    assert suggest_prefix(["https://e.dev/", "https://e.dev"]) is None


def test_whole_host_prefix_admits_other_sections(settings):
    """What the 'whole-host link following' checkbox sends: an EXPLICIT empty
    prefix, which means the whole host — the opposite of an omitted one."""
    r = policy.parse(
        {"seeds": ["https://e.dev/research/"], "scope": {"path_prefix": ""}},
        settings,
    )
    seed = r.seeds[0]
    assert in_scope("https://e.dev/posts/article", r, seed) == (True, "")
    assert in_scope("https://other.dev/posts/article", r, seed) == (False, "host")
