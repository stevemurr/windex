"""The crawl's network seam: SSRF guard plus a policy-configured PageFetcher.

This is the first place in windex that fetches a URL supplied by an API caller,
and the API is reachable on the LAN. Every other source fetches a URL that windex
itself derived from a fixed base (Common Crawl, GH Archive, huggingface.co, a
curated feed list), so "the target host is trustworthy" was previously an
invariant of the code rather than a check. Here it must be a check.

The concrete risk: windex runs alongside Postgres (5432), Qdrant (6333), the embed
gateway (4000) and Grafana (3000) on the same host, reachable at 127.0.0.1 and on
the LAN. A crawl of `http://127.0.0.1:6333/collections` would happily fetch and
index internal service responses; on a cloud host, 169.254.169.254 would fetch
instance credentials. So the guard rejects non-public destinations by RESOLVED IP,
and — because a redirect can move a request from a public host to a private one —
re-checks after every hop rather than trusting the seed.

Everything else reuses ``smallweb.poll``'s politeness machinery unchanged
(``PageFetcher`` was already parameterized for a second consumer, ``hf/``); this
module supplies a third configuration, not a third implementation.
"""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlsplit

import httpx

from windex.config import Settings
from windex.crawl import ROBOT_AGENT, USER_AGENT
from windex.crawl.policy import CrawlPolicy


class BlockedTarget(Exception):
    """A URL resolved to a non-public address (or an unresolvable host). Carries
    the short reason written to ``crawl_urls.reason``."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _ip_is_public(ip: ipaddress._BaseAddress) -> bool:
    """Reject everything that is not a normal routable destination.

    `is_global` alone is not enough: it is False for private/loopback but the
    explicit checks below document *which* class each rejection is, and cover
    IPv4-mapped IPv6 (::ffff:127.0.0.1) which would otherwise slip past a naive
    `is_private` test on the v6 object.
    """
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return not (
        ip.is_private          # 10/8, 172.16/12, 192.168/16, fc00::/7
        or ip.is_loopback      # 127/8, ::1
        or ip.is_link_local    # 169.254/16 — cloud instance metadata
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def check_url(url: str, *, resolver=socket.getaddrinfo) -> None:
    """Raise ``BlockedTarget`` unless ``url`` is http(s) to a public address.

    ALL resolved addresses must be public, not just the first: a hostname with
    both a public and a 127.0.0.1 record would otherwise be a coin flip, and
    that coin flip is the DNS-rebinding shape of this attack.
    """
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise BlockedTarget("scheme")
    host = parts.hostname
    if not host:
        raise BlockedTarget("no_host")
    # A literal IP still goes through the same predicate — the check is on the
    # destination, not on how it was spelled.
    try:
        infos = resolver(host, parts.port or (443 if scheme == "https" else 80),
                         proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        raise BlockedTarget("dns")
    except Exception:
        raise BlockedTarget("dns")
    if not infos:
        raise BlockedTarget("dns")
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            raise BlockedTarget("dns")
        if not _ip_is_public(ip):
            raise BlockedTarget("private_ip")


class GuardedFetcher:
    """A ``PageFetcher`` that vets every hop against ``check_url``.

    Redirects are followed manually (the client is built with
    ``follow_redirects=False``) so each Location can be re-checked before it is
    requested. httpx's own redirect handling would perform the next request
    internally — after the guard had already passed on the original URL, which is
    precisely the window this class exists to close.
    """

    MAX_REDIRECTS = 5

    def __init__(self, fetcher, client: httpx.Client, checker=check_url):
        self._fetcher = fetcher
        self._client = client
        self._check = checker
        self.robots = fetcher.robots  # exposed for preview/diagnostics

    def resolve(self, url: str) -> str:
        """Follow redirects with a guard on each hop; return the final URL.

        Raises ``BlockedTarget`` if any hop is non-public, or ``LookupError`` if
        the redirect chain is too long.
        """
        current = url
        for _ in range(self.MAX_REDIRECTS):
            self._check(current)
            try:
                resp = self._client.head(current)
            except Exception:
                return current  # HEAD unsupported/failed: let the GET decide
            if resp.status_code not in (301, 302, 303, 307, 308):
                return current
            location = resp.headers.get("location")
            if not location:
                return current
            current = str(httpx.URL(current).join(location))
        raise LookupError("too many redirects")

    def fetch(self, url: str) -> tuple[str | None, str, str]:
        """Return ``(body_or_None, final_url, reason)``.

        ``reason`` is "" on success, else the short tag stored on the frontier
        row — the control page shows it, so "why did this page not get indexed"
        is answerable without re-running the crawl.
        """
        try:
            final = self.resolve(url)
        except BlockedTarget as exc:
            return None, url, exc.reason
        except LookupError:
            return None, url, "redirects"
        if not self._fetcher.robots.allowed(final):
            return None, final, "robots"
        body = self._fetcher.fetch(final)
        if body is None:
            # PageFetcher collapses non-200, disallowed content-type and oversize
            # into None. Distinguishing them would need a second request, which is
            # not worth a round trip against a host we are being polite to.
            return None, final, "http"
        return body, final, ""


def build_fetcher(
    client: httpx.Client, settings: Settings, policy: CrawlPolicy,
) -> GuardedFetcher:
    """The single seam between the crawl driver and the network.

    A JS-rendering backend would be introduced by returning a different object
    with this same ``fetch(url) -> (body, final_url, reason)`` shape; nothing in
    ``run.py`` would change.
    """
    from windex.smallweb.http import HostRateLimiter, PageFetcher

    page = PageFetcher(
        client, settings,
        robots_ttl=settings.crawl_robots_ttl,
        max_bytes=policy.limits.max_page_bytes,
        allowed_types=("html", "xhtml", "text/plain", "markdown"),
        limiter=HostRateLimiter(policy.limits.host_interval),
        user_agent=USER_AGENT,
    )
    page.robots.agent = ROBOT_AGENT
    return GuardedFetcher(page, client)
