"""Politeness for a ONE-HOST crawl — the piece smallweb's poller cannot supply.

smallweb is windex's other FETCH-based source and has the right machinery
(RobotsCache, HostRateLimiter, a bounded content-type-checked GET), but its
premise inverts here. From docs/smallweb-source.md: *"~37.6k hosts spread load
naturally"*. HF is ONE host, so:

  * ``smallweb_concurrency = 12`` is inert — twelve workers would all serialize
    behind one host's limiter. This module fetches serially and doesn't pretend
    otherwise.
  * ``smallweb_host_interval = 10.0`` would cost 11 hours for 4,014 pages. That
    value is calibrated for hitting a stranger's personal blog. HF publishes its
    own number, so we use HF's: 3s (``hf_request_interval``).
  * ``PageFetcher.fetch()`` dropped every non-HTML content-type, which would
    have SILENTLY DISCARDED EVERY .md PAGE (.md serves ``text/markdown``,
    llms.txt serves ``text/plain``). It now takes an ``allowed_types``
    allowlist; HF passes markdown/plain/html, smallweb's default is unchanged.

THE RATE LIMIT. HF runs three buckets and the one governing page routes is by
far the tightest — verified live on every response:

    ratelimit-policy: "fixed window";"pages";q=100;w=300     → 1 req / 3s
    ratelimit: "pages";r=98;t=83                             → live counter

``/docs/*``, ``/blog/*`` and ``.md`` all ride ``pages`` (checked — the looser
``resolvers`` bucket, 10/s, would have been a convenient assumption). Since the
budget is published on every response, ``PagesRateLimiter`` reads it and
self-throttles instead of open-loop sleeping: the interval widens to t/r
whenever the bucket is running hotter than nominal — which is what happens when
something else has already spent part of this IP's shared budget — and never
narrows below the configured base. A crawler that only counts its own requests
cannot see that; one that reads the header can.
"""

from __future__ import annotations

import re

import httpx

from windex.hf import USER_AGENT
from windex.smallweb.poll import HostRateLimiter, PageFetcher

# `ratelimit: "pages";r=98;t=83` — remaining requests and seconds to reset.
_R_RE = re.compile(r"\br=(\d+)")
_T_RE = re.compile(r"\bt=(\d+)")

# Content types this source accepts, ALL verified live against huggingface.co:
#   .md            -> text/markdown; charset=utf-8   (3,175 doc pages)
#   llms.txt       -> text/plain; charset=utf-8      (the enumeration)
#   /blog/<slug>   -> text/html; charset=utf-8       (829 posts)
#   sitemap*.xml   -> application/xml                (the frontier itself)
# The default HTML-only allowlist would drop 3,175 of our 4,014 pages without a
# word — and, as a live smoke test proved, the sitemaps before them, which is
# the same trap one level up: every one of these is a content type that a
# crawler written for HTML silently treats as "not a page".
ALLOWED_TYPES = ("html", "markdown", "text/plain", "xml")


def parse_ratelimit(header: str | None) -> tuple[int | None, int | None]:
    """(remaining, reset_seconds) from HF's ``ratelimit`` header, or (None, None).

    Deliberately lenient: this drives a politeness *widening*, so an
    unparseable header must degrade to "use the configured interval", never
    raise and never speed us up.
    """
    if not header:
        return None, None
    r, t = _R_RE.search(header), _T_RE.search(header)
    return (int(r.group(1)) if r else None, int(t.group(1)) if t else None)


class PagesRateLimiter(HostRateLimiter):
    """HostRateLimiter that honors HF's published `pages` budget.

    ``observe(response)`` after each fetch: with r requests left and t seconds
    until the window resets, the sustainable spacing is t/r. Take the wider of
    that and the configured base, so:

      * a healthy bucket (r=99, t=86 → 0.87s) leaves the base 3s in force —
        we never go faster than HF's own nominal rate;
      * a bucket someone else has been spending (r=5, t=200 → 40s) stretches us
        out instead of walking into a 429;
      * an exhausted bucket (r=0) waits out the window.

    Self-correcting: once the window resets the header reports r≈100/t≈300 →
    3.0s → base wins again.
    """

    def __init__(self, interval: float, clock=None, sleep=None):
        kwargs = {}
        if clock is not None:
            kwargs["clock"] = clock
        if sleep is not None:
            kwargs["sleep"] = sleep
        super().__init__(interval, **kwargs)
        self.base = interval

    def observe(self, resp: httpx.Response) -> None:
        remaining, reset = parse_ratelimit(resp.headers.get("ratelimit"))
        if remaining is None or reset is None:
            return  # no header (robots.txt/sitemaps don't carry one): keep base
        with self._lock:
            if remaining <= 0:
                # Bucket spent: nothing may go out until the window flips. +1s
                # of slack — the server's clock is the one that matters.
                self.interval = max(self.base, reset + 1)
            else:
                self.interval = max(self.base, reset / remaining)


def build_fetcher(client: httpx.Client, settings) -> PageFetcher:
    """A PageFetcher configured for HF: HF's interval (not smallweb's 10s), HF's
    content types (not HTML-only), and header-driven self-throttling.

    RobotsCache comes along unchanged — correct even though HF's robots.txt is
    `Allow: /` with nothing we want disallowed. A permissive file today is not a
    licence to stop looking, and re-checking costs one request per TTL.
    """
    limiter = PagesRateLimiter(settings.hf_request_interval)
    return PageFetcher(
        client,
        settings,
        robots_ttl=settings.hf_robots_ttl,
        max_bytes=settings.hf_max_page_bytes,
        limiter=limiter,
        allowed_types=ALLOWED_TYPES,
        on_response=limiter.observe,
        # Explicit, even though every windex source's UA string is identical
        # today: PageFetcher overrides the client's default header, so leaving
        # this off would send smallweb's constant and make ours dead code that
        # still looks live.
        user_agent=USER_AGENT,
    )
