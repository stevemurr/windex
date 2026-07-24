"""URL canonicalization and the in-scope decision.

Canonicalization decides what counts as ONE document. It runs before the frontier
dedup and before the doc id is derived, so `/x`, `/x#section` and `/x?utm_source=y`
converge on a single crawl_urls row and a single indexed doc instead of three.

Scope decides what belongs to the cluster. The rules compose as:

    same_host AND path_prefix AND (include is empty OR any include matches)
              AND no exclude matches

`exclude` always wins — it is the rule an operator reaches for when something
slipped through, and a precedence where an `include` could resurrect an excluded
URL would make that unfixable.
"""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

# Tracking parameters carry no document identity: the same page arrived at from a
# newsletter and from search must not become two documents. Stripped before the
# frontier dedup so both spellings collapse to one row.
TRACKING_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "gclid", "fbclid", "mc_cid", "mc_eid", "ref", "ref_src",
    "source", "_ga", "igshid", "si",
})

# Schemes we will follow. Everything else (mailto:, javascript:, data:, ftp:) is
# out of scope by construction, not by rule — the SSRF guard in fetch.py enforces
# this again at request time, since a redirect can change the scheme.
ALLOWED_SCHEMES = frozenset({"http", "https"})


def canonicalize(url: str) -> str:
    """Normalize a URL to its document identity.

    Drops the fragment (same document), lowercases scheme/host, removes a default
    port, strips tracking params, and sorts what remains so parameter order is not
    an identity. Path case is PRESERVED — many sites are case-sensitive and
    lowercasing it would 404 or, worse, soft-404.
    """
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    host = parts.hostname or ""
    if parts.port and not ((scheme == "http" and parts.port == 80)
                           or (scheme == "https" and parts.port == 443)):
        host = f"{host}:{parts.port}"
    query = "&".join(sorted(
        part for part in parts.query.split("&")
        if part and part.split("=", 1)[0].lower() not in TRACKING_PARAMS
    ))
    # A bare host must keep its "/" so "https://h" and "https://h/" are one URL.
    path = parts.path or "/"
    return urlunsplit((scheme, host, path, query, ""))


def same_host(a: str, b: str) -> bool:
    """Host equality ignoring a leading `www.` — a site that links to itself both
    ways is one cluster, and treating them as two would halve most crawls."""
    def key(u: str) -> str:
        h = (urlsplit(u).hostname or "").lower()
        return h[4:] if h.startswith("www.") else h
    return key(a) == key(b)


def suggest_prefix(urls: list[str]) -> tuple[str, int] | None:
    """The dominant first path segment among URLs, as ``("/posts/", count)``.

    Exists for the "hub page" shape: an index at ``/research/`` listing articles
    at ``/posts/``. Scope defaults to the seed's own directory (so a bare seed
    cannot accidentally crawl a whole domain), which makes that shape produce a
    silent zero-page crawl — technically correct and completely baffling. Feeding
    the prefix-rejected links through this turns the preview from "found nothing"
    into "found 15 pages under /posts/, want that?".

    Returns None when the rejects share no single dominant segment, which is the
    honest answer for a genuinely scattered site — a confident wrong guess would
    be worse than none.
    """
    counts: dict[str, int] = {}
    for url in urls:
        segments = [s for s in urlsplit(url).path.split("/") if s]
        if not segments:
            continue
        key = f"/{segments[0]}/"
        counts[key] = counts.get(key, 0) + 1
    if not counts:
        return None
    prefix, n = max(counts.items(), key=lambda kv: kv[1])
    # Require a real majority: on a site whose links scatter across many
    # sections there is no single right answer to offer.
    return (prefix, n) if n * 2 > sum(counts.values()) else None


def in_scope(url: str, recipe, seed: str) -> tuple[bool, str]:
    """Return ``(allowed, reason)``. ``reason`` is "" when allowed, otherwise the
    rule that rejected it — it is written to ``crawl_urls.reason`` and shown in the
    control page, so a surprising crawl is diagnosable without re-running it."""
    parts = urlsplit(url)
    if parts.scheme.lower() not in ALLOWED_SCHEMES:
        return False, "scheme"
    if recipe.scope.same_host and not same_host(url, seed):
        return False, "host"
    if recipe.scope.path_prefix and not parts.path.startswith(recipe.scope.path_prefix):
        return False, "prefix"
    # Exclude is evaluated against path+query, not the whole URL: an operator
    # writing `\.png$` means the path, and matching the host too would make a
    # rule like `/assets/` accidentally match a host containing that string.
    target = parts.path + (f"?{parts.query}" if parts.query else "")
    for pattern in recipe.scope.exclude_re:
        if pattern.search(target):
            return False, "exclude"
    if recipe.scope.include_re:
        if not any(p.search(target) for p in recipe.scope.include_re):
            return False, "include"
    return True, ""
