"""Crawl policy parsing, validation, normalization, and round-tripping.

A policy is the whole crawl contract and the unit of reproducibility — every
crawl request reduces to one of these, it is stored on the custom source, and a
frozen copy is written to each run so history stays truthful when the source's
policy is later edited.

Validation here is a security boundary, not a convenience: policies may arrive over the
LAN-exposed API, so limits are clamped to the operator's ceilings from Settings
rather than trusted, and regexes are compiled (and thus rejected if malformed) at
parse time instead of blowing up inside the worker. ``ValueError`` is what the
route maps to HTTP 422, mirroring ``custom_source.registry.validate_name``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from windex.config import Settings

VERSION = 1

# A regex is caller-supplied and runs against every discovered URL. Cap the source
# length: this is not a defence against catastrophic backtracking (no bound is),
# but it keeps an accidental paste from becoming a per-URL cost.
MAX_PATTERN_LEN = 500
MAX_PATTERNS = 25
MAX_SEEDS = 25


@dataclass(frozen=True)
class Scope:
    same_host: bool = True
    path_prefix: str = ""
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    include_re: tuple[re.Pattern, ...] = field(default=(), compare=False, repr=False)
    exclude_re: tuple[re.Pattern, ...] = field(default=(), compare=False, repr=False)


@dataclass(frozen=True)
class Limits:
    max_pages: int = 500
    max_depth: int = 2
    host_interval: float = 2.0
    request_timeout: float = 15.0
    max_page_bytes: int = 4_000_000


@dataclass(frozen=True)
class Extract:
    quality_filters: bool = False
    min_chars: int = 200


@dataclass(frozen=True)
class Dedup:
    drop_boilerplate: bool = True
    # Tombstone docs this run did NOT see — makes narrowing scope self-cleaning
    # instead of leaving orphans from the previous scope. Opt-in because it is
    # the one setting that can DELETE content: see run.prune_missing for the
    # completeness conditions that must hold before it is allowed to act.
    prune: bool = False


@dataclass(frozen=True)
class CrawlPolicy:
    seeds: tuple[str, ...]
    scope: Scope
    limits: Limits
    extract: Extract
    dedup: Dedup
    version: int = VERSION

    def to_dict(self) -> dict:
        """The stored/jsonb form. Round-trips through ``parse`` unchanged, which
        makes a frozen historic Run reproducible."""
        return {
            "version": self.version,
            "seeds": list(self.seeds),
            "scope": {
                "same_host": self.scope.same_host,
                "path_prefix": self.scope.path_prefix,
                "include": list(self.scope.include),
                "exclude": list(self.scope.exclude),
            },
            "limits": {
                "max_pages": self.limits.max_pages,
                "max_depth": self.limits.max_depth,
                "host_interval": self.limits.host_interval,
                "request_timeout": self.limits.request_timeout,
                "max_page_bytes": self.limits.max_page_bytes,
            },
            "extract": {
                "quality_filters": self.extract.quality_filters,
                "min_chars": self.extract.min_chars,
            },
            "dedup": {"drop_boilerplate": self.dedup.drop_boilerplate,
                      "prune": self.dedup.prune},
        }


def _section(body: dict, key: str) -> dict:
    value = body.get(key) or {}
    if not isinstance(value, dict):
        raise ValueError(f"{key!r} must be an object")
    return value


def _clamp_int(value, default: int, lo: int, hi: int, label: str) -> int:
    if value is None:
        return min(default, hi)
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be an integer")
    if n < lo:
        raise ValueError(f"{label} must be >= {lo}")
    return min(n, hi)  # silently clamped to the operator ceiling, never rejected


def _clamp_float(value, default: float, lo: float, label: str) -> float:
    if value is None:
        return max(default, lo)
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be a number")
    return max(f, lo)  # a policy may ask to be slower, never faster than the floor


def _compile(patterns, label: str) -> tuple[tuple[str, ...], tuple[re.Pattern, ...]]:
    if patterns is None:
        return (), ()
    if isinstance(patterns, str):
        patterns = [patterns]
    if not isinstance(patterns, list):
        raise ValueError(f"{label} must be a list of regex strings")
    if len(patterns) > MAX_PATTERNS:
        raise ValueError(f"{label}: at most {MAX_PATTERNS} patterns")
    out, compiled = [], []
    for p in patterns:
        if not isinstance(p, str) or not p:
            raise ValueError(f"{label}: each pattern must be a non-empty string")
        if len(p) > MAX_PATTERN_LEN:
            raise ValueError(f"{label}: pattern longer than {MAX_PATTERN_LEN} chars")
        try:
            compiled.append(re.compile(p))
        except re.error as exc:
            raise ValueError(f"{label}: invalid regex {p!r} ({exc})")
        out.append(p)
    return tuple(out), tuple(compiled)


def parse(body: dict, settings: Settings) -> CrawlPolicy:
    """Validate and normalize a crawl policy. Raises ValueError on failure."""
    if not isinstance(body, dict):
        raise ValueError("crawl policy must be an object")

    seeds_in = body.get("seeds") or ([body["seed"]] if body.get("seed") else [])
    if isinstance(seeds_in, str):
        seeds_in = [seeds_in]
    if not isinstance(seeds_in, list) or not seeds_in:
        raise ValueError("at least one seed URL is required")
    if len(seeds_in) > MAX_SEEDS:
        raise ValueError(f"at most {MAX_SEEDS} seeds")

    from windex.crawl.scope import ALLOWED_SCHEMES, canonicalize

    seeds = []
    for raw in seeds_in:
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError("each seed must be a non-empty string")
        parts = urlsplit(raw.strip())
        if parts.scheme.lower() not in ALLOWED_SCHEMES:
            raise ValueError(f"seed must be http(s): {raw!r}")
        if not parts.hostname:
            raise ValueError(f"seed has no host: {raw!r}")
        seeds.append(canonicalize(raw))

    sc = _section(body, "scope")
    prefix = sc.get("path_prefix")
    if prefix is None:
        # Default the prefix to the seed's own directory. Without this, a seed of
        # https://host/cookbook/ with same_host would crawl the ENTIRE host — a
        # surprising and expensive reading of "crawl this cluster". Explicit "" in
        # the policy still means whole-host.
        seed_path = urlsplit(seeds[0]).path
        prefix = seed_path if seed_path.endswith("/") else seed_path.rsplit("/", 1)[0] + "/"
    if not isinstance(prefix, str):
        raise ValueError("scope.path_prefix must be a string")
    include, include_re = _compile(sc.get("include"), "scope.include")
    exclude, exclude_re = _compile(sc.get("exclude"), "scope.exclude")

    li = _section(body, "limits")
    ex = _section(body, "extract")
    de = _section(body, "dedup")

    return CrawlPolicy(
        seeds=tuple(seeds),
        scope=Scope(
            same_host=bool(sc.get("same_host", True)),
            path_prefix=prefix,
            include=include, exclude=exclude,
            include_re=include_re, exclude_re=exclude_re,
        ),
        limits=Limits(
            max_pages=_clamp_int(li.get("max_pages"), settings.crawl_max_pages,
                                 1, settings.crawl_max_pages_ceiling, "limits.max_pages"),
            max_depth=_clamp_int(li.get("max_depth"), settings.crawl_max_depth,
                                 0, settings.crawl_max_depth_ceiling, "limits.max_depth"),
            host_interval=_clamp_float(li.get("host_interval"), settings.crawl_host_interval,
                                       settings.crawl_host_interval_min, "limits.host_interval"),
            request_timeout=_clamp_float(li.get("request_timeout"),
                                         settings.crawl_request_timeout, 1.0,
                                         "limits.request_timeout"),
            max_page_bytes=_clamp_int(li.get("max_page_bytes"), settings.crawl_max_page_bytes,
                                      1024, settings.crawl_max_page_bytes,
                                      "limits.max_page_bytes"),
        ),
        extract=Extract(
            quality_filters=bool(ex.get("quality_filters", False)),
            min_chars=_clamp_int(ex.get("min_chars"), settings.crawl_min_chars,
                                 0, 100_000, "extract.min_chars"),
        ),
        dedup=Dedup(drop_boilerplate=bool(de.get("drop_boilerplate", True)),
                    prune=bool(de.get("prune", False))),
        version=int(body.get("version") or VERSION),
    )
