"""Which settings may be edited at runtime, and within what bounds.

This module is a SECURITY BOUNDARY, and it is an **allowlist** rather than a
denylist on purpose. `pg_dsn`, every `*_api_key`, `write_token`, `github_tokens`,
`data_root` and `serve_host` are not editable because they never appear here — a
denylist would eventually miss one as `Settings` grows, and the failure mode is
handing a LAN caller the credentials. Same discipline as `api/jobs.py` ("strictly
whitelisted — fixed argv templates plus typed, bounded parameters"), which exists
for exactly this reason.

Bounds are the second half of it. A value that passes the allowlist can still
wedge a source (a 10,000s request interval stalls ingest as surely as a crash),
so every numeric field carries a range and is CLAMPED to it rather than rejected
— the same call `crawl/recipe.py` makes for recipe limits: a caller may ask to be
slower or smaller, never faster or bigger than the operator's ceiling.

Deliberately absent even though they look editable:

  * ``embed_model`` / ``embed_dim`` — these define the vector space. Changing one
    is a re-embed plus a Qdrant alias flip, not a settings edit; offering it here
    would silently corrupt every search until someone noticed.
  * anything under ``crawl_*_ceiling`` — those ARE the operator ceilings that
    bound what a recipe may request, so making them editable through the same API
    that they constrain defeats them.
"""

from __future__ import annotations

from windex.schema.param import Param

# The scope key for settings that are not per-source (the embed knobs).
GLOBAL = "_global"

# One editable setting; ``key`` is the attribute name on ``Settings``. This is
# `windex.schema.Param` under its original name — the same allowlist-and-clamp
# semantics, now shared with job params and recipe module config so a client
# renders one form shape instead of three. Param's first seven fields are Field's
# in the same order, so the positional `_f(...)` calls below are unchanged, and
# `describe()` still emits the six legacy keys the console reads.
Field = Param


def _f(key, kind, lo=None, hi=None, choices=(), label="", help=""):
    return Field(key, kind, lo, hi, choices, label, help)


# Keys are the corpus-vocabulary source names used by /v1/loops and the control
# table (gh, not github; ccnews and news both appear in the codebase — the loop
# vocabulary is `ccnews`).
SCHEMA: dict[str, tuple[Field, ...]] = {
    "ccnews": (
        _f("news_backfill_days", "int", 1, 3650, label="Backfill window (days)",
           help="How far back the CC-News sync looks for unprocessed WARC files."),
        _f("news_language", "str", label="Language",
           help="ISO code kept by the language filter (e.g. en)."),
        _f("minhash_window_days", "int", 1, 365, label="Dedup window (days)",
           help="Near-duplicate comparison window; news syndication crosses days."),
    ),
    "wiki": (
        _f("wiki_dump", "str", label="Dump",
           help="CirrusSearch dump to track, e.g. enwiki."),
        _f("wiki_chunk_rows", "int", 100, 50_000, label="Chunk rows",
           help="Row-group / commit / pause-check granularity within a shard."),
    ),
    "arxiv": (
        # 3.0s is arXiv's published ToU rate, not a guess — the floor enforces it.
        _f("arxiv_request_interval", "float", 3.0, 60, label="Request interval (s)",
           help="arXiv ToU is 1 request / 3s from a single connection. The floor is their number."),
        _f("arxiv_incremental_days", "int", 1, 365, label="Incremental window (days)"),
        _f("arxiv_earliest_year", "int", 1990, 2100, label="Earliest year",
           help="Backfill floor; arXiv's earliestDatestamp is 2005-09-16."),
    ),
    "smallweb": (
        _f("smallweb_host_interval", "float", 1.0, 300, label="Per-host interval (s)",
           help="Minimum seconds between hits to one blog host."),
        _f("smallweb_max_items", "int", 1, 200, label="Items per feed"),
        _f("smallweb_poll_batch", "int", 1, 5_000, label="Feeds per batch"),
        _f("smallweb_concurrency", "int", 1, 64, label="Concurrency"),
        _f("smallweb_min_chars", "int", 0, 100_000, label="Minimum post length",
           help="Light quality gate; FineWeb/C4 filters over-reject personal blogs."),
        _f("smallweb_max_fail", "int", 1, 100, label="Failures before 'dead'"),
        _f("smallweb_request_timeout", "float", 1, 300, label="Request timeout (s)"),
    ),
    "docs": (
        _f("docs_slugs", "csv", label="Docsets",
           help="Comma-separated DevDocs slugs to track."),
    ),
    "hn": (
        _f("hn_request_interval", "float", 0.1, 60, label="Request interval (s)"),
        _f("hn_incremental_days", "int", 1, 365, label="Incremental window (days)"),
    ),
    "hf": (
        _f("hf_roots", "csv", label="Doc roots",
           help="Which HF doc roots to index. Empty = every root in the sitemap."),
        # 3.0s mirrors HF's published `pages` bucket (q=100;w=300).
        _f("hf_request_interval", "float", 3.0, 60, label="Request interval (s)",
           help="HF's own budget is 1 req/3s; the crawler also self-throttles off their live header."),
        _f("hf_blog_batch", "int", 1, 1_000, label="Blog batch size"),
        _f("hf_request_timeout", "float", 1, 300, label="Request timeout (s)"),
    ),
    "gh": (
        _f("repo_star_threshold", "int", 0, 100_000, label="Star threshold",
           help="Minimum stars for a repo to be indexed."),
    ),
    GLOBAL: (
        # The knobs actually retuned in anger — each previously needed an .env
        # edit plus a container recreate.
        _f("embed_concurrency", "int", 1, 64, label="Embed concurrency",
           help="In-flight embed requests per process."),
        _f("embed_global_budget", "int", 1, 64, label="Fleet embed budget",
           help="Fleet-wide cap on in-flight BULK embed requests. Must not exceed the "
                "bulk key's server-side cap or the gateway 429s instead of queueing."),
        _f("embed_throttle_seconds", "float", 0, 60, label="Embed throttle (s)",
           help="Pause per worker between batches, leaving idle gaps for live queries."),
        _f("embed_order", "choice", choices=("oldest", "newest"), label="Embed order",
           help="oldest = drain the backlog; newest = index fresh docs first."),
        _f("crawl_host_interval", "float", 1.0, 300, label="Crawl per-host interval (s)",
           help="Default politeness for web-cluster crawls; a recipe may go slower, never faster."),
        _f("crawl_max_pages", "int", 1, 20_000, label="Crawl page budget (default)"),
        _f("crawl_max_depth", "int", 0, 8, label="Crawl depth (default)"),
    ),
}

# Flat index for validation: key -> (scope, Field). A key belongs to exactly one
# scope, which is what makes "is this key editable *here*" answerable.
_BY_KEY: dict[str, tuple[str, Field]] = {
    f.key: (scope, f) for scope, fields in SCHEMA.items() for f in fields
}


def scopes() -> tuple[str, ...]:
    return tuple(SCHEMA)


def fields_for(scope: str) -> tuple[Field, ...]:
    return SCHEMA.get(scope, ())


def describe(scope: str) -> list[dict]:
    return [f.describe() for f in fields_for(scope)]


def coerce(scope: str, key: str, value) -> object:
    """Validate + coerce + clamp one setting. Raises ValueError (→ 422).

    Rejects a key that is not editable at all, and a key edited under the wrong
    scope — otherwise `PATCH /v1/sources/wiki/settings {"embed_concurrency": 99}`
    would quietly work and the console would show it in the wrong place.
    """
    hit = _BY_KEY.get(key)
    if hit is None:
        raise ValueError(f"setting is not editable: {key!r}")
    owner, spec = hit
    if owner != scope:
        raise ValueError(f"setting {key!r} belongs to scope {owner!r}, not {scope!r}")
    # Typing, bounds and the clamp-don't-reject rule live on Param, shared with job
    # params and recipe module config. What stays here is the part that is specific
    # to settings: which keys are editable at all, and under which scope.
    return spec.coerce(value)


def coerce_all(scope: str, values: dict) -> dict:
    """Validate a whole patch. All-or-nothing: one bad key rejects the batch, so
    a form submit never lands half-applied."""
    if not isinstance(values, dict):
        raise ValueError("settings must be an object")
    return {k: coerce(scope, k, v) for k, v in values.items()}
