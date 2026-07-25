"""The module catalog: what a recipe is allowed to reference.

A recipe is INERT DATA. It names modules; it never carries code, and there is no
import-by-string, no entry-point scan and no plugin directory anywhere in this
package. That is what makes "installing a source cannot execute anything" a
structural property rather than a policy — the marketplace ships YAML, and a
recipe naming a module this registry does not have is a 422 at install time with
the exact missing name, not a download.

Each module declares its config as `Param`s (windex.schema.param), the same type
behind editable settings and job arguments. One form shape, three uses: the Swift
inspector, the settings screen and the job dialog all render from the same JSON.

`allowed_hosts` is declared BY THE MODULE and a recipe may only narrow it. Only
`http.get` accepts an arbitrary host — and only it carries robots, per-host rate
limiting, size caps and the SSRF guard. Every other network module can reach
exactly the upstream it was written for.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass

from windex.schema.param import Param

# Capability flags a module may declare. These are advisory to the operator and
# load-bearing to the compiler: `network.outbound` is what makes the SSRF guard
# mandatory, and `stateful` is what stops a module being parallelised across
# batches when it keeps cross-batch state (dedup windows, boilerplate counters).
CAPABILITIES = ("network.outbound", "gpu", "stateful", "destructive")


@dataclass(frozen=True)
class Module:
    """One registered behaviour. `impl` is wired in Phase 7; the declaration is
    what the editor and the validator need, and it comes first on purpose — a
    module whose config schema is wrong is useless however good its code is."""

    name: str
    kind: str
    title: str
    summary: str
    fields: tuple[Param, ...] = ()
    allowed_hosts: tuple[str, ...] = ()      # () = no network; ("*",) = caller-chosen
    capabilities: tuple[str, ...] = ()
    # Which worker lane this runs in, and what must hold before it may be claimed.
    # Assigned below rather than inferred from `kind`: the lane is a property of
    # what a module DOES, and inferring it would put a 333MB shard reader in the
    # same lane as a status query. cpu_heavy is capped at 1 concurrent because
    # that cap IS the memory ceiling on this box.
    lane: str = "io"                         # gpu | net | cpu_heavy | io | maint
    preconditions: tuple[str, ...] = ()      # worker.preconditions.KNOWN
    version: str = "1.0"
    stability: str = "stable"                # stable | beta | experimental
    # Relaxations of the one-in/one-out contract, each for exactly one real reason.
    batched: bool = False        # consumes a LIST of inputs (warc.datatrove)
    thread_safe: bool = True     # False -> must run on the main thread
    built_from: str = ""         # the existing code this generalizes; for reviewers

    def describe(self) -> dict:
        return {
            "id": self.name, "kind": self.kind, "version": self.version,
            "title": self.title, "summary": self.summary,
            "stability": self.stability,
            "capabilities": list(self.capabilities),
            "allowed_hosts": list(self.allowed_hosts),
            "batched": self.batched, "thread_safe": self.thread_safe,
            "lane": self.lane, "preconditions": list(self.preconditions),
            "config": {"fields": [f.describe() for f in self.fields]},
        }


def _p(key, kind, **kw) -> Param:
    return Param(key=key, kind=kind, **kw)


# --- discover / receive ------------------------------------------------------

_DISCOVER = (
    Module(
        "state.pending", "discover", "Pending units",
        "Selects units whose upstream has moved past what was last ingested.",
        built_from="docs_source/sync.py:83, hf/sync.py:268, arxiv/harvest.py:256, "
                   "ccnews/sync.py:86, wiki/sync.py:117, hn/harvest.py:221",
        fields=(
            _p("store", "str", required=True, label="Store",
               help="Which of the recipe's declared stores to claim from."),
            _p("predicate", "choice",
               choices=("unseen", "token_moved", "stage_in", "rearm", "rotate"),
               default="token_moved", label="Freshness rule",
               help="How 'needs work' is decided. token_moved compares the upstream "
                    "token against the last fully-ingested one; rotate is for stores "
                    "with no upstream signal at all (38k blog feeds)."),
            _p("stages", "csv", default="", label="Stages",
               help="For stage_in: which lifecycle stages count as pending.",
               depends_on={"field": "predicate", "equals": "stage_in"}),
            _p("rearm_days", "int", lo=1, hi=365, default=7, label="Re-arm after (days)",
               depends_on={"field": "predicate", "equals": "rearm"}),
            _p("order", "choice", choices=("key", "ord", "processed_at", "stars_desc"),
               default="ord", label="Order"),
            _p("batch", "int", lo=1, hi=10_000, default=50, label="Units per slice"),
            _p("claim", "choice", choices=("none", "lease"), default="none",
               label="Claim policy",
               help="none: pending-ness never consults status, so a killed worker's "
                    "rows simply re-run. lease: for units too expensive to redo "
                    "(a 1GB WARC download). Three separate incidents in this "
                    "codebase were status-gated queues stranding rows invisibly."),
            _p("stale_minutes", "int", lo=1, hi=1440, default=60,
               label="Lease expiry (min)",
               depends_on={"field": "claim", "equals": "lease"}),
        ),
    ),
    Module(
        "static.once", "discover", "Run once",
        "Emits exactly one unit of work. The root for a flow that polls a fixed "
        "upstream rather than iterating a store.",
        built_from="ccnews/sync.py, docs_source/sync.py, hf/sync.py, wiki/sync.py "
                   "— every `sync` step that fetches one known listing",
        fields=(
            _p("key", "str", default="once", label="Unit key",
               help="Recorded on the unit so a sync flow's history is readable. "
                    "Not a partition: nothing is claimed and nothing is watermarked."),
            _p("payload", "csv", default="", label="Payload",
               help="Optional key=value pairs passed to the fetch node's template."),
        ),
    ),
    Module(
        "state.repos_pending", "discover", "Pending repos",
        "The repos-table adapter of the pending selector.",
        built_from="github/hydrate.py:82, github/embed_index.py:51",
        fields=(
            _p("store", "str", required=True, label="Store",
               help="The repos store. A dedicated wide table behind a store adapter: "
                    "millions of rows with indexed stars and a topics array, which "
                    "a generic jsonb payload would be a real regression on."),
            _p("stages", "csv", default="candidate", label="Stages"),
            _p("min_star_events", "int", lo=0, hi=100_000, default=0),
            _p("order", "choice", choices=("star_events_desc", "stars_desc"),
               default="stars_desc"),
            _p("batch", "int", lo=1, hi=5_000, default=40),
            _p("limit", "int", lo=1, hi=1_000_000, default=100_000),
        ),
    ),
    Module(
        "time.calendar", "discover", "Calendar keys",
        "Synthesizes hour/day/month keys over a range and seeds them into a store.",
        built_from="github/tail.py:18, ccnews/sync.py:23",
        fields=(
            _p("unit", "choice", choices=("hour", "day", "month", "year"),
               default="day", required=True),
            _p("trailing_days", "int", lo=0, hi=3650, default=2,
               label="Trailing window (days)"),
            _p("into", "str", required=True, label="Store"),
            _p("format", "str", default="", label="Key format",
               help="strftime pattern for the generated key."),
        ),
    ),
    Module(
        "time.windows", "discover", "Time windows",
        "Plans [from,until) windows: a backfill sweep plus a rolling re-armed tail.",
        built_from="arxiv/harvest.py:208, hn/harvest.py:132",
        fields=(
            _p("unit", "choice", choices=("day", "month", "year"), default="month"),
            _p("incremental_days", "int", lo=1, hi=365, default=7),
            _p("earliest", "date", label="Backfill floor"),
        ),
    ),
    Module(
        "crawl.frontier", "discover", "Crawl frontier",
        "BFS frontier claim: shallowest-first, budget-bounded, persisted so a "
        "resumed run continues rather than restarting.",
        built_from="crawl/run.py:78-105, :221-231",
        fields=(
            _p("store", "str", required=True, label="Frontier store",
               help="Where the BFS frontier is persisted. Run-scoped: it cascade-"
                    "deletes with the run, unlike a permanent watermark store."),
            _p("seeds", "url_list", required=True, max_items=25, max_len=2000,
               label="Seed URLs", stage="install",
               help="Where the crawl starts. Scope defaults to the seed's own "
                    "directory unless path_prefix says otherwise."),
            _p("max_pages", "int", lo=1, hi=20_000, default=500,
               ceiling="crawl_max_pages_ceiling", label="Page budget"),
            _p("batch", "int", lo=1, hi=1_000, default=50),
        ),
    ),
    Module(
        "push.docs", "receive", "Pushed documents",
        "Documents arrive over HTTP instead of being fetched.",
        built_from="custom_source/ingest.py:48, memory_source/ingest.py",
        fields=(
            _p("mode", "choice", choices=("delta", "full_set"), default="delta",
               label="Push semantics",
               help="delta: upsert, absent ids are left alone (custom sources). "
                    "full_set: the push is the whole set and absent ids are "
                    "tombstoned (chat memory)."),
            _p("max_docs", "int", lo=1, hi=10_000, default=500),
            _p("max_text_chars", "int", lo=1, hi=1_000_000, default=16_000),
        ),
    ),
)

# --- fetch -------------------------------------------------------------------

_FETCH = (
    Module(
        "http.get", "fetch", "HTTP fetch",
        "Polite GET with robots.txt, per-host rate limiting, size caps and an "
        "SSRF guard re-checked on every redirect hop by resolved IP.",
        allowed_hosts=("*",), capabilities=("network.outbound",),
        built_from="smallweb/poll.py:197 (PageFetcher), crawl/fetch.py:64-190",
        fields=(
            _p("host_interval", "float", lo=1.0, hi=300, default=2.0, unit="s",
               floor="crawl_host_interval_min", label="Per-host interval",
               clamp_note="Raised to the operator's floor if lower. A recipe may "
                          "ask to be slower, never faster.",
               section="politeness"),
            _p("request_timeout", "float", lo=1, hi=300, default=15, unit="s",
               section="politeness"),
            _p("retries", "int", lo=0, hi=10, default=3,
               help="Retry 429 and transient server errors inside the task so "
                    "a published reset window does not consume its run retry budget.",
               section="politeness"),
            _p("max_bytes", "int", lo=1024, hi=4_000_000, default=4_000_000,
               ceiling="crawl_max_page_bytes", unit="bytes", section="limits"),
            _p("allowed_types", "csv",
               default="html,xhtml,text/plain,markdown,xml,rss,atom",
               section="limits"),
            _p("robots", "bool", default=True, label="Honour robots.txt",
               locked_reason="Always on. windex does not ship an override.",
               section="politeness"),
            _p("ssrf_guard", "bool", default=True, label="Block private addresses",
               locked_reason="Always on. Checked by RESOLVED IP on every redirect "
                             "hop, not by hostname — hostname matching is bypassable "
                             "by a redirect or a rebinding DNS record.",
               section="politeness"),
            _p("conditional", "bool", default=True, label="Conditional GET",
               help="Send If-None-Match / If-Modified-Since when the store has them.",
               section="politeness"),
        ),
    ),
    Module(
        "http.download", "fetch", "Download to disk",
        "Fetches a large artifact to the downloads tier rather than into memory.",
        capabilities=("network.outbound",),
        built_from="ccnews/download.py, github/tail.py:67, docs_source/ingest.py:172",
        fields=(
            _p("url_template", "str", required=True, max_len=500,
               help="Substitution only, over the unit's declared placeholders."),
            _p("allowed_hosts", "csv", required=True, label="Allowed hosts",
               help="A recipe may only narrow what the module already permits."),
            _p("keep", "bool", default=False, label="Keep after processing"),
            _p("missing_ok", "bool", default=False,
               help="Treat 404 as an empty result rather than a failure — upstream "
                    "hourly archives have real gaps."),
            _p("retries", "int", lo=0, hi=10, default=3),
        ),
    ),
    Module(
        "http.paginate", "fetch", "Paginated API",
        "Walks a paginated upstream by a declared protocol.",
        capabilities=("network.outbound",),
        built_from="arxiv/harvest.py:403, hn/harvest.py:264, github/discover.py:33",
        fields=(
            _p("protocol", "choice", required=True,
               choices=("oai_resumption", "algolia_numeric", "github_search_pages",
                        "link_header"), label="Pagination protocol"),
            _p("allowed_hosts", "csv", required=True),
            _p("page_size", "int", lo=1, hi=1000, default=100),
            _p("result_cap", "int", lo=1, hi=100_000, default=1000,
               help="Upstream's own ceiling on a single query's results."),
            _p("split_on_cap", "bool", default=False,
               help="When a query hits result_cap, split its range and retry — the "
                    "only way to sweep past a hard API limit."),
            _p("request_interval", "float", lo=0.0, hi=60, default=3.0, unit="s"),
            _p("token_ref", "secret_ref", allow=("github_tokens",), stage="install"),
        ),
    ),
    Module(
        "github.graphql_batch", "fetch", "GitHub GraphQL batch",
        "Hydrates repo metadata + README in batches of up to 40.",
        allowed_hosts=("api.github.com",), capabilities=("network.outbound",),
        built_from="github/hydrate.py:51-247",
        fields=(
            _p("batch", "int", lo=1, hi=40, default=40),
            _p("token_ref", "secret_ref", allow=("github_tokens",), required=True,
               stage="install"),
            _p("max_readme_bytes", "int", lo=1024, hi=1_000_000, default=200_000),
        ),
    ),
    Module(
        "local.parquet_lookup", "fetch", "Local parquet lookup",
        "Reads a previously-staged sidecar by key. No network.",
        built_from="github/embed_index.py:31-47",
        fields=(
            _p("dir", "str", required=True),
            _p("key_column", "str", required=True),
            _p("value_column", "str", required=True),
            _p("skip_unreadable", "bool", default=True,
               help="A truncated sidecar from an interrupted write should skip that "
                    "row, not abort the pass."),
        ),
    ),
)

# --- catalog / extract / transform / sinks -----------------------------------
# Declared more compactly: these differ mostly in their parser, and their config
# surface is small. Fields are added as the Phase 7 implementations need them.

def _simple(name, kind, title, summary, built_from="", **kw) -> Module:
    return Module(name, kind, title, summary, built_from=built_from, **kw)


_CATALOG = (
    _simple("list.lines", "catalog", "Line list", "One entry per line.",
            "smallweb/sync.py:35",
            fields=(_p("scheme_allow", "csv", default="http,https"),
                    _p("shrink_floor", "int", lo=0, hi=1_000_000, default=200,
                       help="Refuse a listing that shrank below this — a truncated "
                            "upstream fetch must not look like mass deletion."),)),
    _simple("list.json_manifest", "catalog", "JSON manifest", "Manifest of entries.",
            "docs_source/sync.py:24",
            fields=(_p("key_field", "str", required=True),
                    _p("upstream_field", "str", default=""),)),
    _simple("list.sitemap", "catalog", "Sitemap", "sitemap.xml urls + lastmod.",
            "hf/sync.py:74",
            fields=(_p("shard_allow", "csv", default=""),)),
    _simple("list.apache_index", "catalog", "Apache index", "Directory listing.",
            "wiki/sync.py:31",
            fields=(_p("name_pattern", "regex_list", max_items=4, max_len=500),
                    _p("require_marker", "str", default="",
                       help="Only accept a directory carrying this completeness "
                            "marker — a dump still being written has none."),
                    _p("newest_only", "bool", default=True),)),
    _simple("list.path_manifest_gz", "catalog", "gzipped path manifest",
            "Newline-delimited paths inside a .gz.", "ccnews/sync.py:32",
            fields=(_p("key_pattern", "regex_list", max_items=4, max_len=500),
                    _p("min_age_days", "int", lo=0, hi=3650, default=0),
                    _p("max_age_days", "int", lo=0, hi=3650, default=0,
                       help="Exclude dated paths older than this rolling window; "
                            "zero keeps the lower bound open."),)),
    _simple("list.llms_txt", "catalog", "llms.txt", "Link list from an llms.txt.",
            "hf/sync.py:120"),
    _simple("github.watch_events", "catalog", "GH Archive watch events",
            "WatchEvents from an hourly archive.", "github/tail.py:91",
            fields=(_p("event_type", "str", default="WatchEvent"),)),
    _simple("github.search_items", "catalog", "GitHub search results",
            "Repos from a Search API page.", "github/discover.py:109"),
    _simple("github.hydrated_repos", "catalog", "Hydrated repos",
            "Accept/reject a hydrated repo against thresholds.", "github/hydrate.py:137",
            fields=(_p("stars_gte", "int", lo=0, hi=100_000, default=10),)),
    _simple("feed.entries", "catalog", "Feed entries",
            "Per-post units from a summary-only feed.", "smallweb/poll.py:71",
            fields=(_p("max_items", "int", lo=1, hi=200, default=20),)),
    _simple("crawl.links", "catalog", "Discovered links",
            "In-scope links from a fetched page, enqueued at depth+1.",
            "crawl/links.py, crawl/scope.py:97",
            fields=(
                _p("into", "str", required=True, label="Frontier store"),
                _p("max_depth", "int", lo=0, hi=8, default=2,
                   ceiling="crawl_max_depth_ceiling"),
                _p("same_host", "bool", default=True, stage="install"),
                _p("path_prefix", "str", default=None, stage="install",
                   help="Unset means the seed's own directory. An explicit empty "
                        "string means the whole host — they are different, and the "
                        "difference is a 500-page crawl versus a 20,000-page one."),
                _p("include", "regex_list", max_items=25, max_len=500, stage="install"),
                _p("exclude", "regex_list", max_items=25, max_len=500, stage="install"),
            )),
)

_EXTRACT = (
    _simple("html.trafilatura", "extract", "HTML article", "Main-content extraction.",
            "crawl/extract.py:117, smallweb/extract.py, ccnews/pipeline.py:14",
            thread_safe=False,
            fields=(
                _p("min_chars", "int", lo=0, hi=100_000, default=200),
                _p("quality_filters", "bool", default=False,
                   help="FineWeb/C4 gates. Off by default: they over-reject short, "
                        "code-heavy and personal-blog pages."),
                _p("structural_fallback", "bool", default=True,
                   help="When trafilatura declines, try main/article/[role=main]. "
                        "Measured: rescues ~2% of documentation pages."),
                _p("honor_canonical", "choice",
                   choices=("never", "in_scope", "always"), default="in_scope",
                   help="in_scope: adopt rel=canonical only when it stays inside the "
                        "crawl's scope, so a canonical pointing off-site cannot "
                        "smuggle in an out-of-scope id."),
            )),
    _simple("html.devdocs_page", "extract", "DevDocs page", "A DevDocs entry.",
            "docs_source/ingest.py:87"),
    _simple("markdown.passthrough", "extract", "Markdown", "Markdown as-is.",
            "hf/crawl.py:106"),
    _simple("feed.inline_docs", "extract", "Inline feed bodies",
            "Full-text feed entries, no page fetch.", "smallweb/poll.py:89"),
    _simple("oai.arxiv_records", "extract", "arXiv OAI records",
            "OAI-PMH metadata records, including tombstones.", "arxiv/harvest.py:108"),
    _simple("algolia.hn_stories", "extract", "HN stories", "Algolia hits.",
            "hn/harvest.py:78"),
    _simple("parquet.rows", "extract", "Parquet rows", "Rows from a parquet mirror.",
            "hn/backfill.py"),
    _simple("cirrus.articles", "extract", "CirrusSearch articles",
            "Wikipedia dump articles, streamed.", "wiki/reader.py",
            fields=(_p("chunk_rows", "int", lo=100, hi=50_000, default=2000),)),
    _simple("warc.datatrove", "extract", "WARC via datatrove",
            "The FineWeb block pipeline: extraction, language and quality in one "
            "pass, with its own task-per-WARC parallelism and resume.",
            "ccnews/pipeline.py:48-101", batched=True,
            fields=(
                _p("language", "str", default="en"),
                _p("workers", "int", lo=1, hi=8, default=4,
                   help="Was cpu_count()-2, which forked 18 extraction processes on "
                        "a 20-core box and is the prime suspect for the memory-"
                        "pressure resets. Capped deliberately."),
            )),
    _simple("github.compose_doc", "extract", "Compose repo document",
            "Repo metadata + cleaned README into one document.",
            "github/clean.py, github/embed_index.py:81"),
)

_TRANSFORM = (
    _simple("canonical.url", "transform", "Canonical id", "Derives the stable doc id.",
            "ccnews/dedup.py:42, crawl/run.py:58, docs_source/canonical.py",
            fields=(_p("strategy", "choice", required=True,
                       choices=("sha1_of_canonical", "path_suffix", "field")),)),
    _simple("dedup.exact", "transform", "Exact dedup", "text_hash equality.",
            "ccnews/dedup.py:64",
            fields=(_p("scope", "choice", choices=("batch", "ledger", "both"),
                       default="both"),)),
    _simple("dedup.minhash", "transform", "Near-duplicate dedup",
            "MinHash LSH over a rolling window.", "ccnews/minhash.py",
            capabilities=("stateful",),
            fields=(_p("window_days", "int", lo=1, hi=365, default=30,
                       help="News syndication crosses days; the window is why."),)),
    _simple("dedup.boilerplate", "transform", "Boilerplate guard",
            "Drops pages whose text repeats — soft-404 SPA shells return 200 for "
            "every path and would otherwise fill a source with one page.",
            "crawl/run.py:25-30, :199-271", capabilities=("stateful",),
            fields=(_p("repeat_cap", "int", lo=2, hi=100, default=2),)),
    _simple("filter.quality", "transform", "Quality filters",
            "Gopher/C4/FineWeb gates.", "smallweb/extract.py"),
    _simple("filter.lang", "transform", "Language filter", "Keeps declared languages.",
            "datatrove LanguageFilter",
            fields=(_p("languages", "csv", default="en"),)),
)

_SINKS = (
    _simple("store.upsert", "collect", "Write store", "Upserts partition records.",
            "smallweb/sync.py:84, docs_source/sync.py:56, hf/sync.py:162",
            fields=(
                _p("store", "str", required=True),
                _p("on_conflict", "choice", choices=("merge", "increment", "skip"),
                   default="merge"),
                _p("max_attempts", "int", lo=1, hi=100, default=10,
                   help="Beyond this a unit is marked dead rather than retried "
                        "forever — smallweb's fail_count, generalized."),
            )),
    _simple("store.repos", "collect", "Write repos", "The repos-table adapter.",
            "github/tail.py:114, github/hydrate.py:172",
            fields=(_p("store", "str", default="repos"),)),
    Module(
        "ledger.stage", "load", "Stage documents",
        "Writes a per-batch parquet and a text_hash-guarded ledger delta. The one "
        "place documents enter the index.",
        capabilities=("destructive",),
        built_from="custom_source/ingest.py:129-216, memory_source/ingest.py:131-247, "
                   "crawl/run.py:124-162, plus six identical ledger upserts",
        fields=(
            _p("write_mode", "choice", choices=("upsert",), default="upsert",
               locked_reason="Upsert is the only safe mode; deletion is expressed by "
                             "`replace` with a guard, never by write mode."),
            _p("replace", "bool", default=False, label="Tombstone unseen documents",
               help="The only setting that can DELETE content."),
            _p("replace_scope", "choice", choices=("partition", "source"),
               default="partition",
               help="partition: a batch may only tombstone ids under its own "
                    "PartitionRef.id_scope. This one rule covers full-replace "
                    "memory, per-slug docs, per-root hf and whole-source crawl.",
               depends_on={"field": "replace", "equals": True}),
            _p("replace_guard", "choice", choices=("none", "census"), default="census",
               help="census: refuse to tombstone anything if the run was truncated, "
                    "had any failure, or wrote no documents. A partial run deleting "
                    "'missing' documents is data loss dressed as tidying.",
               depends_on={"field": "replace", "equals": True}),
            _p("batch_rows", "int", lo=1, hi=100_000, default=1000),
        ),
    ),
)

# Lane and precondition assignment, applied after construction so it reads as one
# table rather than a kwarg buried in forty declarations. Anything not listed is
# `io` with no preconditions: it waits on Postgres or a small local read.
_PLACEMENT: dict[str, tuple[str, tuple[str, ...]]] = {
    # net — bounded by upstream politeness, not by this box, so several may run
    "http.get":             ("net", ()),
    "http.download":        ("net", ("storage:downloads",)),
    "http.paginate":        ("net", ()),
    "github.graphql_batch": ("net", ("gh_token",)),
    # cpu_heavy — the two memory-hungry paths, and the extraction that is both
    # CPU-bound and not thread-safe
    "warc.datatrove":       ("cpu_heavy", ("storage:downloads", "storage:staging")),
    "cirrus.articles":      ("cpu_heavy", ("storage:staging",)),
    "html.trafilatura":     ("cpu_heavy", ()),
    # io, but they touch the staging tree, so they wait on it being present and
    # above its free-space reserve
    "local.parquet_lookup": ("io", ("storage:staging",)),
    "parquet.rows":         ("io", ("storage:staging",)),
    "ledger.stage":         ("io", ("storage:staging",)),
}

MODULES: dict[str, Module] = {}
for _m in (_DISCOVER + _FETCH + _CATALOG + _EXTRACT + _TRANSFORM + _SINKS):
    _lane, _pre = _PLACEMENT.get(_m.name, ("io", ()))
    MODULES[_m.name] = dataclasses.replace(_m, lane=_lane, preconditions=_pre)

# Modules the compiler injects itself, which a recipe may neither name nor skip.
# Sanitization is not optional: smuggled/invisible code points must be stripped
# before text reaches the embedder, or a Tags-block payload slips under the char
# cap while blowing the model's token window.
ALWAYS_BEFORE_LOAD = ("text.sanitize",)


def get(name: str) -> Module | None:
    return MODULES.get(name)


def describe() -> dict:
    """The whole palette. What `GET /admin/v1/registry` serves and the graph editor
    renders from — no vocabulary is hardcoded client-side."""
    from windex.recipe import ports, runners

    return {
        "registry_version": 2,
        "port_types": ports.PORT_TYPES,
        "kinds": ports.describe_kinds(),
        "modules": [
            {**m.describe(), "implemented": m.name in runners.RUNNERS}
            for m in MODULES.values()
        ],
        "always_before_load": list(ALWAYS_BEFORE_LOAD),
    }
