"""Module name -> the callable that executes a slice of it.

Populated in-tree only. A Pipeline names a key in this dict; it can never name an
import path, which is what makes "installing a source cannot execute anything" a
property of the design rather than a rule someone has to remember.

Implementations land in-tree, a coherent slice at a time. Declared modules that
are not yet in this mapping still fail resolution with the explicit
"declared but not yet implemented" error.
"""

from __future__ import annotations

from collections.abc import Callable

from windex.modules.catalog import (
    crawl_links,
    feed_entries,
    github_hydrated_repos,
    github_search_items,
    github_watch_events,
    list_apache_index,
    list_json_manifest,
    list_lines,
    list_llms_txt,
    list_path_manifest_gz,
    list_sitemap,
)
from windex.modules.collect import store_repos, store_upsert
from windex.modules.discover import (
    crawl_frontier,
    state_pending,
    state_repos_pending,
    static_once,
    time_calendar,
    time_windows,
)
from windex.modules.extract import (
    algolia_hn_stories,
    cirrus_articles,
    feed_inline_docs,
    github_compose_doc,
    html_devdocs_page,
    html_trafilatura,
    markdown_passthrough,
    oai_arxiv_records,
    parquet_rows,
)
from windex.modules.fetch import (
    github_graphql_batch,
    http_download,
    http_get,
    http_paginate,
    local_parquet_lookup,
)
from windex.modules.load import ledger_stage
from windex.modules.receive import push_docs
from windex.modules.warc import warc_datatrove
from windex.modules.transform import (
    canonical_url,
    dedup_boilerplate,
    dedup_exact,
    dedup_minhash,
    filter_lang,
    filter_quality,
)
from windex.worker.protocol import Runner
from windex.pipeline.indexing import platform_index
from windex.pipeline.reset import platform_reset

RUNNERS: dict[str, Callable[..., Runner]] = {
    "platform.index": platform_index,
    "platform.reset": platform_reset,
    "state.pending": state_pending,
    "static.once": static_once,
    "state.repos_pending": state_repos_pending,
    "time.calendar": time_calendar,
    "time.windows": time_windows,
    "crawl.frontier": crawl_frontier,
    "push.docs": push_docs,
    "http.get": http_get,
    "http.download": http_download,
    "http.paginate": http_paginate,
    "github.graphql_batch": github_graphql_batch,
    "local.parquet_lookup": local_parquet_lookup,
    "list.lines": list_lines,
    "list.json_manifest": list_json_manifest,
    "list.sitemap": list_sitemap,
    "list.apache_index": list_apache_index,
    "list.path_manifest_gz": list_path_manifest_gz,
    "list.llms_txt": list_llms_txt,
    "github.watch_events": github_watch_events,
    "github.search_items": github_search_items,
    "github.hydrated_repos": github_hydrated_repos,
    "feed.entries": feed_entries,
    "crawl.links": crawl_links,
    "html.trafilatura": html_trafilatura,
    "html.devdocs_page": html_devdocs_page,
    "markdown.passthrough": markdown_passthrough,
    "feed.inline_docs": feed_inline_docs,
    "oai.arxiv_records": oai_arxiv_records,
    "algolia.hn_stories": algolia_hn_stories,
    "parquet.rows": parquet_rows,
    "cirrus.articles": cirrus_articles,
    "warc.datatrove": warc_datatrove,
    "github.compose_doc": github_compose_doc,
    "canonical.url": canonical_url,
    "dedup.exact": dedup_exact,
    "dedup.minhash": dedup_minhash,
    "dedup.boilerplate": dedup_boilerplate,
    "filter.quality": filter_quality,
    "filter.lang": filter_lang,
    "store.upsert": store_upsert,
    "store.repos": store_repos,
    "ledger.stage": ledger_stage,
}


def register(name: str):
    """Decorator used by module implementations as they land."""
    def wrap(fn):
        if name in RUNNERS:
            raise RuntimeError(f"module {name!r} already has an implementation")
        RUNNERS[name] = fn
        return fn
    return wrap
