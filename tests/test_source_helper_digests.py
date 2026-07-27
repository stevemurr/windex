"""Regression guards for behavior-bearing helpers outside runner modules."""

from windex.pipeline.runners import RUNNERS


def _dependency_modules(name: str) -> set[str]:
    return {
        dependency.__module__
        for dependency in getattr(
            RUNNERS[name],
            "__windex_digest_dependencies__",
            (),
        )
    }


def test_source_helper_files_participate_in_module_digests():
    expected = {
        "crawl.frontier": {"windex.crawl.scope"},
        "crawl.links": {
            "windex.crawl.links",
            "windex.crawl.scope",
        },
        "html.trafilatura": {
            "windex.crawl.extract",
            "windex.crawl.ids",
            "windex.crawl.scope",
            "windex.dateparse",
            "windex.smallweb.extract",
        },
        "html.devdocs_page": {
            "windex.docs_source.canonical",
            "windex.docs_source.html",
        },
        "feed.inline_docs": {
            "windex.ccnews.identity",
            "windex.dateparse",
            "windex.smallweb.extract",
            "windex.smallweb.feed",
        },
        "oai.arxiv_records": {"windex.arxiv.oai", "windex.dateparse"},
        "algolia.hn_stories": {"windex.dateparse", "windex.hn.algolia"},
        "parquet.rows": {"windex.dateparse", "windex.hn.mirror"},
        "cirrus.articles": {"windex.dateparse", "windex.wiki.reader"},
        "github.compose_doc": {"windex.dateparse", "windex.github.clean"},
        "list.sitemap": {"windex.hf.formats"},
        "dedup.exact": {"windex.ccnews.identity"},
        "dedup.minhash": {"windex.ccnews.minhash"},
        "ledger.stage": {
            "windex.ccnews.identity",
            "windex.sanitize",
            "windex.textguard",
        },
        "warc.datatrove": {
            "windex.ccnews.identity",
            "windex.ccnews.pipeline",
            "windex.dateparse",
        },
    }
    for module, helper_modules in expected.items():
        assert helper_modules <= _dependency_modules(module), module
