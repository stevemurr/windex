"""Crawl an arbitrary cluster of the web from a single seed link.

The unit of work is a **recipe** — a seed URL plus scope rules and limits — which
is persisted so a cluster can be re-crawled or scheduled. Everything downstream of
"produce documents" is deliberately NOT reimplemented here: a crawl stages its
pages through ``custom_source.ingest.upsert_docs``, so a crawled cluster is just
another custom source and inherits its parquet staging, ``text_hash``-guarded
ledger, Qdrant collection, embed loop, and search path unchanged.

Modules:

  * ``recipe`` — parse/validate/normalize the recipe document (the API contract).
  * ``scope``  — URL canonicalization and the in/out-of-scope decision.
  * ``fetch``  — the network seam: PageFetcher + the SSRF guard.
  * ``run``    — the BFS driver over the persisted ``crawl_urls`` frontier.

Static HTML only, by choice. A client-rendered SPA yields no links and the run
records that plainly ("0 in-scope links from seed") rather than silently indexing
shells — ``hf/crawl.py`` documents hitting exactly that failure on HF's
client-rendered sidebar. ``fetch.build_fetcher`` is the single seam a renderer
would attach to later.
"""

# Honest and descriptive, matching every other windex source's UA constant. The
# `crawl` token distinguishes recipe-driven cluster crawls from the fixed-source
# fetchers in a target's access log, so an operator who wants to rate-limit or
# block just this behaviour can do so without touching the rest.
USER_AGENT = (
    "windex-crawl/0.1 (self-hosted search index; "
    "+https://github.com/stevemurr/windex)"
)

# The token robots.txt is evaluated against. urllib's RobotFileParser matches a
# User-agent line by substring, so this stays a bare token rather than the full
# UA string above.
ROBOT_AGENT = "windex-crawl"
