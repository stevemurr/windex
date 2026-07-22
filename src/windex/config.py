from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WINDEX_", env_file=".env", extra="ignore")

    # Storage. data_root holds everything bulky: downloads, parquet staging.
    data_root: Path = Path("/Volumes/External/windex")
    pg_dsn: str = "postgresql://windex:windex@127.0.0.1:5432/windex"
    qdrant_url: str = "http://127.0.0.1:6333"
    # Base URL of the Grafana that scrapes windex's /metrics (the ops box at
    # 192.168.1.237). Surfaced to the console header via /v1/stats; empty ⇒ the
    # header link stays hidden, so installs without Grafana show no dead link and
    # the IP is never hardcoded in source. Set WINDEX_GRAFANA_URL in .env.
    grafana_url: str = ""
    # Interface `windex serve` binds. Default loopback-only; set
    # WINDEX_SERVE_HOST=0.0.0.0 to expose the API + /metrics on the LAN (required
    # for the remote Prometheus on 192.168.1.237 to scrape). `windex up` and the
    # watchdog read this, so a supervised restart keeps the same binding instead
    # of silently reverting to loopback.
    serve_host: str = "127.0.0.1"

    # Embedding model is user-supplied; dim must be set before collections exist.
    embed_backend: Literal["http-tei", "http-openai", "st"] = "http-tei"
    embed_endpoint: str = "http://127.0.0.1:8080"
    embed_api_key: str = ""
    # Tiered gateway keys. bulk = indexing: the server hard-caps this key's
    # concurrency (currently 6 in flight; keep embed_global_budget <= that) and
    # REJECTS the excess with 429 rather than queueing it. query = interactive
    # search: no concurrency cap; excess requests queue server-side and stay
    # fast while indexing runs. Each falls back to embed_api_key when unset, so
    # a single-key server needs only the one variable.
    embed_bulk_api_key: str = ""
    embed_query_api_key: str = ""
    embed_model: str = "placeholder"
    embed_dim: int = 0
    embed_max_tokens: int = 512
    embed_batch_size: int = 64
    embed_concurrency: int = 8  # in-flight requests; GPU servers batch these
    # Fleet-wide cap on in-flight BULK embed requests per endpoint (0 = off).
    # embed_concurrency is per-process, so N jobs multiply it: 6 jobs x 8 put ~48
    # requests at one GPU and a query embed took 67s vs its 8s deadline. See
    # embed/budget.py. Live queries are never budgeted.
    # MUST NOT exceed the bulk key's server-side concurrency cap (6): the
    # gateway 429s past it, it doesn't queue. Keep the two equal — lower wastes
    # paid-for slots, higher guarantees rejection churn.
    embed_global_budget: int = 6
    # Pause per worker between batches: creates idle gaps on the embedding
    # server so live queries aren't stuck behind indexing (0 = full speed)
    embed_throttle_seconds: float = 0.0
    # Prepended to *queries* only (retrieval-instruction models like qwen3-embedding)
    embed_query_prefix: str = ""
    # Per-source query prefix override for the `memory` source: chat-history
    # recall is framed differently from web search, so when non-empty this is
    # used instead of embed_query_prefix for source=memory queries (single
    # branch at the query-embed site in index/search.py). Empty ⇒ fall back to
    # the global embed_query_prefix.
    embed_query_prefix_memory: str = ""
    # Opt-in bearer token guarding the write side of the `memory` source
    # (POST/DELETE /v1/memory/* and GET /v1/memory/status). Empty (default) =
    # open, matching windex's trusted-LAN posture; when set, those endpoints
    # require `Authorization: Bearer <token>`. Read endpoints stay open.
    write_token: str = ""
    # --- search-quality eval (windex eval) ---
    eval_per_source: int = 25      # known-item samples per source
    eval_k: int = 10               # cutoff for NDCG@k / Recall@k
    # LLM-as-judge (optional): a self-hosted OpenAI-compatible chat endpoint (the
    # Spark's LLM gateway). Empty endpoint/model ⇒ the judge leg is skipped.
    judge_endpoint: str = ""
    judge_model: str = ""
    judge_api_key: str = ""
    judge_timeout: float = 30.0
    # --- cross-encoder reranker (optional): reorders the fused pool by true
    # (query, passage) relevance. Empty endpoint/model ⇒ reranking is skipped. ---
    rerank_endpoint: str = ""
    rerank_model: str = ""
    rerank_api_key: str = ""
    rerank_path: str = "/rerank"
    # Instruction-tuned rerankers (Qwen3-Reranker) need the query wrapped as
    # "<Instruct>: {instruct}\n<Query>: {query}"; sending the RAW query collapses
    # scores to ~0.5 and mis-ranks real passages (verified: relevant 0.49 vs
    # irrelevant 0.79 raw → 0.91 vs 0.63 wrapped). Empty ⇒ send the query as-is
    # (for rerankers that take no instruction).
    rerank_query_instruct: str = "Given a web search query, retrieve relevant passages that answer the query"
    rerank_timeout: float = 10.0
    rerank_top_k: int = 50         # candidates fetched per source to rerank
    # Query-time embedding deadline; hybrid search degrades to lexical past it
    # (indexing load on the GPU server must not stall searches)
    embed_query_timeout: float = 8.0
    # Bulk embed queue order: "oldest" (created_at ASC — drain the backlog) or
    # "newest" (created_at DESC — embed freshly-harvested docs ahead of the
    # backlog). Flip to "newest" + restart the loops for a freshness push.
    embed_order: str = "oldest"
    # Circuit breaker on the *query* embed only (index/embed_breaker.py) — the
    # bulk embed path is never breakered. Once the GPU is saturated the timeout
    # above is paid on every search to rediscover a known answer; after this many
    # consecutive failures the dense leg is skipped outright for the cooldown.
    # 3: a lone timeout is noise (failures were ~22% when measured), three in a
    # row is a saturated server. 0 disables the breaker.
    embed_breaker_threshold: int = 3
    # 30s: one probe costs at most a timeout (~9s), so probing this often adds
    # negligible load to the GPU, and hybrid returns within 30s of it recovering.
    embed_breaker_cooldown: float = 30.0

    # Corpus policy
    news_backfill_days: int = 90
    news_language: str = "en"
    minhash_window_days: int = 14
    repo_star_threshold: int = 10
    # Wikipedia (CirrusSearch index bz2 dumps). One dump file = one shard (64
    # per weekly snapshot); each shard streams to its own clean parquet so the
    # embed pass (which reads a text_ref whole) stays bounded. chunk_rows is the
    # row-group / commit / pause-check granularity within a shard.
    wiki_dump: str = "enwiki"
    wiki_chunk_rows: int = 2_000
    # arXiv (OAI-PMH metadata harvest). Metadata is CC0; we harvest metadata only
    # (title + abstract), never full text. arXiv ToU: 1 request / 3 seconds from a
    # single connection with a descriptive User-Agent. The backfill is chunked into
    # per-year date windows so each is independently restartable (resumption tokens
    # expire at the next 00:00 UTC).
    arxiv_oai_endpoint: str = "https://oaipmh.arxiv.org/oai"
    arxiv_metadata_prefix: str = "arXiv"
    arxiv_request_interval: float = 3.0
    arxiv_incremental_days: int = 7
    arxiv_earliest_year: int = 2005  # earliestDatestamp is 2005-09-16
    # Small Web (Kagi smallweb.txt: RSS/Atom feeds of personal blogs). windex's
    # only FETCH-based source — a polite feed + HTML fetcher. Attribution: the
    # list is MIT (github.com/kagisearch/smallweb); windex links out to the
    # blogs, honoring robots.txt with an honest, descriptive User-Agent. The
    # quality gate is deliberately light (see smallweb/extract.py).
    smallweb_list_url: str = "https://raw.githubusercontent.com/kagisearch/smallweb/main/smallweb.txt"
    smallweb_max_items: int = 15            # newest items polled per feed
    smallweb_poll_batch: int = 200          # feeds per pause-checked batch
    smallweb_concurrency: int = 12          # global in-flight feed/page fetches
    smallweb_host_interval: float = 10.0    # min seconds between hits to one host
    smallweb_robots_ttl: float = 3600.0     # per-host robots.txt cache TTL
    smallweb_max_page_bytes: int = 2_000_000  # skip a post page larger than ~2MB
    smallweb_request_timeout: float = 15.0
    smallweb_max_fail: int = 5              # consecutive failures → status 'dead'
    smallweb_min_chars: int = 200           # light quality gate: minimum post length
    smallweb_inline_summary_min: int = 600  # a description this long is a full-text feed (no page fetch)

    # Programming docs (DevDocs pre-built bundles, https://devdocs.io/docs.json).
    # docs_slugs is the seed list: which of the ~819 docsets to index, comma-
    # separated. The default covers the mainstream stack; the whales (openjdk
    # 120MB, dom 63MB, cpp 42MB) are deliberately excluded — add them per need.
    # Version-pinned slugs (python~3.14, postgresql~18, …) should be bumped in
    # step with the canonical-URL table (docs_source/canonical.py).
    docs_manifest_url: str = "https://devdocs.io/docs.json"
    docs_cdn_url: str = "https://documents.devdocs.io"
    docs_slugs: str = (
        "python~3.14,javascript,typescript,node,go,rust,c,react,vue~3,html,css,"
        "http,postgresql~18,git,bash,php,ruby~3.4,django~6.1,flask,tailwindcss,"
        "docker,kubernetes"
    )

    # Hacker News (stories only, never comments). Tail + authoritative backfill:
    # the Algolia HN Search API (free, no auth, ~10k req/hr/IP; every query is
    # hard-capped at 1000 hits, so windows recursively split until they fit —
    # keep the pacing polite anyway). Fast backfill: the open-index/hacker-news
    # parquet mirror on Hugging Face (ODC-By 1.0). The incremental window trails
    # a couple of days ON PURPOSE: re-pulling unchanged stories refreshes their
    # points/num_comments payloads in place (set_payload, never a re-embed).
    hn_algolia_url: str = "https://hn.algolia.com/api/v1/search_by_date"
    hn_mirror_url: str = "https://huggingface.co/datasets/open-index/hacker-news/resolve/main/data"
    hn_request_interval: float = 0.4
    hn_incremental_days: int = 2

    # Hugging Face docs/courses/blog (huggingface.co). windex's second FETCH-based
    # source and the first pointed at a SINGLE host, which is why none of the
    # smallweb_* knobs are reused: concurrency is inert behind one host's limiter,
    # and smallweb's 10s stranger-blog interval would cost 11 hours for 4,014
    # pages. HF publishes its own budget instead — the `pages` bucket governing
    # /docs/*, /blog/* and .md is q=100;w=300 = 1 req/3s — and the limiter
    # self-throttles off the live `ratelimit:` header (hf/fetch.py).
    # hf_roots is the seed list: which doc roots to index, comma-separated.
    # EMPTY = all 52 roots from sitemap-doc.xml, which is only ~3,175 pages —
    # unlike DevDocs there is no reason to be selective.
    hf_sitemap_url: str = "https://huggingface.co/sitemap.xml"
    hf_base_url: str = "https://huggingface.co"
    hf_roots: str = ""                      # "" = every root in sitemap-doc.xml
    hf_request_interval: float = 3.0        # HF's own number (pages bucket: 100/300s)
    hf_robots_ttl: float = 3600.0           # per-host robots.txt cache TTL
    hf_max_page_bytes: int = 4_000_000      # a big doc page is ~1.3MB of HTML
    hf_request_timeout: float = 30.0
    hf_blog_batch: int = 100                # posts per staged parquet / pause-checked batch

    github_tokens: str = ""  # comma-separated PATs for hydration

    # Threads draining Qdrant upserts in the embed pass. The embed workers hand
    # finished points off to these instead of blocking a GPU slot on PUT /points
    # (avg 355ms, worst case 36s observed). Upserts stay wait=True — the pass
    # commits status='embedded' only after the vectors are durable — so this is
    # about getting the round-trip off the embed thread, not about skipping it.
    # 0 = match embed_concurrency, which is the upsert parallelism the old
    # inline-upsert code effectively had; a smaller fixed pool would be a
    # *narrower* funnel than before whenever Qdrant hits its latency tail.
    embed_upsert_workers: int = 0

    @property
    def downloads_dir(self) -> Path:
        return self.data_root / "downloads"

    @property
    def ccnews_downloads_dir(self) -> Path:
        return self.downloads_dir / "ccnews"

    @property
    def gharchive_downloads_dir(self) -> Path:
        return self.downloads_dir / "gharchive"

    @property
    def staging_dir(self) -> Path:
        return self.data_root / "staging"

    @property
    def news_staging_dir(self) -> Path:
        return self.staging_dir / "news"

    @property
    def repos_staging_dir(self) -> Path:
        return self.staging_dir / "repos"

    @property
    def wiki_downloads_dir(self) -> Path:
        return self.downloads_dir / "wiki"

    @property
    def wiki_staging_dir(self) -> Path:
        return self.staging_dir / "wiki"

    @property
    def arxiv_staging_dir(self) -> Path:
        return self.staging_dir / "arxiv"

    @property
    def smallweb_staging_dir(self) -> Path:
        return self.staging_dir / "smallweb"

    @property
    def docs_downloads_dir(self) -> Path:
        return self.downloads_dir / "docs"

    @property
    def docs_staging_dir(self) -> Path:
        return self.staging_dir / "docs"

    @property
    def hn_downloads_dir(self) -> Path:
        return self.downloads_dir / "hn"

    @property
    def hn_staging_dir(self) -> Path:
        return self.staging_dir / "hn"

    @property
    def hf_staging_dir(self) -> Path:
        return self.staging_dir / "hf"

    @property
    def memory_staging_dir(self) -> Path:
        # Push-based chat-memory source: one full-replace parquet per
        # conversation under memory/clean/<conversation_id>.parquet.
        return self.staging_dir / "memory"

    def all_dirs(self) -> list[Path]:
        return [
            self.ccnews_downloads_dir,
            self.gharchive_downloads_dir,
            self.news_staging_dir,
            self.repos_staging_dir,
            self.wiki_staging_dir,
            self.arxiv_staging_dir,
            self.smallweb_staging_dir,
            self.docs_downloads_dir,
            self.docs_staging_dir,
            self.hn_downloads_dir,
            self.hn_staging_dir,
            self.hf_staging_dir,
            self.memory_staging_dir,
        ]

    def github_token_list(self) -> list[str]:
        return [t.strip() for t in self.github_tokens.split(",") if t.strip()]

    def docs_slug_list(self) -> list[str]:
        return [s.strip() for s in self.docs_slugs.split(",") if s.strip()]

    def hf_root_list(self) -> list[str]:
        """Configured HF doc roots; [] means "every root the sitemap lists"."""
        return [s.strip() for s in self.hf_roots.split(",") if s.strip()]


def get_settings() -> Settings:
    return Settings()
