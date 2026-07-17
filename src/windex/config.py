from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WINDEX_", env_file=".env", extra="ignore")

    # Storage. data_root holds everything bulky: downloads, parquet staging.
    data_root: Path = Path("/Volumes/External/windex")
    pg_dsn: str = "postgresql://windex:windex@127.0.0.1:5432/windex"
    qdrant_url: str = "http://127.0.0.1:6333"

    # Embedding model is user-supplied; dim must be set before collections exist.
    embed_backend: Literal["http-tei", "http-openai", "st"] = "http-tei"
    embed_endpoint: str = "http://127.0.0.1:8080"
    embed_api_key: str = ""
    embed_model: str = "placeholder"
    embed_dim: int = 0
    embed_max_tokens: int = 512
    embed_batch_size: int = 64
    embed_concurrency: int = 8  # in-flight requests; GPU servers batch these
    # Pause per worker between batches: creates idle gaps on the embedding
    # server so live queries aren't stuck behind indexing (0 = full speed)
    embed_throttle_seconds: float = 0.0
    # Prepended to *queries* only (retrieval-instruction models like qwen3-embedding)
    embed_query_prefix: str = ""
    # Query-time embedding deadline; hybrid search degrades to lexical past it
    # (indexing load on the GPU server must not stall searches)
    embed_query_timeout: float = 8.0
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
        ]

    def github_token_list(self) -> list[str]:
        return [t.strip() for t in self.github_tokens.split(",") if t.strip()]

    def docs_slug_list(self) -> list[str]:
        return [s.strip() for s in self.docs_slugs.split(",") if s.strip()]


def get_settings() -> Settings:
    return Settings()
