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
    # Prepended to *queries* only (retrieval-instruction models like qwen3-embedding)
    embed_query_prefix: str = ""
    # Query-time embedding deadline; hybrid search degrades to lexical past it
    # (indexing load on the GPU server must not stall searches)
    embed_query_timeout: float = 8.0

    # Corpus policy
    news_backfill_days: int = 90
    news_language: str = "en"
    minhash_window_days: int = 14
    repo_star_threshold: int = 10

    github_tokens: str = ""  # comma-separated PATs for hydration

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

    def all_dirs(self) -> list[Path]:
        return [
            self.ccnews_downloads_dir,
            self.gharchive_downloads_dir,
            self.news_staging_dir,
            self.repos_staging_dir,
        ]

    def github_token_list(self) -> list[str]:
        return [t.strip() for t in self.github_tokens.split(",") if t.strip()]


def get_settings() -> Settings:
    return Settings()
