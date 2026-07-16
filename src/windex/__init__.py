import os

# Every HF asset windex pulls (fastembed BM25, tokenizers) is public. A stale
# stored HF login otherwise breaks anonymous downloads (observed: expired token
# → 401 → fastembed "Could not load model Qdrant/bm25"). Export
# HF_HUB_DISABLE_IMPLICIT_TOKEN=0 to re-enable the stored token if ever needed.
os.environ.setdefault("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")

__version__ = "0.1.0"
