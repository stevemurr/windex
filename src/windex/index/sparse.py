"""Shared BM25 sparse encoder.

fastembed loads the model from disk on construction, so building one per
embed_pending() pass (which every source used to do) reloads it once per batch
across 7 concurrent embed processes. It's stateless after load — one lazy
process-wide instance is all any of them need.
"""

import threading

_bm25 = None
_bm25_lock = threading.Lock()


def bm25_model():
    global _bm25
    # Double-checked locking: /v1/search runs in Starlette's threadpool, so
    # several concurrent cold requests could each construct (disk-load) a model
    # and all but one would be leaked. The lock makes construction happen once.
    if _bm25 is None:
        with _bm25_lock:
            if _bm25 is None:
                from fastembed import SparseTextEmbedding

                _bm25 = SparseTextEmbedding("Qdrant/bm25")
    return _bm25
