"""Shared BM25 sparse encoder.

fastembed loads the model from disk on construction, so building one per
embed_pending() pass (which every source used to do) reloads it once per batch
across 7 concurrent embed processes. It's stateless after load — one lazy
process-wide instance is all any of them need.
"""

_bm25 = None


def bm25_model():
    global _bm25
    if _bm25 is None:
        from fastembed import SparseTextEmbedding

        _bm25 = SparseTextEmbedding("Qdrant/bm25")
    return _bm25
