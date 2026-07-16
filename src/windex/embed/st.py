from collections.abc import Sequence

from windex.embed.base import Embedder


class SentenceTransformersEmbedder(Embedder):
    """In-process fallback; requires the `st` extra. Prefer the HTTP backends —
    an external server keeps GPU memory out of pipeline workers."""

    def __init__(self, model_id: str, dim: int = 0, device: str | None = None):
        from sentence_transformers import SentenceTransformer

        self.model_id = model_id
        self._model = SentenceTransformer(model_id, device=device)
        self.dim = dim or self._model.get_sentence_embedding_dimension()

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        return self._model.encode(list(texts), normalize_embeddings=True).tolist()
