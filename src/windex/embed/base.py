import abc
from collections.abc import Sequence


class Embedder(abc.ABC):
    """Text → dense vector. The model is user-supplied; everything downstream
    (collection naming, dim, batching) is driven by config, so swapping models
    never touches pipeline code."""

    model_id: str
    dim: int

    @abc.abstractmethod
    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]: ...

    def ping(self) -> bool:
        try:
            vecs = self.embed_batch(["ping"])
            return len(vecs) == 1 and len(vecs[0]) == self.dim
        except Exception:
            return False
