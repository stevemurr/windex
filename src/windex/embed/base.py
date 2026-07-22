import abc
from collections.abc import Sequence


class EmbedRejected(Exception):
    """The server permanently refused this input (an HTTP 4xx that is not a
    retryable 429 — e.g. 400/413/422 for an over-long or malformed document).
    Retrying the identical payload cannot succeed, so callers should isolate the
    offending document rather than loop on it forever (2026-07-20 gh wedge)."""

    def __init__(self, status: int, detail: str = ""):
        self.status = status
        self.detail = detail
        super().__init__(f"embedding input rejected (HTTP {status}): {detail}")


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

    def close(self) -> None:
        """Release any held resources (an HTTP connection pool). Default no-op so
        in-process backends need not implement it. Callers that build a one-off
        embedder (a query, one embed pass) must close it so its pool doesn't leak."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def embed_isolating(
    embedder: Embedder, texts: Sequence[str]
) -> tuple[list[list[float] | None], list[bool]]:
    """Embed `texts`, isolating any the server *permanently* rejects.

    Returns (vectors, ok): when ok[i] is True, vectors[i] is texts[i]'s
    embedding; when ok[i] is False the server rejected that input even on its
    own (EmbedRejected), so vectors[i] is None and the caller should skip/mark
    that document rather than retry it. Retryable failures (5xx, timeouts, no
    route) are NOT swallowed — embed_batch still raises them after its retries,
    so a transient outage still trips the loop's circuit breaker.

    A whole batch that rejects is bisected to find the offending input(s), so a
    single poison document never fails the good ones alongside it."""
    texts = list(texts)
    if not texts:
        return [], []
    try:
        return list(embedder.embed_batch(texts)), [True] * len(texts)
    except EmbedRejected:
        if len(texts) == 1:
            return [None], [False]
        mid = len(texts) // 2
        v1, ok1 = embed_isolating(embedder, texts[:mid])
        v2, ok2 = embed_isolating(embedder, texts[mid:])
        return v1 + v2, ok1 + ok2
