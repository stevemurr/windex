import time
from collections.abc import Sequence
from typing import Literal

import httpx

from windex.embed.base import Embedder, EmbedRejected

Style = Literal["tei", "openai"]

# HTTP codes that mean *this document* is unacceptable (over-long / malformed) and
# retrying the identical payload cannot succeed — the only codes a caller should
# isolate the input on. Deliberately narrow: 401/403 (bad/rotated key), 404/405
# (wrong route), 409 etc. are problems with the *request*, not the document, and
# must stay retryable — treating them as rejections let one auth blip bisect a
# whole batch and mass-mark good documents 'failed' (silent, unrecoverable loss).
_REJECT_CODES = frozenset({400, 413, 422})


class HttpEmbedder(Embedder):
    """Client for a self-hosted embedding server.

    style="tei":    POST {endpoint}/embed          {"inputs": [...]}
    style="openai": POST {endpoint}/v1/embeddings  {"model": ..., "input": [...]}
    Covers TEI, infinity, vLLM, llama.cpp and most other self-hosted servers.
    """

    def __init__(
        self,
        endpoint: str,
        model_id: str,
        dim: int,
        style: Style = "tei",
        api_key: str = "",
        # Bulk default. MUST stay BELOW the gateway's own request_timeout
        # (litellm: 180s) so the client gives up first with a clean, retryable
        # httpx.TimeoutException. When the gateway deadline fires first it
        # cancels the upstream task and drops the socket WITHOUT a response, so
        # the loop sees a bare "Server disconnected" instead of a timeout —
        # that mismatch (client 120s vs gateway 45s) was the disconnect source
        # measured 2026-07-23. Queue wait, not payload size, drives latency
        # here: under backlog load a 2-token embed measured 13.2s and a full
        # 16K-token batch 16.6s.
        timeout: float = 150.0,
        retries: int = 3,
        transport: httpx.BaseTransport | None = None,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.model_id = model_id
        self.dim = dim
        self.style = style
        self.retries = retries
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = httpx.Client(timeout=timeout, headers=headers, transport=transport)

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        last_exc: Exception | None = None
        for attempt in range(self.retries):
            try:
                return self._request(list(texts))
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                # Only a document-validation 4xx (400/413/422) is unretryable and
                # isolatable — retrying the identical payload can only fail again
                # and wedge the loop, so surface it as EmbedRejected. Every other
                # 4xx (auth/routing/429) is a request-level problem: retry it.
                if code in _REJECT_CODES:
                    detail = exc.response.text[:200].replace("\n", " ")
                    raise EmbedRejected(code, detail) from exc
                last_exc = exc
                self._backoff(attempt)
            except (httpx.HTTPError, KeyError, ValueError) as exc:
                last_exc = exc
                self._backoff(attempt)
        # The cause goes INTO the message: consumers log str(exc) only, and
        # `from last_exc` alone left the real error invisible — during the
        # 2026-07-19 gateway flap every loop logged "failed after 3 attempts"
        # with no way to tell no-route from 429 from a validation error.
        raise RuntimeError(
            f"embedding request failed after {self.retries} attempts: {last_exc!r}"
        ) from last_exc

    def _backoff(self, attempt: int) -> None:
        # Only sleep when another attempt actually follows — the old code slept
        # after the final attempt too, adding dead time (≥1s at retries=1, the
        # live-query config) before raising even though no retry would happen.
        if attempt < self.retries - 1:
            time.sleep(2**attempt)

    def close(self) -> None:
        self._client.close()

    def _request(self, texts: list[str]) -> list[list[float]]:
        if self.style == "tei":
            resp = self._client.post(f"{self.endpoint}/embed", json={"inputs": texts})
            resp.raise_for_status()
            return resp.json()
        resp = self._client.post(
            f"{self.endpoint}/v1/embeddings",
            json={"model": self.model_id, "input": texts, "encoding_format": "float"},
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        return [item["embedding"] for item in sorted(data, key=lambda d: d["index"])]
