import time
from collections.abc import Sequence
from typing import Literal

import httpx

from windex.embed.base import Embedder, EmbedRejected

Style = Literal["tei", "openai"]


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
        timeout: float = 120.0,
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
                # A 4xx (except 429 rate-limit) means the request itself is
                # unacceptable — an over-long or malformed document. Retrying the
                # identical payload can only fail again and wedge the loop, so
                # surface it as EmbedRejected for the caller to isolate the input.
                if 400 <= code < 500 and code != 429:
                    detail = exc.response.text[:200].replace("\n", " ")
                    raise EmbedRejected(code, detail) from exc
                last_exc = exc
                time.sleep(2**attempt)
            except (httpx.HTTPError, KeyError, ValueError) as exc:
                last_exc = exc
                time.sleep(2**attempt)
        # The cause goes INTO the message: consumers log str(exc) only, and
        # `from last_exc` alone left the real error invisible — during the
        # 2026-07-19 gateway flap every loop logged "failed after 3 attempts"
        # with no way to tell no-route from 429 from a validation error.
        raise RuntimeError(
            f"embedding request failed after {self.retries} attempts: {last_exc!r}"
        ) from last_exc

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
