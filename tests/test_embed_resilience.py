"""Embed-path resilience to a permanently-rejected document (the 2026-07-20 gh
wedge): a 4xx must be non-retryable and isolatable so one poison doc can't stall
the loop forever."""

import httpx
import pytest

from windex.embed.base import EmbedRejected, embed_isolating
from windex.embed.http import HttpEmbedder


class FakeEmb:
    """Rejects any batch containing a 'POISON' text; else returns 1-d vectors."""

    def __init__(self):
        self.calls = 0

    def embed_batch(self, texts):
        self.calls += 1
        if any("POISON" in t for t in texts):
            raise EmbedRejected(400, "context length exceeded")
        return [[float(len(t))] for t in texts]


def test_isolating_bisects_out_the_poison():
    emb = FakeEmb()
    vecs, ok = embed_isolating(emb, ["a", "b", "POISON", "c", "d"])
    assert ok == [True, True, False, True, True]
    assert vecs[2] is None
    assert vecs[0] == [1.0] and vecs[4] == [1.0]


def test_isolating_all_good_is_one_call():
    emb = FakeEmb()
    vecs, ok = embed_isolating(emb, ["x", "y", "z"])
    assert ok == [True, True, True] and emb.calls == 1  # no bisection needed


def test_isolating_single_poison_and_empty():
    assert embed_isolating(FakeEmb(), ["POISON"]) == ([None], [False])
    assert embed_isolating(FakeEmb(), []) == ([], [])


def test_isolating_does_not_swallow_retryable_errors():
    class Flaky:
        def embed_batch(self, texts):
            raise RuntimeError("no route to host")  # 5xx/network, already retried

    with pytest.raises(RuntimeError):
        embed_isolating(Flaky(), ["a", "b"])


def _embedder(status, retries=3):
    def handler(request):
        return httpx.Response(status, json={"error": "x", "data": []})

    return HttpEmbedder("http://x", "m", 3, style="openai", retries=retries,
                        transport=httpx.MockTransport(handler))


@pytest.mark.parametrize("status", [400, 413, 422])
def test_http_4xx_is_rejected_without_retry(status):
    emb = _embedder(status)
    with pytest.raises(EmbedRejected) as ei:
        emb.embed_batch(["t"])
    assert ei.value.status == status


@pytest.mark.parametrize("status", [429, 500, 503])
def test_http_retryable_status_is_not_rejected(status):
    # 429 (rate limit) and 5xx are transient: retried, then RuntimeError — never
    # EmbedRejected (which would make a caller drop a good doc on a blip).
    with pytest.raises(RuntimeError):
        _embedder(status).embed_batch(["t"])
