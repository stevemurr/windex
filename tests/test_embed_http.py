import httpx
import pytest

from windex.embed.http import HttpEmbedder


def _transport(handler):
    return httpx.MockTransport(handler)


def test_tei_style_posts_inputs():
    seen = {}

    def handler(request):
        import json

        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        n = len(json.loads(request.content)["inputs"])
        return httpx.Response(200, json=[[0.1, 0.2]] * n)

    e = HttpEmbedder("http://emb:8080", "m", dim=2, style="tei", api_key="sekret",
                     transport=_transport(handler))
    vecs = e.embed_batch(["a", "b"])
    assert vecs == [[0.1, 0.2], [0.1, 0.2]]
    assert seen["url"] == "http://emb:8080/embed"
    assert seen["auth"] == "Bearer sekret"
    assert e.ping()


def test_openai_style_orders_by_index_and_sets_encoding():
    def handler(request):
        import json

        body = json.loads(request.content)
        assert body["encoding_format"] == "float"  # litellm/vllm strictness
        assert body["model"] == "qwen3-embedding-8b"
        return httpx.Response(200, json={"data": [
            {"index": 1, "embedding": [1.0, 1.0]},
            {"index": 0, "embedding": [0.0, 0.0]},
        ]})

    e = HttpEmbedder("http://emb:4000", "qwen3-embedding-8b", dim=2, style="openai",
                     transport=_transport(handler))
    assert e.embed_batch(["first", "second"]) == [[0.0, 0.0], [1.0, 1.0]]


def test_retries_then_raises(monkeypatch):
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(500)

    import windex.embed.http as mod

    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    e = HttpEmbedder("http://emb:8080", "m", dim=2, style="tei", retries=3,
                     transport=_transport(handler))
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        e.embed_batch(["x"])
    assert calls["n"] == 3


def test_no_backoff_sleep_after_the_final_attempt(monkeypatch):
    # The retry loop must not sleep after the last attempt — there is no further
    # retry to space out. With retries=1 (the live-query config, on an 8s
    # deadline) an unconditional sleep added ≥1s of dead time to every failure.
    sleeps: list[float] = []

    import windex.embed.http as mod

    monkeypatch.setattr(mod.time, "sleep", lambda s: sleeps.append(s))
    e = HttpEmbedder("http://emb:8080", "m", dim=2, style="tei", retries=1,
                     transport=_transport(lambda r: httpx.Response(500)))
    with pytest.raises(RuntimeError):
        e.embed_batch(["x"])
    assert sleeps == []  # retries=1 → zero backoff sleeps

    # retries=3 sleeps only between attempts (after #1 and #2, not after #3)
    sleeps.clear()
    e3 = HttpEmbedder("http://emb:8080", "m", dim=2, style="tei", retries=3,
                      transport=_transport(lambda r: httpx.Response(503)))
    with pytest.raises(RuntimeError):
        e3.embed_batch(["x"])
    assert len(sleeps) == 2


def test_close_releases_the_http_client():
    e = HttpEmbedder("http://emb:8080", "m", dim=2, style="tei",
                     transport=_transport(lambda r: httpx.Response(200, json=[[0.1, 0.2]])))
    assert not e._client.is_closed
    e.close()
    assert e._client.is_closed


def test_context_manager_closes_on_exit():
    with HttpEmbedder("http://emb:8080", "m", dim=2, style="tei",
                      transport=_transport(lambda r: httpx.Response(200, json=[[0.1, 0.2]]))) as e:
        assert not e._client.is_closed
    assert e._client.is_closed
