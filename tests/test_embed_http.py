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
