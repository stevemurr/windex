import httpx
import pytest

from windex.ccnews import download


def test_download_one_writes_and_skips_existing(tmp_path):
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(200, content=b"WARC/1.0 data")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    path = "crawl-data/CC-NEWS/2026/07/CC-NEWS-x-00001.warc.gz"
    dest = download.download_one(client, path, tmp_path)
    assert dest.read_bytes() == b"WARC/1.0 data"
    assert not dest.with_suffix(dest.suffix + ".part").exists()
    download.download_one(client, path, tmp_path)  # cached — no second request
    assert calls["n"] == 1


def test_download_one_retries_then_succeeds(tmp_path, monkeypatch):
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, content=b"ok")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    dest = download.download_one(client, "a/b/c-00002.warc.gz", tmp_path)
    assert dest.read_bytes() == b"ok" and calls["n"] == 3


def test_download_one_gives_up_cleanly(tmp_path):
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    with pytest.raises(RuntimeError, match="download failed"):
        download.download_one(client, "a/b/c-00003.warc.gz", tmp_path)
    assert list(tmp_path.iterdir()) == []  # no partial files left behind
