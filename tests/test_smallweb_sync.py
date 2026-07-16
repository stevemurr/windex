"""Small Web list sync: URL-per-line parsing and feeds-table reconciliation
(idempotency + removed/reactivated handling). Uses the pg fixture and a fake
list client so no network is touched."""

from windex.smallweb import sync as swsync


class _FakeResp:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass


class _FakeListClient:
    """Serves a canned smallweb.txt body."""

    def __init__(self, urls):
        self._text = "\n".join(urls)

    def get(self, url):
        return _FakeResp(self._text)

    def close(self):
        pass


def test_parse_list_skips_comments_blanks_and_dupes():
    body = (
        "# Kagi small web list\n"
        "https://a.example/feed.xml\n"
        "\n"
        "   https://b.example/rss   \n"    # whitespace trimmed
        "https://a.example/feed.xml\n"      # duplicate collapsed
        "not-a-url\n"                        # not http(s) — ignored
        "ftp://c.example/feed\n"            # wrong scheme — ignored
        "https://d.example/atom\n"
    )
    assert swsync.parse_list(body) == [
        "https://a.example/feed.xml",
        "https://b.example/rss",
        "https://d.example/atom",
    ]


def test_host_of_lowercases_netloc():
    assert swsync.host_of("https://Blog.Example.COM/feed.xml") == "blog.example.com"


def _feeds(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT url, host, status FROM feeds ORDER BY url")
        return {r[0]: (r[1], r[2]) for r in cur.fetchall()}


def test_sync_is_idempotent_and_handles_removed_and_reactivated(pg):
    urls = ["https://a.example/feed", "https://b.example/feed", "https://c.example/feed"]

    # first sync: three new feeds
    stats = swsync.sync(pg, client=_FakeListClient(urls))
    assert stats == {"total": 3, "added": 3, "reactivated": 0, "removed": 0}
    feeds = _feeds(pg)
    assert set(feeds) == set(urls)
    assert all(s == "active" for _, s in feeds.values())
    assert feeds["https://a.example/feed"][0] == "a.example"  # host populated

    # re-sync same list: no-op
    assert swsync.sync(pg, client=_FakeListClient(urls)) == {
        "total": 3, "added": 0, "reactivated": 0, "removed": 0,
    }

    # c drops off the list → marked removed (row survives)
    stats = swsync.sync(pg, client=_FakeListClient(urls[:2]))
    assert stats == {"total": 2, "added": 0, "reactivated": 0, "removed": 1}
    assert _feeds(pg)["https://c.example/feed"][1] == "removed"

    # c reappears (plus a brand-new d): c reactivated, d added
    plus = urls + ["https://d.example/feed"]
    stats = swsync.sync(pg, client=_FakeListClient(plus))
    assert stats == {"total": 4, "added": 1, "reactivated": 1, "removed": 0}
    feeds = _feeds(pg)
    assert feeds["https://c.example/feed"][1] == "active"  # reactivated
    assert feeds["https://d.example/feed"][1] == "active"  # new


def test_sync_preserves_watermark_across_removal(pg):
    urls = ["https://a.example/feed"]
    swsync.sync(pg, client=_FakeListClient(urls))
    with pg.cursor() as cur:
        cur.execute(
            "UPDATE feeds SET etag = '\"v9\"', items_seen = 42, last_polled = now() "
            "WHERE url = 'https://a.example/feed'"
        )
    pg.commit()
    # remove then re-add: etag/items_seen must survive (a reappearance is cheap)
    swsync.sync(pg, client=_FakeListClient([]))
    swsync.sync(pg, client=_FakeListClient(urls))
    with pg.cursor() as cur:
        cur.execute(
            "SELECT status, etag, items_seen FROM feeds WHERE url = 'https://a.example/feed'"
        )
        assert cur.fetchone() == ("active", '"v9"', 42)
