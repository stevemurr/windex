from datatrove.data import Document

from windex.ccnews.pipeline import NewsExtractor

ARTICLE_HTML = """
<html><head><title>City approves transit plan</title>
<meta property="article:published_time" content="2026-07-13T08:00:00Z"></head>
<body><article><h1>City approves transit plan</h1>
<p>{}</p></article></body></html>
""".format(
    "The city council voted on Tuesday to approve the new transit plan, which "
    "includes dedicated bus lanes and expanded night service across the city. " * 8
)


def _doc(html: str, url: str = "https://example.com/story") -> Document:
    return Document(text=html, id="rec-1", metadata={"url": url})


def test_extracts_text_and_uniform_metadata():
    docs = list(NewsExtractor().run([_doc(ARTICLE_HTML)]))
    assert len(docs) == 1
    doc = docs[0]
    assert "transit plan" in doc.text and "<p>" not in doc.text
    # every key always present — parquet schema stability contract
    for key in ("title", "date", "author", "sitename"):
        assert key in doc.metadata
    assert doc.metadata["title"].startswith("City approves")


def test_drops_docs_with_no_extractable_text():
    junk = "<html><body><nav>a b c</nav></body></html>"
    assert list(NewsExtractor().run([_doc(junk)])) == []


def test_survives_pathological_input():
    assert list(NewsExtractor().run([_doc("\x00\x01 not html at all")])) == []
