from windex.ccnews.dedup import canonical_url, doc_id, text_hash
from windex.ccnews.minhash import band_hashes, signature
from windex.github.clean import clean_readme, compose_doc

BASE = (
    "The city council voted on Tuesday to approve the new transit plan, "
    "which includes dedicated bus lanes, expanded night service, and a pilot "
    "program for fare-free rides on weekends starting this autumn season. "
) * 5


def test_canonical_url_normalizes():
    assert (
        canonical_url("HTTPS://Example.com/News/Story/?utm_source=x&fbclid=1&id=7#frag")
        == "https://example.com/News/Story?id=7"
    )
    assert canonical_url("https://example.com/a/") == canonical_url("https://example.com/a")


def test_exact_ids_stable():
    assert doc_id("https://example.com/a") == doc_id("https://example.com/a")
    assert text_hash("Hello  World") == text_hash("hello world")


def test_minhash_near_dup_collides():
    edited = BASE.replace("Tuesday", "Wednesday", 1).replace("autumn", "fall", 1)
    sig_a, sig_b = signature(BASE), signature(edited)
    shared = set(enumerate(band_hashes(sig_a))) & set(enumerate(band_hashes(sig_b)))
    assert shared, "near-identical docs must collide in at least one band"


def test_minhash_distinct_docs_do_not_collide():
    other = (
        "Quarterly earnings at the semiconductor firm beat analyst expectations, "
        "driven by strong datacenter demand and improving margins in the mobile "
        "division, while guidance for the next quarter remained conservative. "
    ) * 5
    shared = set(enumerate(band_hashes(signature(BASE)))) & set(
        enumerate(band_hashes(signature(other)))
    )
    assert not shared


def test_clean_readme_strips_noise():
    md = (
        "# MyProject\n\n"
        "![build](https://img.shields.io/badge/build-passing-green)\n\n"
        "A tool for [parsing](https://example.com/docs) things.\n\n"
        "```python\nprint('hidden')\n```\n\n"
        "<div>html junk</div>\n"
    )
    text = clean_readme(md)
    assert "shields.io" not in text and "print(" not in text and "<div>" not in text
    assert "MyProject" in text and "parsing things" in text


def test_compose_doc_caps_length():
    doc = compose_doc("o/r", "desc", ["a", "b"], "x" * 10_000, max_chars=500)
    assert len(doc) <= 500 and doc.startswith("o / r")