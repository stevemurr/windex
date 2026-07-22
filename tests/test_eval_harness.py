"""Harness aggregation tests — search + DB are mocked, so no Qdrant/PG needed."""

from windex.eval import golden as G
from windex.eval import harness


def test_golden_eval_scores_a_rank2_hit(monkeypatch):
    # search returns the relevant doc at rank 2
    monkeypatch.setattr(harness, "index_search",
                        lambda s, q, source, limit, mode: {
                            "results": [{"doc_id": "x"}, {"doc_id": "rel"}, {"doc_id": "y"}]})
    r = harness.golden_eval(None, [{"query": "q", "source": "arxiv", "relevant": ["rel"]}], 10, "hybrid")
    assert r["n"] == 1
    assert r["mrr"] == 0.5              # relevant at rank 2
    assert r["recall@10"] == 1.0
    assert r["precision@10"] == 1 / 10


def test_golden_eval_empty_is_empty():
    assert harness.golden_eval(None, [], 10, "hybrid") == {}


def test_run_eval_overall_blends_legs(monkeypatch):
    monkeypatch.setattr(harness, "known_item_eval", lambda s, p, k, m: {
        "news": {"n": 2, "ndcg@10": 1.0, "mrr": 1.0, "hit@10": 1.0},
        "docs": {"n": 2, "ndcg@10": 0.5, "mrr": 0.5, "hit@10": 0.5}})
    monkeypatch.setattr(harness, "golden_eval", lambda s, g, k, m: {
        "n": 1, "ndcg@10": 0.0, "mrr": 0.0, "recall@10": 0.0, "precision@10": 0.0})
    r = harness.run_eval(None, k=10, mode="hybrid", golden=[])
    assert r["overall"]["known_item_ndcg@10"] == 0.75   # mean(1.0, 0.5)
    assert r["overall"]["known_item_mrr"] == 0.75
    assert r["overall"]["golden_ndcg@10"] == 0.0
    assert r["judge"] == {}                              # llm_judge default off


def test_golden_seed_has_coverage_anchor():
    g = G.load_golden()
    assert any(e["relevant"] == ["arxiv:1706.03762"] for e in g)  # the regression anchor
