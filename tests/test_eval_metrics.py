"""Pure ranking-metric tests for windex.eval.metrics."""

import math

from windex.eval import metrics as M


def test_reciprocal_rank():
    assert M.reciprocal_rank(["a", "b", "c"], {"b"}) == 0.5
    assert M.reciprocal_rank(["a", "b"], {"a"}) == 1.0
    assert M.reciprocal_rank(["a", "b"], {"z"}) == 0.0
    assert M.reciprocal_rank([], {"a"}) == 0.0


def test_recall_and_precision_at_k():
    ranked = ["a", "b", "c", "d"]
    rel = {"a", "c", "z"}  # z is not retrievable
    assert M.recall_at_k(ranked, rel, 4) == 2 / 3       # a,c found of 3 relevant
    assert M.precision_at_k(ranked, rel, 4) == 2 / 4
    assert M.recall_at_k(ranked, set(), 4) == 0.0        # no relevant → 0
    assert M.hit_at_k(ranked, {"c"}, 2) == 0.0           # c at rank 3, not in top-2
    assert M.hit_at_k(ranked, {"b"}, 2) == 1.0


def test_ndcg_binary_perfect_and_imperfect():
    # perfect ranking (relevant first) → 1.0
    assert M.ndcg_at_k(["a", "b", "c"], {"a"}, 3) == 1.0
    # single relevant at rank 2: DCG=1/log2(3), IDCG=1/log2(2)=1
    got = M.ndcg_at_k(["x", "a", "y"], {"a"}, 3)
    assert math.isclose(got, (1 / math.log2(3)) / 1.0)
    assert M.ndcg_at_k(["x", "y"], {"a"}, 2) == 0.0      # relevant not retrieved


def test_ndcg_graded():
    # graded relevance from an LLM judge: {doc: grade}
    grades = {"a": 3.0, "b": 1.0, "c": 0.0}
    # ranking a,b,c is ideal → 1.0
    assert math.isclose(M.ndcg_at_k(["a", "b", "c"], grades, 3), 1.0)
    # swapping to b,a,c is worse than ideal
    assert M.ndcg_at_k(["b", "a", "c"], grades, 3) < 1.0


def test_mean_and_empty():
    assert M.mean([1.0, 0.0]) == 0.5
    assert M.mean([]) == 0.0
    assert M.dcg([]) == 0.0
