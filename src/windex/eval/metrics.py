"""Pure ranking metrics — no I/O, no windex imports, so they unit-test trivially.

A judgment is a ranked list of doc ids plus a relevance lookup: either a set of
relevant ids (binary) or a {doc_id: grade} map (graded, e.g. an LLM judge's
0-3). All functions take the *ranked* ids as returned by search and are safe on
empty input (return 0.0)."""

import math
from collections.abc import Iterable, Mapping


def _grade(doc_id: str, rel: Iterable[str] | Mapping[str, float]) -> float:
    """Relevance of one doc: 1.0 if in a relevant set, else its graded value."""
    if isinstance(rel, Mapping):
        return float(rel.get(doc_id, 0.0))
    return 1.0 if doc_id in rel else 0.0


def dcg(gains: list[float]) -> float:
    """Discounted cumulative gain: sum g_i / log2(i+2), i 0-based."""
    return sum(g / math.log2(i + 2) for i, g in enumerate(gains))


def ndcg_at_k(ranked_ids: list[str], rel: Iterable[str] | Mapping[str, float], k: int) -> float:
    """nDCG@k. Ideal DCG is the same grades sorted descending; 0 if no relevance."""
    rel = dict(rel) if isinstance(rel, Mapping) else set(rel)
    gains = [_grade(d, rel) for d in ranked_ids[:k]]
    ideal_grades = sorted(
        (rel.values() if isinstance(rel, dict) else [1.0] * len(rel)), reverse=True
    )[:k]
    idcg = dcg([float(g) for g in ideal_grades])
    return dcg(gains) / idcg if idcg > 0 else 0.0


def reciprocal_rank(ranked_ids: list[str], relevant: Iterable[str] | Mapping[str, float]) -> float:
    """1/rank of the first relevant hit (rank 1-based), else 0."""
    rel = relevant if isinstance(relevant, (set, dict)) else set(relevant)
    for i, d in enumerate(ranked_ids):
        if _grade(d, rel) > 0:
            return 1.0 / (i + 1)
    return 0.0


def recall_at_k(ranked_ids: list[str], relevant: Iterable[str], k: int) -> float:
    """|relevant ∩ top-k| / |relevant|. `relevant` is the set of ids that should
    be found (for graded input, pass the keys with grade > 0)."""
    rel = set(relevant)
    if not rel:
        return 0.0
    return len(rel & set(ranked_ids[:k])) / len(rel)


def precision_at_k(ranked_ids: list[str], relevant: Iterable[str], k: int) -> float:
    """|relevant ∩ top-k| / k."""
    if k <= 0:
        return 0.0
    rel = set(relevant)
    return len(rel & set(ranked_ids[:k])) / k


def hit_at_k(ranked_ids: list[str], relevant: Iterable[str], k: int) -> float:
    """1.0 if any relevant id is in the top-k, else 0.0 (a.k.a. success@k)."""
    return 1.0 if set(relevant) & set(ranked_ids[:k]) else 0.0


def mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0
