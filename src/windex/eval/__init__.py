"""Search-quality evaluation: the yardstick for ranking changes.

windex tracks search *performance* (latency, degraded-fallback) but had no
*quality* signal — relevance changes were eyeballed. This package measures
NDCG@k / MRR / Recall@k / Precision@k over three complementary sources (no click
data exists): a curated golden set, an LLM-as-judge, and a label-free known-item
(title-as-query) recall proxy. `windex eval` runs them, persists a row to
`search_quality`, and the scheduler runs it on a cadence so quality is operated,
not ad hoc.
"""

from windex.eval.harness import run_eval

__all__ = ["run_eval"]
