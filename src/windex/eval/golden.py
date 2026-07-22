"""The curated golden set: (query -> relevant doc ids) regression anchors.

Small and hand-maintained — its job is catching obvious regressions and pinning
known-answer queries, not broad coverage (the known-item proxy does breadth).
Grows over time; a deployment can extend it without a code change by pointing
WINDEX_EVAL_GOLDEN at a JSON file of the same shape."""

import json
import logging
import os

log = logging.getLogger("windex.eval")

# Each: {"query", "source", "relevant": [doc_id, ...], "note"?}
# doc ids follow the stable convention: news:<hash>, gh:owner/repo, arxiv:<id>, …
SEED: list[dict] = [
    {
        "query": "attention is all you need",
        "source": "arxiv",
        "relevant": ["arxiv:1706.03762"],
        "note": "COVERAGE REGRESSION ANCHOR — corpus ends 2017-12-28, so this "
                "scores 0 until the arxiv 2018+ backfill (Phase 5). Expected.",
    },
    {
        "query": "transformer architecture self-attention",
        "source": "arxiv",
        "relevant": ["arxiv:1706.03762"],
        "note": "Same coverage anchor from a longer query.",
    },
]


def load_golden() -> list[dict]:
    """SEED plus any entries in the JSON file at WINDEX_EVAL_GOLDEN (if set)."""
    golden = list(SEED)
    path = os.environ.get("WINDEX_EVAL_GOLDEN", "")
    if path and os.path.exists(path):
        try:
            with open(path) as f:
                extra = json.load(f)
            if isinstance(extra, list):
                golden.extend(e for e in extra if "query" in e and "relevant" in e)
        except Exception as e:  # noqa: BLE001 — a bad golden file must not break eval
            log.warning("ignoring unreadable golden file %s: %r", path, e)
    return golden
