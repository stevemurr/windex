"""The curated golden set: (query -> relevant doc ids) regression anchors.

Hand/agent-curated and VERIFIED against the live index — each anchor retrieves
its doc at rank <=3 under production hybrid search (the two arxiv coverage anchors
are the deliberate exception: they score 0 until the corpus gap they pin is
filled). Its job is catching regressions and pinning known-answer queries; the
known-item title proxy does breadth.

The anchors live in golden_seed.json (DATA, not code) so the set grows without a
code change; a deployment can add more via WINDEX_EVAL_GOLDEN pointing at a JSON
file of the same shape. Each entry: {"query", "source", "relevant": [doc_id, ...],
"note"?}; doc ids follow the stable convention news:<hash>, gh:owner/repo,
arxiv:<id>, wiki:<pageid>, docs:<set>/<page>, hn:<id>, smallweb:<...>.
"""

import json
import logging
import os
from pathlib import Path

log = logging.getLogger("windex.eval")

_SEED_PATH = Path(__file__).with_name("golden_seed.json")


def _bundled() -> list[dict]:
    """The verified anchors shipped in golden_seed.json (next to this module)."""
    try:
        data = json.loads(_SEED_PATH.read_text())
        return [e for e in data if e.get("query") and e.get("relevant")]
    except Exception as e:  # noqa: BLE001 — a missing/bad seed must not break eval
        log.warning("could not load golden_seed.json: %r", e)
        return []


def load_golden() -> list[dict]:
    """The bundled seed plus any entries in the JSON file at WINDEX_EVAL_GOLDEN."""
    golden = _bundled()
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
