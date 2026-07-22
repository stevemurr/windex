"""LLM-as-judge relevance grading (config-gated).

Grades (query, result) pairs 0-3 with a self-hosted chat LLM — the Spark's
qwen LLM behind the same OpenAI-compatible gateway as the embedder. Enabled only
when WINDEX_JUDGE_ENDPOINT + WINDEX_JUDGE_MODEL are set (config.py); otherwise
judge_eval returns {} so `windex eval` still runs its other legs. Scales the
golden queries to a graded nDCG without hand-labeling every result.

Kept dependency-light (httpx, already a dep) and best-effort: a judge failure
degrades to an empty leg, never fails the eval (mirrors the embed breaker)."""

from __future__ import annotations

import json
import logging

import httpx

from windex.config import Settings
from windex.eval import metrics as M
from windex.index.search import search as index_search

log = logging.getLogger("windex.eval")

_PROMPT = (
    "You are grading search relevance. Given a QUERY and a RESULT (title + "
    "snippet), rate how well the result answers the query on a 0-3 scale: "
    "3=highly relevant, 2=relevant, 1=marginal, 0=irrelevant. "
    "Reply with ONLY the digit.\n\nQUERY: {q}\n\nRESULT: {title}\n{snippet}\n\nGrade:"
)


def _enabled(settings: Settings) -> bool:
    return bool(getattr(settings, "judge_endpoint", "")
               and getattr(settings, "judge_model", ""))


def _grade_one(client: httpx.Client, settings: Settings, q: str, r: dict) -> float:
    body = {
        "model": settings.judge_model,
        "messages": [{"role": "user", "content": _PROMPT.format(
            q=q, title=r.get("title", ""), snippet=(r.get("snippet") or "")[:500])}],
        "max_tokens": 2, "temperature": 0,
        # A reasoning judge model (e.g. qwen3.6) spends max_tokens in <think> and
        # returns content=None, so every grade would silently fail. Disable
        # thinking; non-reasoning models ignore the kwarg.
        "chat_template_kwargs": {"enable_thinking": False},
    }
    resp = client.post(f"{settings.judge_endpoint.rstrip('/')}/v1/chat/completions",
                       json=body)
    resp.raise_for_status()
    txt = (resp.json()["choices"][0]["message"].get("content") or "").strip()
    digit = next((c for c in txt if c in "0123"), None)
    return float(digit) if digit is not None else 0.0


def judge_eval(settings: Settings, golden: list[dict], k: int, mode: str) -> dict:
    """Graded nDCG@k over the golden queries using the LLM's relevance grades.
    Returns {} when the judge isn't configured or all calls fail."""
    if not _enabled(settings) or not golden:
        return {}
    key = getattr(settings, "judge_api_key", "")
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    ndcg, graded_queries = [], 0
    try:
        with httpx.Client(timeout=getattr(settings, "judge_timeout", 30.0),
                          headers=headers) as client:
            for g in golden:
                resp = index_search(settings, g["query"], source=g.get("source", "all"),
                                    limit=k, mode=mode)
                results = resp.get("results", [])
                if not results:
                    continue
                grades = {}
                for r in results:
                    try:
                        grades[r["doc_id"]] = _grade_one(client, settings, g["query"], r)
                    except Exception as e:  # noqa: BLE001 — best-effort per result
                        log.warning("judge grade failed: %r", e)
                ranked = [r["doc_id"] for r in results]
                ndcg.append(M.ndcg_at_k(ranked, grades, k))
                graded_queries += 1
    except Exception as e:  # noqa: BLE001 — judge is optional; never fail the eval
        log.warning("llm-judge leg failed, skipping: %r", e)
        return {}
    if not graded_queries:
        return {}
    return {"n": graded_queries, f"graded_ndcg@{k}": round(M.mean(ndcg), 4)}
