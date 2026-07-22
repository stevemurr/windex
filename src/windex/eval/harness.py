"""Run the eval legs and aggregate into one quality snapshot.

Three legs, any of which may be empty:
- known-item: sample embedded docs, query by title, check the doc ranks top-k
  (label-free; a doc that can't find *itself* signals an embed/index problem).
- golden: curated (query -> relevant doc ids) pairs (regression anchors).
- llm-judge: grade (query, result) pairs with a hosted LLM (config-gated).

Everything routes through `index.search` (the same path the API uses) so the
numbers reflect real search. Returns a dict of metrics broken out by source and
mode, ready to persist to `search_quality` and export as Prometheus gauges."""

from __future__ import annotations

import logging

from windex import db
from windex.config import Settings
from windex.eval import metrics as M
from windex.index.search import search as index_search

log = logging.getLogger("windex.eval")

# documents.source == the search `source` param for every source, so a sample
# row's source can be passed straight back to search().
SOURCES = ["news", "github", "wiki", "arxiv", "smallweb", "docs", "hn", "hf"]


def _ranked_ids(settings: Settings, q: str, source: str, k: int, mode: str) -> list[str]:
    resp = index_search(settings, q, source=source, limit=k, mode=mode)
    return [r.get("doc_id") for r in resp.get("results", []) if r.get("doc_id")]


def _sample_docs(conn, source: str, n: int) -> list[tuple[str, str]]:
    """(id, title) for up to n embedded docs of a source. TABLESAMPLE keeps this
    off a 14M-row full scan; small/rare sources fall back to a plain scan."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT id, title FROM documents TABLESAMPLE SYSTEM (1)
               WHERE source = %s AND status = 'embedded'
                     AND title IS NOT NULL AND title <> '' LIMIT %s""",
            (source, n),
        )
        rows = cur.fetchall()
        if len(rows) < n:  # rare source: TABLESAMPLE missed its blocks
            cur.execute(
                """SELECT id, title FROM documents
                   WHERE source = %s AND status = 'embedded'
                         AND title IS NOT NULL AND title <> '' LIMIT %s""",
                (source, n),
            )
            rows = cur.fetchall()
    return rows


def known_item_eval(settings: Settings, per_source: int, k: int, mode: str) -> dict:
    """Title-as-query recall proxy, per source. One relevant doc (itself)."""
    out = {}
    with db.pooled(settings.pg_dsn) as conn:
        for source in SOURCES:
            docs = _sample_docs(conn, source, per_source)
            rr, hit, ndcg = [], [], []
            for doc_id, title in docs:
                ranked = _ranked_ids(settings, title, source, k, mode)
                rr.append(M.reciprocal_rank(ranked, {doc_id}))
                hit.append(M.hit_at_k(ranked, {doc_id}, k))
                ndcg.append(M.ndcg_at_k(ranked, {doc_id}, k))
            if docs:
                out[source] = {"n": len(docs), "mrr": M.mean(rr),
                               f"hit@{k}": M.mean(hit), f"ndcg@{k}": M.mean(ndcg)}
    return out


def golden_eval(settings: Settings, golden: list[dict], k: int, mode: str) -> dict:
    """Curated (query -> relevant ids) pairs. Each entry: {query, source, relevant:[ids]}."""
    ndcg, mrr, recall, prec = [], [], [], []
    per_query = []
    for g in golden:
        relevant = set(g["relevant"])
        ranked = _ranked_ids(settings, g["query"], g.get("source", "all"), k, mode)
        row = {
            "query": g["query"],
            f"ndcg@{k}": M.ndcg_at_k(ranked, relevant, k),
            "mrr": M.reciprocal_rank(ranked, relevant),
            f"recall@{k}": M.recall_at_k(ranked, relevant, k),
            f"precision@{k}": M.precision_at_k(ranked, relevant, k),
        }
        per_query.append(row)
        ndcg.append(row[f"ndcg@{k}"]); mrr.append(row["mrr"])
        recall.append(row[f"recall@{k}"]); prec.append(row[f"precision@{k}"])
    if not golden:
        return {}
    return {"n": len(golden), f"ndcg@{k}": M.mean(ndcg), "mrr": M.mean(mrr),
            f"recall@{k}": M.mean(recall), f"precision@{k}": M.mean(prec),
            "per_query": per_query}


def run_eval(settings: Settings, per_source: int = 25, k: int = 10,
             mode: str = "hybrid", golden: list[dict] | None = None,
             llm_judge: bool = False) -> dict:
    """Run the enabled legs and return one snapshot: {mode, k, known_item, golden,
    judge, overall}. `overall` is the headline NDCG/MRR the gauges + dashboard use."""
    from windex.eval.golden import load_golden

    golden = load_golden() if golden is None else golden
    ki = known_item_eval(settings, per_source, k, mode)
    gold = golden_eval(settings, golden, k, mode)
    judge = {}
    if llm_judge:
        from windex.eval.judge import judge_eval  # config-gated; imported lazily
        judge = judge_eval(settings, golden, k, mode)

    # Headline = mean known-item nDCG across sources (the always-available signal),
    # blended with the golden nDCG when a golden set exists.
    ki_ndcg = M.mean([v[f"ndcg@{k}"] for v in ki.values()]) if ki else 0.0
    ki_mrr = M.mean([v["mrr"] for v in ki.values()]) if ki else 0.0
    overall = {
        f"known_item_ndcg@{k}": round(ki_ndcg, 4),
        "known_item_mrr": round(ki_mrr, 4),
        f"golden_ndcg@{k}": round(gold.get(f"ndcg@{k}", 0.0), 4) if gold else None,
        "golden_mrr": round(gold.get("mrr", 0.0), 4) if gold else None,
    }
    return {"mode": mode, "k": k, "known_item": ki, "golden": gold,
            "judge": judge, "overall": overall}


def persist_run(settings: Settings, result: dict, git_sha: str = "") -> None:
    """Write one eval run to `search_quality` (the collector reads the latest)."""
    import orjson

    k = result["k"]
    ov = result["overall"]
    judge_ndcg = (result.get("judge") or {}).get(f"graded_ndcg@{k}")
    with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO search_quality
               (mode, k, known_item_ndcg, known_item_mrr, golden_ndcg, golden_mrr,
                judge_ndcg, git_sha, detail)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (result["mode"], k, ov.get(f"known_item_ndcg@{k}"), ov.get("known_item_mrr"),
             ov.get(f"golden_ndcg@{k}"), ov.get("golden_mrr"), judge_ndcg, git_sha,
             orjson.dumps(result).decode()),
        )
        conn.commit()


def latest_quality(settings: Settings) -> dict | None:
    """Most recent eval row as a dict, or None. Resilient to a cold/missing DB
    so the Prometheus collector never 500s on it."""
    try:
        with db.pooled(settings.pg_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """SELECT ts, mode, k, known_item_ndcg, known_item_mrr,
                          golden_ndcg, golden_mrr, judge_ndcg
                   FROM search_quality ORDER BY ts DESC LIMIT 1""")
            row = cur.fetchone()
    except Exception:  # noqa: BLE001
        return None
    if not row:
        return None
    cols = ["ts", "mode", "k", "known_item_ndcg", "known_item_mrr",
            "golden_ndcg", "golden_mrr", "judge_ndcg"]
    return dict(zip(cols, row))
