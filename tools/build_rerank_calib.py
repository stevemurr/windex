#!/usr/bin/env python
"""Build a reranker calibration + guardrail-eval set from windex's OWN corpus.

Why this exists
---------------
The NVFP4 reranker scores a spurious ~1.0 on messy real docs (CSS `@media`
fragments, boilerplate, off-topic "Madame Tussauds" pages). Confirmed root
cause: the NVFP4 checkpoint is W4A4 (4-bit weights AND activations) and was
BOTH calibrated and evaluated on MS MARCO — so the bakeoff passed (-1.1% MRR)
while never seeing windex's real corpus. windex docs (HN/wiki/arxiv/CSS-docs/
smallweb) are out-of-distribution for the 4-bit activation scales, which clip
and saturate the (bf16, already-spared) score head. The fix — see
`docs/reranker-requant.md` — is W4A4 -> W4A16 plus a calibration set that
MATCHES what windex actually serves. This produces that set, and an eval set
that finally includes the failing docs.

Serving-match is the whole point. windex feeds the reranker exactly

    f"{title}\n{snippet}".strip()          # index/search.py:344

with `snippet = text[:400]`               # per-source embed_index.py

so we reconstruct that exact string, at that exact (short) length — NOT the
full document and NOT 2-8k tokens.

Outputs are RAW (query, doc) text — the reranker template lives ONLY in the
build scripts (quantize_reranker.format_pair for calibration, rerank_capture
for eval), so there is one source of truth for it, not two.

What it emits (drop-in for ~/models/qwen3-reranker-nvfp4-build/)
---------------------------------------------------------------
  calibration.json      [{query, doc, label}] — feed to a patched
                        build_calibration_set() that maps each through
                        format_pair() (see the runbook). Balanced relevant/
                        irrelevant, weighted toward the messy sources.

  eval_set.windex.json  [{query, docs:[str], labels:[0/1], is_junk:[bool]}] —
                        the build's eval_set.json shape PLUS is_junk.
                        rerank_capture.py / rerank_compare.py consume it
                        unmodified (they ignore is_junk); rerank_guardrail.py
                        reads is_junk for "max score on any known-junk doc" —
                        the metric the MS MARCO bakeoff lacked.

  calibration.debug.jsonl   the grouped pairs, for eyeballing what got sampled.

Run it with the model stack UP (needs the embedder for retrieval AND the LLM for
query generation) and the drain paused (daytime default):

    uv run python tools/build_rerank_calib.py --out ./rerank_calib

Then STOP the model stack before re-quantizing (see the runbook) — loading the
bf16 reranker + the calibration forward passes needs the RAM the served models
hold, or the box OOMs.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
from pathlib import Path

import httpx
from qdrant_client import QdrantClient

from windex import db
from windex.config import Settings
from windex.embed.pipeline import point_id
from windex.index import qdrant as qidx
from windex.index.search import search as index_search

log = logging.getLogger("rerank_calib")

# documents.source uses "github"; its Qdrant collection/alias uses "repos".
_ALIAS_SRC = {"github": "repos"}

# Over-weight the messy sources: HTML/CSS boilerplate, raw markdown, nav chrome
# and off-topic pages cluster here, and those are what the reranker saturates on.
SOURCE_WEIGHTS = {
    "docs": 3, "github": 3, "smallweb": 3, "hf": 2,
    "news": 2, "wiki": 2, "hn": 1, "arxiv": 1,
}

_QUERYGEN = (
    "You are generating a realistic web search query. Given a search result "
    "(its title and snippet), write the short query (3-10 words, keyword-style, "
    "no quotes) a user would type to find it. Output ONLY the query.\n\n"
    "TITLE: {title}\nSNIPPET: {snippet}\n\nQuery:"
)


def alias_for(source: str) -> str:
    return qidx.alias_name(_ALIAS_SRC.get(source, source))


def served_doc_text(title: str, snippet: str) -> str:
    """Reconstruct EXACTLY what search.py hands the reranker."""
    return f"{title or ''}\n{snippet or ''}".strip()


# Junk heuristic: markup/boilerplate with little natural-language content — the
# docs that must score LOW. Cheap on purpose; a few false flags don't hurt.
_CSS_HINTS = re.compile(r"@media|@font-face|[{};]\s*$|:\s*\d+px|<\/?[a-z][^>]*>", re.I | re.M)


def is_junk(text: str) -> bool:
    t = (text or "").strip()
    if len(t) < 20:
        return True
    if len(_CSS_HINTS.findall(t)) >= 3:
        return True
    words = re.findall(r"[A-Za-z][A-Za-z'-]+", t)
    alpha_ratio = sum(len(w) for w in words) / max(len(t), 1)
    return alpha_ratio < 0.45


def sample_docs(settings: Settings, total: int) -> list[dict]:
    """Stratified (id, title, source) sample of embedded docs, weighted toward
    the messy sources. TABLESAMPLE keeps this off a 14M-row full scan."""
    wsum = sum(SOURCE_WEIGHTS.values())
    out: list[dict] = []
    with db.pooled(settings.pg_dsn) as conn:
        for source, w in SOURCE_WEIGHTS.items():
            n = max(1, round(total * w / wsum))
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT id, title FROM documents TABLESAMPLE SYSTEM (2)
                       WHERE source = %s AND status = 'embedded'
                             AND title IS NOT NULL AND title <> '' LIMIT %s""",
                    (source, n),
                )
                rows = cur.fetchall()
                if len(rows) < n:  # rare/small source: TABLESAMPLE missed its blocks
                    cur.execute(
                        """SELECT id, title FROM documents
                           WHERE source = %s AND status = 'embedded'
                                 AND title IS NOT NULL AND title <> '' LIMIT %s""",
                        (source, n),
                    )
                    rows = cur.fetchall()
            out.extend({"doc_id": r[0], "title": r[1], "source": source} for r in rows)
    random.shuffle(out)
    return out


def fetch_payloads(settings: Settings, docs: list[dict]) -> dict[str, dict]:
    """doc_id -> {title, snippet} from Qdrant by deterministic point id, batched
    one retrieve per collection."""
    qc = QdrantClient(url=settings.qdrant_url, timeout=30)
    by_source: dict[str, list[dict]] = {}
    for d in docs:
        by_source.setdefault(d["source"], []).append(d)
    payloads: dict[str, dict] = {}
    for source, group in by_source.items():
        ids = [point_id(d["doc_id"]) for d in group]
        id_to_doc = {point_id(d["doc_id"]): d["doc_id"] for d in group}
        try:
            recs = qc.retrieve(alias_for(source), ids=ids, with_payload=True)
        except Exception as e:  # noqa: BLE001 — a missing collection just skips its docs
            log.warning("retrieve failed for %s: %r", source, e)
            continue
        for rec in recs:
            p = rec.payload or {}
            payloads[id_to_doc[str(rec.id)]] = {
                "title": p.get("title") or "", "snippet": p.get("snippet") or ""}
    return payloads


def gen_query(client: httpx.Client, settings: Settings, title: str, snippet: str) -> str | None:
    body = {
        "model": settings.judge_model,
        "messages": [{"role": "user", "content": _QUERYGEN.format(
            title=title[:200], snippet=snippet[:400])}],
        "max_tokens": 32, "temperature": 0,
        "chat_template_kwargs": {"enable_thinking": False},
    }
    try:
        resp = client.post(f"{settings.judge_endpoint.rstrip('/')}/v1/chat/completions", json=body)
        resp.raise_for_status()
        q = resp.json()["choices"][0]["message"]["content"].strip().strip('"').strip()
        return q or None
    except Exception as e:  # noqa: BLE001 — best-effort per doc
        log.warning("query-gen failed: %r", e)
        return None


def mine_hard_negatives(settings: Settings, query: str, positive_id: str,
                        k: int) -> list[dict]:
    """Near-miss docs windex's own retrieval surfaces for `query`, minus the
    positive — what the reranker must rank BELOW the positive."""
    try:
        resp = index_search(settings, query, source="all", limit=k + 3, mode="hybrid")
    except Exception as e:  # noqa: BLE001
        log.warning("retrieval failed for %r: %r", query, e)
        return []
    negs = []
    for r in resp.get("results", []):
        if r.get("doc_id") == positive_id:
            continue
        text = served_doc_text(r.get("title", ""), r.get("snippet", ""))
        if not text:
            continue
        negs.append({"document": text, "is_junk": is_junk(text)})
        if len(negs) >= k:
            break
    return negs


def build(settings: Settings, out_dir: Path, n_docs: int, hard_negs: int,
          calib_size: int, n_eval: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    docs = sample_docs(settings, n_docs)
    log.info("sampled %d docs across %d sources", len(docs), len(SOURCE_WEIGHTS))
    payloads = fetch_payloads(settings, docs)
    log.info("fetched %d payloads", len(payloads))

    key = getattr(settings, "judge_api_key", "")
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    groups: list[dict] = []    # {query, docs:[str], labels:[int], is_junk:[bool], source}
    junk_pool: list[str] = []  # detected-junk docs, reused as guardrail negatives

    with httpx.Client(timeout=getattr(settings, "judge_timeout", 30.0), headers=headers) as llm:
        for d in docs:
            pay = payloads.get(d["doc_id"])
            if not pay:
                continue
            pos_text = served_doc_text(pay["title"], pay["snippet"])
            if not pos_text:
                continue
            if is_junk(pos_text):
                junk_pool.append(pos_text)
            query = gen_query(llm, settings, pay["title"], pay["snippet"])
            if not query:
                continue
            negs = mine_hard_negatives(settings, query, d["doc_id"], hard_negs)
            docs_list = [pos_text] + [n["document"] for n in negs]
            labels = [1] + [0] * len(negs)
            junk_flags = [is_junk(pos_text)] + [n["is_junk"] for n in negs]
            groups.append({"query": query, "docs": docs_list, "labels": labels,
                           "is_junk": junk_flags, "source": d["source"]})

    if not groups:
        raise SystemExit("no groups built — is the model stack up (embedder + LLM)?")
    random.shuffle(groups)
    eval_groups = groups[:n_eval]
    calib_groups = groups[n_eval:] or groups  # tiny corpora: reuse for calibration

    # Guardrail: seed each eval group with a junk doc drawn from ELSEWHERE (an
    # unrelated doc), label 0. A calibrated reranker scores these near 0; a
    # broken one pins them ~1.0. This is the signal the MS MARCO eval lacked.
    random.shuffle(junk_pool)
    ji = 0
    for g in eval_groups:
        if ji >= len(junk_pool):
            break
        g["docs"].append(junk_pool[ji])
        g["labels"].append(0)
        g["is_junk"].append(True)
        ji += 1

    # calibration.json: balanced relevant/irrelevant RAW pairs (mirrors the
    # build's 50/50 same-row/random-row scheme, on windex text). One positive +
    # one hard negative per group; top up from leftover junk.
    calib_pairs: list[dict] = []
    for g in calib_groups:
        calib_pairs.append({"query": g["query"], "doc": g["docs"][0], "label": 1})
        for text, lab in zip(g["docs"][1:], g["labels"][1:]):
            if lab == 0:
                calib_pairs.append({"query": g["query"], "doc": text, "label": 0})
                break
    for junk in junk_pool[ji:]:
        if len(calib_pairs) >= calib_size:
            break
        calib_pairs.append({"query": random.choice(calib_groups)["query"],
                            "doc": junk, "label": 0})
    random.shuffle(calib_pairs)
    calib_pairs = calib_pairs[:calib_size]

    (out_dir / "calibration.json").write_text(
        json.dumps(calib_pairs, ensure_ascii=False, indent=1))
    (out_dir / "eval_set.windex.json").write_text(
        json.dumps([{k: g[k] for k in ("query", "docs", "labels", "is_junk")}
                    for g in eval_groups], ensure_ascii=False, indent=1))
    with (out_dir / "calibration.debug.jsonl").open("w") as f:
        for g in calib_groups:
            f.write(json.dumps(g, ensure_ascii=False) + "\n")

    n_junk_ev = sum(sum(1 for j in g["is_junk"] if j) for g in eval_groups)
    log.info("wrote %d calibration pairs + %d eval groups (%d junk docs in eval)",
             len(calib_pairs), len(eval_groups), n_junk_ev)
    if len(calib_pairs) < 256:
        log.warning("only %d calibration pairs — the build uses num_calibration_"
                    "samples=256; raise --n-docs", len(calib_pairs))
    if n_junk_ev < 20:
        log.warning("only %d junk docs in eval — raise --n-docs or SOURCE_WEIGHTS "
                    "for a stronger guardrail", n_junk_ev)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=Path("./rerank_calib"))
    ap.add_argument("--n-docs", type=int, default=400,
                    help="positive docs to sample (each yields 1 pos + N hard negs)")
    ap.add_argument("--hard-negs", type=int, default=4, help="hard negatives per positive")
    ap.add_argument("--calib-size", type=int, default=512,
                    help="pairs in calibration.json (the build picks 256 of them)")
    ap.add_argument("--n-eval", type=int, default=120,
                    help="query groups in eval_set.windex.json (the build's is 100)")
    args = ap.parse_args()

    settings = Settings()
    if not getattr(settings, "judge_endpoint", "") or not getattr(settings, "judge_model", ""):
        raise SystemExit("WINDEX_JUDGE_ENDPOINT + WINDEX_JUDGE_MODEL must be set "
                         "(query generation uses the local LLM via the gateway).")
    build(settings, args.out, args.n_docs, args.hard_negs, args.calib_size, args.n_eval)


if __name__ == "__main__":
    main()
