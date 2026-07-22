# Reranker investigation — findings, disproven hypotheses, and where to look next

**Status (2026-07-21): rerank is OFF, by decision, backed by data.** windex search
runs on hybrid fusion (dense qwen3-embedding-8b + sparse BM25, RRF-fused) and is
relevant + fast (~0.5s hybrid). A cross-encoder reranker was trialed and does not
earn its slot on windex's corpus at any precision. This document records what was
tried, what we ruled out, and the open leads, so the same ground isn't re-covered.

Companion artifacts (all on `feat/search-overhaul`):
- `docs/reranker-requant.md` — the requant runbook (kept as a record; its premise
  is disproven, see below).
- `tools/build_rerank_calib.py` — corpus calibration/eval-set builder.
- `tools/rerank_guardrail.py` — the junk-score gate.
- `src/windex/embed/rerank.py` — `HttpReranker` (the client, still wired, gated off).
- Commits: `163fae9` (format fix), `ba74195` (tooling), `04dc937` (judge fix),
  `c457000` (runbook outcome).

---

## 1. Why a reranker at all

Hybrid fusion returns a good candidate set but its `score` is an RRF rank-reciprocal,
which is **not comparable across collections** — so `source=all` ties rank-1 across
the 8 sources, and short/ambiguous queries can land the right neighborhood in the
wrong order. A cross-encoder reranker scores true `(query, passage)` relevance, which
is both a better within-collection order and a cross-collection-comparable score. The
model on hand is `qwen3-reranker-4b` (Qwen3-Reranker-4B seq-cls), served NVFP4 on the
Spark gateway; windex's `HttpReranker` over-fetches `rerank_top_k` per collection,
reranks the fused pool, and replaces `score` with the relevance score, degrading to
the fused order on any failure.

windex feeds the reranker **`f"{title}\n{snippet}"`** with `snippet = text[:400]`
(`index/search.py`, per-source `embed_index.py`) — i.e. short text, not full docs.

## 2. The symptom that started it

With rerank ON, some **real docs scored a spurious ~1.0** and outranked the right
answer on the full ~400-candidate pool — e.g. `@media` CSS fragments, a "Madame
Tussauds" page, a docs "transform" page surfacing above on-topic results. rerank-ON
measured *worse* than rerank-OFF, so it was disabled pending a fix.

## 3. Hypotheses, in the order we chased them

### 3a. Caller-side format bug — REAL, fixed, not sufficient
Qwen3-Reranker-4B (seq-cls) expects the query wrapped as
`<Instruct>: {instruct}\n<Query>: {q}` (+ the system/`<Document>` template the
server applies); windex was sending the raw query. Fixed in `HttpReranker` (adds
`WINDEX_RERANK_QUERY_INSTRUCT`, commit `163fae9`). Necessary, but the spurious-1.0
behavior persisted after it.

### 3b. NVFP4 quantization (W4A4 activation saturation) — DISPROVEN
The leading hypothesis (mine, and the research agent's): the NVFP4 checkpoint is
**W4A4** (4-bit weights *and* activations), and its calibration used **256 MS MARCO
samples** — with the bakeoff eval *also* on MS MARCO. So the 4-bit **activation**
scales were fit to a distribution windex's corpus never matches; out-of-distribution
docs push activations past the calibrated range → the score head saturates to ~1.0.
(The score head itself was already spared — `ignore=["score"]` — so that was never
the culprit.)

Predicted fix: re-quantize **W4A16** (drop activation quant) + calibrate/evaluate on
windex's own corpus + add a junk-score guardrail the MS MARCO bakeoff lacked.

**This was wrong.** See §5.

## 4. What we built to test it

- **`tools/build_rerank_calib.py`** — samples embedded docs stratified across all 8
  sources (over-weighting the messy ones: docs/github/smallweb/hf), reconstructs the
  *exact* served text (`title\n snippet[:400]`), generates a realistic query per doc
  with the local LLM, and **mines hard negatives via windex's own hybrid retrieval**
  (the near-misses the reranker must rank below the positive). Emits a balanced
  `calibration.json` (`{query,doc,label}`, drop-in for the build's `format_pair`) and
  an `eval_set.windex.json` (the build's `{query,docs,labels}` shape + an `is_junk`
  flag). Serving-match is deliberate — calibrating/evaluating at the real (short)
  input length, not the "2–8k tokens" generic advice.
- **`tools/rerank_guardrail.py`** — the metric the MS MARCO bakeoff lacked: **max
  score on any known-junk doc**, plus positive/junk separation. Consumes the existing
  `rerank_capture.py` output unmodified.
- **`eval/judge.py` fix** (commit `04dc937`) — discovered mid-run: qwen3.6 is a
  **reasoning** model, so a small `max_tokens` is spent in `<think>` and `content`
  comes back `None`. This silently broke query-gen *and* had been silently making the
  LLM-judge eval leg grade everything 0. Fix: `chat_template_kwargs={"enable_thinking":
  False}` + tolerate `None`. **Keep this regardless of the reranker outcome.**

The set built cleanly: 470 calibration pairs (235/235 balanced), 120 eval groups /
611 docs / 20 junk; junk docs genuinely ugly and off-topic.

## 5. The result — quantization exonerated by a bf16 control

| variant | junk_max | pos_min | top-1 | MRR | guardrail |
|---|---|---|---|---|---|
| **old NVFP4 (W4A4)** baseline | 0.9976 | 0.0009 | 14.2% | 0.4393 | FAIL |
| **new NVFP4A16 (W4A16)** candidate | 0.9960 | 0.0009 | 14.2% | 0.4375 | FAIL |
| **bf16 (unquantized) ceiling** | 0.9980 | 0.0005 | 16.7% | 0.4529 | FAIL |

- W4A16 came out **≈ identical** to W4A4 (pearson 0.977, spearman 0.935, 95% top-1
  agreement). Dropping activation quant changed almost nothing. (Note: llm-compressor
  W4A16 is a **data-free** weight-only pipeline — the corpus calibration set wasn't
  even consumed, which is fine: with no activation quant there's nothing to calibrate.)
- The **unquantized bf16 base fails the same guardrail**, at only ~2.5% higher top-1.
  A problem the bf16 model *also* has cannot be a quantization problem. **The bf16
  control test is the clean disproof** — and the single most valuable thing we ran.

## 6. What the behavior actually is (characterized)

The raw metrics oversell the failure:
- **Junk reaches rank-1 only 2/120 times.** Junk rarely tops. The scary
  `junk_max=0.998` is a *single outlier* (a Rust "error code E0214" doc scoring 0.998
  under a category-theory query) — and it's present even in bf16.
- **~15% top-1 is mostly a hard eval, not a broken reranker.** windex's own retrieval
  surfaces genuinely competitive hard-negatives — e.g. for "godot rss reader xml
  parser" it ranks *RSS Bandit* / *Java XML API* / *ASP RSS feed* (all real RSS/XML
  docs) above the Godot-specific positive. There are some real mis-ranks too (a
  Cambridgeshire *storage facility* at 0.997 beat "Bourn Castle" at 0.064).
- **The guardrail metric is single-outlier-strict.** `max`-over-junk and `min`-over-
  positives are dominated by one example, strict enough that even the bf16 ceiling
  fails. It's a good *conservative "did we regress"* gate, but a poor *"does rerank
  help"* metric.

Accurate summary: **the reranker does not clearly beat hybrid fusion on windex's
corpus at any precision** — so the value isn't in the checkpoint.

## 7. Learnings (generalizable)

1. **Run the unquantized ceiling FIRST when blaming quantization.** A 20-minute bf16
   control would have pre-empted the entire requant track. Make it step 1 next time.
2. **Never let calib == eval on the same distribution.** The original build calibrated
   *and* evaluated on MS MARCO, hiding the very distribution-shift it needed to catch.
   Always evaluate on the deployment corpus.
3. **Know the quant regime before investing in calibration data.** W4A16 is weight-only
   / data-free; only W4A4 (activation quant) consumes calibration. We built a
   calibration set that the chosen fix couldn't use.
4. **Guardrail metrics: prefer distributional over extremal.** `max`/`min`-over-a-
   handful fails even a perfect model. Use rank-1 junk rate, score percentiles, and a
   *paired* fusion-vs-rerank comparison.
5. **Reasoning models return `content=None` under small `max_tokens`.** Any judge /
   query-gen call through the gateway (qwen3.6) needs `enable_thinking: False`.
6. **A reranker must beat first-stage fusion on a fair metric to earn its slot.**
   Hard-negatives mined from your own retrieval make the bar (correctly) high; a
   reranker that merely re-shuffles competitive candidates without lifting relevance
   is not worth its latency.

## 8. Future explorations (parked, none urgent)

Ordered by value-to-effort:

1. **Fair fusion-vs-rerank eval (do this before anything else reranker-related).**
   NDCG@10 / MRR, rerank-OFF vs rerank-ON, on **LLM-judged** results (judge.py is now
   fixed) and/or the golden set — a *paired* A/B, not the junk_max gate. This is the
   real question ("does a reranker ever help windex?") and it's ~1h. If it's a clear
   no → keep rerank OFF permanently and stop. If yes for some segment → §2/§3 below.
2. **Serving-path discrepancy.** The reranker was observed scoring a doc differently
   in isolation vs inside the full ~400-candidate pool. A pointwise cross-encoder's
   score for a doc should be **independent of the other docs in the batch** — if it
   isn't, that's a vLLM seq-cls batching/pooling or `/rerank` truncation/normalization
   bug, and fixing it could change the whole picture. Test: score one `(q, doc)` alone
   vs in a batch of 400 and diff.
3. **Cheaper fix for the original cross-collection problem.** The reranker was partly
   motivated by RRF scores being incomparable across collections. That specific issue
   can be addressed with **normalized dense cosine** across collections (dense-only
   query gives a real similarity — see `_query_collection`) rather than a cross-encoder
   — a targeted fix without a reranker's cost.
4. **Input representation.** windex reranks on `title\n snippet[:400]`. A reranker may
   do materially better with fuller passage text (latency/storage tradeoff) — worth an
   ablation if #1 shows promise.
5. **Query-segmented reranking.** Rerank may only help short/ambiguous queries. Gate
   reranking to those segments and measure per-segment lift rather than globally.
6. **A different reranker** better matched to short inputs, or a listwise approach.
   Lowest priority; only after #1 shows a reranker can help at all.

## 9. Current state / how to re-enable (unlikely)

- Rerank OFF: `WINDEX_RERANK_ENDPOINT` empty. Old `…-NVFP4` checkpoint serving on the
  gateway; the new `…-NVFP4A16` checkpoint is parked on disk unserved (available if
  someone wants to inspect *why* W4A16 == W4A4).
- To re-enable after a future fix: set `WINDEX_RERANK_ENDPOINT=http://host.containers.
  internal:4000` + `WINDEX_RERANK_MODEL=qwen3-reranker-4b`, redeploy `windex-serve`,
  and gate on the §8.1 fair eval — not the junk_max guardrail alone.
