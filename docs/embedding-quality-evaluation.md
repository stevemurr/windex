# Embedding-Quality Evaluation — Playbook

> **Status:** research + experiment plan, ready to act on. Compiled 2026-07-22 from a
> five-track literature/codebase review. Nothing here has been run yet — this is the
> map, not the results.
>
> **Goal (per the request):** once the embedding backlog finishes draining, run a
> battery of experiments to find *where the index is weak* and *what the failure modes
> of our embeddings are*, then turn the useful probes into a standing `windex`
> subcommand + Grafana signals so quality is monitored, not spot-checked.

---

## 0. How to use this document

- **When to start.** Most retrieval-quality probes want a mostly-embedded corpus, so the
  headline suite is gated on the backlog draining (as of writing: **6.75M embedded / 42%**,
  **8.84M deduped backlog / 55%**, draining ~2.8M/day on the `full` profile → roughly a few
  days out). **Tiers 0 is runnable today** (pure SQL/parquet, no dependence on the backlog).
- **Structure.** §1 is the baseline we're measuring against. §2 is the prioritized probe
  suite — the core deliverable, each probe a runnable spec. §3–§5 are the larger builds
  (grow the eval set, ANN/quantization/model-swap methodology, monitoring wiring). §6 is the
  failure-mode taxonomy reference. §7 is a suggested order of operations.
- **Bias for reuse.** Every probe is written to *extend* the existing `windex eval` harness
  (`src/windex/eval/`) and search path (`src/windex/index/search.py`), not to stand up a
  parallel notebook. Where a probe earns its keep, the intended end state is a
  `windex embed-health` / `windex ann-recall` subcommand emitting Prometheus gauges.

---

## 1. Ground truth — what we are actually evaluating

Established by direct code inspection + live DB/Qdrant queries (2026-07-22).

### 1.1 Model & embedding
| Fact | Value | Source |
|---|---|---|
| Model | `qwen3-embedding-8b` via litellm gateway `:4000` | `.env`, `embed/http.py` |
| Dim / distance | **4096**, cosine | live Qdrant read, `index/qdrant.py:96` |
| Decoder-only, last-token pooled, **Matryoshka (MRL)**, instruction-aware | — | Qwen3 model card |
| Query prefix | `"Instruct: Given a web search query, retrieve relevant passages…\nQuery: "` — **applied to queries only, never documents** | `config.py`, `index/search.py:343` |
| `memory` source gets a distinct recall-oriented prefix | — | `config.py:56` |
| Composed text | `title + "\n\n" + body`, whole string capped at `embed_max_tokens*4 ≈ 8192 chars` | `embed/pipeline.py:114,292` |
| Chunking | **one vector per doc**, no sub-doc chunking (except `memory`, client-pre-chunked) | `hf/embed_index.py:10-39` |

### 1.2 Corpus (live snapshot, ~16.1M rows)
| source | deduped (backlog) | embedded | duplicate | deleted | notes |
|---|---:|---:|---:|---:|---|
| wiki | 5,537,948 | 1,668,284 | — | — | full article text → **truncation-exposed** |
| hn | 2,079,966 | 2,225,850 | 415,566 | 13,569 | mostly **title-only** link posts |
| arxiv | 1,157,113 | 1,681,984 | — | — | **title+abstract only** (no full text → no truncation) |
| news | 58,999 | 1,014,789 | 136,306 | — | MinHash near-dedup (ccnews only) |
| smallweb | — | 89,488 | — | — | |
| github | 4,981 | 52,131 | — | — | **README prose only** (code fences stripped) |
| docs | — | 18,108 | — | — | version-forked reference pages |
| memory | — | 611 | — | 20 | multi-vector, client-chunked |
| hf | — | — | — | — | provisioned, **0 ingested** |
| **total** | **8.84M** | **6.75M** | **0.55M** | **13.6K** | |

**Reconciliation gap to watch:** Qdrant holds **~117K more `hn` points than `documents.status='embedded'`** (and +14K wiki). Consistent with HN's best-effort point-drop on late-dedup (`hn/cleanup.py`), not corruption — but it means the vector store and the ledger disagree; probe #3 (coverage census) should reconcile it.

### 1.3 Retrieval config
- **9 per-source Qdrant collections** `<source>__qwen3-embedding-8b` behind `<source>_current` aliases. Cross-source failure modes are mostly *per-collection*; the exception is the `source=all` merge (§6, probe #2).
- HNSW **defaults** `m=16, ef_construct=100`; query `hnsw_ef=96`.
- **INT8 scalar quantization**, `always_ram=False`, dense vectors `on_disk=True`.
- Dense query runs with **`rescore=False`** — a deliberate disk-I/O trade (`store-tuning.md`), **never validated for top-k rank fidelity.**
- Hybrid = dense + sparse BM25 (fastembed, IDF modifier), **RRF-fused server-side**, each leg 4× oversampled. Optional cross-encoder reranker over a `top_k=50` pool, **best-effort** (a failure degrades to fused order).

### 1.4 Existing eval harness (extend this, don't replace it)
- `windex eval` → **known-item leg** (title-as-query, label-free, per-source, `per_source=25`) + **golden leg** (`golden_seed.json`, **91 anchors** across 7 sources) + optional **LLM-judge leg** (`qwen3.6`). Metrics: NDCG/MRR/Recall/Precision/hit at a single `k` (default **10**).
- Persists to `search_quality` table; exports `windex_search_quality_ndcg{leg}` / `_mrr{leg}`; nightly at 06:30; alert `SearchQualityRegression` (`known_item ndcg < 0.8 for 30m`, baseline ~0.93).
- **Missing entirely:** any ANN-recall-vs-exact check, any quantization-delta check, any per-`lang` or per-length stratification, any retrievability/coverage metric.

### 1.5 The four "live but unvalidated" decisions (test these first)
1. **`rescore=False`** on int8 — ranking served purely on quantized distances (probe #1).
2. **`source=all` merges collections by raw RRF score** — incomparable across collections when the reranker is off (probe #2).
3. **Near-dedup (MinHash) is ccnews-only + rolling-window** — 6 other sources have exact-hash dedup only (probe #6).
4. **`embed_max_tokens=2048`** is a self-imposed pipeline cap (model supports 32K) — silently truncates long wiki/docs pages (probe #4).

---

## 2. The probe suite (prioritized)

Ordered by *(how live/likely the risk is) × (how cheap the probe is with existing machinery)*. The first two rows are the highest value because they test **already-deployed** decisions, not open questions.

| # | Probe | Tier | Needs backlog drained? | Cost |
|---|---|---|---|---|
| 1 | Quantization rank-fidelity (`rescore` on/off) | 1 · live-config | no (works on embedded subset) | ~1h script |
| 2 | `source=all` fan-out dominance | 1 · live-config | partial | ~1h script |
| 3 | Coverage-debt census + PG↔Qdrant reconcile | 0 · SQL | **runnable now** | minutes |
| 4 | Truncation exposure (wiki/docs/news/smallweb/repos) | 0 · parquet | **runnable now** | minutes |
| 5 | ANN recall vs exact (HNSW fidelity) | 1 · systems | yes | ~2h script |
| 6 | Near-duplicate NN-distance mass | 2 · geometry | yes | ~1h/collection |
| 7 | Hubness / k-occurrence skew | 2 · geometry | yes | minutes/collection |
| 8 | HN title-only collapse | 2 · retrieval | yes | eval extension |
| 9 | Lexical-vs-dense value-add on exact-match queries | 2 · retrieval | yes | curate ~40 q |
| 10 | Intrinsic geometry battery (anisotropy/rank/LID/norms) | 3 · geometry | yes | ~30min |
| 11 | Numeric/version/temporal & negation & multilingual | 3 · retrieval | yes | curate probes |

### Probe 1 — Quantization rank-fidelity *(highest value)*
**Tests:** whether serving dense ranking on int8-only distances (`rescore=False`) actually costs top-k order, vs float32 rescoring. The code comment claims "int8 recall at 4096-dim doesn't need it" — but that was decided to relieve a saturated disk, never measured against rank displacement.
**Run:** sample ~200 queries (golden set + random real query vectors) against the 2–3 largest collections (`news`, `wiki`, `hn`). For each, call a copy of `index/search.py::_query_collection` twice — `QuantizationSearchParams(rescore=False)` (prod) vs `rescore=True, oversampling=2.0` — same query, same collection, no reindex.
**Measure:** per query, **Kendall's τ** on the top-20 orderings, **Jaccard** of the top-10 sets, and **% of queries whose #1 result changes**.
**Problem if:** median τ noticeably < 1, or >10–15% of queries flip their #1. That means the disk trade is costing real ranking quality → re-enable `rescore=True` selectively (only for the reranker-bound `fetch_limit` pool), or move int8 copies `always_ram=True` (store-tuning notes RAM headroom is now ample on the Spark).

### Probe 2 — `source=all` fan-out dominance
**Tests:** the `source=all` path merges 9 collections by **raw score sort** (`index/search.py`), but RRF's score is a rank-reciprocal, *not* a calibrated cross-collection relevance — the code says so ("a real cross-collection score comes from the reranker"). When the reranker is off/times out (best-effort), whichever collection's RRF scores run structurally higher dominates.
**Run:** ~100 diverse `source=all` queries with the reranker disabled; record each source's share of the merged top-10. Compare to the same queries run per-source (`mode=hybrid`, single source).
**Problem if:** a source's share in `all` is systematically higher/lower than its per-source relevance predicts → make the reranker **mandatory** for `source=all`, or switch the cross-collection merge to Qdrant **DBSF** (score-distribution fusion) instead of raw-RRF sort.

### Probe 3 — Coverage-debt census *(run today)*
**Tests:** the silent hole — docs marked `failed` (embed server rejected) or `empty` (`is_empty_text`) are absent from search with no query-time symptom; nothing measures that population.
**Run:** `SELECT source, status, count(*) FROM documents GROUP BY 1,2;` plus a PG↔Qdrant point-count reconcile per collection (`curl :6333/collections/<name>`), to explain the +117K hn / +14K wiki gap.
**Problem if:** `failed`/`empty` share is large or concentrated in one source (cross-check against probe #4 — long-doc truncation can manifest as reject-failures). Report per-source coverage-debt as a standing gauge.

### Probe 4 — Truncation exposure *(run today)*
**Tests:** how much content the 8192-char cap silently drops. Concentrated in **wiki/docs/news/smallweb/repos** (arxiv=abstract-only is exempt).
**Run:** parquet scan per source: `len(compose_text(row,…))` vs `max_chars`; report **% truncated** and **median bytes dropped**. Then for the truncated subset, curate ~20–30 queries whose answer lives *only* in the dropped tail (grep raw staged text past the cutoff) and check `hit@10`.
**Problem if:** wiki/docs show high truncation + tail-answer queries miss → raise `embed_max_tokens` for those sources (model ceiling is 32K, not 2048), or move them to chunked multi-vector (the `memory` `doc_id#chunk_idx` pattern already exists).

### Probe 5 — ANN recall vs exact
**Tests:** HNSW is approximate; nobody has measured whether `hnsw_ef=96` actually delivers the recall we assume at 4096-dim. This is layer 1 (index fidelity), distinct from relevance.
**Run:** 50–200 query vectors; per collection compare the **live** path (`hnsw_ef=96, rescore=False`) to `SearchParams(exact=True)` on the same collection. `recall@k = |ann ∩ exact| / k`. Sweep `hnsw_ef ∈ {32,64,96,128,256}` offline on one collection to find the plateau.
**Target:** ≥0.95 recall@10 (Qdrant's typical target; @10 is what our golden/known-item legs score). If raising `hnsw_ef` plateaus below target, the fix is a rebuild with larger `m`/`ef_construct` (free via the parquet re-embed path — vectors don't change).
**Deeper tier:** for before/after a config change, build an independent faiss `IndexFlatIP` ground truth from parquet (10M×4096 fp32 ≈ 164GB — CPU-RAM-feasible on the Spark as an isolated batch job, or sharded) to catch bugs in Qdrant's own exact path.

### Probe 6 — Near-duplicate NN-distance mass
**Tests:** semantic near-dupes (syndicated news past the dedup window, forked READMEs, mirrored docs versions, HN reposts) collapse to near-identical vectors and crowd top-k. **repos/wiki/docs/hn have no near-dedup safety net.**
**Run:** ~5,000 docs/collection; each doc's 1-NN cosine distance (`query_points(limit=2)`, take the 2nd). Histogram; report **% with NN distance < 0.03**. Cross-check those pairs' `text_hash` (differ ⇒ not exact) and `canonical_url`/`source`.
**Problem if:** a distinct low-distance spike → add MinHash/embedding-distance near-dedup to the shared `embed/pipeline.py` driver for all sources; and/or a query-time MMR diversity pass over top-k.

### Probe 7 — Hubness / k-occurrence
**Tests:** a few "universal neighbor" docs (boilerplate, disambiguation stubs, license files) that appear in top-k for unrelated queries. Worsens under int8 (quantile clipping hits hub-adjacent outliers hardest).
**Run:** *must hit live Qdrant* (needs the true neighbor relation). Per collection (start with `repos`, smallest): ~2,000 vectors as both query & corpus, top-50 each, tally `N_50(doc_id)`. Compute **skewness** + **Robin-Hood/Gini** of `N_50`; flag any doc with `N_50 > 10× mean`; report the **antihub fraction** (`N_50=0`, ties to retrievability). Eyeball the top-20 hubs.
**Problem if:** a doc claims >0.5% of all neighbor slots, or hubs are boilerplate/empty-ish → tighten the `is_empty_text` gate (removes hub candidates at source), centering, or mutual-proximity re-ranking.

### Probe 8 — HN title-only collapse
**Tests:** a natural-language query vs a 3–8 token bare headline is a granularity mismatch; "most HN stories are title-only" (per `hn/embed_index.py`).
**Run:** split embedded `hn` by `story_text` empty vs non-empty; run known-item title-as-query NDCG/hit@k per bucket (one extra WHERE in `eval/harness.py::known_item_eval`).
**Problem if:** title-only bucket materially underperforms → embed `title + domain(target_url)` or an HN-specific instruct; consider a title-aware reranker boost.

### Probe 9 — Lexical-vs-dense value-add
**Tests:** whether dense ever *hurts* exact-match queries inside RRF, and whether hybrid actually lifts paraphrase queries. `search()` already exposes `mode: hybrid|dense|lexical` — no new code.
**Run:** ~40 curated queries, half exact-match (repo names, version strings, error codes, arxiv/hn ids), half paraphrase (golden-set style). Diff hit@k/MRR across the three modes per class.
**Problem if:** exact-match class does *worse* under `hybrid` than `lexical` (dense hurting), or paraphrase shows no `lexical→hybrid` lift → query-side routing (detect id/version tokens → bias the sparse leg), or DBSF fusion.

### Probe 10 — Intrinsic geometry battery (label-free)
**Tests:** whether the space's geometry can support good retrieval at all. Compute from a **100k parquet sample** (no Qdrant round-trip except where noted), **globally and per-source** — global anisotropy from real domain separation is fine; the same *within* one source is pathological. **MRL caveat:** Qwen3 is Matryoshka-trained, so steep spectral decay and front-loaded variance are *expected/healthy* — the signal is a hard zero-floor, not decay itself.
- **Mean random cosine sim + IsoScore** (`pip install isoscore`): flag global mean-cos > 0.5, or any source > 0.15 above global.
- **Effective rank / stable rank / spectral decay** (truncated SVD, top ~512): flag a hard cliff to the noise floor well before ~1000 rank, or effective rank < ~5% of 4096.
- **Local intrinsic dimensionality** (`skdim.MLE`/`TwoNN`, faiss kNN on 30k): flag a source collapsing to LID < 3–5 while others sit higher.
- **Norm distribution** (`np.linalg.norm`): flag |Spearman(norm, length)| > 0.3 within a source, or a bimodal/heavy-tailed norm (often mis-extracted/empty docs) — these also stretch int8 quantization ranges. Diff float32-parquet norms vs dequantized int8 pulled from Qdrant for 5k docs.
- **Alignment & uniformity** (Wang–Isola, self-supervised (title, body) / (README, file) pairs — no labels): track across re-embeds; a jump in alignment loss after a model swap = extraction/chunking broke.

### Probe 11 — Numeric/version/temporal, negation, multilingual
- **Version/temporal** (`docs`, `repos`, `news`): pairs differing only by version/date where the right pages differ; does `mode=dense` pick the matching version, or just "a page about X"? Mitigation is *structural*, not a better vector — route to the existing `published_after`/`version`/`framework` Qdrant payload filters via a query-side extractor.
- **Negation** (bi-encoders score ~random on negation): ~15 minimal negation pairs over `repos`/`docs`; route negation-bearing queries to the reranker or to `language != X` filters.
- **Multilingual** (`news` indexes `lang`): `SELECT lang, count(*) … GROUP BY lang`; run known-item stratified by `lang`; flag a material `en` vs non-`en` NDCG gap. Surface `lang` as a first-class agent filter (already indexed).

---

## 3. Growing the eval set (91 anchors → ≥300 judged, stratified)

The 91 golden anchors, split across 7 sources (~13 each), are **below the 25-query-per-stratum floor** for a stable per-slice estimate (Buckley & Voorhees). Target **≥50 judged queries per source × 7 = ≥350**, plus the auto-generated known-item pool as a high-N supplement.

**Recipe (each step self-hostable, open-source):**
1. **Harden known-item:** LLM-paraphrase titles into natural queries (strip verbatim substrings) so it stops rewarding pure lexical match; cap it at ≤20% of each source's budget; de-dupe near-identical titles first.
2. **Per-source synthetic queries (Promptagator-style):** 5–8 hand-picked (query, doc) exemplars *per source* reflecting how an agent phrases queries for that content ("find a repo that implements X"; "what did <event> say about Y"). Task-specific beats one generic InPars prompt across heterogeneous sources.
3. **Filter:** lexical-overlap reject (kills paraphrases) → **round-trip** (query must retrieve its source doc in top-20 of live hybrid) → LLM self-critique ("could many docs answer this?"). Use **pairwise/relative** generation against a near-duplicate doc to force discriminative queries.
4. **Hard negatives (GPL-style):** mine windex's own top-ranked *other* docs for each query; **cross-encoder-denoise** with the Qwen judge (drop any it scores ≥2 — likely false negatives) before locking as negatives.
5. **Graded judge (UMBRELA-style DNA prompt):** 0–3 scale, **pointwise independent scoring** (sidesteps position bias), constrained `##final score:` output. Pool candidates from **multiple retrieval strategies** (dense-only + sparse-only + a second model) before judging — else you inherit BEIR's lexical-pool-holes bias and under-credit any future config that finds different-but-relevant docs.
6. **Calibrate the judge:** 150–300 pairs, 2–3 human labels, report human–human κ (ceiling) vs judge-vs-human κ; iterate the rubric until close. LLM judges skew **lenient** — trust them for *ranking configs*, not absolute scores. Re-run on any judge-model swap.
7. **Store BEIR-format JSONL**, tagged by source *and* generation method. Compute with **`ranx`** (MIT, Numba; metrics + fusion + significance in one). Report a **per-source table**, not just a pooled number, plus the worst source as a headline.
8. **Mine real agent queries** from API logs once traffic exists (realistic query *text*, judged by the calibrated judge — clicks are only weak labels).

**Metrics for a find-and-link agent:** primary **MRR@10 / Success@{1,3}** (does it get a correct link fast); secondary **NDCG@10** (graded, dashboard headline); **Recall@100** as the retriever-ceiling diagnostic (high Recall@100 + low NDCG@10 ⇒ fix reranking, not retrieval).

**Significance:** paired **bootstrap or Fisher's randomization** on per-query deltas (~1,900+ resamples). **Not** Wilcoxon/sign (Smucker et al. — poor sensitivity). Run *within each stratum* and pooled; validate a change only if it regresses no individual source.

---

## 4. ANN recall, quantization QA, and model-swap A/B

- **`windex ann-recall` subcommand** (sibling to `windex eval`): probe #5 on a schedule, persisted parallel to `search_quality`, exported as `windex_ann_recall{source,k}`.
- **Quantization QA leg:** probe #1 as a standing `quantization_ndcg_delta` leg in `run_eval` — reuses `eval/metrics.py`, alerts if the NDCG delta from `rescore=False` exceeds ~0.02.
- **MRL truncation as a cost lever:** the corpus at 4096-dim ≈ 21GB int8; truncating to 2048 roughly **halves** RAM/disk. Since it only changes vector width (not the model), it's compatible with the parquet re-embed path and doesn't touch the Embedder interface. **Measure the quality cost locally** (exact-recall + NDCG at 4096 vs 2048 vs 1024) — the 0.6B-model knee doesn't transfer to 8B.
- **Model-swap A/B (re-embed + alias flip):** because a swap creates a *new, separately-aliased* collection before the flip, point `index.search` at the candidate collection and run the **identical** golden + known-item set against both, same day — no "corpus changed between runs" confound. Report aggregate ΔNDCG/ΔMRR + bootstrap CI, **per-query win/loss table**, and **per-source breakdown** (a swap can help prose and hurt code). Candidates worth benchmarking if a swap is ever on the table: **NV-Embed-v2**, **BGE-en-ICL** (both open-weight, now scoring at/above Qwen3-Embedding-8B on MTEB as of 2026 — "MTEB #1" on the model card is a June-2025 snapshot).

---

## 5. Wiring into monitoring (Prometheus/Grafana already exist)

The plumbing is there (`api/prom.py` + `ops/grafana/`); the gaps are the *recall layer* and *trend alerting*, not tooling. Add:
- `windex_ann_recall{source,k}`, `windex_quantization_ndcg_delta{source}` (§4).
- **Retrievability Gini** per collection (antihub fraction from probe #7) as a standing **corpus-health** gauge — catches slow coverage rot that per-query eval misses.
- **Coverage-debt** gauge (`failed`/`empty` share per source, probe #3).
- **Cosine-score-distribution** panel (p50/p95 top-k score per source) — a near-free label-free canary that shifts *before* NDCG does.
- A **rate-of-change alert** alongside the absolute floor: `ndcg{leg=known_item} - ndcg offset 7d < -0.05` — catches a slow bleed that never trips the `< 0.8` rule.
- **Diagnostic runbook line** (`ops/README.md`): `ann_recall down + search_quality flat ⇒ index/config regression`; `ann_recall flat + search_quality down ⇒ corpus/content issue`.
- Tag scheduled eval/recall runs **cold vs warm** so a post-cold-start Qdrant (measured 4.2–6.2s cold dense queries) doesn't page as a false regression.

---

## 6. Failure-mode taxonomy (reference)

| Mode | windex exposure | Detect | Mitigate |
|---|---|---|---|
| **Quantization rank error** | **live** (`rescore=False`, unvalidated) | probe #1 | selective rescore / int8 `always_ram` |
| **`source=all` incomparability** | **live** (raw-RRF merge) | probe #2 | mandatory reranker / DBSF |
| **Near-dup collapse** | repos/wiki/docs/hn unprotected | probe #6 | shared-driver near-dedup / MMR |
| **Hubness / antihubs** | per-collection, worse under int8 | probe #7 | empty-gate, centering, mutual proximity |
| **Truncation loss** | wiki/docs/news/smallweb/repos (not arxiv) | probe #4 | raise cap / chunk those sources |
| **Coverage debt** (`failed`/`empty`) | silent, per-source | probe #3 | census + gauge |
| **Query↔doc granularity** | hn title-only, thin repos | probe #8 | enrich short-doc text / instruct |
| **Lexical/semantic gap** | exact-id vs paraphrase | probe #9 | query routing / DBSF |
| **Numeric/version/temporal** | docs/repos/news | probe #11 | structural payload filters |
| **Negation** | bi-encoder ~random | probe #11 | reranker / `!=` filters |
| **Multilingual clustering** | news `lang` | probe #11 | expose `lang` filter / translate-index |
| **Anisotropy / dim-collapse** | model-family baseline (MRL-expected) | probe #10 | interpret vs MRL baseline, centering |
| **Instruction-prefix drift** | **currently correct** | regression test | assert prefix non-empty in CI |
| **Capacity ceiling** (sign-rank) | intrinsic to 4096-dim single-vector | — | hybrid + reranker (already have) |

**Two things the old `search-overhaul-plan.md` got wrong (now corrected):** the query instruction prefix is **set and applied correctly** in production (not empty), and **arXiv is not truncated** (abstract-only, well under the cap). Don't chase either as a bug.

---

## 7. Suggested order of operations

1. **Today, no waiting:** probes #3 (coverage census + PG↔Qdrant reconcile) and #4 (truncation exposure) — pure SQL/parquet, minutes, and they inform everything downstream.
2. **First scripts (highest value):** probes #1 (quantization rescore) and #2 (`source=all` dominance) — they validate live production decisions and each is a ~1h param-flip script over `_query_collection`.
3. **Once the backlog is fully embedded:** probe #5 (ANN recall) + the geometry battery #10 + hubness #7 — a single `windex embed-health` pass reading parquet + one Qdrant round-trip, emitting gauges.
4. **Retrieval failure modes:** #8, #9, #11 as `windex eval` extensions.
5. **Then invest:** grow the eval set (§3) and stand up the model-swap A/B harness (§4) so the *next* embedding decision (MRL truncation, a model swap, re-enabling rescore) is made on evidence.

Turn whatever earns its keep into a scheduled subcommand + Grafana signal — the point is standing monitoring, not a one-time audit.

---

### Appendix — sampling & statistics cheat-sheet
- Scalar means (cosine, norms): 50k–100k sample → CI half-width ≈ 0.002.
- Covariance/spectral (d=4096): ≥100k vectors (d/n ≤ 0.1) for stable eigenvalues.
- Hubness: 10k–50k queries against the **full** collection (don't sample the corpus side).
- Silhouette: cluster on 200k–1M (MiniBatchKMeans), score on a 10k subsample.
- Per-stratum eval: ≥50 judged queries/source; ≥1,900 bootstrap resamples for a p≈0.05 estimate.

*Full literature citations for every claim above are preserved in the source research (five agent reports, 2026-07-22); key anchors: Radovanović et al. (hubness, JMLR 2010), Wang & Isola (alignment/uniformity, ICML 2020), Rudman et al. (IsoScore, ACL 2022), Thakur et al. (BEIR, 2021), Dai et al. (Promptagator, 2022), Wang et al. (GPL, 2021), Upadhyay et al. (UMBRELA, 2024), Smucker et al. (IR significance testing, CIKM 2007), Buckley & Voorhees (evaluation stability, SIGIR 2000), Qdrant ANN-recall & quantization docs, Qwen3-Embedding model card.*
