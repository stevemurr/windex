# Reranker re-quantize runbook — fix the NVFP4 spurious-1.0 (W4A4 → W4A16)

> **OUTCOME (2026-07-21): the premise below was DISPROVEN — do NOT follow this as a fix.**
> A W4A16 requant produced a checkpoint ≈ identical to the W4A4 original, and the
> **unquantized bf16 base fails the same guardrail** (junk_max 0.998, top-1 16.7%).
> The reranker doesn't beat hybrid fusion on windex's corpus at *any* precision, so
> this is not a quantization problem. **Decision: rerank stays OFF.** This doc is kept
> as a record + a working requant recipe; the lasting wins are
> `tools/build_rerank_calib.py` (corpus eval-set builder), `tools/rerank_guardrail.py`,
> and the `eval/judge.py` reasoning-model fix. If ever revisited, the fair test is a
> fusion-vs-rerank NDCG@10 on judged data — not the single-outlier junk_max — and the
> open lead is the vLLM seq-cls isolation-vs-pool serving discrepancy.

Fixes: the NVFP4 `qwen3-reranker-4b` scores a spurious ~1.0 on messy real docs
(CSS `@media`, boilerplate, off-topic pages), so on the full ~400-candidate pool
rerank-ON ranked worse than OFF. Rerank is currently **OFF** in windex; this is
how to get it back ON, correctly.

## Root cause (confirmed, not hypothesised)

The served checkpoint `/home/murr/models/Qwen3-Reranker-4B-seq-cls-NVFP4` is
**W4A4** — 4-bit weights *and* 4-bit input activations (`config.json →
quantization_config`: weights + `input_activations` both `num_bits: 4`). It was
quantized with `llm-compressor` `oneshot`, `scheme="NVFP4"`, calibrated on **256
MS MARCO** pairs — and the bakeoff *also* evaluated on MS MARCO, so it passed at
−1.1% MRR while **never seeing windex's corpus** (HN/wiki/arxiv/CSS-docs/
smallweb). Those docs are out-of-distribution for the 4-bit activation scales,
which clip and saturate the score head.

Two things this means:
- The **score head is already spared** (`ignore=["score"]`; the final norm is
  RMSNorm, so `targets="Linear"` never quantized it). There is **no head work to
  do** — don't chase that.
- The lever is the **activation quantization** (the A4), and the calibration/eval
  distribution. Fix both.

## The fix, in order (all re-quantize; NO retraining)

1. **W4A4 → W4A16** — drop activation quantization (keep 4-bit weights). This
   removes the OOD-activation blowup entirely and is the single most likely
   durable fix. One scheme change. You keep the 4-bit weight-memory win; you only
   give up the FP4 *activation* compute speedup, which is irrelevant for a 4B
   reranker over ≤400 short candidates.
2. **Calibrate + evaluate on windex's own corpus** — mandatory for the eval (so
   the bakeoff finally *catches* this class of regression), and it also lets you
   keep W4A4 as a fallback if you ever want the activation-compute speed back.

`tools/build_rerank_calib.py` builds both sets from the live corpus;
`tools/rerank_guardrail.py` adds the metric the MS MARCO bakeoff lacked.

---

## ⚠️ Sequencing — build with models UP, re-quantize with models DOWN

- **Building the calib/eval set needs the model stack UP**: it mines hard
  negatives through windex's own retrieval (embedder) and generates queries with
  the local LLM. Do it during the day with the **drain paused** (the default
  Option-B daytime state).
- **Re-quantizing needs the model stack DOWN**: `oneshot` loads the bf16
  reranker (~8 GB) and runs calibration forward passes. With the three served
  models resident (~98 G of 121 G), that OOMs. **Stop the model stack first.**
  windex search degrades to lexical (~5 ms) during the requant window — it's a
  maintenance window; don't manually enable the overnight drain during it.

---

## Step 0 — Build the windex calib + eval sets (stack UP, drain paused)

On the Spark, in the windex checkout, with the stack up:

```bash
cd /home/murr/Code/windex
uv run python tools/build_rerank_calib.py --out /home/murr/models/qwen3-reranker-nvfp4-build/windex_calib
# -> windex_calib/calibration.json        [{query, doc, label}]
# -> windex_calib/eval_set.windex.json     [{query, docs, labels, is_junk}]
# -> windex_calib/calibration.debug.jsonl  (inspection)
```

Eyeball `calibration.debug.jsonl` — confirm it contains the ugly stuff (CSS/
markup/boilerplate snippets, off-topic near-misses), not just clean prose. If
the run warns "only N junk docs in eval", raise `--n-docs` or the messy-source
weights in `SOURCE_WEIGHTS`.

> If windex runs containerized (the Spark deploy), copy the tool in and run it
> inside the serve container — it has the deps and the right hostnames:
> ```
> podman cp tools/. windex_windex-serve_1:/app/tools/
> podman exec -w /app windex_windex-serve_1 .venv/bin/python tools/build_rerank_calib.py --out /app/windex_calib
> podman cp windex_windex-serve_1:/app/windex_calib <build-dir>/windex_calib
> ```
> It needs `WINDEX_JUDGE_ENDPOINT` + `WINDEX_JUDGE_MODEL` set (the LLM does
> query-gen). Note: `gen_query()` sends `chat_template_kwargs={"enable_thinking":
> False}` because the judge (qwen3.6) is a **reasoning** model — without it the
> model spends its tokens in `<think>` and returns `content=None`, so every
> query-gen (and every LLM-judge grade in `eval/judge.py`) silently fails.

## Step 1 — Baseline capture (prove the failure first)

Serve the *current* NVFP4 (already at :8007) and the bf16 reference, capture on
the **windex** eval set, and run the guardrail. This is what should have gated
the original build:

```bash
cd /home/murr/models/qwen3-reranker-nvfp4-build
# current nvfp4 (served at :8007) on the windex eval set:
python3 rerank_capture.py http://localhost:8007 cap_nvfp4_windex.json windex_calib/eval_set.windex.json
# bf16 reference (serve_reranker.sh brings it up on :8009):
./serve_reranker.sh bf16 8009 &   # wait for load
python3 rerank_capture.py http://localhost:8009 cap_bf16_windex.json windex_calib/eval_set.windex.json

python3 tools/rerank_guardrail.py cap_nvfp4_windex.json windex_calib/eval_set.windex.json   # expect FAIL (junk_max ~1.0)
python3 tools/rerank_guardrail.py cap_bf16_windex.json  windex_calib/eval_set.windex.json   # expect PASS-ish
python3 rerank_compare.py cap_bf16_windex.json cap_nvfp4_windex.json                          # bf16 vs nvfp4 divergence on real docs
```

(Point the `tools/` path at the windex checkout, or copy `rerank_guardrail.py`
next to the other build scripts.)

## Step 2 — Stop the model stack (free RAM/GPU)

```bash
cd /home/murr/Code/llm-inference-platform
podman-compose -p llm-inference-platform down   # or stop at least qwen3.6 + the reranker
# also stop the bf16 bakeoff server from step 1 if still up
```

## Step 3 — Patch `quantize_reranker.py` (two edits)

**Edit A — W4A16 scheme.** Change the recipe:

```python
recipe = QuantizationModifier(
    targets="Linear",
    scheme="NVFP4A16",              # was "NVFP4" (W4A4). NVFP4A16 = 4-bit NVFP4
    ignore=["lm_head", "score", "classifier"],   # weights, fp16 activations.
)
```

`NVFP4A16` is the built-in weight-only NVFP4 preset (identical 4-bit weights,
**no** activation quant). Verify it resolves in the installed version:

```bash
python3 -c "from compressed_tensors.quantization import preset_name_to_scheme; \
print(preset_name_to_scheme('NVFP4A16', ['Linear']))"
```

If your `compressed-tensors` predates the preset, upgrade it, or define the
scheme explicitly by copying the `NVFP4` weight args and setting
`input_activations=None`.

**Edit B — calibrate on windex, not MS MARCO.** Replace the body of
`build_calibration_set()` so it reads the windex pairs and reuses the existing
`format_pair()` (keep `PREFIX`/`SUFFIX`/`INSTRUCT` and the seq-cls shim exactly
as they are — one source of truth for the template):

```python
def build_calibration_set() -> Dataset:
    pairs = json.load(open("/calib/calibration.json"))   # [{query, doc, label}]
    texts = [format_pair(p["query"], p["doc"]) for p in pairs[:NUM_SAMPLES]]
    print(f"calibration samples: {len(texts)} (windex corpus)")
    return Dataset.from_dict({"text": texts})
```

(Add `import json` at the top; `load_dataset` is no longer needed.) Also bump the
output path so you don't clobber the current checkpoint:

```python
OUT_DIR = "/out/Qwen3-Reranker-4B-seq-cls-NVFP4A16"
```

## Step 4 — Re-quantize

Run `oneshot` in the same image the original build used, GPU via CDI, mounting
the calib set, the HF cache (base model is already cached), and the output dir:

```bash
cd /home/murr/models/qwen3-reranker-nvfp4-build
podman run --rm --device nvidia.com/gpu=all \
  -v "$PWD":/work -w /work \
  -v "$PWD/windex_calib":/calib:ro \
  -v /home/murr/models:/out \
  -v /home/murr/.cache/huggingface:/root/.cache/huggingface \
  docker.io/vllm/vllm-openai:v0.25.0 \
  python /work/quantize_reranker.py
# -> /home/murr/models/Qwen3-Reranker-4B-seq-cls-NVFP4A16
```

## Step 5 — Gate the new checkpoint (offline, before serving it for real)

Serve the candidate on a scratch port and capture on the **windex** eval set:

```bash
./serve_reranker.sh nvfp4 8009   # after pointing its nvfp4 MODEL at ...-NVFP4A16, or serve that path directly
python3 rerank_capture.py http://localhost:8009 cap_nvfp4a16_windex.json windex_calib/eval_set.windex.json

python3 tools/rerank_guardrail.py cap_nvfp4a16_windex.json windex_calib/eval_set.windex.json
python3 rerank_compare.py cap_bf16_windex.json cap_nvfp4a16_windex.json
```

**Ship only if:**
- guardrail **PASS** — `junk_max` well below threshold, and `pos_min > junk_max`
  (the worst real doc still beats the worst junk doc). This is the check the
  original build lacked.
- `rerank_compare` vs bf16: top-1 agreement and MRR ≈ bf16 (small delta OK), not
  the divergence the W4A4 build showed on real docs.

## Step 6 — Promote + re-enable in windex

1. Point the production reranker container (`llm-inference-platform`, :8007) at
   `/home/murr/models/Qwen3-Reranker-4B-seq-cls-NVFP4A16` and bring the stack back
   up (`podman-compose -p llm-inference-platform up -d`). Confirm the drain timers
   and models autostart as before.
2. Flip rerank ON in windex (`.env` on the Spark):
   ```
   WINDEX_RERANK_ENDPOINT=http://host.containers.internal:4000
   WINDEX_RERANK_MODEL=qwen3-reranker-4b
   ```
   (`WINDEX_RERANK_QUERY_INSTRUCT` already defaults to the same instruction the
   calibration used; `HttpReranker` wraps the query — `embed/rerank.py`.) Redeploy
   `windex-serve`.
3. End-to-end check with the online harness (real search path, not just the
   reranker in isolation):
   ```bash
   windex eval --mode hybrid          # rerank OFF baseline (or a tagged run)
   # enable rerank, redeploy, then:
   windex eval --mode hybrid          # rerank ON — NDCG@10/MRR should improve, not regress
   ```
   Spot-check the originals: "attention", "transformer self-attention", and a
   query that previously surfaced a junk doc at rank 1.

## Fallbacks (only if W4A16 doesn't pass)

- **Keep W4A4, recalibrate on windex**: leave `scheme="NVFP4"`, apply only Edit B.
  The corpus calibration re-fits the activation scales to real data. Try this if
  you specifically want the FP4 activation-compute speed back and W4A16 latency
  is somehow a problem (it won't be for a reranker).
- **Last resort — QAD (Quantization-Aware *Distillation*)**, not QAT: distill the
  bf16 teacher into the NVFP4 student. This is a real training run (models DOWN,
  GPU-heavy) and the evidence says you won't need it. Don't start here.

## Why this won't silently regress again

The MS MARCO bakeoff couldn't see this failure because calib==eval==MS MARCO and
it only measured MRR. Now the eval set is windex's own corpus *including* junk
docs, and `rerank_guardrail.py` asserts junk scores low with positive/junk
separation — so the next requant that saturates on junk **fails the gate**
instead of shipping.
