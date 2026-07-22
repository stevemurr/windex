#!/usr/bin/env python3
"""Guardrail metric for a reranker capture: does junk score LOW?

The MS MARCO bakeoff measured MRR (which stayed fine) but never checked whether
known-irrelevant junk docs float to ~1.0 — which is exactly how the broken
NVFP4 quant shipped. This closes that hole.

Usage:
    python3 rerank_guardrail.py <capture.json> <eval_set.windex.json>

  <capture.json>            output of rerank_capture.py (has results[].scores)
  <eval_set.windex.json>    output of tools/build_rerank_calib.py (has is_junk[])

Joins the two by position (rerank_capture preserves eval order and doc order),
then reports, over all docs:
  - JUNK   : max / mean score on is_junk docs  (want LOW — the guardrail)
  - POS    : min / mean score on label==1 docs  (want HIGH)
  - HARDNEG: mean / max on non-junk label==0 docs
  - top-1 accuracy + MRR of the label==1 doc    (ranking sanity)

Verdict PASS requires: junk_max < --junk-max AND pos_min > junk_max (the worst
real doc still beats the worst junk doc). Pure stdlib, like rerank_compare.py.
"""

import json
import statistics
import sys


def _f(x):
    return None if x is None else float(x)


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    cap = json.load(open(sys.argv[1]))
    ev = json.load(open(sys.argv[2]))
    junk_max_thresh = float(sys.argv[3]) if len(sys.argv) > 3 else 0.5

    cres = cap["results"]
    if len(cres) != len(ev):
        print(f"WARNING: capture has {len(cres)} queries, eval has {len(ev)} — "
              f"joining the first {min(len(cres), len(ev))} by position")

    junk, pos, hardneg = [], [], []
    top1_hits, rr = 0, []
    n_q = 0
    for cr, ee in zip(cres, ev):
        scores = cr.get("scores") or []
        is_junk = ee.get("is_junk") or [False] * len(scores)
        labels = ee.get("labels") or cr.get("labels") or [0] * len(scores)
        if cr.get("query") and ee.get("query") and cr["query"] != ee["query"]:
            print(f"WARNING: query mismatch at row {n_q} — order may be off")
        n_q += 1
        for s, lab, jk in zip(scores, labels, is_junk):
            s = _f(s)
            if s is None:
                continue
            if jk:
                junk.append(s)
            elif lab == 1:
                pos.append(s)
            else:
                hardneg.append(s)
        # ranking sanity on this query: where does the (first) label==1 doc land?
        ranked = sorted(
            ((_f(s) if s is not None else float("-inf"), lab)
             for s, lab in zip(scores, labels)),
            key=lambda t: t[0], reverse=True)
        rank_of_pos = next((i + 1 for i, (_, lab) in enumerate(ranked) if lab == 1), None)
        if rank_of_pos:
            rr.append(1.0 / rank_of_pos)
            if rank_of_pos == 1:
                top1_hits += 1

    def stats(xs):
        if not xs:
            return "  (none)"
        return (f"n={len(xs):<4} min={min(xs):.4f} mean={statistics.mean(xs):.4f} "
                f"max={max(xs):.4f}")

    junk_max = max(junk) if junk else 0.0
    pos_min = min(pos) if pos else 0.0

    print(f"queries: {n_q}")
    print(f"JUNK    {stats(junk)}   <- guardrail (want LOW)")
    print(f"POS     {stats(pos)}")
    print(f"HARDNEG {stats(hardneg)}")
    if rr:
        print(f"top-1 accuracy: {top1_hits}/{len(rr)} = {top1_hits / len(rr):.3f}   "
              f"MRR: {statistics.mean(rr):.4f}")

    pass_junk = junk_max < junk_max_thresh
    pass_sep = (pos_min > junk_max) if (pos and junk) else True
    verdict = "PASS" if (pass_junk and pass_sep) else "FAIL"
    print(f"\njunk_max={junk_max:.4f} (threshold {junk_max_thresh})  "
          f"pos_min={pos_min:.4f}")
    print(f"guardrail: junk_max<thresh={pass_junk}  pos_min>junk_max={pass_sep}  "
          f"=> {verdict}")
    sys.exit(0 if verdict == "PASS" else 1)


if __name__ == "__main__":
    main()
