"""MinHash signatures with the FineWeb configuration: word 5-grams, 112 hashes
in 14 bands of 8. A band-hash collision against the rolling window marks a
near-duplicate. Parameters mirror datatrove's MinhashConfig defaults; the
incremental band store (Postgres, pruned by day) is ours."""

import re

import numpy as np
import xxhash

N_PERM = 112
N_BANDS = 14
BAND_SIZE = 8
NGRAM = 5

_MERSENNE = np.uint64((1 << 61) - 1)
_rng = np.random.default_rng(seed=112_358)
_A = _rng.integers(1, _MERSENNE, size=N_PERM, dtype=np.uint64)
_B = _rng.integers(0, _MERSENNE, size=N_PERM, dtype=np.uint64)

_token_re = re.compile(r"\w+", re.UNICODE)


def signature(text: str) -> np.ndarray | None:
    """112 minhash values, or None if the doc is too short to shingle."""
    words = _token_re.findall(text.lower())
    if len(words) < NGRAM:
        return None
    shingles = {" ".join(words[i : i + NGRAM]) for i in range(len(words) - NGRAM + 1)}
    hashes = np.array(
        [xxhash.xxh64_intdigest(s) for s in shingles], dtype=np.uint64
    ).reshape(-1, 1)
    # (a*h + b) mod mersenne prime, one permutation per column
    projected = (hashes * _A + _B) % _MERSENNE
    return projected.min(axis=0)


def band_hashes(sig: np.ndarray) -> list[int]:
    """One 64-bit hash per band, as signed ints for Postgres bigint."""
    out = []
    for b in range(N_BANDS):
        chunk = sig[b * BAND_SIZE : (b + 1) * BAND_SIZE].tobytes()
        out.append(np.uint64(xxhash.xxh64_intdigest(chunk)).astype(np.int64).item())
    return out
