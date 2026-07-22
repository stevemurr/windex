"""A cap on in-flight *bulk* embed requests that holds across processes.

The problem: `embed_concurrency` is per-process. Six embed jobs at profile
`full` (8 each) put ~48 requests in flight at one endpoint. Measured
2026-07-17 against the live server: a single two-word embed took 67s, against
an 8s query deadline — so hybrid search fell back to lexical whenever indexing
ran. The throttle profiles exist to prevent exactly that and structurally
could not: a per-process knob cannot bound a fleet. This can.

Why a cap costs (almost) nothing: the GPU is already saturated, so the deep
queue buys no extra throughput — it only buys latency. Cutting in-flight work
shortens the queue in front of a live query without idling the GPU. Where the
curve actually turns is empirical, which is why the budget is a runtime knob
(PROFILES in embed/__init__.py) rather than a constant: measure, then set it.

Why flock, and not the postgres advisory locks originally proposed: both are
crash-safe, and crash-safety is mandatory here (these jobs are SIGKILLed
routinely; a counter table would leak slots forever). But an advisory lock is
session-scoped, so holding one means holding a postgres connection for the
whole GPU call — the pool is 16/process and the pass needs it for ledger
commits. The kernel drops an flock when the holder dies, needs no connection
and no round trip, and every windex job runs on one box by design (CLAUDE.md),
so file locks coordinate exactly the processes that exist. If jobs ever run on
more than one machine, this must become an advisory lock.

Slots are keyed per endpoint, so a second embedding server gets its own budget
rather than sharing one — adding an endpoint needs no change here.

Queries deliberately never take a slot. The entire point is that a live search
walks past the bulk queue instead of queueing behind it; index/search.py builds
its embedder without a budget, and that asymmetry is the feature.
"""

import contextlib
import fcntl
import hashlib
import logging
import os
import time
from collections.abc import Iterator, Sequence
from pathlib import Path

from windex.embed.base import Embedder

log = logging.getLogger("windex.embed.budget")

# Local disk on purpose: the external staging drive has detached twice and
# taken the pipeline with it. Lock files are empty; this costs no space.
SLOT_ROOT = Path.home() / ".windex" / "embed-slots"

# A wait longer than this means something is wrong (a real batch is seconds,
# and 67s under the pathological load this exists to fix). Proceed anyway
# rather than stall a backfill forever on a slot that never frees — a throttle
# that can wedge the pipeline is worse than no throttle. Since the gateway
# grew a per-key cap on the bulk key, proceeding unbudgeted means eating a
# 429 (rejected, retried with backoff by HttpEmbedder) — not piling queue
# onto the GPU, which is what this timeout used to risk.
WAIT_TIMEOUT = 900.0
POLL = 0.2


def slot_dir(endpoint: str) -> Path:
    key = hashlib.sha1(endpoint.encode()).hexdigest()[:12]
    return SLOT_ROOT / key


@contextlib.contextmanager
def embed_slot(endpoint: str, budget: int, wait_timeout: float = WAIT_TIMEOUT,
               poll: float = POLL) -> Iterator[bool]:
    """Hold one of `budget` slots for this endpoint. budget <= 0 disables.

    Yields True if a slot was held, False if it proceeded unbudgeted.
    """
    if budget <= 0:
        yield False
        return
    d = slot_dir(endpoint)
    d.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + wait_timeout
    while True:
        for i in range(budget):
            fd = os.open(d / f"slot-{i}.lock", os.O_CREAT | os.O_RDWR, 0o644)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                os.close(fd)  # taken by another worker; try the next slot
                continue
            try:
                yield True
                return
            finally:
                # flock releases on close, and on process death — which is the
                # whole reason this is a file lock.
                os.close(fd)
        if time.monotonic() > deadline:
            log.warning(
                "embed budget: no slot for %.0fs (budget=%d) — proceeding unbudgeted",
                wait_timeout, budget,
            )
            yield False
            return
        time.sleep(poll)


class BudgetedEmbedder(Embedder):
    """Wraps any Embedder so each batch holds a global slot.

    A decorator rather than a change inside HttpEmbedder: the budget is a
    property of *how the embedder is used* (bulk vs live query), not of the
    transport, and CLAUDE.md requires everything keep flowing through this
    interface.
    """

    def __init__(self, inner: Embedder, endpoint: str, budget: int) -> None:
        self.inner = inner
        self.endpoint = endpoint
        self.budget = budget
        self.model_id = inner.model_id
        self.dim = inner.dim

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        with embed_slot(self.endpoint, self.budget):
            return self.inner.embed_batch(texts)

    def ping(self) -> bool:
        return self.inner.ping()  # health checks must not queue behind a backfill

    def close(self) -> None:
        self.inner.close()  # the budget wrapper holds no resources; the inner does
