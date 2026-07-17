"""Circuit breaker for the *query* embedding round trip.

Why this lives in `index/` and not `embed/`: the `Embedder` interface is shared
with the bulk embed pipeline, and the pipeline is *supposed* to hammer the GPU —
it's the binding constraint by design. A breaker inside `embed/` would trip the
backfill it is meant to protect. So the breaker wraps the one call site that is
a live user query (index/search.py) and nothing else.

The problem it solves: while a backfill saturates the embedding server, every
hybrid search pays the full `embed_query_timeout` (8s) plus HttpEmbedder's 1s
retry backoff before degrading to lexical — measured p95 9171ms, of which
Qdrant's own work is 44-79ms. The outcome (lexical results) was predictable from
the previous request. After `embed_breaker_threshold` consecutive failures we
skip the dense leg for `embed_breaker_cooldown` seconds and serve lexical
immediately (~50ms), which also stops doomed queries from queuing on the GPU the
pipeline is competing for.

State machine (standard, single probe):
    closed    -> normal; every query embeds. N consecutive failures -> open.
    open      -> dense skipped, searches degrade immediately. After the cooldown
                 the next query becomes the half-open probe.
    half_open -> exactly one probe in flight; everyone else still short-circuits
                 so a recovering GPU gets one request, not a thundering herd.
                 Probe succeeds -> closed (hybrid restored). Fails -> open again.

Recovery is automatic and unconditional: `open` is only ever entered with a
timestamp, and the cooldown always expires, so the breaker cannot latch open —
when the backfill ends, the next probe closes it with no restart.
"""

import threading
import time

from windex.config import Settings

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"


class EmbedBreakerOpen(RuntimeError):
    """An explicit `mode=dense` query arrived while the breaker was open.

    Subclasses RuntimeError so it surfaces exactly like the embedder's own
    failure did before the breaker existed: a dense request that cannot embed
    fails loudly and never silently returns lexical results.
    """


class QueryEmbedBreaker:
    """Thread-safe breaker state.

    Locking rule: the lock is taken only to read/mutate the counters, never held
    across the embed call or the Qdrant query. Concurrent searches therefore
    still run fully in parallel — they contend for a handful of nanoseconds of
    bookkeeping, not for the network round trip.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = CLOSED
        self._failures = 0          # consecutive; reset by any success
        self._opened_at = 0.0
        self._probe_started_at: float | None = None
        self._last_error: str | None = None
        self._trips = 0             # closed -> open transitions since start
        self._short_circuits = 0    # queries that skipped a doomed embed

    def allow(self, settings: Settings) -> bool:
        """True if this caller should attempt the query embed.

        A True return in the open/half-open state means the caller has taken the
        probe slot and MUST report back via record_success/record_failure.
        """
        threshold = settings.embed_breaker_threshold
        if threshold <= 0:
            return True  # breaker disabled: preserve pre-breaker behavior exactly
        cooldown = settings.embed_breaker_cooldown
        now = time.monotonic()
        with self._lock:
            if self._state == CLOSED:
                return True
            if self._state == OPEN:
                if now - self._opened_at < cooldown:
                    self._short_circuits += 1
                    return False
                self._state = HALF_OPEN  # cooldown elapsed: this caller probes
                self._probe_started_at = now
                return True
            # HALF_OPEN: one probe at a time. A probe older than the cooldown was
            # abandoned (its thread died without reporting) — hand the slot to
            # this caller rather than let the breaker wedge half-open forever.
            if self._probe_started_at is None or now - self._probe_started_at >= cooldown:
                self._probe_started_at = now
                return True
            self._short_circuits += 1
            return False

    def record_success(self) -> None:
        """A query embed returned a vector: dense works, close the breaker.

        A slow-but-successful embed is deliberately NOT counted as a failure.
        It delivered a full-quality hybrid result inside the deadline, and the
        deadline is what defines "too slow" — counting successes toward the
        threshold would drop the dense leg while it was still working.
        """
        with self._lock:
            self._state = CLOSED
            self._failures = 0
            self._probe_started_at = None
            self._last_error = None

    def record_failure(self, exc: BaseException, settings: Settings) -> None:
        """A query embed failed. Timeout and connection-refused get identical
        treatment on purpose: the caller's outcome is the same either way (no
        dense vector -> lexical), both cost real wall time (even a refused
        connection eats HttpEmbedder's 1s retry backoff), and splitting them
        would buy a second threshold knob for no behavioral difference. The
        distinction is kept where it's actually useful — `last_error` in the
        snapshot, for diagnosing *why* dense is down.
        """
        if settings.embed_breaker_threshold <= 0:
            return
        with self._lock:
            self._last_error = f"{type(exc).__name__}: {exc}"[:200]
            if self._state == HALF_OPEN:
                self._state = OPEN  # probe failed: still down, wait another cooldown
                self._opened_at = time.monotonic()
                self._probe_started_at = None
                return
            self._failures += 1
            if self._state == CLOSED and self._failures >= settings.embed_breaker_threshold:
                self._state = OPEN
                self._opened_at = time.monotonic()
                self._trips += 1

    def snapshot(self, settings: Settings) -> dict:
        """Observable state for /v1/stats. Cheap enough for the SSE tick."""
        with self._lock:
            retry_in = 0.0
            if self._state == OPEN:
                retry_in = max(
                    0.0, settings.embed_breaker_cooldown - (time.monotonic() - self._opened_at)
                )
            return {
                "state": self._state,
                "consecutive_failures": self._failures,
                "trips": self._trips,
                "short_circuited": self._short_circuits,
                "retry_in_s": round(retry_in, 1),
                "last_error": self._last_error,
            }

    def reset(self) -> None:
        """Back to a cold closed breaker (tests; process-global state)."""
        with self._lock:
            self._state = CLOSED
            self._failures = 0
            self._opened_at = 0.0
            self._probe_started_at = None
            self._last_error = None
            self._trips = 0
            self._short_circuits = 0


# One breaker per process: it models the health of the one embedding endpoint
# this API serves, and must be shared across FastAPI worker threads to be worth
# anything (per-request state would never see two failures in a row).
breaker = QueryEmbedBreaker()
