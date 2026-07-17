from windex.config import Settings
from windex.embed.base import Embedder

# Dashboard-selectable throughput profiles (control-table key: embed_profile).
# "polite" keeps the GPU responsive for live queries; "full" drains backlogs.
# embed_global_budget is the fleet-wide in-flight cap (embed/budget.py); the
# other keys are per-process. Before it existed, "polite" could not bound the
# fleet as advertised — 6 jobs x 2 still queued 12 deep at one endpoint.
#
# These numbers are PROVISIONAL and conservative, pending a valid measurement.
#
# History worth keeping, because it nearly got written in as fact: on 2026-07-17
# a probe "with indexing paused and the queue drained" returned 28s for a single
# two-word embed, and that was read as proof the endpoint had a per-request
# latency floor no budget could fix. The measurement was invalid. `indexing`
# pauses at PASS boundaries, so in-flight passes keep draining their queued work
# for minutes afterward (observed: connections fell 21 -> 6 over two minutes) —
# the endpoint was still flooded when it was probed as "idle". A truly idle
# endpoint was never measured.
#
# Ground truth came from the box: the endpoint is FAST, and windex flooded it
# into a stall. Unbounded in-flight work (7 loops x 8 concurrency) plus a retry
# storm — every timeout raising and retrying, adding load — is textbook
# congestion collapse. So this budget is the fix, not a consolation prize, and
# the queueing argument that motivated it was right.
#
# To pause for real, kill the loops; the flag alone is the polite stop.
# Re-measure against a genuinely idle endpoint (processes stopped, zero
# connections to the endpoint, verified) before raising these. Slots are keyed
# per endpoint, so a second server gets its own budget.
PROFILES = {
    "polite": {"embed_concurrency": 2, "embed_batch_size": 16,
               "embed_throttle_seconds": 1.0, "embed_global_budget": 4},
    "full": {"embed_concurrency": 8, "embed_batch_size": 32,
             "embed_throttle_seconds": 0.0, "embed_global_budget": 8},
}


def with_runtime_profile(conn, settings: Settings) -> Settings:
    """Overlay the dashboard-set embed profile onto settings. Read at the start
    of every embed pass, so switching applies within a minute — no restarts.
    'env' (or unset) means the .env values stand."""
    from windex import db

    overrides = PROFILES.get(db.get_control(conn, "embed_profile", "env"))
    return settings.model_copy(update=overrides) if overrides else settings


def build_embedder(settings: Settings, timeout: float | None = None,
                   bulk: bool = False) -> Embedder:
    """bulk=True wraps the embedder in the fleet-wide budget (embed/budget.py).
    Live queries pass bulk=False so a search never queues behind a backfill —
    that asymmetry is the point."""
    inner = _build_inner(settings, timeout)
    if bulk and settings.embed_global_budget > 0:
        from windex.embed.budget import BudgetedEmbedder

        return BudgetedEmbedder(inner, settings.embed_endpoint, settings.embed_global_budget)
    return inner


def _build_inner(settings: Settings, timeout: float | None = None) -> Embedder:
    if settings.embed_backend == "st":
        from windex.embed.st import SentenceTransformersEmbedder

        return SentenceTransformersEmbedder(settings.embed_model, settings.embed_dim)

    from windex.embed.http import HttpEmbedder

    style = "tei" if settings.embed_backend == "http-tei" else "openai"
    return HttpEmbedder(
        endpoint=settings.embed_endpoint,
        model_id=settings.embed_model,
        dim=settings.embed_dim,
        style=style,
        api_key=settings.embed_api_key,
        **({"timeout": timeout, "retries": 1} if timeout else {}),
    )
