from windex.config import Settings
from windex.embed.base import Embedder

# Dashboard-selectable throughput profiles (control-table key: embed_profile).
# "polite" keeps the GPU responsive for live queries; "full" drains backlogs.
# embed_global_budget is the fleet-wide in-flight cap (embed/budget.py); the
# other keys are per-process. Before it existed, "polite" could not bound the
# fleet as advertised — 6 jobs x 2 still queued 12 deep at one endpoint.
#
# These numbers are PROVISIONAL, and deliberately not derived from the queueing
# argument that motivated the budget. Measured 2026-07-17 against the live
# endpoint: with indexing PAUSED and the queue fully drained, a single two-word
# embed still took 28s. So its latency is per-request, not queue depth, and no
# budget can bring a query embed under the 8s deadline while that holds — the
# breaker (index/embed_breaker.py) is what makes that survivable, not this.
# Throughput, meanwhile, still rises with concurrency (48 in-flight -> 9.6
# docs/s; 8 in-flight -> 3.8), so a tight cap costs backlog speed and buys
# nothing today. `full` is therefore set to bound the fleet only enough to stay
# off the cliff where requests time out and jobs die, not to protect a query
# latency that is unreachable anyway. Re-tune once the endpoint serves a single
# embed in ~ms (and once the second endpoint lands — slots are keyed per
# endpoint, so each gets its own budget).
PROFILES = {
    "polite": {"embed_concurrency": 2, "embed_batch_size": 16,
               "embed_throttle_seconds": 1.0, "embed_global_budget": 4},
    "full": {"embed_concurrency": 8, "embed_batch_size": 32,
             "embed_throttle_seconds": 0.0, "embed_global_budget": 32},
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
