from windex.config import Settings
from windex.embed.base import Embedder

# Dashboard-selectable throughput profiles (control-table key: embed_profile).
# "polite" keeps the GPU responsive for live queries; "full" drains backlogs.
# embed_global_budget is the fleet-wide in-flight cap (embed/budget.py); the
# other keys are per-process. Before it existed, "polite" could not bound the
# fleet as advertised — 6 jobs x 2 still queued 12 deep at one endpoint.
#
# Measured 2026-07-17 against a VERIFIED-idle endpoint (loops killed, zero
# connections confirmed by lsof — not the flag, which only pauses at pass
# boundaries and left an earlier probe reading a flood as "idle"):
#   single embed 0.12-0.21s, a 16-doc batch 0.40s. The endpoint is fast.
#
# Under load, throughput is FLAT across in-flight depth: budget 4 -> 6.9 docs/s,
# budget 8 -> 8.5, ~48 (unbounded) -> 9.6. The GPU saturates around 8; past that
# every extra request is pure queue, and enough of it collapses the endpoint —
# that is what windex did to the box tonight (unbounded in-flight plus a retry
# storm, each timeout retrying and adding load: congestion collapse).
#
# So concurrency past saturation buys nothing, and batch size is the lever that
# matters for latency: a slot holds one request's worth of GPU time, and real
# docs are up to embed_max_tokens (2048) each, so batch 32 parks ~10x more work
# in front of a live query than the 200-token probe above suggests. Batch 8 at
# budget 8 = 64 docs in flight instead of 256, for the same saturated GPU.
#
# Live queries are never budgeted (index/search.py), but they still queue behind
# whatever a slot is already chewing — which is why batch size, not budget, is
# what gets a query under its 8s deadline while indexing runs.
# "full" sat at the measured knee (12) until the gateway grew per-key tiers:
# the bulk key now hard-caps at 6 concurrent and 429s the rest, so 6 is the
# binding limit (and the flat throughput curve above says it costs little).
PROFILES = {
    "polite": {"embed_concurrency": 2, "embed_batch_size": 8,
               "embed_throttle_seconds": 1.0, "embed_global_budget": 4},
    "full": {"embed_concurrency": 8, "embed_batch_size": 8,
             "embed_throttle_seconds": 0.0, "embed_global_budget": 6},
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
    """bulk=True signs with the gateway's bulk key and wraps the embedder in
    the fleet-wide budget (embed/budget.py). Live queries pass bulk=False:
    they sign with the interactive key (uncapped, queues instead of 429ing)
    and never take a budget slot, so a search never queues behind a backfill —
    that asymmetry is the point."""
    inner = _build_inner(settings, timeout, bulk)
    if bulk and settings.embed_global_budget > 0:
        from windex.embed.budget import BudgetedEmbedder

        return BudgetedEmbedder(inner, settings.embed_endpoint, settings.embed_global_budget)
    return inner


def _build_inner(settings: Settings, timeout: float | None = None,
                 bulk: bool = False) -> Embedder:
    if settings.embed_backend == "st":
        from windex.embed.st import SentenceTransformersEmbedder

        return SentenceTransformersEmbedder(settings.embed_model, settings.embed_dim)

    from windex.embed.http import HttpEmbedder

    style = "tei" if settings.embed_backend == "http-tei" else "openai"
    tier_key = settings.embed_bulk_api_key if bulk else settings.embed_query_api_key
    return HttpEmbedder(
        endpoint=settings.embed_endpoint,
        model_id=settings.embed_model,
        dim=settings.embed_dim,
        style=style,
        api_key=tier_key or settings.embed_api_key,
        **({"timeout": timeout, "retries": 1} if timeout else {}),
    )
