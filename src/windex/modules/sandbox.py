"""Client for the dedicated rootless custom-Module executor."""

from __future__ import annotations

from typing import Any

import httpx

from windex.config import Settings


def execute(
    *,
    source: str,
    records: list[dict[str, Any]],
    config: dict[str, Any],
    limits: dict[str, Any],
    settings: Settings | None = None,
) -> dict[str, Any]:
    active = settings or Settings()
    response = httpx.post(
        active.module_sandbox_url.rstrip("/") + "/v1/execute",
        json={
            "runtime": "python",
            "source": source,
            "records": records,
            "config": config,
            "limits": limits,
        },
        timeout=float(limits.get("wall_seconds", 10)) + 5,
    )
    response.raise_for_status()
    result = response.json()
    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "custom Module failed")
    return result


__all__ = ["execute"]


def custom_runner(ctx):
    """Execute an approved version through the isolated JSONL service."""
    from windex.modules.common import finish_batch, pending_batches
    from windex.pipeline import ports
    from windex.pipeline import wire
    from windex.worker.protocol import PermanentTaskError, SliceResult

    with ctx.conn.cursor() as cur:
        cur.execute(
            """SELECT v.source, v.source_digest, v.resource_limits,
                      v.approval_state
                 FROM module_versions v
                 JOIN module_definitions d ON d.id = v.module_id
                WHERE d.name = %s AND v.version = %s""",
            (ctx.module, int(ctx.module_version)),
        )
        row = cur.fetchone()
    if row is None or row[3] != "available":
        raise PermanentTaskError("module_revoked")
    source, digest, limits, _state = row
    if digest != ctx.module_digest:
        raise PermanentTaskError("Module digest does not match the frozen Run")
    batches, more = pending_batches(ctx, limit=100)
    done = 0
    for batch in batches:
        result = execute(
            source=source,
            records=[wire.encode(value) for value in batch.values],
            config=ctx.config,
            limits=limits,
        )
        try:
            expected = ports.KINDS[ctx.kind].out
            if any(value.get("type") != expected for value in result["outputs"]):
                raise ValueError(
                    f"expected only {expected} outputs for {ctx.kind}")
            outputs = [wire.decode(value) for value in result["outputs"]]
        except (KeyError, ValueError, TypeError) as exc:
            raise PermanentTaskError(
                f"custom Module returned invalid wire values: {exc}") from exc
        finish_batch(ctx, batch, outputs=outputs)
        done += 1
        if ctx.should_yield():
            break
    ctx.conn.commit()
    if done:
        ctx.heartbeat(done, 0, {"custom_module": ctx.module})
    return SliceResult(
        units_done=done, exhausted=not more and done == len(batches),
        stats={"batches": done})


__all__.append("custom_runner")
