"""Platform-owned asynchronous Source corpus reset."""

from __future__ import annotations

from qdrant_client import QdrantClient
from qdrant_client import models as qm

from windex.config import Settings
from windex.index import qdrant as qidx
from windex.pipeline.events import append
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext


def _delete_vectors(ctx: TaskContext) -> None:
    client = QdrantClient(url=Settings().qdrant_url, timeout=120)
    try:
        collection = qidx.alias_name(ctx.collection_key)
        if not client.collection_exists(collection):
            return
        client.delete(
            collection_name=collection,
            points_selector=qm.FilterSelector(filter=qm.Filter(
                must=[qm.FieldCondition(
                    key="source", match=qm.MatchValue(value=ctx.search_name))],
            )),
            wait=True,
        )
    finally:
        client.close()


def platform_reset(ctx: TaskContext) -> SliceResult:
    if ctx.source_id is None or not ctx.collection_key:
        raise PermanentTaskError(
            "platform.reset requires a frozen Source deployment binding")

    # Reset is admitted while the Source is paused. Existing leased work gets a
    # chance to observe that pause and yield before any state is removed.
    with ctx.conn.cursor() as cur:
        cur.execute(
            """SELECT count(*) FROM run_tasks
                WHERE source_id = %s AND id <> %s AND state = 'running'""",
            (ctx.source_id, ctx.task_id),
        )
        active = cur.fetchone()[0]
    ctx.conn.commit()
    if active:
        return SliceResult(
            exhausted=False, stats={"waiting_for_running_tasks": active})

    _delete_vectors(ctx)
    with ctx.conn.cursor() as cur:
        cur.execute(
            """UPDATE run_tasks
                  SET state = 'cancelled', finished_at = now(),
                      error = 'cancelled by corpus reset'
                WHERE source_id = %s AND id <> %s
                  AND state IN ('pending','ready','blocked')
                RETURNING id, run_id, node, module""",
            (ctx.source_id, ctx.task_id),
        )
        cancelled_tasks = cur.fetchall()
        for task_id, run_id, node, module in cancelled_tasks:
            append(
                cur, component="source", event="task.cancelled", level="warn",
                source_name=ctx.source_name, run_id=run_id, task_id=task_id,
                node=node, module=module, message="cancelled by corpus reset",
            )
        cur.execute(
            """UPDATE runs
                  SET state = 'cancelled', cancel_requested = true,
                      finished_at = coalesce(finished_at, now()),
                      updated_at = now(), error = 'cancelled by corpus reset'
                WHERE source_id = %s AND id <> %s
                  AND state IN ('queued','running','blocked')
                RETURNING id, pipeline_name, pipeline_version""",
            (ctx.source_id, ctx.run_id),
        )
        cancelled_runs = cur.fetchall()
        for run_id, pipeline_name, version in cancelled_runs:
            append(
                cur, component="source", event="run.cancelled", level="warn",
                source_name=ctx.source_name, pipeline_name=pipeline_name,
                pipeline_version=version, run_id=run_id,
                message="cancelled by corpus reset",
            )
        cur.execute(
            "DELETE FROM source_units WHERE source_id = %s", (ctx.source_id,))
        cur.execute(
            "DELETE FROM minhash_bands WHERE source_id = %s", (ctx.source_id,))
        cur.execute("DELETE FROM repos WHERE source_id = %s", (ctx.source_id,))
        cur.execute("DELETE FROM documents WHERE source_id = %s", (ctx.source_id,))
        cur.execute(
            """UPDATE sources
                  SET generation = generation + 1, updated_at = now()
                WHERE id = %s RETURNING generation""",
            (ctx.source_id,),
        )
        generation = cur.fetchone()[0]
        cur.execute(
            """UPDATE source_control
                  SET paused = %s, pause_reason = %s,
                      paused_at = CASE WHEN %s THEN paused_at ELSE NULL END,
                      updated_at = now()
                WHERE source_id = %s""",
            (
                bool(ctx.config.get("was_paused")),
                str(ctx.config.get("pause_reason") or ""),
                bool(ctx.config.get("was_paused")),
                ctx.source_id,
            ),
        )
        append(
            cur, component="source", event="source.reset_completed",
            source_name=ctx.source_name, pipeline_name=ctx.pipeline_name,
            pipeline_version=ctx.pipeline_version, run_id=ctx.run_id,
            task_id=ctx.task_id, module=ctx.module,
            data={
                "generation": generation,
                "cancelled_tasks": len(cancelled_tasks),
                "cancelled_runs": len(cancelled_runs),
            },
        )
    ctx.conn.commit()
    return SliceResult(
        units_done=1, units_total=1, exhausted=True,
        stats={"generation": generation, "corpus_cleared": True},
    )


__all__ = ["platform_reset"]
