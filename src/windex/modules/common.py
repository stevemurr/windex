"""Shared mechanics for non-root recipe module runners."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from psycopg import sql
from psycopg.types.json import Jsonb

from windex.recipe import wire
from windex.recipe.ports import RawBlob
from windex.worker.protocol import PermanentTaskError, TaskContext

T = TypeVar("T", bound=wire.WireValue)


@dataclass(frozen=True)
class InputItem:
    """One value from one upstream unit, with a replay-stable lineage key."""

    key: str
    value: wire.WireValue


def pending_inputs(ctx: TaskContext, *, limit: int) -> tuple[list[InputItem], bool]:
    """Read unconsumed upstream values from all dependency tasks.

    ``task_units.outputs`` is an array because one input may fan into many
    records/documents. The key includes upstream task, unit and array ordinal, so
    two fan-in branches cannot collide and a slice replay sees what it committed.
    """
    query = sql.SQL(
        """
        WITH upstream AS (
            SELECT p.id AS upstream_task_id,
                   u.id AS upstream_unit_id,
                   out.ordinality AS output_ordinal,
                   out.value AS value,
                   p.id::text || ':' || u.id::text || ':' ||
                       out.ordinality::text AS lineage
              FROM run_tasks current
              CROSS JOIN LATERAL unnest(current.depends_on) AS dep(node)
              JOIN run_tasks p
                ON p.run_id = current.run_id AND p.node = dep.node
              JOIN task_units u
                ON u.task_id = p.id AND u.state = 'done'
              CROSS JOIN LATERAL
                   jsonb_array_elements(u.outputs) WITH ORDINALITY AS out(value, ordinality)
             WHERE current.id = %s
        )
        SELECT lineage, value
          FROM upstream i
         WHERE NOT EXISTS (
               SELECT 1 FROM task_units consumed
                WHERE consumed.task_id = %s AND consumed.unit_key = i.lineage)
         ORDER BY upstream_task_id, upstream_unit_id, output_ordinal
         LIMIT %s
        """)
    with ctx.conn.cursor() as cur:
        cur.execute(query, (ctx.task_id, ctx.task_id, limit + 1))
        rows = cur.fetchall()
    items = [InputItem(key=key, value=wire.decode(value))
             for key, value in rows[:limit]]
    return items, len(rows) > limit


def require_type(item: InputItem, expected: type[T], module: str) -> T:
    if not isinstance(item.value, expected):
        raise PermanentTaskError(
            f"{module} received {type(item.value).__name__}, "
            f"expected {expected.__name__}")
    return item.value


def finish_input(ctx: TaskContext, item: InputItem, *,
                 outputs: list[wire.WireValue] | None = None,
                 doc_id: str | None = None) -> None:
    """Record an input and its outputs in the caller's open transaction."""
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, doc_id, finished_at)
            VALUES (%s, %s, %s, 'done', %s, %s, now())
            """,
            (ctx.run_id, ctx.task_id, item.key,
             Jsonb(wire.encode_many(outputs or [])), doc_id),
        )


def blob_bytes(blob: RawBlob) -> bytes:
    """Read a RawBlob regardless of whether its fetcher spooled or inlined it."""
    if blob.body is not None and blob.path is not None:
        raise PermanentTaskError("RawBlob has both body and path")
    if blob.body is not None:
        return blob.body
    if blob.path is not None:
        try:
            return Path(blob.path).read_bytes()
        except OSError as exc:
            raise RuntimeError(f"cannot read fetched artifact {blob.path}: {exc}") from exc
    raise PermanentTaskError("RawBlob has neither body nor path")


def downstream_store(ctx: TaskContext) -> str:
    """The one store written by the collect node immediately downstream."""
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT config->>'store'
              FROM run_tasks
             WHERE run_id = %s AND %s = ANY(depends_on)
               AND kind = 'collect' AND config ? 'store'
            """,
            (ctx.run_id, ctx.node),
        )
        stores = [row[0] for row in cur.fetchall() if row[0]]
    if len(stores) != 1:
        raise PermanentTaskError(
            f"{ctx.module} must feed exactly one configured store, found {stores}")
    return stores[0]
