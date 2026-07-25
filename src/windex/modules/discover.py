"""Generic discover-node implementations.

Discover runners are roots: they create durable ``WorkUnit`` values rather than
consuming another task's output. They never advance ``source_units.ingested``;
only a clean terminal load may do that.
"""

from __future__ import annotations

import csv
import io
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from typing import Any

from psycopg import sql
from psycopg.types.json import Jsonb

from windex.crawl.scope import canonicalize
from windex.recipe.ports import PartitionRef, WorkUnit
from windex.recipe.wire import encode_many
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext


def _payload(raw: Any) -> dict[str, str]:
    if raw in (None, ""):
        return {}
    if not isinstance(raw, str):
        raise PermanentTaskError("static.once payload must be a comma-separated string")
    try:
        parts = next(csv.reader(io.StringIO(raw), skipinitialspace=True))
    except csv.Error as exc:
        raise PermanentTaskError(f"static.once payload is invalid CSV: {exc}") from exc
    result: dict[str, str] = {}
    for part in parts:
        key, sep, value = part.partition("=")
        key = key.strip()
        if not sep or not key:
            raise PermanentTaskError(
                f"static.once payload item {part!r} must be key=value")
        result[key] = value.strip()
    return result


def static_once(ctx: TaskContext) -> SliceResult:
    """Emit one stable work unit, exactly once even after a slice replay."""
    key = str(ctx.config.get("key", "once"))
    with ctx.conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM task_units WHERE task_id = %s AND unit_key = %s LIMIT 1",
            (ctx.task_id, key),
        )
        if cur.fetchone() is not None:
            ctx.conn.commit()
            return SliceResult(exhausted=True, units_total=1)
        unit = WorkUnit(
            ref=PartitionRef(store="", key=key),
            payload=_payload(ctx.config.get("payload", "")),
            epoch=ctx.run_id,
        )
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, finished_at)
            VALUES (%s, %s, %s, 'done', %s, now())
            """,
            (ctx.run_id, ctx.task_id, key, Jsonb(encode_many([unit]))),
        )
    ctx.conn.commit()
    ctx.heartbeat(1, 0, {"last": key})
    return SliceResult(
        units_done=1,
        exhausted=True,
        units_total=1,
        stats={"emitted": 1},
    )


def _task_unit(ctx: TaskContext, key: str, values: list[WorkUnit]) -> None:
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, finished_at)
            VALUES (%s, %s, %s, 'done', %s, now())
            """,
            (ctx.run_id, ctx.task_id, key, Jsonb(encode_many(values))),
        )


_ORDER = {
    "key": sql.SQL("u.unit_key"),
    "ord": sql.SQL("u.ord NULLS LAST, u.unit_key"),
    "processed_at": sql.SQL("u.processed_at NULLS FIRST, u.unit_key"),
    "stars_desc": sql.SQL(
        "coalesce((u.attrs->>'stars')::bigint, 0) DESC, u.unit_key"),
}


def _predicate(config: dict) -> tuple[sql.Composable, list[Any]]:
    name = config.get("predicate", "token_moved")
    if name == "unseen":
        return sql.SQL("u.ingested IS NULL"), []
    if name == "token_moved":
        return sql.SQL("u.upstream IS DISTINCT FROM u.ingested"), []
    if name == "stage_in":
        stages = [s.strip() for s in str(config.get("stages", "")).split(",") if s.strip()]
        if not stages:
            raise PermanentTaskError("state.pending stage_in requires at least one stage")
        return sql.SQL("u.stage = ANY(%s)"), [stages]
    if name == "rearm":
        days = int(config.get("rearm_days", 7))
        return (
            sql.SQL(
                "(u.ingested IS NULL OR u.processed_at IS NULL "
                "OR u.processed_at < now() - (%s * interval '1 day'))"),
            [days],
        )
    if name == "rotate":
        return sql.SQL("TRUE"), []
    raise PermanentTaskError(f"state.pending has unknown predicate {name!r}")


def state_pending(ctx: TaskContext) -> SliceResult:
    """Select one bounded slice of pending permanent store rows.

    The task's own emitted unit keys are the run-local snapshot. Replaying after
    a commit excludes those rows, while a later run may select them again until
    a successful load advances ``ingested``.
    """
    store = str(ctx.config.get("store", ""))
    if not store:
        raise PermanentTaskError("state.pending requires a store")
    source = ctx.recipe or ctx.source
    batch = int(ctx.config.get("batch", 50))
    if source == "gh" and store == "gh_shards":
        threshold = int(ctx.params.get("star_threshold", 10))
        first, last = "2008-01-01", date.today().isoformat()
        key = f"{first}..{last}@{threshold}"
        with ctx.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO source_units
                       (source, store, unit_key, ord, upstream, attrs)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, store, unit_key) DO NOTHING
                """,
                (
                    source, store, key, first,
                    Jsonb({"from": first, "to": last,
                           "star_threshold": threshold}),
                    Jsonb({"from": first, "to": last,
                           "star_threshold": threshold}),
                ),
            )
    order_name = str(ctx.config.get("order", "ord"))
    order = _ORDER.get(order_name)
    if order is None:
        raise PermanentTaskError(f"state.pending has unknown order {order_name!r}")
    pending, args = _predicate(ctx.config)
    claim = str(ctx.config.get("claim", "none"))
    if claim not in ("none", "lease"):
        raise PermanentTaskError(f"state.pending has unknown claim policy {claim!r}")
    stale = int(ctx.config.get("stale_minutes", 60))
    anchor_filter = sql.SQL("")
    anchor_args: list[Any] = []
    raw_anchors = ctx.params.get("anchor_ids")
    if source == "hf" and raw_anchors:
        anchors = (
            [value.strip() for value in raw_anchors.split(",")]
            if isinstance(raw_anchors, str) else
            [str(value).strip() for value in raw_anchors]
        )
        if store == "post":
            keys = [
                value[len("hf:blog/"):]
                for value in anchors if value.startswith("hf:blog/")
            ]
        elif store == "root":
            keys = sorted({
                "/".join(value[len("hf:"):].split("/")[:2])
                for value in anchors
                if value.startswith(("hf:docs/", "hf:learn/"))
            })
        else:
            keys = []
        anchor_filter = sql.SQL("AND u.unit_key = ANY(%s)")
        anchor_args.append(keys)
    lease = (
        sql.SQL(
            "AND (u.status <> 'processing' OR u.claimed_at IS NULL "
            "OR u.claimed_at < now() - (%s * interval '1 minute'))")
        if claim == "lease" else sql.SQL("")
    )
    query = sql.SQL(
        """
        SELECT u.unit_key, u.upstream, u.attempts, u.attrs
          FROM source_units u
         WHERE u.source = %s AND u.store = %s
           AND {pending}
           {lease}
           {anchor_filter}
           AND NOT EXISTS (
                 SELECT 1 FROM task_units t
                  WHERE t.task_id = %s AND t.unit_key = u.unit_key)
         ORDER BY {order}
         LIMIT %s
        """).format(
            pending=pending,
            lease=lease,
            anchor_filter=anchor_filter,
            order=order,
        )
    params: list[Any] = [source, store, *args]
    if claim == "lease":
        params.append(stale)
    params.extend(anchor_args)
    params.extend([ctx.task_id, batch + 1])

    with ctx.conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        selected = []
        for key, upstream, attempts, attrs in rows[:batch]:
            attrs = dict(attrs or {})
            unit = WorkUnit(
                ref=PartitionRef(
                    store=store,
                    key=key,
                    id_scope=attrs.get("id_scope"),
                ),
                payload=attrs,
                upstream=dict(upstream or {}),
                attempt=int(attempts),
                epoch=ctx.run_id,
            )
            cur.execute(
                """
                INSERT INTO task_units
                       (run_id, task_id, unit_key, state, outputs, finished_at)
                VALUES (%s, %s, %s, 'done', %s, now())
                """,
                (ctx.run_id, ctx.task_id, key, Jsonb(encode_many([unit]))),
            )
            selected.append((key, upstream, attempts, attrs))
            if ctx.should_yield():
                break
        if selected:
            keys = [row[0] for row in selected]
            if claim == "lease":
                cur.execute(
                    """
                    UPDATE source_units
                       SET status = 'processing', claimed_at = now(),
                           last_run_id = %s, updated_at = now()
                     WHERE source = %s AND store = %s AND unit_key = ANY(%s)
                    """,
                    (ctx.run_id, source, store, keys),
                )
            else:
                cur.execute(
                    """
                    UPDATE source_units
                       SET last_run_id = %s, updated_at = now()
                     WHERE source = %s AND store = %s AND unit_key = ANY(%s)
                    """,
                    (ctx.run_id, source, store, keys),
                )
    ctx.conn.commit()
    done = len(selected)
    if done:
        ctx.heartbeat(done, 0, {"last": selected[-1][0], "store": store})
    return SliceResult(
        units_done=done,
        exhausted=len(selected) == len(rows),
        stats={"emitted": done, "store": store},
    )


def state_repos_pending(ctx: TaskContext) -> SliceResult:
    """Select candidate/hydrated rows from GitHub's indexed wide-table adapter."""
    store = str(ctx.config.get("store", "repos"))
    stages = [
        value.strip()
        for value in str(ctx.config.get("stages", "candidate")).split(",")
        if value.strip()
    ]
    if not stages:
        raise PermanentTaskError("state.repos_pending requires at least one stage")
    order_name = str(ctx.config.get("order", "stars_desc"))
    orders = {
        "stars_desc": sql.SQL("coalesce(r.stars, 0) DESC, r.repo_id"),
        "star_events_desc": sql.SQL(
            "coalesce(r.star_events, 0) DESC, coalesce(r.stars, 0) DESC, r.repo_id"),
    }
    order = orders.get(order_name)
    if order is None:
        raise PermanentTaskError(
            f"state.repos_pending has unknown order {order_name!r}")
    batch = int(ctx.config.get("batch", 40))
    overall = int(ctx.config.get("limit", 100_000))
    minimum = int(ctx.config.get("min_star_events", 0))

    with ctx.conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM task_units WHERE task_id = %s",
            (ctx.task_id,),
        )
        already = int(cur.fetchone()[0])
        room = max(0, min(batch, overall - already))
        if room:
            query = sql.SQL(
                """
                SELECT r.repo_id, r.full_name, r.stars, r.star_events,
                       r.description, r.topics, r.primary_language,
                       r.default_branch, r.pushed_at, r.readme_fetched_at,
                       r.status
                  FROM repos r
                 WHERE r.status = ANY(%s)
                   AND coalesce(r.star_events, 0) >= %s
                   AND NOT EXISTS (
                         SELECT 1 FROM task_units t
                          WHERE t.task_id = %s
                            AND t.unit_key = r.repo_id::text)
                 ORDER BY {order}
                 LIMIT %s
                """).format(order=order)
            cur.execute(
                query,
                (stages, minimum, ctx.task_id, room + 1),
            )
            rows = cur.fetchall()
        else:
            rows = []

    selected = []
    for row in rows[:room]:
        (repo_id, full_name, stars, star_events, description, topics,
         language, branch, pushed_at, readme_at, status) = row
        payload = {
            "repo_id": int(repo_id),
            "full_name": full_name,
            "stars": stars,
            "star_events": star_events,
            "description": description,
            "topics": list(topics or []),
            "primary_language": language,
            "default_branch": branch,
            "pushed_at": pushed_at.isoformat() if pushed_at else None,
            "readme_fetched_at": readme_at.isoformat() if readme_at else None,
            "status": status,
        }
        unit = WorkUnit(
            ref=PartitionRef(store=store, key=str(repo_id)),
            payload=payload,
            epoch=ctx.run_id,
        )
        _task_unit(ctx, str(repo_id), [unit])
        selected.append(str(repo_id))
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(selected)
    if done:
        ctx.heartbeat(done, 0, {"last": selected[-1], "store": store})
    exhausted = already + done >= overall or len(rows) <= room
    return SliceResult(
        units_done=done,
        exhausted=exhausted,
        units_total=overall if already + len(rows) > overall else -1,
        stats={"emitted": done, "store": store},
    )


def _calendar_keys(unit: str, trailing_days: int, pattern: str,
                   today: date) -> list[str]:
    start = today - timedelta(days=trailing_days)
    dates = [start + timedelta(days=n) for n in range((today - start).days)]
    if unit == "hour":
        return [
            day.strftime(pattern.replace("{h}", str(hour)))
            for day in dates for hour in range(24)
        ]
    if unit == "day":
        return [day.strftime(pattern or "%Y-%m-%d") for day in dates]
    if unit == "month":
        return sorted({
            day.strftime(pattern or "%Y-%m") for day in dates
        })
    if unit == "year":
        return sorted({
            day.strftime(pattern or "%Y") for day in dates
        })
    raise PermanentTaskError(f"time.calendar has unknown unit {unit!r}")


def time_calendar(ctx: TaskContext) -> SliceResult:
    """Seed and emit immutable calendar partitions not yet ingested."""
    unit = str(ctx.config.get("unit", "day"))
    trailing = int(ctx.config.get("trailing_days", 2))
    store = str(ctx.config.get("into", ""))
    if not store:
        raise PermanentTaskError("time.calendar requires into")
    pattern = str(ctx.config.get("format", ""))
    keys = _calendar_keys(unit, trailing, pattern, date.today())
    source = ctx.recipe or ctx.source
    with ctx.conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO source_units
                   (source, store, unit_key, ord, upstream, attrs)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, store, unit_key) DO NOTHING
            """,
            [
                (source, store, key, key, Jsonb({"key": key}),
                 Jsonb({"calendar_unit": unit}))
                for key in keys
            ],
        )
        cur.execute(
            """
            SELECT u.unit_key, u.upstream, u.attrs
              FROM source_units u
             WHERE u.source = %s AND u.store = %s
               AND u.unit_key = ANY(%s) AND u.ingested IS NULL
               AND NOT EXISTS (
                     SELECT 1 FROM task_units t
                      WHERE t.task_id = %s AND t.unit_key = u.unit_key)
             ORDER BY u.ord
             LIMIT 501
            """,
            (source, store, keys, ctx.task_id),
        )
        rows = cur.fetchall()
    selected = []
    for key, upstream, attrs in rows[:500]:
        _task_unit(ctx, key, [WorkUnit(
            ref=PartitionRef(store=store, key=key),
            payload=dict(attrs or {}),
            upstream=dict(upstream or {}),
            epoch=ctx.run_id,
        )])
        selected.append(key)
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(selected)
    if done:
        ctx.heartbeat(done, 0, {"last": selected[-1], "store": store})
    return SliceResult(
        units_done=done,
        exhausted=len(rows) <= 500 and done == len(rows),
        units_total=len(keys),
        stats={"emitted": done, "store": store},
    )


def _months(first: date, last: date):
    current = first.replace(day=1)
    while current <= last:
        year, month = current.year, current.month
        yield year, month
        current = (
            date(year + 1, 1, 1) if month == 12
            else date(year, month + 1, 1)
        )


def _window_rows(ctx: TaskContext, today: date) -> list[tuple[str, dict, bool]]:
    unit = str(ctx.config.get("unit", "month"))
    if unit not in {"day", "month", "year"}:
        raise PermanentTaskError(f"time.windows has unknown unit {unit!r}")
    incremental = int(ctx.config.get("incremental_days", 7))
    if ctx.source == "hn":
        floor = date(2006, 10, 1)
        rows = []
        for year, month in _months(floor, today):
            start = datetime(year, month, 1, tzinfo=timezone.utc)
            end = (
                datetime(year + 1, 1, 1, tzinfo=timezone.utc)
                if month == 12 else
                datetime(year, month + 1, 1, tzinfo=timezone.utc)
            )
            frm, until = int(start.timestamp()), int(end.timestamp())
            rows.append((
                f"{frm}..{until}",
                {"from_ts": frm, "until_ts": until},
                False,
            ))
        day0 = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
        frm = int((day0 - timedelta(days=incremental)).timestamp())
        until = int((day0 + timedelta(days=1)).timestamp())
        rows.append((
            f"{frm}..{until}",
            {"from_ts": frm, "until_ts": until},
            True,
        ))
        return rows

    raw_floor = ctx.config.get("earliest") or "2005-09-16"
    floor = date.fromisoformat(str(raw_floor))
    rows = []
    if unit == "month":
        ranges = (
            (
                max(floor, date(year, month, 1)),
                date(year, month, monthrange(year, month)[1]),
            )
            for year, month in _months(floor, today)
        )
    elif unit == "year":
        ranges = (
            (max(floor, date(year, 1, 1)), date(year, 12, 31))
            for year in range(floor.year, today.year + 1)
        )
    else:
        ranges = (
            (floor + timedelta(days=n), floor + timedelta(days=n))
            for n in range((today - floor).days + 1)
        )
    for frm, until in ranges:
        rows.append((
            f"{frm.isoformat()}..{until.isoformat()}",
            {"from": frm.isoformat(), "until": until.isoformat()},
            False,
        ))
    frm = today - timedelta(days=incremental)
    rows.append((
        f"{frm.isoformat()}..{today.isoformat()}",
        {"from": frm.isoformat(), "until": today.isoformat()},
        True,
    ))
    return rows


def time_windows(ctx: TaskContext) -> SliceResult:
    """Plan stable backfill windows plus a rolling re-armed tail."""
    source = ctx.recipe or ctx.source
    store = "window"
    rows = _window_rows(ctx, date.today())
    with ctx.conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO source_units
                   (source, store, unit_key, ord, upstream, attrs)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, store, unit_key) DO UPDATE
               SET upstream = EXCLUDED.upstream,
                   attrs = source_units.attrs || EXCLUDED.attrs,
                   updated_at = now()
            """,
            [
                (source, store, key, key, Jsonb(payload),
                 Jsonb({"rolling": rolling, **payload}))
                for key, payload, rolling in rows
            ],
        )
        cur.execute(
            """
            SELECT u.unit_key, u.upstream, u.attrs
              FROM source_units u
             WHERE u.source = %s AND u.store = %s
               AND (u.ingested IS NULL OR (u.attrs->>'rolling')::boolean)
               AND NOT EXISTS (
                     SELECT 1 FROM task_units t
                      WHERE t.task_id = %s AND t.unit_key = u.unit_key)
             ORDER BY (u.attrs->>'rolling')::boolean, u.ord
             LIMIT 101
            """,
            (source, store, ctx.task_id),
        )
        pending = cur.fetchall()
    selected = []
    for key, upstream, attrs in pending[:100]:
        payload = dict(attrs or {})
        payload.pop("rolling", None)
        _task_unit(ctx, key, [WorkUnit(
            ref=PartitionRef(store=store, key=key),
            payload=payload,
            upstream=dict(upstream or {}),
            epoch=ctx.run_id,
        )])
        selected.append(key)
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(selected)
    if done:
        ctx.heartbeat(done, 0, {"last": selected[-1], "store": store})
    return SliceResult(
        units_done=done,
        exhausted=len(pending) <= 100 and done == len(pending),
        units_total=len(rows),
        stats={"emitted": done, "store": store},
    )


def crawl_frontier(ctx: TaskContext) -> SliceResult:
    """Emit canonical crawl seeds; http.get expands them under the run budget."""
    seeds = ctx.config.get("seeds") or []
    if isinstance(seeds, str):
        seeds = [seeds]
    if not isinstance(seeds, list) or not seeds:
        raise PermanentTaskError("crawl.frontier requires at least one seed")
    store = str(ctx.config.get("store", ""))
    if not store:
        raise PermanentTaskError("crawl.frontier requires store")
    batch = int(ctx.config.get("batch", 50))
    max_pages = int(ctx.config.get("max_pages", 500))
    emitted = []
    for index, raw in enumerate(seeds):
        key = canonicalize(str(raw))
        with ctx.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM task_units WHERE task_id = %s AND unit_key = %s",
                (ctx.task_id, key),
            )
            if cur.fetchone():
                continue
        unit = WorkUnit(
            ref=PartitionRef(store=store, key=key),
            payload={
                "url": key,
                "seed": key,
                "depth": 0,
                "seq": index,
                "max_pages": max_pages,
            },
            epoch=ctx.run_id,
        )
        _task_unit(ctx, key, [unit])
        emitted.append(key)
        if len(emitted) >= batch or ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(emitted)
    if done:
        ctx.heartbeat(done, 0, {"last": emitted[-1], "store": store})
    with ctx.conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM task_units WHERE task_id = %s",
            (ctx.task_id,),
        )
        total = int(cur.fetchone()[0])
    return SliceResult(
        units_done=done,
        exhausted=total >= len(seeds),
        units_total=len(seeds),
        stats={"emitted": done, "store": store, "max_pages": max_pages},
    )
