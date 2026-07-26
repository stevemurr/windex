"""Durable, redacted operational event journal."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

_TOKEN = re.compile(
    r"(?i)(bearer\s+|token[=:]\s*|api[_-]?key[=:]\s*)"
    r"([A-Za-z0-9._~+/=-]{8,})")
_MAX_MESSAGE = 4096
_MAX_DATA_BYTES = 32 * 1024


def redact(value: Any, secrets: tuple[str, ...] = ()) -> Any:
    if isinstance(value, str):
        result = _TOKEN.sub(r"\1[REDACTED]", value)
        for secret in secrets:
            if secret and len(secret) >= 4:
                result = result.replace(secret, "[REDACTED]")
        return result
    if isinstance(value, list):
        return [redact(item, secrets) for item in value]
    if isinstance(value, Mapping):
        return {
            str(key): (
                "[REDACTED]"
                if any(part in str(key).lower() for part in ("token", "secret", "key"))
                else redact(item, secrets)
            )
            for key, item in value.items()
        }
    return value


def append(
    cur: psycopg.Cursor,
    *,
    component: str,
    event: str,
    message: str = "",
    level: str = "info",
    source_name: str | None = None,
    pipeline_name: str | None = None,
    pipeline_version: int | None = None,
    run_id: int | None = None,
    task_id: int | None = None,
    node: str | None = None,
    module: str | None = None,
    data: Mapping[str, Any] | None = None,
    secrets: tuple[str, ...] = (),
) -> int | None:
    try:
        from windex.config import get_settings

        settings = get_settings()
        configured = (
            settings.write_token,
            settings.module_admin_token,
            settings.embed_api_key,
            settings.embed_bulk_api_key,
            settings.embed_query_api_key,
            settings.judge_api_key,
            settings.rerank_api_key,
            *settings.github_token_list(),
        )
    except Exception:  # pragma: no cover - redaction still has shape matching
        configured = ()
    all_secrets = tuple(value for value in (*secrets, *configured) if value)
    clean_message = redact(message, all_secrets)[:_MAX_MESSAGE]
    clean_data = redact(dict(data or {}), all_secrets)
    import json

    encoded = json.dumps(clean_data, default=str, separators=(",", ":"))
    if len(encoded.encode()) > _MAX_DATA_BYTES:
        clean_data = {
            "truncated": True,
            "original_size": len(encoded.encode()),
        }
    cur.execute("SAVEPOINT operational_event")
    try:
        cur.execute(
            """INSERT INTO operational_events
                   (level, component, source_name, pipeline_name, pipeline_version,
                    run_id, task_id, node, module, event, message, data)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING seq""",
            (
                level, component, source_name, pipeline_name, pipeline_version,
                run_id, task_id, node, module, event, clean_message,
                Jsonb(clean_data),
            ),
        )
        seq = cur.fetchone()[0]
    except psycopg.Error:
        # Event loss must not abort the domain transition if partition rolling
        # was missed; startup health will report the partition problem.
        cur.execute("ROLLBACK TO SAVEPOINT operational_event")
        return None
    cur.execute("RELEASE SAVEPOINT operational_event")
    return seq


def list_events(
    conn: psycopg.Connection,
    *,
    after: int = 0,
    before: int | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    limit: int = 200,
    level: str | None = None,
    component: str | None = None,
    source: str | None = None,
    pipeline: str | None = None,
    run_id: int | None = None,
    node: str | None = None,
    module: str | None = None,
    text: str | None = None,
) -> list[dict[str, Any]]:
    clauses = ["seq > %s"]
    args: list[Any] = [after]
    for column, value in (
        ("level", level), ("component", component), ("source_name", source),
        ("pipeline_name", pipeline), ("run_id", run_id), ("node", node),
        ("module", module),
    ):
        if value is not None:
            clauses.append(f"{column} = %s")
            args.append(value)
    if before is not None:
        clauses.append("seq < %s")
        args.append(before)
    if started_at is not None:
        clauses.append("ts >= %s")
        args.append(started_at)
    if ended_at is not None:
        clauses.append("ts < %s")
        args.append(ended_at)
    if text:
        clauses.append("(message ILIKE %s OR event ILIKE %s)")
        args.extend([f"%{text}%", f"%{text}%"])
    args.append(min(max(limit, 1), 1000))
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT seq, ts, level, component, source_name, pipeline_name,
                       pipeline_version, run_id, task_id, node, module, event,
                       message, data
                  FROM operational_events
                 WHERE {' AND '.join(clauses)}
                 ORDER BY seq LIMIT %s""",
            args,
        )
        keys = (
            "seq", "ts", "level", "component", "source_name", "pipeline_name",
            "pipeline_version", "run_id", "task_id", "node", "module", "event",
            "message", "data",
        )
        result = []
        for row in cur.fetchall():
            item = dict(zip(keys, row))
            item["ts"] = item["ts"].isoformat()
            result.append(item)
        return result


def facets(conn: psycopg.Connection) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    with conn.cursor() as cur:
        for key, column in (
            ("levels", "level"), ("components", "component"),
            ("sources", "source_name"), ("pipelines", "pipeline_name"),
            ("nodes", "node"), ("modules", "module"),
        ):
            cur.execute(
                f"SELECT DISTINCT {column} FROM operational_events "
                f"WHERE {column} IS NOT NULL ORDER BY {column} LIMIT 500")
            result[key] = [row[0] for row in cur.fetchall()]
    return result


__all__ = ["append", "facets", "list_events", "redact"]
