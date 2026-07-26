"""Live control-plane projection over canonical tables."""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from typing import Any

import psycopg

from windex.config import Settings


def run_progress(
    conn: psycopg.Connection, run_ids: list[int],
) -> dict[int, dict[str, Any]]:
    """Compute the live weighted projection without hot-writing parent Runs."""
    if not run_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """SELECT run_id, state, weight, units_total, units_done, units_failed
                 FROM run_tasks WHERE run_id = ANY(%s) ORDER BY run_id, id""",
            (run_ids,),
        )
        rows = cur.fetchall()
    grouped: dict[int, list[tuple[Any, ...]]] = {}
    for row in rows:
        grouped.setdefault(row[0], []).append(row[1:])
    result: dict[int, dict[str, Any]] = {}
    terminal = {"succeeded", "failed", "skipped", "cancelled"}
    for run_id, tasks in grouped.items():
        total_weight = sum(float(task[1]) for task in tasks)
        completed_weight = 0.0
        unknown = False
        units_done = units_failed = 0
        known_total = 0
        for state, weight, units_total, done, failed in tasks:
            weight = float(weight)
            units_done += int(done)
            units_failed += int(failed)
            if state in terminal:
                completed_weight += weight
            elif state == "running":
                if int(units_total) < 0:
                    unknown = True
                elif int(units_total) > 0:
                    completed_weight += weight * min(
                        max((int(done) + int(failed)) / int(units_total), 0.0),
                        1.0,
                    )
            if int(units_total) >= 0:
                known_total += int(units_total)
        result[run_id] = {
            "mode": "indeterminate" if unknown else "weighted",
            "fraction": (
                round(completed_weight / total_weight, 6)
                if total_weight else 0.0
            ),
            "completed_weight": round(completed_weight, 6),
            "total_weight": round(total_weight, 6),
            "units_done": units_done,
            "units_failed": units_failed,
            "known_units_total": known_total,
        }
    return result


def snapshot(
    conn: psycopg.Connection, settings: Settings | None = None,
) -> dict[str, Any]:
    as_of = datetime.now(UTC).isoformat()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT state, count(*) FROM runs GROUP BY state")
        run_counts = dict(cur.fetchall())
        cur.execute(
            """SELECT r.id, r.source_name, r.pipeline_name, r.pipeline_version,
                      r.flow_name, r.state, r.queued_at, r.started_at,
                      count(t.id) FILTER (WHERE t.state = 'ready'),
                      count(t.id) FILTER (WHERE t.state = 'blocked')
                 FROM runs r LEFT JOIN run_tasks t ON t.run_id = r.id
                WHERE r.state IN ('queued','running','blocked')
                GROUP BY r.id ORDER BY r.priority DESC, r.id LIMIT 100""")
        active_rows = cur.fetchall()
        progress = run_progress(conn, [row[0] for row in active_rows])
        active = [{
            "id": row[0], "source_name": row[1], "pipeline_name": row[2],
            "pipeline_version": row[3], "flow_name": row[4], "state": row[5],
            "queued_at": row[6].isoformat(), "started_at": (
                row[7].isoformat() if row[7] else None),
            "progress": progress.get(row[0], {
                "mode": "weighted", "fraction": 0.0,
                "completed_weight": 0.0, "total_weight": 0.0,
                "units_done": 0, "units_failed": 0, "known_units_total": 0,
            }),
            "ready_tasks": row[8], "blocked_tasks": row[9],
        } for row in active_rows]
        cur.execute(
            """SELECT lane, state, count(*)
                 FROM run_tasks
                WHERE state IN ('ready','running','blocked')
                GROUP BY lane, state ORDER BY lane, state""")
        lanes: dict[str, dict[str, int]] = {}
        for lane, state, count in cur.fetchall():
            lanes.setdefault(lane, {})[state] = count
        cur.execute(
            """SELECT preconditions, error, count(*)
                 FROM run_tasks WHERE state = 'blocked'
                GROUP BY preconditions, error ORDER BY count(*) DESC""")
        blocked_preconditions = [{
            "preconditions": row[0],
            "reason": row[1],
            "tasks": row[2],
        } for row in cur.fetchall()]
        cur.execute(
            """SELECT s.name, s.enabled, c.paused, count(d.id),
                      count(d.id) FILTER (WHERE d.status = 'searchable'),
                      max(d.indexed_at)
                 FROM sources s
                 JOIN source_control c ON c.source_id = s.id
                 LEFT JOIN documents d ON d.source_id = s.id
                WHERE s.archived_at IS NULL
                GROUP BY s.id, c.paused ORDER BY s.name""")
        sources = [{
            "name": row[0], "enabled": row[1], "paused": row[2],
            "documents": row[3], "searchable": row[4],
            "last_indexed_at": row[5].isoformat() if row[5] else None,
            "as_of": as_of,
        } for row in cur.fetchall()]
        cur.execute(
            """SELECT count(*), count(*) FILTER (WHERE status = 'searchable')
                 FROM documents""")
        documents, searchable = cur.fetchone()
        cur.execute(
            """SELECT max(seq) FROM operational_events""")
        revision = cur.fetchone()[0] or 0
        cur.execute(
            """SELECT s.name, min(t.next_fire_at)
                 FROM sources s LEFT JOIN source_triggers t
                   ON t.source_id = s.id AND t.enabled
                WHERE s.archived_at IS NULL GROUP BY s.id ORDER BY s.name""")
        schedules = [{
            "source": row[0],
            "next_trigger": row[1].isoformat() if row[1] else None,
        } for row in cur.fetchall()]
        cur.execute(
            """SELECT id, source_name, pipeline_name, pipeline_version, flow_name,
                      state, finished_at, error
                 FROM runs
                WHERE state IN ('succeeded','failed','cancelled')
                ORDER BY id DESC LIMIT 20""")
        recent_runs = [{
            "id": row[0], "source_name": row[1], "pipeline_name": row[2],
            "pipeline_version": row[3], "flow_name": row[4], "state": row[5],
            "finished_at": row[6].isoformat() if row[6] else None,
            "error": row[7],
        } for row in cur.fetchall()]
        cur.execute(
            """SELECT id, source, title, indexed_at
                 FROM documents WHERE indexed_at IS NOT NULL
                ORDER BY indexed_at DESC LIMIT 20""")
        recent_documents = [{
            "id": row[0], "source": row[1], "title": row[2],
            "indexed_at": row[3].isoformat(),
        } for row in cur.fetchall()]
        cur.execute(
            """SELECT count(*) FROM documents
                WHERE indexed_at >= now() - interval '1 hour'""")
        indexed_last_hour = cur.fetchone()[0]
        cur.execute(
            """SELECT resource_name FROM storage_ownership
                WHERE resource_type = 'qdrant_collection'
                ORDER BY resource_name""")
        owned_collections = [row[0] for row in cur.fetchall()]
    health: dict[str, Any] = {
        "service": "ok",
        "postgres": "ok",
        "vector": "unknown",
        "storage": "unknown",
        "degraded": False,
    }
    from windex.source.store import module_statuses

    try:
        stranded = [
            item for item in module_statuses(conn, enabled_only=True)
            if not item["available"]
        ]
        health["module_locks"] = "degraded" if stranded else "ok"
        health["stranded_sources"] = [
            item["source"] for item in stranded
        ]
        if stranded:
            health["degraded"] = True
    except Exception:  # noqa: BLE001 - Overview remains inspectable when degraded
        health["module_locks"] = "error"
        health["stranded_sources"] = []
        health["degraded"] = True
    vector_count: int | None = None
    if settings is not None:
        try:
            usage = shutil.disk_usage(settings.active_data_root)
            health["storage"] = "ok"
            health["storage_free_bytes"] = usage.free
        except OSError as exc:
            health["storage"] = "error"
            health["storage_error"] = str(exc)
            health["degraded"] = True
        client = None
        try:
            from windex.index.qdrant import client_from_url

            client = client_from_url(settings.qdrant_url)
            client.get_collections()
            vector_count = sum(
                int(client.get_collection(name).points_count or 0)
                for name in owned_collections
            )
            health["vector"] = "ok"
            health["vector_collections"] = len(owned_collections)
        except Exception as exc:  # noqa: BLE001 - Overview degrades, never fails
            health["vector"] = "error"
            health["vector_error"] = str(exc)
            health["degraded"] = True
        finally:
            if client is not None:
                client.close()
    return {
        "revision": revision,
        "as_of": as_of,
        "health": health,
        "runs": {
            "counts": run_counts,
            "active": active,
            "recent": recent_runs,
        },
        "workers": {
            "lanes": lanes,
            "blocked_preconditions": blocked_preconditions,
        },
        "sources": sources,
        "schedules": schedules,
        "recent_documents": recent_documents,
        "totals": {
            "documents": documents,
            "searchable": searchable,
            "vectors": vector_count,
            "indexed_last_hour": indexed_last_hour,
            "as_of": as_of,
        },
    }


__all__ = ["run_progress", "snapshot"]
