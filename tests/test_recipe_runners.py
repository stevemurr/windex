"""Executable recipe roots and their durable edge stream."""

from __future__ import annotations

import gzip
import json

from psycopg.types.json import Jsonb

from windex.modules.catalog import list_json_manifest, list_lines, list_path_manifest_gz
from windex.modules.collect import store_upsert
from windex.modules.discover import state_pending, static_once
from windex.recipe import wire
from windex.recipe.ports import PartitionRecord, PartitionRef, RawBlob, WorkUnit
from windex.recipe.wire import decode_many
from windex.worker.protocol import TaskContext


def _ctx(pg, *, task_id: int, config: dict, module: str, run_id: int = 71,
         recipe: str = "demo", source: str = "demo", node: str = "root",
         should_yield=lambda: False):
    beats = []
    ctx = TaskContext(
        run_id=run_id,
        task_id=task_id,
        source=source,
        node=node,
        module=module,
        config=config,
        spec={},
        cursor={},
        conn=pg,
        should_yield=should_yield,
        heartbeat=lambda done, failed, stats: beats.append((done, failed, stats)),
        recipe=recipe,
    )
    return ctx, beats


def _outputs(pg, task_id: int):
    with pg.cursor() as cur:
        cur.execute(
            "SELECT unit_key, outputs FROM task_units WHERE task_id = %s ORDER BY unit_key",
            (task_id,),
        )
        return [(key, decode_many(outputs)) for key, outputs in cur.fetchall()]


def _seed(pg, *, source="demo", store="items", key, upstream=None, ingested=None,
          stage=None, processed_at=None, attrs=None):
    with pg.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_units
                   (source, store, unit_key, upstream, ingested, stage,
                    processed_at, attrs)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (source, store, key, Jsonb(upstream or {}),
             Jsonb(ingested) if ingested is not None else None, stage,
             processed_at, Jsonb(attrs or {})),
        )
    pg.commit()


def test_static_once_is_durable_and_idempotent(pg):
    ctx, beats = _ctx(
        pg,
        task_id=7101,
        module="static.once",
        config={"key": "manifest",
                "payload": '"url=https://example.test/a,b", mode=full'},
    )
    first = static_once(ctx)
    second = static_once(ctx)

    assert first.exhausted and first.units_done == 1
    assert second.exhausted and second.units_done == 0
    assert len(beats) == 1
    [(key, values)] = _outputs(pg, ctx.task_id)
    assert key == "manifest"
    assert values == [
        WorkUnit(
            ref=values[0].ref,
            payload={"url": "https://example.test/a,b", "mode": "full"},
            epoch=ctx.run_id,
        )
    ]


def test_state_pending_token_moved_batches_and_resumes(pg):
    _seed(pg, key="a", upstream={"v": 2}, ingested={"v": 1})
    _seed(pg, key="b", upstream={"v": 1}, ingested={"v": 1})
    _seed(pg, key="c", upstream={"v": 3}, ingested=None, attrs={"kind": "doc"})
    ctx, beats = _ctx(
        pg,
        task_id=7102,
        module="state.pending",
        config={
            "store": "items",
            "predicate": "token_moved",
            "order": "key",
            "batch": 1,
            "claim": "none",
        },
    )

    one = state_pending(ctx)
    two = state_pending(ctx)
    empty = state_pending(ctx)

    assert not one.exhausted
    assert two.exhausted
    assert empty.exhausted and empty.units_done == 0
    assert [key for key, _ in _outputs(pg, ctx.task_id)] == ["a", "c"]
    assert _outputs(pg, ctx.task_id)[1][1][0].payload == {"kind": "doc"}
    assert len(beats) == 2


def test_state_pending_stage_and_lease_claim(pg):
    _seed(pg, key="candidate", stage="candidate")
    _seed(pg, key="hydrated", stage="hydrated")
    ctx, _ = _ctx(
        pg,
        task_id=7103,
        module="state.pending",
        config={
            "store": "items",
            "predicate": "stage_in",
            "stages": "candidate",
            "order": "key",
            "batch": 10,
            "claim": "lease",
            "stale_minutes": 60,
        },
    )

    result = state_pending(ctx)

    assert result.exhausted and result.units_done == 1
    assert [key for key, _ in _outputs(pg, ctx.task_id)] == ["candidate"]
    with pg.cursor() as cur:
        cur.execute(
            "SELECT status, claimed_at IS NOT NULL, last_run_id "
            "FROM source_units WHERE source = 'demo' AND unit_key = 'candidate'")
        assert cur.fetchone() == ("processing", True, ctx.run_id)


def test_state_pending_uses_recipe_not_corpus_source_as_store_namespace(pg):
    _seed(pg, source="gh", key="shard")
    ctx, _ = _ctx(
        pg,
        task_id=7104,
        recipe="gh",
        source="github",
        module="state.pending",
        config={
            "store": "items",
            "predicate": "unseen",
            "order": "key",
            "batch": 10,
            "claim": "none",
        },
    )

    assert state_pending(ctx).units_done == 1


def test_state_pending_yields_after_a_committed_unit_and_resumes(pg):
    _seed(pg, key="a")
    _seed(pg, key="b")
    config = {
        "store": "items",
        "predicate": "unseen",
        "order": "key",
        "batch": 10,
        "claim": "none",
    }
    first, _ = _ctx(
        pg,
        task_id=7105,
        module="state.pending",
        config=config,
        should_yield=lambda: True,
    )

    yielded = state_pending(first)

    assert yielded.units_done == 1 and not yielded.exhausted
    resumed, _ = _ctx(
        pg,
        task_id=7105,
        module="state.pending",
        config=config,
    )
    finished = state_pending(resumed)
    assert finished.units_done == 1 and finished.exhausted
    assert [key for key, _ in _outputs(pg, first.task_id)] == ["a", "b"]


def test_durable_stream_fans_in_and_store_upsert_consumes_it(pg):
    from windex.worker import dag

    run_id = dag.submit_run(
        pg,
        recipe="demo",
        source="corpus",
        spec={},
        dedupe_key="recipe-runner-fan-in",
        tasks=[
            {"node": "left", "module": "test.left", "kind": "catalog"},
            {"node": "right", "module": "test.right", "kind": "catalog"},
            {"node": "sink", "module": "store.upsert", "kind": "collect",
             "depends_on": ["left", "right"]},
        ],
    )
    assert run_id is not None
    with pg.cursor() as cur:
        cur.execute(
            "SELECT node, id FROM run_tasks WHERE run_id = %s",
            (run_id,),
        )
        tasks = dict(cur.fetchall())
        for node, key in (("left", "a"), ("right", "b")):
            record = PartitionRecord(
                store="items",
                key=key,
                upstream={"v": 1},
                payload={"branch": node},
            )
            cur.execute(
                """
                INSERT INTO task_units
                       (run_id, task_id, unit_key, state, outputs, finished_at)
                VALUES (%s, %s, %s, 'done', %s, now())
                """,
                (run_id, tasks[node], key, Jsonb(wire.encode_many([record]))),
            )
    pg.commit()

    ctx, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["sink"],
        recipe="demo",
        source="corpus",
        module="store.upsert",
        config={"store": "items", "on_conflict": "merge"},
    )
    result = store_upsert(ctx)
    replay = store_upsert(ctx)

    assert result.exhausted and result.units_done == 2
    assert replay.exhausted and replay.units_done == 0
    with pg.cursor() as cur:
        cur.execute(
            "SELECT unit_key, attrs->>'branch' FROM source_units "
            "WHERE source = 'demo' ORDER BY unit_key")
        assert cur.fetchall() == [("a", "left"), ("b", "right")]


def test_common_stream_rejects_wrong_port_type(pg):
    """The install-time port check is repeated at the durable-state boundary."""
    from windex.worker import dag

    run_id = dag.submit_run(
        pg,
        recipe="demo",
        source="demo",
        spec={},
        dedupe_key="recipe-runner-wire-type",
        tasks=[
            {"node": "root", "module": "static.once", "kind": "discover"},
            {"node": "sink", "module": "store.upsert", "kind": "collect",
             "depends_on": ["root"]},
        ],
    )
    assert run_id is not None
    with pg.cursor() as cur:
        cur.execute("SELECT node, id FROM run_tasks WHERE run_id = %s", (run_id,))
        tasks = dict(cur.fetchall())
    root, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["root"],
        module="static.once",
        config={"key": "once", "payload": ""},
    )
    static_once(root)
    sink, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["sink"],
        module="store.upsert",
        config={"store": "items"},
    )

    import pytest
    from windex.worker.protocol import PermanentTaskError

    with pytest.raises(PermanentTaskError, match="received WorkUnit"):
        store_upsert(sink)


def _catalog_graph(pg, *, suffix: str, module: str, config: dict, body: bytes):
    from windex.worker import dag

    run_id = dag.submit_run(
        pg,
        recipe="demo",
        source="demo",
        spec={},
        dedupe_key=f"recipe-catalog-{suffix}",
        tasks=[
            {"node": "fetch", "module": "test.fetch", "kind": "fetch"},
            {"node": "parse", "module": module, "kind": "catalog",
             "depends_on": ["fetch"]},
            {"node": "store", "module": "store.upsert", "kind": "collect",
             "config": {"store": "items"}, "depends_on": ["parse"]},
        ],
    )
    assert run_id is not None
    with pg.cursor() as cur:
        cur.execute("SELECT node, id FROM run_tasks WHERE run_id = %s", (run_id,))
        tasks = dict(cur.fetchall())
        blob = RawBlob(
            ref=PartitionRef(store="", key="listing"),
            uri=f"memory://{suffix}",
            body=body,
            epoch=run_id,
        )
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, finished_at)
            VALUES (%s, %s, 'listing', 'done', %s, now())
            """,
            (run_id, tasks["fetch"], Jsonb(wire.encode_many([blob]))),
        )
    pg.commit()
    ctx, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["parse"],
        node="parse",
        module=module,
        config=config,
    )
    return ctx, tasks


def test_list_json_manifest_emits_typed_records(pg):
    body = json.dumps([
        {"slug": "python", "mtime": 3, "title": "Python"},
        {"slug": "", "mtime": 4},
        {"slug": "rust", "mtime": 5},
    ]).encode()
    ctx, _ = _catalog_graph(
        pg,
        suffix="json",
        module="list.json_manifest",
        config={"key_field": "slug", "upstream_field": "mtime"},
        body=body,
    )

    result = list_json_manifest(ctx)

    assert result.exhausted and result.stats["records"] == 2
    outputs = _outputs(pg, ctx.task_id)[0][1]
    assert [(record.key, record.upstream) for record in outputs] == [
        ("python", {"mtime": 3}),
        ("rust", {"mtime": 5}),
    ]


def test_line_and_gzip_catalogs_filter_inputs(pg):
    lines, _ = _catalog_graph(
        pg,
        suffix="lines",
        module="list.lines",
        config={"scheme_allow": "https", "shrink_floor": 1},
        body=b"# comment\nhttps://a.test/feed\nftp://bad.test/x\nhttps://a.test/feed\n",
    )
    assert list_lines(lines).stats["records"] == 1
    [line_record] = _outputs(pg, lines.task_id)[0][1]
    assert line_record.payload["host"] == "a.test"

    paths, _ = _catalog_graph(
        pg,
        suffix="paths",
        module="list.path_manifest_gz",
        config={"key_pattern": [r"CC-NEWS-\d+\.warc\.gz"], "min_age_days": 0},
        body=gzip.compress(
            b"crawl-data/CC-NEWS/2026/07/CC-NEWS-123.warc.gz\nnot-a-warc\n"),
    )
    assert list_path_manifest_gz(paths).stats["records"] == 1
