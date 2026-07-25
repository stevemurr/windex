"""Executable recipe roots and their durable edge stream."""

from __future__ import annotations

import gzip
import json
from datetime import date
from types import SimpleNamespace

import httpx
from psycopg.types.json import Jsonb

from windex.modules import fetch as fetch_module
from windex.modules.catalog import list_json_manifest, list_lines, list_path_manifest_gz
from windex.modules.collect import store_repos, store_upsert
from windex.modules.discover import _github_shard_rows, state_pending, static_once
from windex.modules.fetch import (
    _github_search,
    _hf_root_pages,
    _hf_sync_blob,
    _oai,
    _page,
    _page_limiter,
    _smallweb_feed,
    _template_url,
)
from windex.modules.load import ledger_stage
from windex.modules.receive import push_docs
from windex.modules.transform import dedup_exact
from windex.recipe import wire
from windex.recipe.ports import (
    ExtractedDoc,
    PartitionRecord,
    PartitionRef,
    RawBlob,
    WorkUnit,
)
from windex.recipe.wire import decode_many
from windex.worker.protocol import TaskContext


def _ctx(pg, *, task_id: int, config: dict, module: str, run_id: int = 71,
         recipe: str = "demo", source: str = "demo", node: str = "root",
         should_yield=lambda: False, spec=None, params=None):
    beats = []
    ctx = TaskContext(
        run_id=run_id,
        task_id=task_id,
        source=source,
        node=node,
        module=module,
        config=config,
        spec=spec or {},
        cursor={},
        conn=pg,
        should_yield=should_yield,
        heartbeat=lambda done, failed, stats: beats.append((done, failed, stats)),
        recipe=recipe,
        params=params or {},
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


def test_recipe_http_get_uses_response_aware_limiter_for_hf():
    from windex.hf.fetch import PagesRateLimiter
    from windex.smallweb.poll import HostRateLimiter

    assert isinstance(_page_limiter("hf", 3), PagesRateLimiter)
    assert type(_page_limiter("docs", 3)) is HostRateLimiter


def test_download_template_can_use_frozen_config_and_partition_payload(pg):
    ctx, _ = _ctx(
        pg,
        task_id=7099,
        source="wiki",
        module="http.download",
        config={
            "url_template": (
                "https://dumps.wikimedia.org/{dump_date}/"
                "index_name={dump}_content/{key}"
            ),
        },
        params={"dump": "enwiki", "flow": "ingest"},
    )
    unit = WorkUnit(
        ref=PartitionRef(store="shard", key="enwiki-00000.json.bz2"),
        payload={"dump_date": "20260719"},
    )

    assert _template_url(ctx, unit) == (
        "https://dumps.wikimedia.org/20260719/"
        "index_name=enwiki_content/enwiki-00000.json.bz2"
    )


def test_smallweb_records_a_broken_feed_without_failing_the_task(pg, monkeypatch):
    def broken(*args, **kwargs):
        raise httpx.ConnectError(
            "TLS alert",
            request=httpx.Request("GET", "https://broken.example/feed"),
        )

    monkeypatch.setattr(fetch_module, "_page", broken)
    ctx, _ = _ctx(
        pg,
        task_id=7098,
        recipe="smallweb",
        source="smallweb",
        module="http.get",
        config={},
    )
    unit = WorkUnit(
        ref=PartitionRef(
            store="feed",
            key="https://broken.example/feed",
        ),
        payload={"url": "https://broken.example/feed"},
        upstream={"etag": "old"},
    )

    result = _smallweb_feed(ctx, unit, object(), object(), object())

    assert result.body == b""
    assert result.meta["reason"] == "fetch_error"
    assert result.meta["error"].startswith("ConnectError:")
    assert result.meta["upstream"] == {"etag": "old"}


def test_recipe_page_retries_429_inside_task_budget(pg, monkeypatch):
    """A published pages-bucket reset is not a task failure.

    The task retry budget is for failed work, not the host asking us to wait.
    Keeping this retry inside the fetch also preserves the already committed
    edge units when a long crawl reaches the shared-IP bucket boundary.
    """
    responses = iter([
        httpx.Response(
            429,
            headers={"ratelimit": '"pages";r=0;t=6'},
        ),
        httpx.Response(
            200,
            text="ok",
            headers={"content-type": "text/plain"},
        ),
    ])
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: next(responses)))

    class Limiter:
        interval = 3.0

        def __init__(self):
            self.seen = []
            self.waits = 0

        def wait(self, host):
            self.waits += 1

        def observe(self, response):
            self.seen.append(response.status_code)
            if response.status_code == 429:
                self.interval = 7.0

    limiter = Limiter()
    sleeps = []
    monkeypatch.setattr(fetch_module, "check_url", lambda url: None)
    monkeypatch.setattr(fetch_module.time, "sleep", sleeps.append)
    ctx, _ = _ctx(
        pg,
        task_id=7100,
        source="hf",
        module="http.get",
        config={"robots": False, "retries": 1},
    )
    unit = WorkUnit(
        ref=PartitionRef(store="post", key="example"),
        payload={"url": "https://example.test/post"},
    )

    page = _page(ctx, unit, client, object(), limiter)

    assert page.body == b"ok"
    assert limiter.seen == [429, 200]
    assert limiter.waits == 2
    assert sleeps == [7.0]


def test_oai_finishes_atomic_window_after_yield_request(pg, monkeypatch):
    responses = iter((b"page-1", b"page-2"))
    client = httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=next(responses))))
    monkeypatch.setattr(
        "windex.arxiv.harvest.parse_records",
        lambda body: ([], "next") if body == b"page-1" else ([], None),
    )
    sleeps = []
    monkeypatch.setattr(fetch_module.time, "sleep", sleeps.append)
    ctx, _ = _ctx(
        pg,
        task_id=7108,
        source="arxiv",
        module="http.paginate",
        config={"request_interval": 3},
        should_yield=lambda: True,
    )
    unit = WorkUnit(
        ref=PartitionRef(store="window", key="2026-01-01..2026-01-31"),
        payload={"from": "2026-01-01", "until": "2026-01-31"},
    )

    pages = _oai(ctx, unit, client)

    assert [page.body for page in pages] == [b"page-1", b"page-2"]
    assert sleeps == [3.0]


def test_github_search_finishes_one_day_after_yield_request(pg, monkeypatch):
    monkeypatch.setattr(
        fetch_module,
        "Settings",
        lambda: SimpleNamespace(github_token_list=lambda: ["token"]),
    )
    monkeypatch.setattr(
        "windex.github.discover._get",
        lambda client, token, params: {"total_count": 0, "items": []},
    )
    monkeypatch.setattr(fetch_module.time, "sleep", lambda seconds: None)
    ctx, _ = _ctx(
        pg,
        task_id=7109,
        recipe="gh",
        source="github",
        module="http.paginate",
        config={"request_interval": 2.1, "page_size": 100, "result_cap": 1000},
        params={"star_threshold": 10},
        should_yield=lambda: True,
    )
    unit = WorkUnit(
        ref=PartitionRef(store="gh_shards", key="2026-07-01..2026-07-01@10"),
        payload={"from": "2026-07-01", "to": "2026-07-01", "star_threshold": 10},
    )

    [result] = _github_search(ctx, unit, httpx.Client())

    assert json.loads(result.body) == {
        "items": [],
        "shards": [{
            "from": "2026-07-01",
            "to": "2026-07-01",
            "star_threshold": 10,
            "repos": 0,
            "capped": False,
        }],
    }


def test_github_search_frontier_is_daily_and_complete():
    rows = _github_shard_rows(date(2008, 1, 3), 10)

    assert [key for key, _ in rows] == [
        "2008-01-01..2008-01-01@10",
        "2008-01-02..2008-01-02@10",
        "2008-01-03..2008-01-03@10",
    ]
    assert rows[-1][1] == {
        "from": "2008-01-03",
        "to": "2008-01-03",
        "star_threshold": 10,
    }


def test_hf_anchor_replay_fetches_only_banked_pages(pg, monkeypatch):
    llms = """# Transformers
- [Quickstart](https://huggingface.co/docs/transformers/v5.14.0/quicktour.md)
- [Pipelines](https://huggingface.co/docs/transformers/v5.14.0/main_classes/pipelines.md)
"""
    requested = []

    def handler(request):
        requested.append(request.url.path)
        if request.url.path.endswith("llms.txt"):
            return httpx.Response(
                200, text=llms, headers={"content-type": "text/plain"})
        return httpx.Response(
            200, text="# Quickstart", headers={"content-type": "text/markdown"})

    monkeypatch.setattr(fetch_module, "check_url", lambda url: None)
    ctx, _ = _ctx(
        pg,
        task_id=7107,
        recipe="hf",
        source="hf",
        module="http.get",
        config={"robots": False},
        params={"anchor_ids": ["hf:docs/transformers/quicktour"]},
        should_yield=lambda: True,
    )
    unit = WorkUnit(
        ref=PartitionRef(
            store="root",
            key="docs/transformers",
            id_scope="hf:docs/transformers/",
        ),
        # The catalog stores the public landing URL. The fetcher must use the
        # root's llms.txt instead, then use child URLs for selected pages.
        payload={
            "url": "https://huggingface.co/docs/transformers",
            "kind": "docs",
            "license": "Apache-2.0",
        },
    )

    pages = _hf_root_pages(
        ctx,
        unit,
        httpx.Client(transport=httpx.MockTransport(handler)),
        object(),
        fetch_module.HostRateLimiter(0),
    )

    assert len(pages) == 1
    assert requested[0] == "/docs/transformers/llms.txt"
    assert pages[0].meta["payload"]["path"] == "quicktour"
    assert pages[0].ref.id_scope == "hf:docs/transformers/quicktour"
    assert not any("pipelines" in path for path in requested)


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


def test_state_pending_limit_caps_the_whole_run_not_each_slice(pg):
    for key in ("a", "b", "c"):
        _seed(pg, key=key)
    ctx, _ = _ctx(
        pg,
        task_id=7110,
        module="state.pending",
        config={
            "store": "items",
            "predicate": "unseen",
            "order": "key",
            "batch": 2,
            "limit": 2,
            "claim": "none",
        },
    )

    first = state_pending(ctx)
    replay = state_pending(ctx)

    assert first.exhausted and first.units_done == 2
    assert first.units_total == 2
    assert replay.exhausted and replay.units_done == 0
    assert [key for key, _ in _outputs(pg, ctx.task_id)] == ["a", "b"]


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


def test_state_pending_can_replay_exact_hf_anchors(pg):
    _seed(pg, source="hf", store="post", key="wanted")
    _seed(pg, source="hf", store="post", key="other")
    _seed(
        pg, source="hf", store="root", key="docs/diffusers",
        upstream={"llms_hash": "present"})
    _seed(
        pg, source="hf", store="root", key="docs/unrelated",
        upstream={"llms_hash": "present"})
    _seed(
        pg, source="hf", store="root", key="docs/no-llms",
        upstream={"llms_hash": None})
    ctx, _ = _ctx(
        pg,
        task_id=7106,
        recipe="hf",
        source="hf",
        module="state.pending",
        config={
            "store": "post",
            "predicate": "token_moved",
            "order": "ord",
            "batch": 20,
        },
        params={
            "anchor_ids": [
                "hf:blog/wanted",
                "hf:docs/transformers/quicktour",
            ],
        },
    )

    assert state_pending(ctx).units_done == 1
    [(key, _)] = _outputs(pg, ctx.task_id)
    assert key == "wanted"

    roots, _ = _ctx(
        pg,
        task_id=7108,
        recipe="hf",
        source="hf",
        module="state.pending",
        config={
            "store": "root",
            "predicate": "token_moved",
            "order": "key",
            "batch": 4,
        },
        params={
            "anchor_ids": [
                "hf:docs/diffusers/api/models/ltx_video_transformer3d",
            ],
        },
    )
    assert state_pending(roots).units_done == 1
    [(key, _)] = _outputs(pg, roots.task_id)
    assert key == "docs/diffusers"

    full, _ = _ctx(
        pg,
        task_id=7109,
        recipe="hf",
        source="hf",
        module="state.pending",
        config={
            "store": "root",
            "predicate": "token_moved",
            "order": "key",
            "batch": 10,
        },
    )
    assert state_pending(full).units_done == 2
    assert [key for key, _ in _outputs(pg, full.task_id)] == [
        "docs/diffusers",
        "docs/unrelated",
    ]


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


def test_large_edge_batch_uses_artifact_and_remains_consumable(
        pg, tmp_path, monkeypatch):
    from windex.modules import common
    from windex.worker import dag

    monkeypatch.setattr(common, "_INLINE_BYTES", 1)
    monkeypatch.setattr(common, "_artifact_root", lambda: tmp_path)
    run_id = dag.submit_run(
        pg,
        recipe="demo",
        source="corpus",
        spec={},
        dedupe_key="recipe-runner-artifact",
        tasks=[
            {"node": "parse", "module": "test.parse", "kind": "catalog"},
            {"node": "sink", "module": "store.upsert", "kind": "collect",
             "config": {"store": "items"}, "depends_on": ["parse"]},
        ],
    )
    assert run_id is not None
    with pg.cursor() as cur:
        cur.execute("SELECT node, id FROM run_tasks WHERE run_id = %s", (run_id,))
        tasks = dict(cur.fetchall())
    parse, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["parse"],
        node="parse",
        module="test.parse",
        config={},
    )
    batch = common.InputBatch(key="seed:1", values=())
    record = PartitionRecord(
        store="items",
        key="large",
        payload={"text": "x" * 1_000},
    )
    common.finish_batch(parse, batch, outputs=[record])
    pg.commit()

    with pg.cursor() as cur:
        cur.execute(
            "SELECT outputs FROM task_units WHERE task_id = %s",
            (tasks["parse"],),
        )
        [artifact] = cur.fetchone()[0]
    assert artifact["type"] == "_WireArtifact"
    assert (tmp_path / artifact["path"]).is_file()

    sink, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["sink"],
        node="sink",
        module="store.upsert",
        config={"store": "items"},
    )
    result = store_upsert(sink)
    assert result.exhausted and result.stats["stored"] == 1
    with pg.cursor() as cur:
        cur.execute(
            "SELECT attrs->>'text' FROM source_units "
            "WHERE source = 'demo' AND store = 'items' AND unit_key = 'large'")
        assert cur.fetchone() == ("x" * 1_000,)


def test_push_full_set_stages_and_empty_push_tombstones(
        pg, tmp_path, monkeypatch):
    from windex.worker import dag

    monkeypatch.setenv("WINDEX_DATA_ROOT", str(tmp_path))
    spec = {"corpus": {"source": "memory", "id_prefix": "memory:"}}

    def run_push(dedupe: str, chunks: list[dict]):
        run_id = dag.submit_run(
            pg,
            recipe="memory",
            source="memory",
            spec=spec,
            dedupe_key=dedupe,
            params={
                "conversation_id": "chat-1",
                "title": "A chat",
                "chunks": chunks,
            },
            tasks=[
                {"node": "push", "module": "push.docs", "kind": "receive"},
                {"node": "stage", "module": "ledger.stage", "kind": "load",
                 "depends_on": ["push"]},
            ],
        )
        assert run_id is not None
        with pg.cursor() as cur:
            cur.execute(
                "SELECT node, id FROM run_tasks WHERE run_id = %s", (run_id,))
            tasks = dict(cur.fetchall())
        receive, _ = _ctx(
            pg,
            run_id=run_id,
            task_id=tasks["push"],
            recipe="memory",
            source="memory",
            module="push.docs",
            config={"mode": "full_set", "max_docs": 500,
                    "max_text_chars": 16_000},
            spec=spec,
            params={
                "conversation_id": "chat-1",
                "title": "A chat",
                "chunks": chunks,
            },
        )
        assert push_docs(receive).exhausted
        stage, _ = _ctx(
            pg,
            run_id=run_id,
            task_id=tasks["stage"],
            recipe="memory",
            source="memory",
            node="stage",
            module="ledger.stage",
            config={"replace": True, "replace_scope": "partition",
                    "replace_guard": "census"},
            spec=spec,
        )
        assert ledger_stage(stage).exhausted

    run_push("memory-filled", [{"index": 0, "text": "remember this"}])
    with pg.cursor() as cur:
        cur.execute(
            "SELECT status, text_ref FROM documents WHERE id = 'memory:chat-1/00000'")
        status, text_ref = cur.fetchone()
    assert status == "deduped"
    assert (tmp_path / "staging" / text_ref).is_file()

    run_push("memory-empty", [])
    with pg.cursor() as cur:
        cur.execute(
            "SELECT status FROM documents WHERE id = 'memory:chat-1/00000'")
        assert cur.fetchone() == ("deleted",)


def test_exact_transform_and_loader_preserve_duplicate_ledger_row(
        pg, tmp_path, monkeypatch):
    from windex.worker import dag

    monkeypatch.setenv("WINDEX_DATA_ROOT", str(tmp_path))
    spec = {"corpus": {"source": "hn", "id_prefix": "hn:"}}
    run_id = dag.submit_run(
        pg,
        recipe="hn",
        source="hn",
        spec=spec,
        dedupe_key="exact-load",
        tasks=[
            {"node": "extract", "module": "test.extract", "kind": "extract"},
            {"node": "exact", "module": "dedup.exact", "kind": "transform",
             "depends_on": ["extract"]},
            {"node": "stage", "module": "ledger.stage", "kind": "load",
             "depends_on": ["exact"]},
        ],
    )
    assert run_id is not None
    with pg.cursor() as cur:
        cur.execute("SELECT node, id FROM run_tasks WHERE run_id = %s", (run_id,))
        tasks = dict(cur.fetchall())
        docs = [
            ExtractedDoc(
                ref=PartitionRef(store="window", key="one"),
                suffix=str(index),
                url=f"https://news.ycombinator.com/item?id={index}",
                title="Same",
                text="same body",
                fields={"story_text": "same body"},
            )
            for index in (1, 2)
        ]
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, finished_at)
            VALUES (%s, %s, 'window', 'done', %s, now())
            """,
            (run_id, tasks["extract"], Jsonb(wire.encode_many(docs))),
        )
    pg.commit()
    exact, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["exact"],
        recipe="hn",
        source="hn",
        node="exact",
        module="dedup.exact",
        config={"scope": "batch"},
        spec=spec,
    )
    assert dedup_exact(exact).exhausted
    stage, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["stage"],
        recipe="hn",
        source="hn",
        node="stage",
        module="ledger.stage",
        config={"replace": False},
        spec=spec,
    )
    assert ledger_stage(stage).exhausted
    with pg.cursor() as cur:
        cur.execute(
            "SELECT id, status, duplicate_of, text_ref "
            "FROM documents ORDER BY id")
        rows = cur.fetchall()
    assert rows[0][0:3] == ("hn:1", "deduped", None)
    assert rows[1][0:3] == ("hn:2", "duplicate", "hn:1")
    assert rows[0][3] and rows[1][3] is None


def test_anchor_replay_does_not_advance_partial_source_watermark(
        pg, tmp_path, monkeypatch):
    from windex.worker import dag

    monkeypatch.setenv("WINDEX_DATA_ROOT", str(tmp_path))
    _seed(
        pg,
        source="hf",
        store="root",
        key="docs/transformers",
        upstream={"llms_hash": "new"},
        attrs={"id_scope": "hf:docs/transformers/"},
    )
    spec = {"corpus": {"source": "hf", "id_prefix": "hf:"}}
    run_id = dag.submit_run(
        pg,
        recipe="hf",
        source="hf",
        spec=spec,
        dedupe_key="anchor-watermark",
        params={"anchor_ids": ["hf:docs/transformers/quicktour"]},
        tasks=[
            {"node": "extract", "module": "test.extract", "kind": "extract"},
            {"node": "stage", "module": "ledger.stage", "kind": "load",
             "depends_on": ["extract"]},
        ],
    )
    assert run_id is not None
    with pg.cursor() as cur:
        cur.execute("SELECT node, id FROM run_tasks WHERE run_id = %s", (run_id,))
        tasks = dict(cur.fetchall())
        doc = ExtractedDoc(
            ref=PartitionRef(
                store="root",
                key="docs/transformers",
                id_scope="hf:docs/transformers/quicktour",
            ),
            suffix="docs/transformers/quicktour",
            url="https://huggingface.co/docs/transformers/quicktour",
            title="Quicktour",
            text="A useful guide",
        )
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, finished_at)
            VALUES (%s, %s, 'root', 'done', %s, now())
            """,
            (run_id, tasks["extract"], Jsonb(wire.encode_many([doc]))),
        )
    pg.commit()
    stage, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["stage"],
        recipe="hf",
        source="hf",
        node="stage",
        module="ledger.stage",
        config={"replace": True, "replace_scope": "partition",
                "replace_guard": "census"},
        spec=spec,
        params={"anchor_ids": ["hf:docs/transformers/quicktour"]},
    )

    assert ledger_stage(stage).exhausted
    with pg.cursor() as cur:
        cur.execute(
            "SELECT ingested FROM source_units "
            "WHERE source = 'hf' AND store = 'root' "
            "AND unit_key = 'docs/transformers'")
        assert cur.fetchone() == (None,)


def test_store_repos_writes_wide_table_and_advances_parent(pg):
    from windex.worker import dag

    _seed(pg, source="gh", store="gh_hours", key="2026-01-01-0.json.gz",
          upstream={"key": "2026-01-01-0.json.gz"})
    run_id = dag.submit_run(
        pg,
        recipe="gh",
        source="github",
        spec={},
        dedupe_key="repo-store",
        tasks=[
            {"node": "watch", "module": "github.watch_events", "kind": "catalog"},
            {"node": "repos", "module": "store.repos", "kind": "collect",
             "depends_on": ["watch"]},
        ],
    )
    assert run_id is not None
    with pg.cursor() as cur:
        cur.execute("SELECT node, id FROM run_tasks WHERE run_id = %s", (run_id,))
        tasks = dict(cur.fetchall())
        record = PartitionRecord(
            store="repos",
            key="42",
            ref=PartitionRef(
                store="gh_hours", key="2026-01-01-0.json.gz"),
            stage="candidate",
            payload={"repo_id": 42, "full_name": "openai/example"},
            delta={"star_events": 3},
        )
        cur.execute(
            """
            INSERT INTO task_units
                   (run_id, task_id, unit_key, state, outputs, finished_at)
            VALUES (%s, %s, 'hour', 'done', %s, now())
            """,
            (run_id, tasks["watch"], Jsonb(wire.encode_many([record]))),
        )
    pg.commit()
    sink, _ = _ctx(
        pg,
        run_id=run_id,
        task_id=tasks["repos"],
        recipe="gh",
        source="github",
        node="repos",
        module="store.repos",
        config={"store": "repos"},
    )
    assert store_repos(sink).stats["stored"] == 1
    with pg.cursor() as cur:
        cur.execute(
            "SELECT full_name, star_events FROM repos WHERE repo_id = 42")
        assert cur.fetchone() == ("openai/example", 3)
        cur.execute(
            "SELECT ingested FROM source_units "
            "WHERE source = 'gh' AND store = 'gh_hours'")
        assert cur.fetchone()[0] == {"key": "2026-01-01-0.json.gz"}


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


def test_path_manifest_honors_rolling_backfill_window(pg):
    today = fetch_module.date.today()
    recent = today - fetch_module.timedelta(days=30)
    old = today - fetch_module.timedelta(days=120)
    manifest = "\n".join([
        "crawl-data/CC-NEWS/"
        f"{recent:%Y/%m}/CC-NEWS-{recent:%Y%m%d}120000-00000.warc.gz",
        "crawl-data/CC-NEWS/"
        f"{old:%Y/%m}/CC-NEWS-{old:%Y%m%d}120000-00000.warc.gz",
    ]).encode()
    paths, _ = _catalog_graph(
        pg,
        suffix="rolling-paths",
        module="list.path_manifest_gz",
        config={
            "key_pattern": [r"CC-NEWS-\d+-\d+\.warc\.gz$"],
            "max_age_days": 90,
        },
        body=gzip.compress(manifest),
    )

    assert list_path_manifest_gz(paths).stats["records"] == 1
    [(_, records)] = _outputs(pg, paths.task_id)
    assert records[0].key.endswith(
        f"CC-NEWS-{recent:%Y%m%d}120000-00000.warc.gz")


def test_hf_sync_filters_roots_before_probing_llms(pg, monkeypatch):
    sitemap_index = b"""\
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://huggingface.co/sitemap-doc.xml</loc></sitemap>
</sitemapindex>"""
    sitemap_docs = b"""\
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://huggingface.co/docs/keep</loc></url>
  <url><loc>https://huggingface.co/docs/no-llms</loc></url>
  <url><loc>https://huggingface.co/docs/ignore</loc></url>
</urlset>"""
    requested = []

    def page(_ctx, unit, _client, _robots, _limiter):
        url = str(unit.payload.get("url") or "https://huggingface.co/sitemap.xml")
        requested.append(url)
        if url.endswith("sitemap.xml"):
            body = sitemap_index
        elif url.endswith("sitemap-doc.xml"):
            body = sitemap_docs
        elif url.endswith("docs/keep/llms.txt"):
            body = b"- [Index](https://huggingface.co/docs/keep/en/index)"
        elif url.endswith("docs/no-llms/llms.txt"):
            response = httpx.Response(
                404, request=httpx.Request("GET", url))
            response.raise_for_status()
        else:
            raise AssertionError(f"unexpected fetch: {url}")
        return RawBlob(ref=unit.ref, uri=url, body=body, epoch=unit.epoch)

    monkeypatch.setattr("windex.modules.fetch._page", page)
    ctx, _ = _ctx(
        pg,
        task_id=7199,
        recipe="hf",
        source="hf",
        module="http.get",
        config={},
        params={"roots": "docs/keep,docs/no-llms"},
        should_yield=lambda: True,
    )
    unit = WorkUnit(
        ref=PartitionRef(store="", key="sitemap"),
        epoch=ctx.run_id,
    )

    blob = _hf_sync_blob(ctx, unit, object(), object(), object())
    envelope = json.loads(blob.body)

    assert [entry["url"] for entry in envelope["sitemaps"]] == [
        "https://huggingface.co/docs/keep",
        "https://huggingface.co/docs/no-llms",
    ]
    assert envelope["sitemaps"][1]["llms_hash"] is None
    assert envelope["sitemaps"][1]["pages"] == 0
    assert "https://huggingface.co/docs/keep/llms.txt" in requested
    assert "https://huggingface.co/docs/ignore/llms.txt" not in requested
