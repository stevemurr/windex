"""The eleven shipped recipes.

These are the whole claim of the project made checkable: that every source windex
has — a WARC pipeline, an OAI harvest, a BFS crawler, two push endpoints — is the
same kind of object, expressible in one closed vocabulary. If one of them needs an
escape hatch, the vocabulary is wrong, and that is worth finding here rather than
after the executor is written against it.
"""

import pytest
import yaml

from windex.config import Settings
from windex.recipe import compile as recipe_compile
from windex.recipe import parse as P
from windex.recipe import store


@pytest.fixture()
def settings():
    return Settings(_env_file=None)


def _docs():
    return {p.stem: yaml.safe_load(p.read_text())
            for p in sorted(store.builtin_dir().glob("*.yaml"))}


def test_all_eleven_sources_are_expressible(settings):
    """The vocabulary either covers every source or it does not. No partial credit."""
    loaded = store.load_builtins(settings)
    assert {r.name for r in loaded} == {
        "ccnews", "gh", "wiki", "arxiv", "smallweb", "docs", "hn", "hf",
        "memory", "custom", "crawl"}


@pytest.mark.parametrize("name", list(_docs()))
def test_each_builtin_parses_and_round_trips(name, settings):
    doc = _docs()[name]
    once = P.parse(doc, settings, builtin=True).to_dict()
    twice = P.parse(once, settings, builtin=True).to_dict()
    assert once == twice, f"{name} does not round-trip; a frozen run spec would drift"


@pytest.mark.parametrize("name", list(_docs()))
def test_each_builtin_compiles_to_tasks(name, settings):
    """Placement must resolve for every flow, not just the first — a recipe whose
    second flow cannot compile fails at fan-out, hours later."""
    recipe = P.parse(_docs()[name], settings, builtin=True)
    for flow in recipe.flows:
        tasks = recipe_compile.compile_tasks(
            recipe.to_dict(), flow=flow.name, settings=settings)
        assert tasks, f"{name}/{flow.name} compiled to nothing"
        for t in tasks:
            assert set(t) <= recipe_compile.TASK_KEYS


def test_reserved_names_are_only_available_to_builtins(settings):
    """`builtin` is a caller's claim, never a field in the document — a recipe that
    could declare itself builtin could claim `news` and write into that corpus."""
    doc = dict(_docs()["ccnews"])
    with pytest.raises(ValueError, match="reserved"):
        P.parse(doc, settings)              # same document, builtin=False
    P.parse(doc, settings, builtin=True)    # ...allowed for the shipped one


# --- properties the graphs must have ----------------------------------------

def test_the_hard_shapes_are_actually_present(settings):
    """Guards against the recipes being quietly simplified into linear chains,
    which would make the DAG machinery unjustified."""
    by_name = {r.name: r for r in store.load_builtins(settings)}

    # gh: two independent discovery paths converging on one store
    discover = next(f for f in by_name["gh"].flows if f.name == "discover")
    into_repos = [e for e in discover.edges if e[1] == "repos"]
    assert len(into_repos) == 2, "gh's fan-in is the reason edges exist"

    # hf: two stores, two branches, ONE load — the case that forced
    # partition-scoped replace rather than two load nodes
    crawl = next(f for f in by_name["hf"].flows if f.name == "crawl")
    into_stage = [e for e in crawl.edges if e[1] == "stage"]
    assert len(into_stage) == 2

    # crawl: fan-OUT, one body feeding link discovery and text extraction
    c = next(f for f in by_name["crawl"].flows if f.name == "crawl")
    from_get = [e for e in c.edges if e[0] == "get"]
    assert len(from_get) == 2


def test_ccnews_sync_fetches_monthly_manifests(settings):
    """CC-News publishes one WARC manifest per YYYY/MM, not a global listing."""
    recipe = next(
        item for item in store.load_builtins(settings) if item.name == "ccnews")
    sync = next(flow for flow in recipe.flows if flow.name == "sync")
    by_id = {node.id: node for node in sync.nodes}

    assert by_id["months"].uses == "time.calendar"
    assert by_id["months"].config["unit"] == "month"
    assert by_id["listing"].config["url_template"].endswith(
        "/CC-NEWS/{key}/warc.paths.gz")
    assert not by_id["listing"].config.get("missing_ok")
    ingest = next(flow for flow in recipe.flows if flow.name == "ingest")
    pending = next(node for node in ingest.nodes if node.id == "pending")
    assert pending.config["limit"] == "@config.batch_warcs"


def test_wiki_shards_use_the_dated_cirrus_index(settings):
    recipe = next(
        item for item in store.load_builtins(settings) if item.name == "wiki")
    ingest = next(flow for flow in recipe.flows if flow.name == "ingest")
    shard = next(node for node in ingest.nodes if node.id == "shard")

    assert shard.config["url_template"] == (
        "https://dumps.wikimedia.org/other/cirrus_search_index/"
        "{dump_date}/index_name={dump}_content/{key}"
    )


def test_push_sources_have_no_pull_roots(settings):
    """A source is push or pull. Mixing them makes "what does refresh do"
    unanswerable, and the two have opposite rules for absent ids."""
    for name in ("memory", "custom"):
        r = next(x for x in store.load_builtins(settings) if x.name == name)
        kinds = {n.kind for f in r.flows for n in f.nodes}
        assert "receive" in kinds and "discover" not in kinds
        assert r.refresh == (), f"{name} is push-driven; refresh has nothing to do"


def test_only_the_crawler_may_delete_across_a_whole_source(settings):
    """`replace_scope: source` can tombstone anything the recipe owns. Every other
    source that replaces does it per partition, so one docset's refresh cannot
    reach another's documents."""
    wide = []
    for r in store.load_builtins(settings):
        for f in r.flows:
            for n in f.nodes:
                if n.config.get("replace") and n.config.get("replace_scope") == "source":
                    wide.append(r.name)
    assert wide == ["crawl"]


def test_every_replace_is_census_guarded(settings):
    """A truncated or partly failed run deleting "missing" documents is data loss
    dressed as tidying."""
    for r in store.load_builtins(settings):
        for f in r.flows:
            for n in f.nodes:
                if n.config.get("replace"):
                    assert n.config.get("replace_guard") == "census", \
                        f"{r.name}/{f.name}/{n.id} replaces without a census guard"


def test_published_rate_limits_are_floors_not_defaults(settings):
    """arXiv and HF publish a rate. A recipe edit must not be able to go below it,
    so it is a floor on the field rather than a polite default."""
    for name in ("arxiv", "hf"):
        r = next(x for x in store.load_builtins(settings) if x.name == name)
        interval = next(f for f in r.config if f.key == "request_interval")
        assert interval.lo == 3.0, f"{name} must not be allowed faster than 3.0s"


# --- the store ---------------------------------------------------------------

def test_seeding_is_idempotent_and_registers_everything(pg, settings):
    first = store.seed_builtins(pg, settings)
    assert {r["name"] for r in first} == {r.name for r in store.load_builtins(settings)}
    assert all(r["action"] == "created" for r in first)

    again = store.seed_builtins(pg, settings)
    assert all(r["action"] == "unchanged" for r in again), again


def test_a_locally_edited_builtin_is_not_overwritten(pg, settings):
    """`builtin` means shipped and restorable, not overwritten on every deploy.
    Losing a local change to ccnews on an unrelated init-db would make editing a
    built-in feel unsafe, which defeats them being editable."""
    store.seed_builtins(pg, settings)
    with pg.cursor() as cur:
        cur.execute("UPDATE recipes SET spec_hash = 'sha1:edited', builtin = false "
                    "WHERE name = 'ccnews'")
    pg.commit()

    actions = {r["name"]: r["action"] for r in store.seed_builtins(pg, settings)}
    assert actions["ccnews"] == "kept (locally edited)"
    # ...and --force is the way back
    forced = {r["name"]: r["action"] for r in store.seed_builtins(pg, settings, force=True)}
    assert forced["ccnews"] == "updated"


def test_list_omits_the_spec_by_default(pg, settings):
    store.seed_builtins(pg, settings)
    rows = store.list_recipes(pg)
    assert rows and all(r["spec"] is None for r in rows)
    assert all(r["spec"] is not None for r in store.list_recipes(pg, include_spec=True))


def test_get_exposes_the_graph_and_every_name_a_client_needs(pg, settings):
    """The four scattered copies of {ccnews -> news} are what this replaces."""
    store.seed_builtins(pg, settings)
    got = store.get_recipe(pg, "ccnews")
    assert got["source"] == "news" and got["search_name"] == "news"
    assert got["loop_name"] == "ccnews"
    assert set(got["flows"]) == {"sync", "ingest"}
    assert "download" in got["flows"]["ingest"]["nodes"]
    assert store.get_recipe(pg, "nope") is None
