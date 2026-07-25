"""The recipe parser — the security boundary.

Most of these are about what must be REFUSED. A recipe arrives over a LAN-exposed
API and, later, from a git catalog written by someone else, so the interesting
assertions are all negative: it cannot name a module that does not exist, cannot
reach outside its own id namespace, cannot ask to be faster than the operator
allows, and cannot smuggle in a key that gets silently dropped.
"""

import copy

import pytest

from windex.config import Settings
from windex.recipe import parse as P
from windex.recipe import ports, registry


@pytest.fixture()
def settings():
    return Settings(_env_file=None)


BASE = {
    "name": "claude_docs", "version": 1, "title": "Claude docs",
    "corpus": {"source": "claude_docs", "id_prefix": "claude_docs:",
               "collection": "claude_docs"},
    "config": [
        {"key": "seeds", "kind": "url_list", "required": True, "max_items": 25,
         "stage": "install"},
        {"key": "max_pages", "kind": "int", "lo": 1, "hi": 20000, "default": 500,
         "ceiling": "crawl_max_pages_ceiling"},
    ],
    "state": {"frontier": {"key": "url"}},
    "flows": {"crawl": {
        "nodes": {
            "seed": {"kind": "discover", "uses": "crawl.frontier",
                     "with": {"store": "frontier", "seeds": "@config.seeds",
                              "max_pages": "@config.max_pages"}},
            "get": {"kind": "fetch", "uses": "http.get", "with": {"host_interval": 2.0}},
            "links": {"kind": "catalog", "uses": "crawl.links",
                      "with": {"into": "frontier", "max_depth": 2}},
            "front": {"kind": "collect", "uses": "store.upsert",
                      "with": {"store": "frontier"}},
            "text": {"kind": "extract", "uses": "html.trafilatura",
                     "with": {"min_chars": 200}},
            "stage": {"kind": "load", "uses": "ledger.stage", "with": {"replace": False}},
        },
        "edges": [["seed", "get"], ["get", "links"], ["links", "front"],
                  ["get", "text"], ["text", "stage"]],
    }},
    "refresh": ["crawl"],
}


def mutated(**_ignored):
    return copy.deepcopy(BASE)


def err(settings, mutate) -> str:
    doc = copy.deepcopy(BASE)
    mutate(doc)
    with pytest.raises(ValueError) as exc:
        P.parse(doc, settings)
    return str(exc.value)


# --- the happy path ---------------------------------------------------------

def test_a_valid_recipe_parses_and_orders_the_graph(settings):
    r = P.parse(BASE, settings)
    assert r.name == "claude_docs"
    flow = r.flows[0]
    # Topological: every node appears after everything feeding it.
    pos = {n: i for i, n in enumerate(flow.order)}
    for a, b in flow.edges:
        assert pos[a] < pos[b], f"{a} must be ordered before {b}"


def test_the_stored_form_round_trips(settings):
    """A run freezes its recipe so history stays truthful when the live one is
    edited. If the frozen copy does not reconstruct identically, a re-run is not
    a re-run."""
    once = P.parse(BASE, settings).to_dict()
    twice = P.parse(once, settings).to_dict()
    assert once == twice


def test_fan_out_and_fan_in_are_legal(settings):
    """One fetched page feeds BOTH link discovery and text extraction — the crawler
    already does this inline, and it is why fan-out has to be expressible."""
    r = P.parse(BASE, settings)
    out_edges = [e for e in r.flows[0].edges if e[0] == "get"]
    assert len(out_edges) == 2


# --- clamping ---------------------------------------------------------------

def test_a_recipe_may_ask_to_be_slower_never_faster(settings):
    doc = copy.deepcopy(BASE)
    doc["flows"]["crawl"]["nodes"]["get"]["with"]["host_interval"] = 0.1
    r = P.parse(doc, settings)
    node = next(n for n in r.flows[0].nodes if n.id == "get")
    assert node.config["host_interval"] == settings.crawl_host_interval_min

    doc["flows"]["crawl"]["nodes"]["get"]["with"]["host_interval"] = 30.0
    r = P.parse(doc, settings)
    node = next(n for n in r.flows[0].nodes if n.id == "get")
    assert node.config["host_interval"] == 30.0, "slower must be honoured"


def test_validate_reports_what_it_clamped(settings):
    """Without this the author types 0.1, gets 1.0, and has no idea why."""
    doc = copy.deepcopy(BASE)
    doc["flows"]["crawl"]["nodes"]["get"]["with"]["host_interval"] = 0.1
    out = P.validate(doc, settings)
    assert out["valid"]
    w = next(w for w in out["warnings"] if w["code"] == "clamped")
    assert w["was"] == 0.1 and w["now"] == 1.0


# --- what must be refused ---------------------------------------------------

def test_unknown_module_is_refused(settings):
    """The property that makes a marketplace safe: a recipe references modules this
    windex ships, and an unknown one fails at install rather than downloading."""
    msg = err(settings, lambda d: d["flows"]["crawl"]["nodes"]["get"]
              .__setitem__("uses", "http.evil"))
    assert "unknown module" in msg and "http.evil" in msg


def test_a_misspelled_config_key_is_refused_not_dropped(settings):
    """Silently ignoring it is how you get 'the rate limit setting does nothing'
    reports nobody can reproduce."""
    msg = err(settings, lambda d: d["flows"]["crawl"]["nodes"]["get"]["with"]
              .__setitem__("host_intervall", 5))
    assert "host_intervall" in msg


def test_kind_must_match_the_module(settings):
    msg = err(settings, lambda d: d["flows"]["crawl"]["nodes"]["get"]
              .__setitem__("kind", "extract"))
    assert "is a fetch node" in msg


def test_a_mis_wired_edge_is_refused(settings):
    msg = err(settings, lambda d: d["flows"]["crawl"]["edges"].append(["get", "front"]))
    assert "cannot connect" in msg


def test_cycles_are_refused(settings):
    """A BFS back-edge is expressed as a store write, not an edge — which keeps
    every graph acyclic without losing anything."""
    doc = copy.deepcopy(BASE)
    doc["flows"]["crawl"]["nodes"]["loop"] = {
        "kind": "transform", "uses": "dedup.exact", "with": {}}
    doc["flows"]["crawl"]["edges"] += [["text", "loop"], ["loop", "stage"]]
    P.parse(doc, settings)                       # acyclic: fine
    doc["flows"]["crawl"]["edges"].append(["loop", "loop"])
    with pytest.raises(ValueError, match="cycle|cannot connect"):
        P.parse(doc, settings)


def test_a_recipe_cannot_write_another_sources_ids(settings):
    """`load` forces ids to corpus.id_prefix, so this is the check that stops a
    recipe tombstoning or overwriting another source's documents."""
    msg = err(settings, lambda d: d["corpus"].__setitem__("id_prefix", "gh:"))
    assert "must begin with the recipe name" in msg


def test_reserved_and_malformed_names_are_refused(settings):
    assert "reserved" in err(settings, lambda d: d.__setitem__("name", "news"))
    assert "name must match" in err(settings, lambda d: d.__setitem__("name", "Bad Name"))


def test_a_recipe_cannot_touch_a_store_it_did_not_declare(settings):
    msg = err(settings, lambda d: d["flows"]["crawl"]["nodes"]["front"]["with"]
              .__setitem__("store", "warc_files"))
    assert "undeclared store" in msg


def test_config_references_are_checked_for_existence_and_type(settings):
    assert "not a declared config field" in err(
        settings, lambda d: d["flows"]["crawl"]["nodes"]["seed"]["with"]
        .__setitem__("seeds", "@config.nope"))
    assert "is int, but this field is url_list" in err(
        settings, lambda d: d["flows"]["crawl"]["nodes"]["seed"]["with"]
        .__setitem__("seeds", "@config.max_pages"))


def test_a_secret_may_only_land_on_a_secret_field(settings):
    """A recipe carries the NAME of an operator-provisioned key, never a value —
    and only where the module declared it accepts one."""
    msg = err(settings, lambda d: d["flows"]["crawl"]["nodes"]["get"]["with"]
              .__setitem__("allowed_types", "@secret.github_tokens"))
    assert "secret reference is only allowed" in msg


def test_push_and_pull_cannot_be_mixed(settings):
    doc = copy.deepcopy(BASE)
    doc["flows"]["crawl"]["nodes"]["push"] = {
        "kind": "receive", "uses": "push.docs", "with": {}}
    doc["flows"]["crawl"]["edges"].append(["push", "stage"])
    with pytest.raises(ValueError, match="push or pull"):
        P.parse(doc, settings)


def test_a_dangling_node_is_refused(settings):
    """A graph that parses but silently does nothing is worse than one that fails."""
    doc = copy.deepcopy(BASE)
    doc["flows"]["crawl"]["nodes"]["orphan"] = {
        "kind": "extract", "uses": "markdown.passthrough", "with": {}}
    with pytest.raises(ValueError, match="no input|nothing consumes"):
        P.parse(doc, settings)


def test_required_module_config_is_enforced(settings):
    msg = err(settings, lambda d: d["flows"]["crawl"]["nodes"]["seed"]
              .__setitem__("with", {"store": "frontier"}))
    assert "requires 'seeds'" in msg


@pytest.mark.parametrize("mutate,fragment", [
    (lambda d: d.__setitem__("flows", {}), "at least one flow"),
    (lambda d: d.__setitem__("schema", "windex.recipe/99"), "unsupported schema"),
    (lambda d: d["flows"]["crawl"].__setitem__("edges", [["seed", "nope"]]), "unknown node"),
])
def test_structural_errors(settings, mutate, fragment):
    assert fragment in err(settings, mutate)


# --- the registry itself ----------------------------------------------------

def test_every_module_declares_a_known_kind_and_valid_params():
    for name, m in registry.MODULES.items():
        assert m.kind in ports.KINDS, f"{name} has unknown kind {m.kind}"
        assert m.name == name
        keys = [f.key for f in m.fields]
        assert len(keys) == len(set(keys)), f"{name} has duplicate config keys"
        for c in m.capabilities:
            assert c in registry.CAPABILITIES, f"{name}: unknown capability {c}"


def test_only_http_get_may_reach_a_caller_chosen_host():
    """Every other network module can reach exactly the upstream it was written
    for. http.get is the one that takes an arbitrary host — and the only one
    carrying robots, rate limiting, size caps and the SSRF guard."""
    wild = [m.name for m in registry.MODULES.values() if "*" in m.allowed_hosts]
    assert wild == ["http.get"]
    guard = {f.key: f for f in registry.MODULES["http.get"].fields}
    assert guard["ssrf_guard"].locked_reason
    assert guard["robots"].locked_reason


def test_the_registry_describes_itself_without_hardcoded_vocabulary():
    d = registry.describe()
    assert d["modules"] and d["kinds"] and d["port_types"]
    kinds = {k["id"] for k in d["kinds"]}
    assert {m["kind"] for m in d["modules"]} <= kinds
    # every declared port type is referenced by some kind, and vice versa
    used = {k["in"] for k in d["kinds"]} | {k["out"] for k in d["kinds"]}
    assert set(d["port_types"]) == (used - {None})
