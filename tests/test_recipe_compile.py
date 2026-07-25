"""`compile_tasks` — the seam the worker pool and the scheduler were built against.

Both subsystems were written in parallel against a declared contract while this
did not exist: the scheduler takes `compile_tasks(spec) -> [task]`, the worker
takes `resolve(module) -> Runner`. These tests are mostly about that contract
holding, because a mismatch here is not a compile error anywhere — it is a run
that fans out wrong at 3am.
"""

import pytest

from windex.recipe import compile as C
from windex.recipe import registry
from windex.worker import protocol

BASE = {
    "name": "probe", "corpus": {"source": "probe"},
    "config": [{"key": "seeds", "kind": "url_list", "required": True}],
    "state": {"frontier": {"key": "url"}},
    "flows": {
        "crawl": {"nodes": {
            "seed": {"kind": "discover", "uses": "crawl.frontier",
                     "with": {"store": "frontier", "seeds": "@config.seeds"}},
            "get": {"kind": "fetch", "uses": "http.get", "with": {}},
            "text": {"kind": "extract", "uses": "html.trafilatura", "with": {}},
            "stage": {"kind": "load", "uses": "ledger.stage", "with": {}}},
            "edges": [["seed", "get"], ["get", "text"], ["text", "stage"]]},
        "sweep": {"nodes": {
            "d": {"kind": "discover", "uses": "state.pending",
                  "with": {"store": "frontier"}},
            "g": {"kind": "fetch", "uses": "http.download",
                  "with": {"url_template": "https://e.dev/{key}",
                           "allowed_hosts": "e.dev"}},
            "x": {"kind": "extract", "uses": "warc.datatrove", "with": {}},
            "l": {"kind": "load", "uses": "ledger.stage", "with": {}}},
            "edges": [["d", "g"], ["g", "x"], ["x", "l"]]},
    },
    "refresh": ["crawl", "sweep"],
}


def by_node(tasks):
    return {t["node"]: t for t in tasks}


# --- the contract ------------------------------------------------------------

def test_emits_only_keys_run_tasks_accepts():
    """The scheduler REFUSES unknown keys rather than dropping them, so an extra
    key here is a hard failure at fan-out, not a quiet one."""
    for task in C.compile_tasks(BASE):
        assert set(task) <= C.TASK_KEYS, set(task) - C.TASK_KEYS
        assert {"node", "kind", "module"} <= set(task)


def test_lanes_come_from_the_module_not_the_kind():
    """Inferring the lane from `kind` would put a 333MB shard reader in the same
    lane as a status query. cpu_heavy is capped at 1 because that cap IS the
    memory ceiling on this box."""
    t = by_node(C.compile_tasks(BASE, flow="sweep"))
    assert t["g"]["lane"] == "net"          # http.download
    assert t["x"]["lane"] == "cpu_heavy"    # warc.datatrove
    assert t["d"]["lane"] == "io"           # state.pending
    assert all(t[n]["lane"] in protocol.LANES for n in t)


def test_dependencies_are_the_incoming_edges():
    t = by_node(C.compile_tasks(BASE))
    assert t["seed"]["depends_on"] == []
    assert t["get"]["depends_on"] == ["seed"]
    assert t["stage"]["depends_on"] == ["text"]


def test_tasks_come_out_in_execution_order():
    """`order` is topological from parse, so a run's task list reads correctly
    without being sorted again."""
    nodes = [t["node"] for t in C.compile_tasks(BASE)]
    assert nodes.index("seed") < nodes.index("get") < nodes.index("text")


def test_preconditions_are_declared_and_include_referenced_secrets():
    """A module naming a secret must wait for it, not burn an attempt discovering
    the token is missing."""
    t = by_node(C.compile_tasks(BASE, flow="sweep"))
    assert "storage:staging" in t["l"]["preconditions"]
    assert "storage:downloads" in t["x"]["preconditions"]

    doc = dict(BASE)
    doc["flows"] = {"h": {"nodes": {
        "d": {"kind": "discover", "uses": "state.repos_pending", "with": {}},
        "g": {"kind": "fetch", "uses": "github.graphql_batch",
              "with": {"token_ref": "@secret.github_tokens"}},
        "x": {"kind": "extract", "uses": "github.compose_doc", "with": {}},
        "l": {"kind": "load", "uses": "ledger.stage", "with": {}}},
        "edges": [["d", "g"], ["g", "x"], ["x", "l"]]}}
    doc["refresh"] = ["h"]
    assert "gh_token" in by_node(C.compile_tasks(doc))["g"]["preconditions"]


def test_every_precondition_is_one_the_worker_knows():
    """An unknown precondition parks a task forever, so the two vocabularies have
    to agree — and nothing checks that at import time."""
    from windex.worker import preconditions

    known = set(preconditions.KNOWN) | set(preconditions.ALIASES)
    for m in registry.MODULES.values():
        assert set(m.preconditions) <= known, f"{m.name}: {m.preconditions}"


def test_every_lane_is_one_the_worker_serves():
    for m in registry.MODULES.values():
        assert m.lane in protocol.LANES, f"{m.name} declares lane {m.lane!r}"


def test_weights_are_not_flat():
    """A flat weight makes ccnews's extract node — 80% of the wall time — one
    ninth of the progress bar, which is worse than no bar because it looks
    precise."""
    weights = {t["node"]: t["weight"] for t in C.compile_tasks(BASE)}
    assert weights["get"] > weights["seed"]
    assert len(set(weights.values())) > 1


def test_lease_is_per_lane():
    """A polite 1-req/3s fetch is legitimately quiet for minutes; an embed slice
    that stops reporting for two minutes has stopped."""
    t = by_node(C.compile_tasks(BASE))
    assert t["get"]["lease_seconds"] > t["stage"]["lease_seconds"]


# --- flow selection -----------------------------------------------------------

def test_flow_defaults_to_the_first_refresh_entry():
    assert {t["node"] for t in C.compile_tasks(BASE)} == {"seed", "get", "text", "stage"}


def test_an_unknown_flow_is_an_error_not_an_empty_run():
    with pytest.raises(ValueError, match="no flow"):
        C.compile_tasks(BASE, flow="nope")


def test_flows_compile_independently():
    """Flows are separate runs communicating through stores, never edges — which
    is what keeps every graph acyclic while a store is both read and written."""
    assert {t["node"] for t in C.compile_tasks(BASE, flow="sweep")} == {"d", "g", "x", "l"}


# --- resolve ------------------------------------------------------------------

def test_resolve_refuses_an_unknown_module():
    with pytest.raises(LookupError, match="no such module"):
        C.resolve("evil.exfiltrate")


def test_resolve_says_declared_but_unimplemented_distinctly():
    """The registry describes modules the executor cannot yet run — that ordering
    is deliberate, so the message has to distinguish the two cases or every
    unimplemented module looks like a typo."""
    with pytest.raises(LookupError, match="declared but not yet implemented"):
        C.resolve("http.get")


def test_the_scheduler_default_compiler_path_resolves():
    """The scheduler's default is the string "windex.recipe:compile_tasks". If that
    import path moves, fan-out fails at runtime with an import error and no test
    would otherwise notice."""
    import importlib

    mod, _, attr = "windex.recipe:compile_tasks".partition(":")
    assert callable(getattr(importlib.import_module(mod), attr))
