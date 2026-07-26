from __future__ import annotations

from copy import deepcopy

import pytest

from windex.pipeline import compile_pipeline, parse, validate
from windex.pipeline.contracts import PIPELINE_SCHEMA, SEARCH_SOURCE_CONTRACT
from windex.pipeline.hashing import semantic_hash
from windex.pipeline.store import load_seed_matrix
from windex.pipeline.validation import source_capability, validate_deployment


def pushed_pipeline() -> dict:
    return {
        "schema": PIPELINE_SCHEMA,
        "parameters": [
            {"key": "max_docs", "kind": "int", "default": 100, "lo": 1, "hi": 1000},
        ],
        "state": {},
        "flows": {
            "receive": {
                "inputs": [
                    {"id": "documents", "type": "DocumentBatch",
                     "max_items": 1000, "max_bytes": 1048576},
                ],
                "outputs": [],
                "nodes": {
                    "receive": {
                        "kind": "receive",
                        "uses": "push.docs",
                        "with": {"max_docs": "@param.max_docs"},
                    },
                    "load": {
                        "kind": "load",
                        "uses": "ledger.stage",
                        "with": {},
                    },
                },
                "edges": [
                    {"from": {"input": "documents"}, "to": {"node": "receive"}},
                    {"from": {"node": "receive"}, "to": {"node": "load"}},
                ],
            },
        },
        "refresh": [],
    }


def binding() -> dict:
    return {
        "name": "team_docs",
        "search_name": "team_docs",
        "id_prefix": "team_docs:",
        "collection_key": "team_docs",
        "search_profile": "documents",
        "state_namespace": "team_docs",
        "values": {"max_docs": 50},
    }


def test_pipeline_normalizes_and_round_trips(settings):
    parsed = parse(pushed_pipeline(), settings)
    assert parsed.to_dict() == parse(parsed.to_dict(), settings).to_dict()
    assert "corpus" not in parsed.to_dict()


def test_diagnostics_have_stable_paths_and_codes(settings):
    bad = pushed_pipeline()
    bad["flows"]["receive"]["nodes"]["receive"]["with"]["maximim"] = 3
    result = validate(bad, settings)
    assert result["valid"] is False
    assert result["issues"] == [{
        "path": "flows.receive.nodes.receive.with.maximim",
        "code": "unknown_module_parameter",
        "severity": "error",
        "message": "Module 'push.docs' has no parameter 'maximim'",
    }]


def test_pipeline_hash_includes_locked_module_implementations(settings):
    document = pushed_pipeline()
    first = semantic_hash(document, settings)
    changed = deepcopy(document)
    changed["parameters"][0]["default"] = 51
    assert semantic_hash(changed, settings) != first
    assert semantic_hash(document, settings) == first


def test_source_capability_and_deployment_are_separate(settings):
    pipeline = parse(pushed_pipeline(), settings)
    capability = source_capability(pipeline)
    assert capability["contract"] == SEARCH_SOURCE_CONTRACT
    assert capability["capable"] is True
    assert capability["ingress"] == "push"

    incomplete = binding()
    incomplete.pop("collection_key")
    deployed = validate_deployment(pipeline, incomplete, settings=settings)
    assert deployed["valid"] is False
    assert any(value["path"] == "collection_key" for value in deployed["issues"])


def test_generic_compile_freezes_module_locks_and_values(settings):
    compiled = compile_pipeline(
        pushed_pipeline(),
        flow="receive",
        settings=settings,
        values={"max_docs": 17},
        inputs={"documents": {"documents": []}},
        source_bound=True,
    )
    assert compiled["parameters"]["max_docs"] == 17
    assert set(compiled["module_locks"]) == {"push.docs", "ledger.stage"}
    assert all(task["module_digest"].startswith("sha256:") for task in compiled["tasks"])


def test_ccnews_seed_has_two_bounded_warc_lanes_and_serial_dedup(settings):
    seed = next(
        item for item in load_seed_matrix(settings)
        if item["name"] == "ccnews"
    )
    compiled = compile_pipeline(
        seed["spec"], flow="ingest", settings=settings, values={},
        source_bound=True,
    )
    tasks = {task["node"]: task for task in compiled["tasks"]}

    assert tasks["extract_a"]["lane"] == "warc"
    assert tasks["extract_b"]["lane"] == "warc"
    assert tasks["pending_a"]["config"]["limit"] == 4
    assert tasks["pending_b"]["config"]["limit"] == 4
    assert tasks["canon"]["depends_on"] == ["extract_a", "extract_b"]
    assert tasks["exact"]["depends_on"] == ["canon"]


def test_pipeline_rejects_mixed_ingress(settings):
    document = pushed_pipeline()
    flow = document["flows"]["receive"]
    flow["nodes"]["discover"] = {
        "kind": "discover",
        "uses": "static.once",
        "with": {},
    }
    flow["nodes"]["fetch"] = {"kind": "fetch", "uses": "http.get", "with": {}}
    flow["nodes"]["extract"] = {
        "kind": "extract",
        "uses": "html.trafilatura",
        "with": {},
    }
    flow["nodes"]["load2"] = {"kind": "load", "uses": "ledger.stage", "with": {}}
    flow["edges"] += [
        {"from": {"node": "discover"}, "to": {"node": "fetch"}},
        {"from": {"node": "fetch"}, "to": {"node": "extract"}},
        {"from": {"node": "extract"}, "to": {"node": "load2"}},
    ]
    pipeline = parse(document, settings)
    result = source_capability(pipeline)
    assert result["capable"] is False
    assert any(value["code"] == "mixed_ingress" for value in result["issues"])


def test_required_flow_inputs_are_enforced(settings):
    with pytest.raises(ValueError, match="required Flow input"):
        compile_pipeline(
            pushed_pipeline(),
            flow="receive",
            settings=settings,
            values={},
            inputs={},
        )
