from __future__ import annotations

from copy import deepcopy

import pytest

from windex.pipeline import compile_pipeline, parse, registry, validate
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


def pulled_pipeline() -> dict:
    return {
        "schema": PIPELINE_SCHEMA,
        "parameters": [],
        "state": {},
        "flows": {
            "ingest": {
                "inputs": [],
                "outputs": [],
                "nodes": {
                    "once": {
                        "kind": "discover",
                        "uses": "static.once",
                        "with": {},
                    },
                    "get": {
                        "kind": "fetch",
                        "uses": "http.get",
                        "with": {},
                    },
                    "document": {
                        "kind": "extract",
                        "uses": "html.trafilatura",
                        "with": {},
                    },
                    "stage": {
                        "kind": "load",
                        "uses": "ledger.stage",
                        "with": {},
                    },
                },
                "edges": [
                    {"from": {"node": "once"}, "to": {"node": "get"}},
                    {"from": {"node": "get"}, "to": {"node": "document"}},
                    {"from": {"node": "document"}, "to": {"node": "stage"}},
                ],
            },
        },
        "refresh": ["ingest"],
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


def test_registry_reports_loader_sanitization_without_a_fake_module():
    document = registry.describe()

    assert document["always_before_load"] == []
    assert "sanitizes document text" in registry.get("ledger.stage").summary.lower()


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


def test_source_capability_rejects_disconnected_staging(settings):
    document = pushed_pipeline()
    flow = document["flows"]["receive"]
    flow["inputs"].append({
        "id": "orphan_documents",
        "type": "ExtractedDoc",
    })
    flow["nodes"]["orphan_stage"] = {
        "kind": "load",
        "uses": "ledger.stage",
        "with": {},
    }
    flow["edges"].append({
        "from": {"input": "orphan_documents"},
        "to": {"node": "orphan_stage"},
    })

    result = source_capability(parse(document, settings))

    assert result["capable"] is False
    assert {
        (value["path"], value["code"])
        for value in result["issues"]
    } == {
        ("flows.receive.nodes.orphan_stage", "disconnected_staging"),
    }


def test_source_capability_requires_ingress_to_staging_path(settings):
    document = pushed_pipeline()
    flow = document["flows"]["receive"]
    flow["inputs"].append({
        "id": "orphan_documents",
        "type": "ExtractedDoc",
    })
    flow["outputs"] = [{
        "id": "documents_out",
        "type": "ExtractedDoc",
    }]
    flow["edges"] = [
        edge for edge in flow["edges"]
        if edge["to"] != {"node": "load"}
    ]
    flow["edges"].extend([
        {
            "from": {"node": "receive"},
            "to": {"output": "documents_out"},
        },
        {
            "from": {"input": "orphan_documents"},
            "to": {"node": "load"},
        },
    ])

    result = source_capability(parse(document, settings))
    issues = {(value["path"], value["code"]) for value in result["issues"]}

    assert result["capable"] is False
    assert ("flows.receive.nodes.load", "disconnected_staging") in issues
    assert ("flows.receive.nodes.receive", "unsearchable_terminal") in issues
    assert ("flows", "missing_searchable_path") in issues


@pytest.mark.parametrize("terminal_kind", ["extract", "transform"])
def test_source_capability_rejects_document_terminal_branches(
    settings,
    terminal_kind,
):
    document = pulled_pipeline()
    flow = document["flows"]["ingest"]
    flow["outputs"] = [{
        "id": "documents_out",
        "type": "ExtractedDoc",
    }]
    if terminal_kind == "extract":
        flow["nodes"]["dead_end"] = {
            "kind": "extract",
            "uses": "html.trafilatura",
            "with": {},
        }
        branch_from = "get"
    else:
        flow["nodes"]["dead_end"] = {
            "kind": "transform",
            "uses": "canonical.url",
            "with": {"strategy": "sha1_of_canonical"},
        }
        branch_from = "document"
    flow["edges"].extend([
        {
            "from": {"node": branch_from},
            "to": {"node": "dead_end"},
        },
        {
            "from": {"node": "dead_end"},
            "to": {"output": "documents_out"},
        },
    ])

    result = source_capability(parse(document, settings))

    assert result["capable"] is False
    assert any(
        value["path"] == "flows.ingest.nodes.dead_end"
        and value["code"] == "unsearchable_terminal"
        for value in result["issues"]
    )


def test_source_capability_preserves_valid_fan_in_fan_out_and_capture(settings):
    document = pulled_pipeline()
    flow = document["flows"]["ingest"]
    flow["outputs"] = [{
        "id": "documents_out",
        "type": "ExtractedDoc",
    }]
    flow["nodes"]["document_b"] = {
        "kind": "extract",
        "uses": "html.trafilatura",
        "with": {},
    }
    flow["edges"].extend([
        {
            "from": {"node": "get"},
            "to": {"node": "document_b"},
        },
        {
            "from": {"node": "document_b"},
            "to": {"node": "stage"},
        },
        {
            "from": {"node": "document"},
            "to": {"output": "documents_out"},
        },
    ])

    result = source_capability(parse(document, settings))

    assert result["capable"] is True
    assert result["issues"] == []


def test_all_canonical_seed_topologies_remain_source_capable(settings):
    results = {
        item["name"]: source_capability(parse(item["spec"], settings))
        for item in load_seed_matrix(settings)
    }

    assert all(result["capable"] for result in results.values()), results


def test_required_flow_inputs_are_enforced(settings):
    with pytest.raises(ValueError, match="required Flow input"):
        compile_pipeline(
            pushed_pipeline(),
            flow="receive",
            settings=settings,
            values={},
            inputs={},
        )
