"""Search Source capability and deployment validation."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from windex.config import Settings
from windex.pipeline import ports, registry
from windex.pipeline.contracts import SEARCH_SOURCE_CONTRACT, ValidationIssue, issue
from windex.pipeline.spec import Pipeline

_SOURCE_NAME = re.compile(r"^[a-z][a-z0-9_]{1,63}$")


def _reachable(starts: set[str], outgoing: Mapping[str, set[str]]) -> set[str]:
    """Return nodes reached over the same node-to-node edges the compiler runs."""
    reached: set[str] = set()
    pending = list(starts)
    while pending:
        node_id = pending.pop()
        if node_id in reached:
            continue
        reached.add(node_id)
        pending.extend(outgoing.get(node_id, ()) - reached)
    return reached


def source_capability(pipeline: Pipeline) -> dict[str, Any]:
    """Graph-only Search Source contract check."""
    issues: list[ValidationIssue] = []
    ingress: set[str] = set()
    staged = 0
    reachable_staging = 0

    for flow in pipeline.flows:
        outgoing: dict[str, set[str]] = {
            node.id: set()
            for node in flow.nodes
        }
        for edge in flow.edges:
            if edge.source.kind == "node" and edge.target.kind == "node":
                outgoing[edge.source.id].add(edge.target.id)
        input_targets = {
            edge.target.id
            for edge in flow.edges
            if edge.source.kind == "input" and edge.target.kind == "node"
        }
        ingress_nodes: set[str] = set()
        staging_nodes: set[str] = set()
        roles_by_node: dict[str, set[str]] = {}
        for node in flow.nodes:
            module = registry.get(node.uses)
            if module is None:
                continue
            roles = set(module.contract_roles)
            roles_by_node[node.id] = roles
            if "ingress.pull" in roles:
                ingress.add("pull")
                ingress_nodes.add(node.id)
            if "ingress.push" in roles:
                ingress.add("push")
                if node.id not in input_targets:
                    issues.append(issue(
                        f"flows.{flow.name}.nodes.{node.id}",
                        "push_boundary_missing",
                        "push ingress must be connected to a typed Flow input",
                    ))
                else:
                    ingress_nodes.add(node.id)
            if "document.staging" in roles:
                staged += 1
                staging_nodes.add(node.id)

        reached = _reachable(ingress_nodes, outgoing)
        for node_id in sorted(staging_nodes):
            if node_id in reached:
                reachable_staging += 1
            else:
                issues.append(issue(
                    f"flows.{flow.name}.nodes.{node_id}",
                    "disconnected_staging",
                    "platform staging must be reachable from a pull or push ingress "
                    "in the same Flow",
                ))

        for node in flow.nodes:
            kind = ports.KINDS[node.kind]
            roles = roles_by_node.get(node.id, set())
            document_terminal = (
                kind.out == "ExtractedDoc"
                and not outgoing[node.id]
            )
            non_staging_sink = (
                kind.inp == "ExtractedDoc"
                and kind.out is None
                and "document.staging" not in roles
            )
            if document_terminal or non_staging_sink:
                issues.append(issue(
                    f"flows.{flow.name}.nodes.{node.id}",
                    "unsearchable_terminal",
                    "document-producing path must terminate at platform staging",
                ))

    if not ingress:
        issues.append(issue(
            "flows",
            "missing_ingress",
            "Search Source Pipeline requires one pull or push ingress",
        ))
    elif len(ingress) > 1:
        issues.append(issue(
            "flows",
            "mixed_ingress",
            "Search Source Pipeline cannot mix pull and push ingress",
        ))
    if staged == 0:
        issues.append(issue(
            "flows",
            "missing_searchable_output",
            "Search Source Pipeline requires a document staging terminal",
        ))
    elif reachable_staging == 0:
        issues.append(issue(
            "flows",
            "missing_searchable_path",
            "Search Source Pipeline requires an ingress-to-platform-staging path",
        ))
    for index, flow_name in enumerate(pipeline.refresh):
        if flow_name not in {flow.name for flow in pipeline.flows}:
            issues.append(issue(
                f"refresh.{index}",
                "unknown_flow",
                f"unknown refresh Flow {flow_name!r}",
            ))
    capable = not any(value.severity == "error" for value in issues)
    return {
        "contract": SEARCH_SOURCE_CONTRACT,
        "capable": capable,
        "ingress": next(iter(ingress)) if len(ingress) == 1 else None,
        "issues": [value.to_dict() for value in issues],
    }


def deployment_issues(
    pipeline: Pipeline,
    binding: Mapping[str, Any],
    *,
    settings: Settings | None = None,
    identity_conflicts: Mapping[str, set[str]] | None = None,
) -> list[ValidationIssue]:
    """Validate a capable revision against one concrete Source binding."""
    active_settings = settings or Settings()
    issues: list[ValidationIssue] = []
    capability = source_capability(pipeline)
    issues.extend(
        ValidationIssue(**value)
        for value in capability["issues"]
        if value["severity"] == "error"
    )
    for key in (
        "name", "search_name", "id_prefix", "collection_key",
        "search_profile", "state_namespace",
    ):
        value = binding.get(key)
        if not isinstance(value, str) or not value:
            issues.append(issue(key, "required", f"{key} is required"))
    name = binding.get("name")
    if isinstance(name, str) and not _SOURCE_NAME.fullmatch(name):
        issues.append(issue("name", "invalid_identifier",
                            "Source name must be a lowercase identifier"))
    values = binding.get("values") or {}
    if not isinstance(values, Mapping):
        issues.append(issue("values", "invalid_type", "values must be an object"))
        values = {}
    fields = {value.key: value for value in pipeline.parameters}
    for key in values:
        if key not in fields:
            issues.append(issue(
                f"values.{key}", "unknown_parameter",
                f"unknown Pipeline parameter {key!r}",
            ))
    for key, declaration in fields.items():
        if key not in values or values[key] is None:
            if declaration.required and declaration.default is None:
                issues.append(issue(
                    f"values.{key}", "required",
                    f"required Pipeline parameter {key!r} is not configured",
                ))
            continue
        try:
            declaration.coerce(values[key], active_settings)
        except ValueError as exc:
            issues.append(issue(f"values.{key}", "invalid_value", str(exc)))

    conflicts = identity_conflicts or {}
    for field in ("search_name", "id_prefix", "collection_key", "state_namespace"):
        value = binding.get(field)
        if isinstance(value, str) and value in conflicts.get(field, set()):
            issues.append(issue(
                field, "identity_conflict",
                f"{field} {value!r} is already owned by another Source",
            ))

    for flow in pipeline.flows:
        for node in flow.nodes:
            module = registry.get(node.uses)
            if module is None:
                issues.append(issue(
                    f"flows.{flow.name}.nodes.{node.id}.uses",
                    "module_unavailable",
                    f"Module {node.uses!r} is unavailable",
                ))
                continue
            if not registry.implemented(node.uses):
                issues.append(issue(
                    f"flows.{flow.name}.nodes.{node.id}.uses",
                    "module_unavailable",
                    f"Module {node.uses!r} has no approved implementation",
                ))
    return issues


def validate_deployment(
    pipeline: Pipeline,
    binding: Mapping[str, Any],
    *,
    settings: Settings | None = None,
    identity_conflicts: Mapping[str, set[str]] | None = None,
) -> dict[str, Any]:
    issues = deployment_issues(
        pipeline,
        binding,
        settings=settings,
        identity_conflicts=identity_conflicts,
    )
    return {
        "contract": SEARCH_SOURCE_CONTRACT,
        "valid": not any(value.severity == "error" for value in issues),
        "issues": [value.to_dict() for value in issues],
    }
