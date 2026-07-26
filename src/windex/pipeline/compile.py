"""Compile immutable Pipeline revisions into frozen worker Tasks."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any

from windex.config import Settings
from windex.pipeline import registry
from windex.pipeline.spec import Flow, Pipeline, parse
from windex.pipeline.validation import deployment_issues, source_capability

TASK_KEYS = frozenset({
    "node", "kind", "module", "lane", "config", "depends_on", "preconditions",
    "weight", "max_attempts", "lease_seconds", "module_version",
    "module_digest", "executor", "captures",
})

LEASE_SECONDS = {
    "net": 900,
    "warc": 1800,
    "cpu_heavy": 1800,
    "gpu": 300,
    "io": 300,
    "maint": 600,
}

KIND_WEIGHT = {
    "discover": 0.1,
    "receive": 0.1,
    "catalog": 0.3,
    "collect": 0.2,
    "fetch": 1.0,
    "extract": 1.0,
    "transform": 0.3,
    "load": 0.5,
}

SECRET_PRECONDITIONS = {"github_tokens": "gh_token"}


def _parsed(spec: dict[str, Any] | Pipeline,
            settings: Settings | None = None) -> Pipeline:
    return spec if isinstance(spec, Pipeline) else parse(spec, settings)


def resolve_parameters(
    pipeline: Pipeline,
    settings: Settings,
    values: Mapping[str, Any],
) -> dict[str, Any]:
    fields = {field.key: field for field in pipeline.parameters}
    unknown = set(values) - set(fields)
    if unknown:
        raise ValueError(
            f"unknown Pipeline parameter(s): {', '.join(sorted(unknown))}")
    result: dict[str, Any] = {}
    for key, declaration in fields.items():
        if key in values and values[key] is not None:
            try:
                result[key] = declaration.coerce(values[key], settings)
            except ValueError as exc:
                raise ValueError(f"parameters.{key}: {exc}") from exc
        elif declaration.default is not None:
            result[key] = declaration.default
        elif declaration.required:
            raise ValueError(f"parameters.{key} is required")
        else:
            result[key] = None
    return result


def _flow(pipeline: Pipeline, name: str | None) -> Flow:
    selected = name or (
        pipeline.refresh[0] if pipeline.refresh else pipeline.flows[0].name)
    flow = next((value for value in pipeline.flows if value.name == selected), None)
    if flow is None:
        raise ValueError(f"Pipeline has no Flow {selected!r}")
    return flow


def compile_tasks(
    spec: dict[str, Any] | Pipeline,
    *,
    flow: str | None = None,
    settings: Settings | None = None,
    values: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    active_settings = settings or Settings()
    pipeline = _parsed(spec, active_settings)
    resolved = (
        resolve_parameters(pipeline, active_settings, values or {})
        if values is not None else None
    )
    chosen = _flow(pipeline, flow)
    upstream: dict[str, list[str]] = {node.id: [] for node in chosen.nodes}
    captures: dict[str, list[str]] = {node.id: [] for node in chosen.nodes}
    for edge in chosen.edges:
        if edge.source.kind == "node" and edge.target.kind == "node":
            upstream[edge.target.id].append(edge.source.id)
        if edge.source.kind == "node" and edge.target.kind == "output":
            captures[edge.source.id].append(edge.target.id)

    tasks: list[dict[str, Any]] = []
    for node in chosen.nodes:
        module = registry.get(node.uses)
        if module is None:
            raise ValueError(f"unknown Module {node.uses!r}")
        preconditions = set(module.preconditions)
        for declaration in module.fields:
            if declaration.kind == "secret_ref" and node.config.get(declaration.key):
                preconditions.update(
                    SECRET_PRECONDITIONS.get(value, value)
                    for value in (declaration.allow or ())
                )
        config = dict(node.config)
        if resolved is not None:
            for key, value in config.items():
                if isinstance(value, str) and value.startswith("@param."):
                    parameter = value.removeprefix("@param.")
                    if parameter not in resolved:
                        raise ValueError(f"parameters.{parameter} is required")
                    config[key] = resolved[parameter]
        lock = registry.module_lock(node.uses)
        tasks.append({
            "node": node.id,
            "kind": node.kind,
            "module": node.uses,
            "lane": module.lane,
            "config": config,
            "depends_on": sorted(upstream[node.id]),
            "preconditions": sorted(preconditions),
            "weight": KIND_WEIGHT.get(node.kind, 0.5),
            "max_attempts": 3,
            "lease_seconds": LEASE_SECONDS.get(module.lane, 300),
            "module_version": lock["version"],
            "module_digest": lock["digest"],
            "executor": lock["executor"],
            "captures": sorted(captures[node.id]),
        })
    return tasks


def compile_pipeline(
    spec: dict[str, Any] | Pipeline,
    *,
    flow: str | None = None,
    settings: Settings | None = None,
    values: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Any] | None = None,
    source_bound: bool = False,
) -> dict[str, Any]:
    active_settings = settings or Settings()
    pipeline = _parsed(spec, active_settings)
    chosen = _flow(pipeline, flow)
    supplied = dict(inputs or {})
    declared = {value.id: value for value in chosen.inputs}
    unknown = set(supplied) - set(declared)
    if unknown:
        raise ValueError(f"unknown Flow input(s): {', '.join(sorted(unknown))}")
    missing = [
        value.id for value in chosen.inputs
        if value.required and value.id not in supplied
    ]
    if missing:
        raise ValueError(f"required Flow input(s) missing: {', '.join(missing)}")
    for boundary_id, value in supplied.items():
        declaration = declared[boundary_id]
        if declaration.type == "DocumentBatch":
            documents = (
                value.get("documents") if isinstance(value, Mapping) else value)
            if not isinstance(documents, list):
                raise ValueError(
                    f"inputs.{boundary_id} must contain a document array")
            if declaration.max_items is not None \
                    and len(documents) > declaration.max_items:
                raise ValueError(
                    f"inputs.{boundary_id} exceeds max_items "
                    f"{declaration.max_items}")
        raw_size = len(json.dumps(value, default=str).encode())
        if declaration.max_bytes is not None and raw_size > declaration.max_bytes:
            raise ValueError(
                f"inputs.{boundary_id} exceeds max_bytes "
                f"{declaration.max_bytes}")
    tasks = compile_tasks(
        pipeline, flow=chosen.name, settings=active_settings,
        values=values or {})
    if not source_bound:
        source_only = sorted({
            task["module"] for task in tasks
            if set(registry.get(task["module"]).contract_roles) & {
                "state.read", "state.write", "document.identity",
                "document.provenance", "document.staging",
            }
        })
        if source_only:
            raise ValueError(
                "generic Pipeline Run requires a Source for Module(s): "
                + ", ".join(source_only))
    return {
        "flow": chosen.name,
        "parameters": resolve_parameters(
            pipeline, active_settings, dict(values or {})),
        "inputs": supplied,
        "tasks": tasks,
        "module_locks": {
            node.uses: registry.module_lock(node.uses)
            for node in chosen.nodes
        },
        "search_capability": source_capability(pipeline),
        "boundaries": {
            "inputs": [value.to_dict() for value in chosen.inputs],
            "outputs": [value.to_dict() for value in chosen.outputs],
        },
    }


def compile_source(
    spec: dict[str, Any] | Pipeline,
    source: Mapping[str, Any],
    run_overrides: Mapping[str, Any] | None = None,
    *,
    flow: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    active_settings = settings or Settings()
    pipeline = _parsed(spec, active_settings)
    issues = deployment_issues(pipeline, source, settings=active_settings)
    if issues:
        raise ValueError({
            "message": "Source deployment is invalid",
            "issues": [value.to_dict() for value in issues],
        })
    configured = dict(source.get("values") or {})
    configured.update(dict(run_overrides or {}))
    compiled = compile_pipeline(
        pipeline,
        flow=flow,
        settings=active_settings,
        values=configured,
        inputs=source.get("inputs") or {},
        source_bound=True,
    )
    compiled["source"] = {
        key: source.get(key)
        for key in (
            "id", "name", "search_name", "id_prefix", "collection_key",
            "search_profile", "state_namespace", "generation",
        )
    }
    return compiled


def resolve(module: str):
    from windex.pipeline import runners

    runner = runners.RUNNERS.get(module)
    if module == "platform.index" and runner is not None:
        return runner
    if registry.get(module) is not None \
            and registry.module_lock(module)["executor"] == "sandbox":
        from windex.modules.sandbox import custom_runner

        return custom_runner
    if runner is None:
        known = registry.get(module)
        if known is None:
            raise LookupError(f"no such Module: {module!r}")
        raise LookupError(f"Module {module!r} has no approved implementation")
    return runner


def unavailable_modules(tasks: list[dict[str, Any]]) -> list[str]:
    return sorted({
        str(task["module"]) for task in tasks
        if not registry.implemented(str(task["module"]))
    })


def describe_placement() -> list[dict[str, Any]]:
    return [
        {
            "id": module.name,
            "kind": module.kind,
            "lane": module.lane,
            "preconditions": list(module.preconditions),
            "lease_seconds": LEASE_SECONDS.get(module.lane, 300),
            "weight": KIND_WEIGHT.get(module.kind, 0.5),
            "module_version": module.version,
            "module_digest": registry.implementation_digest(module.name),
        }
        for module in registry.MODULES.values()
    ]


__all__ = [
    "KIND_WEIGHT",
    "LEASE_SECONDS",
    "TASK_KEYS",
    "compile_pipeline",
    "compile_source",
    "compile_tasks",
    "describe_placement",
    "resolve",
    "resolve_parameters",
    "unavailable_modules",
]
