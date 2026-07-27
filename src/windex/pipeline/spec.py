"""Validation and normalization for ``windex.pipeline/1`` graph documents.

The document contains reusable executable semantics only.  Corpus/search
identity, schedules, configured values, and durable state values belong to a
Source deployment and are therefore deliberately absent.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from windex.config import Settings
from windex.pipeline import ports, registry
from windex.pipeline.contracts import (
    PIPELINE_SCHEMA,
    ContractError,
    ValidationIssue,
    issue,
)
from windex.schema.param import Param

MAX_FLOWS = 8
MAX_NODES = 64
MAX_EDGES = 128
MAX_PARAMETERS = 64
MAX_BOUNDARIES = 32
MAX_STORES = 8

_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_PARAM_REF = re.compile(r"^@param\.([a-z][a-z0-9_]{0,63})$")
_SECRET_REF = re.compile(r"^@secret\.([a-z][a-z0-9_]{0,63})$")


class PipelineValidationError(ContractError):
    pass


def _fail(path: str, code: str, message: str) -> None:
    raise PipelineValidationError([issue(path, code, message)])


def _identifier(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        _fail(path, "invalid_identifier", "must be a lowercase identifier")
    return value


@dataclass(frozen=True)
class Boundary:
    id: str
    type: str
    required: bool = True
    max_items: int | None = None
    max_bytes: int | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id,
            "type": self.type,
            "required": self.required,
        }
        if self.max_items is not None:
            out["max_items"] = self.max_items
        if self.max_bytes is not None:
            out["max_bytes"] = self.max_bytes
        return out


@dataclass(frozen=True)
class Endpoint:
    kind: str
    id: str

    @property
    def key(self) -> str:
        return f"{self.kind}.{self.id}"

    def to_dict(self) -> dict[str, str]:
        return {self.kind: self.id}


@dataclass(frozen=True)
class Edge:
    source: Endpoint
    target: Endpoint

    def to_dict(self) -> dict[str, dict[str, str]]:
        return {"from": self.source.to_dict(), "to": self.target.to_dict()}


@dataclass(frozen=True)
class Node:
    id: str
    kind: str
    uses: str
    config: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "uses": self.uses, "with": self.config}


@dataclass(frozen=True)
class Flow:
    name: str
    inputs: tuple[Boundary, ...]
    outputs: tuple[Boundary, ...]
    nodes: tuple[Node, ...]
    edges: tuple[Edge, ...]
    order: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "inputs": [value.to_dict() for value in self.inputs],
            "outputs": [value.to_dict() for value in self.outputs],
            "nodes": {node.id: node.to_dict() for node in self.nodes},
            "edges": [edge.to_dict() for edge in self.edges],
        }


@dataclass(frozen=True)
class Pipeline:
    parameters: tuple[Param, ...]
    state: dict[str, dict[str, Any]]
    flows: tuple[Flow, ...]
    refresh: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": PIPELINE_SCHEMA,
            "parameters": [param.to_spec() for param in self.parameters],
            "state": self.state,
            "flows": {flow.name: flow.to_dict() for flow in self.flows},
            "refresh": list(self.refresh),
        }


def _parameters(raw: Any) -> tuple[Param, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        _fail("parameters", "invalid_type", "parameters must be a list")
    if len(raw) > MAX_PARAMETERS:
        _fail("parameters", "limit_exceeded",
              f"at most {MAX_PARAMETERS} parameters are allowed")
    result: list[Param] = []
    seen: set[str] = set()
    for index, value in enumerate(raw):
        path = f"parameters.{index}"
        if not isinstance(value, dict):
            _fail(path, "invalid_type", "parameter must be an object")
        key = _identifier(value.get("key"), f"{path}.key")
        if key in seen:
            _fail(f"{path}.key", "duplicate_parameter",
                  f"parameter {key!r} is declared more than once")
        seen.add(key)
        try:
            result.append(Param(
                key=key,
                kind=value.get("kind"),
                lo=value.get("lo"),
                hi=value.get("hi"),
                choices=tuple(value.get("choices") or ()),
                default=value.get("default"),
                label=value.get("label", ""),
                help=value.get("help", ""),
                required=bool(value.get("required", False)),
                stage=value.get("stage", "runtime"),
                editor=value.get("editor", ""),
                section=value.get("section", ""),
                unit=value.get("unit", ""),
                advanced=bool(value.get("advanced", False)),
                secret=bool(value.get("secret", False)),
                prefill=value.get("prefill"),
                pattern=value.get("pattern", ""),
                enum_titles=tuple(value.get("enum_titles") or ()),
                locked_reason=value.get("locked_reason", ""),
                depends_on=value.get("depends_on"),
                clamp_note=value.get("clamp_note", ""),
                enforce=value.get("enforce", "clamp"),
                ceiling=value.get("ceiling"),
                floor=value.get("floor"),
                max_items=value.get("max_items"),
                max_len=value.get("max_len"),
                allow=tuple(value.get("allow") or ()),
            ))
        except ValueError as exc:
            _fail(path, "invalid_parameter", str(exc))
    return tuple(result)


def _resolve_config(value: Any, param: Param, parameters: dict[str, Param],
                    settings: Settings, path: str) -> Any:
    if isinstance(value, str):
        match = _PARAM_REF.fullmatch(value)
        if match:
            key = match.group(1)
            referenced = parameters.get(key)
            if referenced is None:
                _fail(path, "unknown_parameter", f"unknown parameter {key!r}")
            if referenced.kind != param.kind:
                _fail(path, "parameter_type_mismatch",
                      f"parameter {key!r} is {referenced.kind}, expected {param.kind}")
            return value
        match = _SECRET_REF.fullmatch(value)
        if match:
            if param.kind != "secret_ref":
                _fail(path, "secret_not_allowed",
                      "secret references are only allowed for secret_ref fields")
            value = match.group(1)
    try:
        return param.coerce(value, settings)
    except ValueError as exc:
        _fail(path, "invalid_value", str(exc))


def _node(node_id: str, raw: Any, parameters: dict[str, Param],
          settings: Settings, path: str) -> Node:
    _identifier(node_id, f"{path}.id")
    if not isinstance(raw, dict):
        _fail(path, "invalid_type", "node must be an object")
    allowed = {"kind", "uses", "with"}
    unknown = set(raw) - allowed
    if unknown:
        key = sorted(unknown)[0]
        _fail(f"{path}.{key}", "unknown_field", f"unknown node field {key!r}")
    kind = raw.get("kind")
    if kind not in ports.KINDS:
        _fail(f"{path}.kind", "unknown_kind", f"unknown Node kind {kind!r}")
    uses = raw.get("uses")
    module = registry.get(uses) if isinstance(uses, str) else None
    if module is None:
        _fail(f"{path}.uses", "unknown_module", f"unknown Module {uses!r}")
    if module.kind != kind:
        _fail(f"{path}.kind", "module_kind_mismatch",
              f"Module {uses!r} has kind {module.kind!r}, not {kind!r}")
    given = raw.get("with") or {}
    if not isinstance(given, dict):
        _fail(f"{path}.with", "invalid_type", "with must be an object")
    fields = {value.key: value for value in module.fields}
    unknown_config = set(given) - set(fields)
    if unknown_config:
        key = sorted(unknown_config)[0]
        _fail(f"{path}.with.{key}", "unknown_module_parameter",
              f"Module {uses!r} has no parameter {key!r}")
    config: dict[str, Any] = {}
    for key, declaration in fields.items():
        if key in given:
            config[key] = _resolve_config(
                given[key], declaration, parameters, settings, f"{path}.with.{key}")
        elif declaration.required:
            _fail(f"{path}.with.{key}", "required",
                  f"Module {uses!r} requires {key!r}")
        elif declaration.default is not None:
            config[key] = declaration.default
    return Node(id=node_id, kind=kind, uses=uses, config=config)


def _boundaries(raw: Any, path: str) -> tuple[Boundary, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        _fail(path, "invalid_type", "boundaries must be a list")
    if len(raw) > MAX_BOUNDARIES:
        _fail(path, "limit_exceeded",
              f"at most {MAX_BOUNDARIES} boundaries are allowed")
    result: list[Boundary] = []
    seen: set[str] = set()
    for index, value in enumerate(raw):
        item_path = f"{path}.{index}"
        if not isinstance(value, dict):
            _fail(item_path, "invalid_type", "boundary must be an object")
        boundary_id = _identifier(value.get("id"), f"{item_path}.id")
        if boundary_id in seen:
            _fail(f"{item_path}.id", "duplicate_boundary",
                  f"boundary {boundary_id!r} is declared more than once")
        seen.add(boundary_id)
        nominal = value.get("type")
        if nominal not in ports.PORT_TYPES:
            _fail(f"{item_path}.type", "unknown_port_type",
                  f"unknown nominal port type {nominal!r}")
        max_items = value.get("max_items")
        max_bytes = value.get("max_bytes")
        if max_items is not None and (
                not isinstance(max_items, int) or isinstance(max_items, bool)
                or max_items < 1 or max_items > 1_000_000):
            _fail(f"{item_path}.max_items", "invalid_limit",
                  "max_items must be between 1 and 1000000")
        if max_bytes is not None and (
                not isinstance(max_bytes, int) or isinstance(max_bytes, bool)
                or max_bytes < 1 or max_bytes > 1_073_741_824):
            _fail(f"{item_path}.max_bytes", "invalid_limit",
                  "max_bytes must be between 1 and 1073741824")
        result.append(Boundary(
            id=boundary_id,
            type=nominal,
            required=bool(value.get("required", True)),
            max_items=max_items,
            max_bytes=max_bytes,
        ))
    return tuple(result)


def _endpoint(raw: Any, path: str) -> Endpoint:
    if not isinstance(raw, dict) or len(raw) != 1:
        _fail(path, "invalid_endpoint",
              "endpoint must contain exactly one of input, node, or output")
    kind, value = next(iter(raw.items()))
    if kind not in {"input", "node", "output"}:
        _fail(path, "invalid_endpoint",
              "endpoint must contain input, node, or output")
    return Endpoint(kind=kind, id=_identifier(value, f"{path}.{kind}"))


def _toposort(nodes: dict[str, Node], edges: tuple[Edge, ...], path: str) -> tuple[str, ...]:
    incoming = dict.fromkeys(nodes, 0)
    outgoing: dict[str, list[str]] = {node_id: [] for node_id in nodes}
    for edge in edges:
        if edge.source.kind == "node" and edge.target.kind == "node":
            outgoing[edge.source.id].append(edge.target.id)
            incoming[edge.target.id] += 1
    queue = sorted(key for key, count in incoming.items() if count == 0)
    result: list[str] = []
    while queue:
        current = queue.pop(0)
        result.append(current)
        for target in sorted(outgoing[current]):
            incoming[target] -= 1
            if incoming[target] == 0:
                queue.append(target)
    if len(result) != len(nodes):
        blocked = ", ".join(sorted(set(nodes) - set(result)))
        _fail(path, "cycle", f"graph contains a cycle involving {blocked}")
    return tuple(result)


def _flow(name: str, raw: Any, parameters: dict[str, Param],
          stores: set[str], settings: Settings) -> Flow:
    path = f"flows.{name}"
    _identifier(name, path)
    if not isinstance(raw, dict):
        _fail(path, "invalid_type", "Flow must be an object")
    unknown = set(raw) - {"inputs", "outputs", "nodes", "edges"}
    if unknown:
        key = sorted(unknown)[0]
        _fail(f"{path}.{key}", "unknown_field", f"unknown Flow field {key!r}")
    inputs = _boundaries(raw.get("inputs"), f"{path}.inputs")
    outputs = _boundaries(raw.get("outputs"), f"{path}.outputs")
    input_types = {value.id: value.type for value in inputs}
    output_types = {value.id: value.type for value in outputs}
    raw_nodes = raw.get("nodes")
    if not isinstance(raw_nodes, dict) or not raw_nodes:
        _fail(f"{path}.nodes", "required", "at least one Node is required")
    if len(raw_nodes) > MAX_NODES:
        _fail(f"{path}.nodes", "limit_exceeded",
              f"at most {MAX_NODES} Nodes are allowed")
    nodes = {
        node_id: _node(
            node_id, value, parameters, settings, f"{path}.nodes.{node_id}")
        for node_id, value in raw_nodes.items()
    }
    raw_edges = raw.get("edges") or []
    if not isinstance(raw_edges, list):
        _fail(f"{path}.edges", "invalid_type", "edges must be a list")
    if len(raw_edges) > MAX_EDGES:
        _fail(f"{path}.edges", "limit_exceeded",
              f"at most {MAX_EDGES} Edges are allowed")
    edges: list[Edge] = []
    seen: set[tuple[str, str]] = set()
    for index, raw_edge in enumerate(raw_edges):
        edge_path = f"{path}.edges.{index}"
        if not isinstance(raw_edge, dict) or set(raw_edge) != {"from", "to"}:
            _fail(edge_path, "invalid_edge",
                  "Edge must contain exactly from and to endpoints")
        source = _endpoint(raw_edge["from"], f"{edge_path}.from")
        target = _endpoint(raw_edge["to"], f"{edge_path}.to")
        if source.kind == "output" or target.kind == "input":
            _fail(edge_path, "edge_direction",
                  "Edges run from input/Node to Node/output")
        if source.kind == "input":
            if source.id not in input_types:
                _fail(f"{edge_path}.from.input", "unknown_boundary",
                      f"unknown input boundary {source.id!r}")
            source_type = input_types[source.id]
        else:
            if source.id not in nodes:
                _fail(f"{edge_path}.from.node", "unknown_node",
                      f"unknown Node {source.id!r}")
            source_type = ports.KINDS[nodes[source.id].kind].out
        if target.kind == "output":
            if target.id not in output_types:
                _fail(f"{edge_path}.to.output", "unknown_boundary",
                      f"unknown output boundary {target.id!r}")
            target_type = output_types[target.id]
        else:
            if target.id not in nodes:
                _fail(f"{edge_path}.to.node", "unknown_node",
                      f"unknown Node {target.id!r}")
            target_type = ports.KINDS[nodes[target.id].kind].inp
        if source_type is None or target_type is None or source_type != target_type:
            _fail(edge_path, "port_type_mismatch",
                  f"{source.key} produces {source_type or 'nothing'}, "
                  f"but {target.key} expects {target_type or 'no input'}")
        key = (source.key, target.key)
        if key in seen:
            _fail(edge_path, "duplicate_edge",
                  f"duplicate Edge {source.key} -> {target.key}")
        seen.add(key)
        edges.append(Edge(source=source, target=target))
    edge_tuple = tuple(edges)
    order = _toposort(nodes, edge_tuple, path)
    has_input = {
        edge.target.id for edge in edge_tuple if edge.target.kind == "node"
    }
    has_output = {
        edge.source.id for edge in edge_tuple if edge.source.kind == "node"
    }
    for node_id, node in nodes.items():
        kind = ports.KINDS[node.kind]
        if kind.inp is not None and node_id not in has_input:
            _fail(f"{path}.nodes.{node_id}", "missing_input",
                  f"{node.kind} Node has no input")
        if kind.out is not None and node_id not in has_output:
            _fail(f"{path}.nodes.{node_id}", "dangling_output",
                  f"{node.kind} Node produces output that nothing consumes")
        for key in ("store", "into"):
            reference = node.config.get(key)
            if isinstance(reference, str) and reference and not reference.startswith("@") \
                    and reference not in stores:
                _fail(f"{path}.nodes.{node_id}.with.{key}", "undeclared_store",
                      f"state store {reference!r} is not declared")
    return Flow(
        name=name,
        inputs=inputs,
        outputs=outputs,
        nodes=tuple(nodes[node_id] for node_id in order),
        edges=edge_tuple,
        order=order,
    )


def parse(body: dict[str, Any], settings: Settings | None = None) -> Pipeline:
    if not isinstance(body, dict):
        _fail("", "invalid_type", "Pipeline document must be an object")
    unknown = set(body) - {"schema", "parameters", "state", "flows", "refresh"}
    if unknown:
        key = sorted(unknown)[0]
        _fail(key, "unknown_field", f"unknown Pipeline field {key!r}")
    if body.get("schema") != PIPELINE_SCHEMA:
        _fail("schema", "unsupported_schema",
              f"schema must be {PIPELINE_SCHEMA!r}")
    active_settings = settings or Settings()
    parameters = _parameters(body.get("parameters"))
    by_key = {value.key: value for value in parameters}
    raw_state = body.get("state") or {}
    if not isinstance(raw_state, dict):
        _fail("state", "invalid_type", "state must be an object")
    if len(raw_state) > MAX_STORES:
        _fail("state", "limit_exceeded", f"at most {MAX_STORES} stores are allowed")
    state: dict[str, dict[str, Any]] = {}
    for key, value in raw_state.items():
        _identifier(key, f"state.{key}")
        if not isinstance(value, dict):
            _fail(f"state.{key}", "invalid_type", "state declaration must be an object")
        state[key] = dict(value)
    raw_flows = body.get("flows")
    if not isinstance(raw_flows, dict) or not raw_flows:
        _fail("flows", "required", "at least one Flow is required")
    if len(raw_flows) > MAX_FLOWS:
        _fail("flows", "limit_exceeded", f"at most {MAX_FLOWS} Flows are allowed")
    flows = tuple(
        _flow(name, value, by_key, set(state), active_settings)
        for name, value in raw_flows.items()
    )
    flow_names = {value.name for value in flows}
    raw_refresh = body.get("refresh")
    if not isinstance(raw_refresh if raw_refresh is not None else [], (list, tuple, set)):
        _fail("refresh", "invalid_type", "refresh must be a list")
    refresh = tuple(raw_refresh if raw_refresh is not None else sorted(flow_names))
    for index, name in enumerate(refresh):
        if name not in flow_names:
            _fail(f"refresh.{index}", "unknown_flow", f"unknown Flow {name!r}")
    return Pipeline(
        parameters=parameters,
        state=state,
        flows=flows,
        refresh=refresh,
    )


def validate(body: dict[str, Any], settings: Settings | None = None) -> dict[str, Any]:
    try:
        pipeline = parse(body, settings)
    except PipelineValidationError as exc:
        return {
            "valid": False,
            "issues": [value.to_dict() for value in exc.issues],
            "normalized": None,
            "graph": None,
        }
    warnings: list[ValidationIssue] = []
    return {
        "valid": True,
        "issues": [value.to_dict() for value in warnings],
        "normalized": pipeline.to_dict(),
        "graph": {
            flow.name: {
                "order": list(flow.order),
                "edges": [edge.to_dict() for edge in flow.edges],
            }
            for flow in pipeline.flows
        },
    }


__all__ = [
    "PIPELINE_SCHEMA",
    "Boundary",
    "Edge",
    "Endpoint",
    "Flow",
    "Node",
    "Pipeline",
    "PipelineValidationError",
    "parse",
    "validate",
]
