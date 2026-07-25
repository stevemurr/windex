"""Immutable, approval-gated local Module versions."""

from __future__ import annotations

import ast
import hashlib
import inspect
import re
from collections.abc import Mapping
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from windex.pipeline.events import append
from windex.schema.param import Param

RUNTIMES = ("python",)
KINDS = ("transform", "extract")
APPROVAL_STATES = ("draft", "validated", "tested", "available", "rejected", "revoked")
_DENIED_CAPABILITIES = {
    "network", "database", "qdrant", "source_state", "host_files", "secrets",
    "container_runtime",
}
_NAME = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
_LIMITS = {
    "cpu_seconds": (1, 30),
    "wall_seconds": (0.1, 60),
    "memory_mb": (32, 1024),
    "processes": (1, 4),
    "output_bytes": (1024, 64 * 1024 * 1024),
}


class ModuleAdminError(RuntimeError):
    pass


def validate_source(source: str, runtime: str = "python") -> dict[str, Any]:
    if runtime != "python":
        raise ModuleAdminError("only the python runtime is currently available")
    if len(source.encode()) > 256_000:
        raise ModuleAdminError("Module source exceeds 256 KiB")
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise ModuleAdminError(
            f"syntax error at line {exc.lineno}: {exc.msg}") from exc
    denied = []
    transforms = 0
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            denied.append({"line": node.lineno, "code": "imports_forbidden"})
        if isinstance(node, ast.ClassDef):
            denied.append({"line": node.lineno, "code": "classes_forbidden"})
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            denied.append({
                "line": node.lineno, "code": "private_attributes_forbidden"})
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            denied.append({
                "line": node.lineno, "code": "private_names_forbidden"})
        if isinstance(node, ast.FunctionDef) and node.name == "transform":
            transforms += 1
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id in {
                    "eval", "exec", "compile", "open", "__import__", "globals",
                    "locals", "vars", "dir", "getattr", "setattr", "delattr",
                    "input", "breakpoint", "help",
                }:
            denied.append({
                "line": node.lineno,
                "code": f"{node.func.id}_forbidden",
            })
    if transforms != 1:
        denied.append({
            "line": 1, "code": "transform_required",
            "message": "define exactly one transform(record, config) function",
        })
    return {"valid": not denied, "issues": denied}


def create_version(
    conn: psycopg.Connection,
    *,
    name: str,
    source: str,
    runtime: str,
    kind: str,
    port_spec: Mapping[str, Any],
    parameter_schema: list[dict[str, Any]],
    requested_capabilities: list[str] | None = None,
    allowed_hosts: list[str] | None = None,
    resource_limits: Mapping[str, Any] | None = None,
    title: str = "",
    description: str = "",
) -> dict[str, Any]:
    from windex.pipeline import ports, registry

    if not _NAME.fullmatch(name):
        raise ModuleAdminError(
            "Module name must be a lowercase dotted identifier")
    if runtime not in RUNTIMES:
        raise ModuleAdminError("unsupported Module runtime")
    if kind not in KINDS:
        raise ModuleAdminError("custom Modules may only be transform or extract")
    expected = ports.KINDS[kind]
    if (
        port_spec.get("input") != expected.inp
        or port_spec.get("output") != expected.out
    ):
        raise ModuleAdminError(
            f"{kind} Modules require {expected.inp} -> {expected.out} ports")
    requested = set(requested_capabilities or ())
    if requested & _DENIED_CAPABILITIES:
        raise ModuleAdminError(
            "forbidden capabilities: " + ", ".join(sorted(requested & _DENIED_CAPABILITIES)))
    if allowed_hosts:
        raise ModuleAdminError("custom Modules cannot request network hosts")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM module_definitions WHERE name = %s)",
            (name,),
        )
        defined = cur.fetchone()[0]
    if registry.get(name) is not None and not defined:
        raise ModuleAdminError(f"Module name collides with built-in {name!r}")

    accepted = set(inspect.signature(Param).parameters)
    seen: set[str] = set()
    normalized_parameters: list[dict[str, Any]] = []
    for index, raw in enumerate(parameter_schema):
        unknown = set(raw) - accepted
        if unknown:
            raise ModuleAdminError(
                f"parameter_schema[{index}] has unknown fields: "
                + ", ".join(sorted(unknown)))
        try:
            parameter = Param(**raw)
        except (TypeError, ValueError) as exc:
            raise ModuleAdminError(
                f"invalid parameter_schema[{index}]: {exc}") from exc
        if parameter.key in seen:
            raise ModuleAdminError(
                f"duplicate Module parameter {parameter.key!r}")
        seen.add(parameter.key)
        normalized_parameters.append(parameter.to_spec())

    supplied_limits = dict(resource_limits or {})
    unknown_limits = set(supplied_limits) - set(_LIMITS)
    if unknown_limits:
        raise ModuleAdminError(
            "unknown resource limits: " + ", ".join(sorted(unknown_limits)))
    digest = "sha256:" + hashlib.sha256(source.encode()).hexdigest()
    validation = validate_source(source, runtime)
    state = "validated" if validation["valid"] else "draft"
    limits = {
        "cpu_seconds": 5,
        "wall_seconds": 10,
        "memory_mb": 256,
        "processes": 1,
        "output_bytes": 8 * 1024 * 1024,
        **supplied_limits,
    }
    for key, (lower, upper) in _LIMITS.items():
        try:
            value = float(limits[key])
        except (TypeError, ValueError) as exc:
            raise ModuleAdminError(f"{key} must be numeric") from exc
        if not lower <= value <= upper:
            raise ModuleAdminError(
                f"{key} must be between {lower} and {upper}")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO module_definitions (name, title, description)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (name) DO UPDATE SET
                       title = EXCLUDED.title, description = EXCLUDED.description,
                       updated_at = now()
                   RETURNING id""",
                (name, title, description),
            )
            module_id = cur.fetchone()[0]
            cur.execute(
                "SELECT id FROM module_definitions WHERE id = %s FOR UPDATE",
                (module_id,),
            )
            cur.execute(
                "SELECT coalesce(max(version), 0) + 1 FROM module_versions "
                "WHERE module_id = %s",
                (module_id,),
            )
            version = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO module_versions
                       (module_id, version, runtime, kind, port_spec,
                        parameter_schema, requested_capabilities, allowed_hosts,
                        source, source_digest, approval_state, resource_limits)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (
                    module_id, version, runtime, kind, Jsonb(dict(port_spec)),
                    Jsonb(normalized_parameters), sorted(requested), [],
                    source, digest, state, Jsonb(limits),
                ),
            )
            cur.fetchone()
            append(
                cur, component="module_admin", event="module.version_created",
                module=name, data={
                    "version": version, "digest": digest, "state": state})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return get_version(conn, name, version, include_source=True)  # type: ignore[return-value]


def get_version(
    conn: psycopg.Connection,
    name: str,
    version: int,
    *,
    include_source: bool = False,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT v.id, d.name, d.title, d.description, v.version, v.runtime,
                      v.kind, v.port_spec, v.parameter_schema,
                      v.requested_capabilities, v.allowed_hosts, v.source_digest,
                      v.approval_state, v.resource_limits, v.approved_by,
                      v.approved_at, v.revoked_at, v.created_at, v.source
                 FROM module_versions v
                 JOIN module_definitions d ON d.id = v.module_id
                WHERE d.name = %s AND v.version = %s""",
            (name, version),
        )
        row = cur.fetchone()
    if row is None:
        return None
    keys = (
        "id", "name", "title", "description", "version", "runtime", "kind",
        "port_spec", "parameter_schema", "requested_capabilities", "allowed_hosts",
        "source_digest", "approval_state", "resource_limits", "approved_by",
        "approved_at", "revoked_at", "created_at", "source",
    )
    result = dict(zip(keys, row))
    for key in ("approved_at", "revoked_at", "created_at"):
        if result[key]:
            result[key] = result[key].isoformat()
    if not include_source:
        result.pop("source")
    return result


def list_versions(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT d.name, v.version FROM module_versions v
                JOIN module_definitions d ON d.id = v.module_id
                ORDER BY d.name, v.version DESC""")
        keys = cur.fetchall()
    return [
        get_version(conn, name, version) for name, version in keys
    ]  # type: ignore[list-item]


def mark_tested(
    conn: psycopg.Connection, name: str, version: int, result: Mapping[str, Any],
) -> dict[str, Any]:
    if not result.get("ok"):
        raise ModuleAdminError("sandbox fixture test did not pass")
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE module_versions v SET approval_state = 'tested'
                 FROM module_definitions d
                WHERE v.module_id = d.id AND d.name = %s AND v.version = %s
                  AND v.approval_state = 'validated'
                RETURNING v.id""",
            (name, version),
        )
        if cur.fetchone() is None:
            raise ModuleAdminError("Module version is not validated")
        append(
            cur, component="module_admin", event="module.fixture_passed",
            module=name, data={"version": version})
    conn.commit()
    return get_version(conn, name, version)  # type: ignore[return-value]


def approve(
    conn: psycopg.Connection, name: str, version: int, *, approved_by: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE module_versions v
                  SET approval_state = 'available', approved_by = %s,
                      approved_at = now()
                 FROM module_definitions d
                WHERE v.module_id = d.id AND d.name = %s AND v.version = %s
                  AND v.approval_state = 'tested'
                RETURNING v.id""",
            (approved_by, name, version),
        )
        if cur.fetchone() is None:
            raise ModuleAdminError("only a fixture-tested version can be approved")
        append(
            cur, component="module_admin", event="module.approved",
            module=name, data={"version": version, "approved_by": approved_by})
    conn.commit()
    result = get_version(conn, name, version)  # type: ignore[assignment]
    from windex.pipeline import registry

    registry.load_custom(conn)
    return result  # type: ignore[return-value]


def revoke(
    conn: psycopg.Connection, name: str, version: int, *, by: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE module_versions v
                  SET approval_state = 'revoked', revoked_at = now()
                 FROM module_definitions d
                WHERE v.module_id = d.id AND d.name = %s AND v.version = %s
                  AND v.approval_state = 'available'
                RETURNING v.id""",
            (name, version),
        )
        if cur.fetchone() is None:
            raise ModuleAdminError("available Module version not found")
        append(
            cur, component="module_admin", event="module.revoked",
            module=name, level="warn", data={"version": version, "by": by})
    conn.commit()
    result = get_version(conn, name, version)
    from windex.pipeline import registry

    registry.load_custom(conn)
    return result  # type: ignore[return-value]


__all__ = [
    "APPROVAL_STATES", "KINDS", "RUNTIMES", "ModuleAdminError", "approve",
    "create_version", "get_version", "list_versions", "mark_tested", "revoke",
    "validate_source",
]
