"""HTTPS-only, separately scoped custom-Module administration."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from windex import db
from windex.config import get_settings
from windex.modules import admin as store
from windex.modules.sandbox import execute

router = APIRouter(prefix="/v1/modules")


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class VersionCreate(Strict):
    name: str
    title: str = ""
    description: str = ""
    runtime: str = "python"
    kind: str = "transform"
    port_spec: dict[str, Any]
    parameter_schema: list[dict[str, Any]] = Field(default_factory=list)
    requested_capabilities: list[str] = Field(default_factory=list)
    allowed_hosts: list[str] = Field(default_factory=list)
    resource_limits: dict[str, Any] = Field(default_factory=dict)
    source: str


class FixtureTest(Strict):
    records: list[dict[str, Any]] = Field(max_length=100)
    config: dict[str, Any] = Field(default_factory=dict)


class ModuleVersionModel(Strict):
    id: int
    name: str
    title: str
    description: str
    version: int
    runtime: str
    kind: str
    port_spec: dict[str, Any]
    parameter_schema: list[dict[str, Any]]
    requested_capabilities: list[str]
    allowed_hosts: list[str]
    source_digest: str
    approval_state: str
    resource_limits: dict[str, Any]
    approved_by: str | None
    approved_at: str | None
    revoked_at: str | None
    created_at: str


class ModuleVersionsResponse(Strict):
    versions: list[ModuleVersionModel]


class FixtureTestResponse(Strict):
    result: dict[str, Any]
    version: ModuleVersionModel


@router.get("", response_model=ModuleVersionsResponse)
def module_versions() -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        return {"versions": store.list_versions(conn)}


@router.post("", status_code=201, response_model=ModuleVersionModel)
def module_create(body: VersionCreate) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return store.create_version(conn, **body.model_dump())
    except store.ModuleAdminError as exc:
        raise HTTPException(422, str(exc))


@router.get(
    "/{name}/versions/{version}", response_model=ModuleVersionModel)
def module_get(name: str, version: int) -> dict[str, Any]:
    with db.pooled(get_settings().pg_dsn) as conn:
        result = store.get_version(conn, name, version)
    if result is None:
        raise HTTPException(404, "Module version not found")
    return result


@router.post(
    "/{name}/versions/{version}/test", response_model=FixtureTestResponse)
def module_test(name: str, version: int, body: FixtureTest) -> dict[str, Any]:
    settings = get_settings()
    with db.pooled(settings.pg_dsn) as conn:
        definition = store.get_version(
            conn, name, version, include_source=True)
        if definition is None:
            raise HTTPException(404, "Module version not found")
        if definition["approval_state"] != "validated":
            raise HTTPException(409, "Module version is not validated")
        try:
            result = execute(
                source=definition["source"], records=body.records,
                config=body.config, limits=definition["resource_limits"],
                settings=settings)
            version_info = store.mark_tested(conn, name, version, result)
        except Exception as exc:
            raise HTTPException(422, str(exc))
    return {"result": result, "version": version_info}


@router.post(
    "/{name}/versions/{version}/approve",
    response_model=ModuleVersionModel,
)
def module_approve(name: str, version: int) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return store.approve(
                conn, name, version, approved_by="module_admin API")
    except store.ModuleAdminError as exc:
        raise HTTPException(409, str(exc))


@router.post(
    "/{name}/versions/{version}/revoke",
    response_model=ModuleVersionModel,
)
def module_revoke(name: str, version: int) -> dict[str, Any]:
    try:
        with db.pooled(get_settings().pg_dsn) as conn:
            return store.revoke(
                conn, name, version, by="module_admin API")
    except store.ModuleAdminError as exc:
        raise HTTPException(409, str(exc))


__all__ = ["router"]
