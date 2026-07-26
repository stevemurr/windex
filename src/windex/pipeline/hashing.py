"""Content addressing for immutable Pipeline revisions."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from windex.config import Settings
from windex.pipeline import registry
from windex.pipeline.contracts import PIPELINE_SCHEMA, REGISTRY_CONTRACT
from windex.pipeline.spec import Pipeline, parse


def module_locks(pipeline: Pipeline) -> dict[str, dict[str, str]]:
    names = sorted({
        node.uses
        for flow in pipeline.flows
        for node in flow.nodes
    })
    return {name: registry.module_lock(name) for name in names}


def semantic_document(
    spec: dict[str, Any] | Pipeline,
    settings: Settings | None = None,
) -> dict[str, Any]:
    pipeline = spec if isinstance(spec, Pipeline) else parse(spec, settings)
    return {
        "pipeline_contract": PIPELINE_SCHEMA,
        "registry_contract": REGISTRY_CONTRACT,
        "module_locks": module_locks(pipeline),
        "spec": pipeline.to_dict(),
    }


def semantic_hash(
    spec: dict[str, Any] | Pipeline,
    settings: Settings | None = None,
) -> str:
    canonical = json.dumps(
        semantic_document(spec, settings),
        sort_keys=True,
        separators=(",", ":"),
    )
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


def layout_etag(layout: dict[str, Any]) -> str:
    canonical = json.dumps(layout, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


__all__ = ["layout_etag", "module_locks", "semantic_document", "semantic_hash"]
