#!/usr/bin/env python
"""One-time mechanical conversion of checked-in Recipe YAML to Pipeline seeds."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
SEEDS = ROOT / "src/windex/pipeline/seeds"


def references(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("@config.", "@param.")
    if isinstance(value, list):
        return [references(item) for item in value]
    if isinstance(value, dict):
        return {key: references(item) for key, item in value.items()}
    return value


def convert(path: Path) -> None:
    document = yaml.safe_load(path.read_text())
    if document.get("schema") == "windex.pipeline/1":
        return
    flows = {}
    for name, raw in document["flows"].items():
        nodes = references(raw["nodes"])
        edges = [
            {"from": {"node": source}, "to": {"node": target}}
            for source, target in raw.get("edges", [])
        ]
        inputs = []
        receive = next(
            (node_id for node_id, node in nodes.items()
             if node.get("kind") == "receive"),
            None,
        )
        if receive is not None:
            inputs.append({
                "id": "documents",
                "type": "DocumentBatch",
                "required": True,
                "max_items": 10_000,
                "max_bytes": 64 * 1024 * 1024,
            })
            edges.insert(
                0,
                {"from": {"input": "documents"}, "to": {"node": receive}},
            )
        flows[name] = {
            "inputs": inputs,
            "outputs": [],
            "nodes": nodes,
            "edges": edges,
        }
    converted = {
        "schema": "windex.pipeline/1",
        "parameters": references(document.get("config") or []),
        "state": document.get("state") or {},
        "flows": flows,
        "refresh": document.get("refresh") or [],
    }
    path.write_text(yaml.safe_dump(converted, sort_keys=False, width=100))


def main() -> None:
    for path in sorted(SEEDS.glob("*.yaml")):
        if path.name == "manifest.yaml":
            continue
        convert(path)


if __name__ == "__main__":
    main()
