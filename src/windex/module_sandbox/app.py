"""Minimal JSONL Python executor.

Isolation is provided by the dedicated rootless container. This process adds
per-execution rlimits, an empty environment, a private temporary directory, and
a restricted builtins set.
"""

from __future__ import annotations

import json
import os
import resource
import subprocess
import sys
import tempfile
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

app = FastAPI(title="windex Module sandbox", docs_url=None, redoc_url=None)


class Request(BaseModel):
    model_config = ConfigDict(extra="forbid")
    runtime: str
    source: str = Field(max_length=256_000)
    records: list[dict[str, Any]] = Field(max_length=10_000)
    config: dict[str, Any] = Field(default_factory=dict)
    limits: dict[str, Any] = Field(default_factory=dict)


_DRIVER = r"""
import json, sys
safe = {
    "abs": abs, "all": all, "any": any, "bool": bool, "dict": dict,
    "enumerate": enumerate, "float": float, "int": int, "len": len,
    "list": list, "max": max, "min": min, "range": range, "round": round,
    "set": set, "sorted": sorted, "str": str, "sum": sum, "tuple": tuple,
    "zip": zip, "Exception": Exception, "ValueError": ValueError,
}
request = json.loads(sys.stdin.readline())
scope = {"__builtins__": safe}
exec(request["source"], scope, scope)
transform = scope["transform"]
for record in request["records"]:
    value = transform(record, request["config"])
    if value is not None:
        print(json.dumps(value, separators=(",", ":")))
"""


def _limits(limits: dict[str, Any]) -> None:
    cpu = min(max(int(limits.get("cpu_seconds", 5)), 1), 30)
    memory = min(max(int(limits.get("memory_mb", 256)), 32), 1024) * 1024 * 1024
    processes = min(max(int(limits.get("processes", 1)), 1), 4)
    output = min(
        max(int(limits.get("output_bytes", 8 * 1024 * 1024)), 1024),
        64 * 1024 * 1024,
    )
    resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))
    resource.setrlimit(resource.RLIMIT_AS, (memory, memory))
    resource.setrlimit(resource.RLIMIT_NPROC, (processes, processes))
    resource.setrlimit(resource.RLIMIT_FSIZE, (output, output))
    resource.setrlimit(resource.RLIMIT_NOFILE, (16, 16))


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "runtimes": ["python"]}


@app.post("/v1/execute")
def execute(body: Request) -> dict[str, Any]:
    if body.runtime != "python":
        raise HTTPException(422, "unsupported runtime")
    from windex.modules.admin import validate_source

    validation = validate_source(body.source, body.runtime)
    if not validation["valid"]:
        raise HTTPException(422, {
            "message": "Module source failed sandbox validation",
            "issues": validation["issues"],
        })
    payload = json.dumps({
        "source": body.source,
        "records": body.records,
        "config": body.config,
    }) + "\n"
    timeout = min(max(float(body.limits.get("wall_seconds", 10)), 0.1), 60)
    maximum = min(
        max(int(body.limits.get("output_bytes", 8 * 1024 * 1024)), 1024),
        64 * 1024 * 1024,
    )
    with tempfile.TemporaryDirectory(prefix="windex-module-") as temporary:
        try:
            completed = subprocess.run(
                [sys.executable, "-I", "-S", "-c", _DRIVER],
                input=payload,
                text=True,
                capture_output=True,
                timeout=timeout,
                cwd=temporary,
                env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
                preexec_fn=lambda: _limits(body.limits),
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "wall_time_limit_exceeded", "outputs": []}
    if completed.returncode != 0:
        return {
            "ok": False,
            "error": completed.stderr[-4096:] or f"exit_{completed.returncode}",
            "outputs": [],
        }
    if len(completed.stdout.encode()) > maximum:
        return {"ok": False, "error": "output_limit_exceeded", "outputs": []}
    try:
        outputs = [
            json.loads(line) for line in completed.stdout.splitlines() if line]
    except json.JSONDecodeError:
        return {"ok": False, "error": "invalid_jsonl_output", "outputs": []}
    return {"ok": True, "outputs": outputs}
