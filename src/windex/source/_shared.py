"""Shared Source persistence contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


class StaleSourceError(RuntimeError):
    pass


class SourceConflictError(RuntimeError):
    pass


def values_hash(values: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        dict(values), sort_keys=True, separators=(",", ":"), default=str)
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
