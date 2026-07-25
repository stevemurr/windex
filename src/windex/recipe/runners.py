"""Module name -> the callable that executes a slice of it.

Populated in-tree only. A recipe names a key in this dict; it can never name an
import path, which is what makes "installing a source cannot execute anything" a
property of the design rather than a rule someone has to remember.

Implementations land in-tree, a coherent slice at a time. Declared modules that
are not yet in this mapping still fail resolution with the explicit
"declared but not yet implemented" error.
"""

from __future__ import annotations

from collections.abc import Callable

from windex.modules.catalog import (
    list_json_manifest,
    list_lines,
    list_path_manifest_gz,
)
from windex.modules.collect import store_upsert
from windex.modules.discover import state_pending, static_once
from windex.worker.protocol import Runner

RUNNERS: dict[str, Callable[..., Runner]] = {
    "state.pending": state_pending,
    "static.once": static_once,
    "list.lines": list_lines,
    "list.json_manifest": list_json_manifest,
    "list.path_manifest_gz": list_path_manifest_gz,
    "store.upsert": store_upsert,
}


def register(name: str):
    """Decorator used by module implementations as they land."""
    def wrap(fn):
        if name in RUNNERS:
            raise RuntimeError(f"module {name!r} already has an implementation")
        RUNNERS[name] = fn
        return fn
    return wrap
