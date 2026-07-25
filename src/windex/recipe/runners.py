"""Module name -> the callable that executes a slice of it.

Populated in-tree only. A recipe names a key in this dict; it can never name an
import path, which is what makes "installing a source cannot execute anything" a
property of the design rather than a rule someone has to remember.

Deliberately EMPTY at this stage. Every module is declared in `registry` — so the
editor can show it, `validate` can type-check a graph using it, and `compile_tasks`
can place it in a lane — while the executor refuses to run it with a legible
message. That ordering is intentional: the declaration is what the client and the
validator need, and a module whose config schema is wrong is useless however good
its code is. Implementations land per source as each one is converted.
"""

from __future__ import annotations

from collections.abc import Callable

from windex.worker.protocol import Runner

RUNNERS: dict[str, Callable[..., Runner]] = {}


def register(name: str):
    """Decorator used by module implementations as they land."""
    def wrap(fn):
        if name in RUNNERS:
            raise RuntimeError(f"module {name!r} already has an implementation")
        RUNNERS[name] = fn
        return fn
    return wrap
