"""Source recipes: a declarative, versioned description of how a corpus gets in.

A recipe is a typed-port DAG of nodes drawn from a closed vocabulary, referencing
modules that ship with windex. It is pure data — no code, no expressions, no
import-by-string — which is what lets one be installed from a git catalog without
that being remote code execution.

  ports.py     the wire types and which kinds may be wired together
  registry.py  the module catalog a recipe may reference
  parse.py     validation, and the security boundary
  compile.py   a validated recipe -> the run_tasks a run fans out to
"""

from windex.recipe import ports, registry
from windex.recipe.compile import compile_tasks, resolve

__all__ = ["ports", "registry", "compile_tasks", "resolve"]
