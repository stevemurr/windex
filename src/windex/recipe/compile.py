"""Turn a validated recipe into the `run_tasks` rows a run fans out to.

This is the seam the worker pool and the scheduler were both built against while
the recipe engine did not exist: the scheduler takes a
`compile_tasks(spec) -> [task]` and the worker takes a `resolve(module) -> Runner`.
Both are supplied here, which is what joins the three halves.

The compiler is a PROJECTION, not a second validator. `parse` already rejected
anything malformed, so this reads a document it trusts and emits rows. The one
thing it adds is placement — lane, preconditions, weight, lease — because those
are properties of the module and the graph rather than of the recipe, and a
recipe author has no business choosing which lane their work competes in.
"""

from __future__ import annotations

from windex.config import Settings
from windex.recipe import parse as recipe_parse
from windex.recipe import ports, registry

# Every key `run_tasks` accepts from a compiler. Anything else is a mistake worth
# surfacing: the scheduler REFUSES unknown keys rather than dropping them, on the
# same reasoning as recipe config — a key the author believed was doing something
# and which silently is not is the worst outcome available.
TASK_KEYS = frozenset({
    "node", "kind", "module", "lane", "config", "depends_on", "preconditions",
    "weight", "max_attempts", "lease_seconds",
})

# How long a task may hold its lease before the reaper assumes the worker died.
# Per LANE, because the honest bound differs by an order of magnitude: a polite
# 1-request-per-3-seconds fetch is legitimately quiet for minutes, while an embed
# slice that stops reporting for two minutes has stopped. Too short reclaims live
# work; too long leaves a dead task parked.
LEASE_SECONDS = {"net": 900, "cpu_heavy": 1800, "gpu": 300, "io": 300, "maint": 600}

# Relative share of a run's progress bar. A flat weight would make ccnews's
# extract node — 80% of the wall time — one ninth of the bar, which is worse than
# no bar at all because it looks precise.
KIND_WEIGHT = {
    "discover": 0.1, "receive": 0.1, "catalog": 0.3, "collect": 0.2,
    "fetch": 1.0, "extract": 1.0, "transform": 0.3, "load": 0.5,
}


def compile_tasks(spec: dict, *, flow: str | None = None,
                  settings: Settings | None = None) -> list[dict]:
    """A recipe spec -> the `run_tasks` rows for one flow.

    `flow` selects which sub-DAG to run; omitted, the recipe's first `refresh`
    flow is used. Flows are separate runs on purpose — they communicate through
    stores, not edges, which is what keeps every graph acyclic while `repos` is
    written by discovery, read by hydration and written back.
    """
    # builtin=True because this spec is already REGISTERED — it came from the
    # recipes table or from a run's frozen copy. The reserved-name guard governs
    # ADMISSION (install), not compilation, and applying it here would make a
    # built-in source uncompilable by its own name.
    recipe = recipe_parse.parse(spec, settings or Settings(), builtin=True)
    name = flow or (recipe.refresh[0] if recipe.refresh else recipe.flows[0].name)
    chosen = next((f for f in recipe.flows if f.name == name), None)
    if chosen is None:
        raise ValueError(f"recipe {recipe.name!r} has no flow {name!r}")

    # Incoming edges, so a task waits on what feeds it. `order` is already
    # topological from parse, so the rows come out in execution order too — which
    # makes a run's task list readable without sorting it again.
    upstream: dict[str, list[str]] = {n.id: [] for n in chosen.nodes}
    for a, b in chosen.edges:
        upstream[b].append(a)

    tasks = []
    for node in chosen.nodes:
        mod = registry.get(node.uses)
        if mod is None:                     # parse guarantees this; belt and braces
            raise ValueError(f"unknown module {node.uses!r} in flow {name!r}")
        pre = set(mod.preconditions)
        # A module naming a secret needs that secret present before it is claimed,
        # not after it has burned an attempt discovering the token is missing.
        for field in mod.fields:
            if field.kind == "secret_ref" and node.config.get(field.key):
                pre.update(field.allow or ())
        tasks.append({
            "node": node.id,
            "kind": node.kind,
            "module": node.uses,
            "lane": mod.lane,
            "config": dict(node.config),
            "depends_on": sorted(upstream[node.id]),
            "preconditions": sorted(pre),
            "weight": KIND_WEIGHT.get(node.kind, 0.5),
            "max_attempts": 3,
            "lease_seconds": LEASE_SECONDS.get(mod.lane, 300),
        })
    return tasks


def resolve(module: str):
    """`run_tasks.module` -> the callable that executes a slice of it.

    Deliberately a lookup in a dict populated by in-tree registration, never an
    import of a name that arrived in a recipe. That is the whole reason installing
    a marketplace recipe is not remote code execution, and it is worth the
    indirection to keep it structurally true rather than conventionally true.

    Raises LookupError for a module that is declared but has no implementation
    yet — the worker turns that into a failed task with a legible message rather
    than a crash, which is what lets the registry describe modules the executor
    cannot yet run.
    """
    from windex.recipe import runners

    fn = runners.RUNNERS.get(module)
    if fn is None:
        known = registry.get(module)
        if known is None:
            raise LookupError(f"no such module: {module!r}")
        raise LookupError(
            f"module {module!r} is declared but not yet implemented "
            f"(kind={known.kind}). It can be validated and shown in the editor; "
            f"it cannot run.")
    return fn


def describe_placement() -> list[dict]:
    """Lane and precondition per module — what the editor shows so an author can
    see WHERE their node will run and what it waits on, without reading source."""
    return [{"id": m.name, "kind": m.kind, "lane": m.lane,
             "preconditions": list(m.preconditions),
             "lease_seconds": LEASE_SECONDS.get(m.lane, 300),
             "weight": KIND_WEIGHT.get(m.kind, 0.5)}
            for m in registry.MODULES.values()]


__all__ = ["compile_tasks", "resolve", "describe_placement",
           "TASK_KEYS", "LEASE_SECONDS", "KIND_WEIGHT", "ports"]
