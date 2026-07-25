"""Validate, normalize and round-trip a recipe document.

THIS IS A SECURITY BOUNDARY, not a convenience. Recipes arrive over a LAN-exposed
API and from git catalogs written by other people, so the discipline is the same
one `crawl/recipe.py` and `settings_schema.py` already state:

  * **An allowlist, never a denylist.** Kinds, modules, config keys, choice values
    and hosts are all checked against a declared set. A denylist over a growing
    vocabulary eventually misses one, and the failure mode is a recipe reaching
    something it was never meant to.
  * **Clamp to the operator's ceiling, don't reject.** A recipe may always ask to
    be slower or smaller; never faster or bigger. Silently honouring the bound
    beats failing an install over a number, and `clamp`/`clampNote` tell the
    client what happened.
  * **Compile at parse time.** A bad regex is a 422 here, not an exception inside
    a worker at 3am.

And the thing that makes the marketplace safe: a recipe is INERT. It carries no
code, no expressions and no template language with logic. A `with:` value is a
scalar, a list of scalars, or a reference of exactly two forms — `@config.<key>`
and `@secret.<name>`. There is deliberately no third.

`ValueError` is what the routes map to HTTP 422, mirroring
`custom_source.registry.validate_name`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from windex.config import Settings
from windex.recipe import ports, registry
from windex.schema.param import Param

SCHEMA = "windex.recipe/1"

# Bounds. Not arbitrary: they are what stops a pathological document turning into
# a pathological amount of work before anything has been executed.
MAX_FLOWS = 8
MAX_NODES = 64
MAX_EDGES = 128
MAX_CONFIG_FIELDS = 64
MAX_STORES = 8

_CONFIG_REF = re.compile(r"^@config\.([a-z][a-z0-9_]{0,63})$")
_SECRET_REF = re.compile(r"^@secret\.([a-z][a-z0-9_]{0,63})$")
# Same shape as custom_source.registry: a recipe name becomes documents.source and
# an id prefix, so it must be a safe identifier before anything else uses it.
_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,31}$")


@dataclass(frozen=True)
class Node:
    id: str
    kind: str
    uses: str
    config: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Flow:
    name: str
    nodes: tuple[Node, ...]
    edges: tuple[tuple[str, str], ...]
    order: tuple[str, ...]          # topological, computed at parse time


@dataclass(frozen=True)
class Corpus:
    source: str
    id_prefix: str
    collection: str


@dataclass(frozen=True)
class Recipe:
    name: str
    version: int
    title: str
    description: str
    corpus: Corpus
    config: tuple[Param, ...]
    state: dict
    flows: tuple[Flow, ...]
    refresh: tuple[str, ...]

    def to_dict(self) -> dict:
        """The stored jsonb form. Round-trips through `parse` unchanged, which is
        what makes a past run's frozen spec re-runnable verbatim."""
        return {
            "schema": SCHEMA,
            "name": self.name, "version": self.version,
            "title": self.title, "description": self.description,
            "corpus": {"source": self.corpus.source,
                       "id_prefix": self.corpus.id_prefix,
                       "collection": self.corpus.collection},
            "config": [f.to_spec() for f in self.config],
            "state": self.state,
            "flows": {f.name: {
                "nodes": {n.id: {"kind": n.kind, "uses": n.uses, "with": n.config}
                          for n in f.nodes},
                "edges": [list(e) for e in f.edges],
            } for f in self.flows},
            "refresh": list(self.refresh),
        }


def _obj(body: dict, key: str, where: str = "") -> dict:
    v = body.get(key) or {}
    if not isinstance(v, dict):
        raise ValueError(f"{where}{key} must be an object")
    return v


def _parse_config_schema(raw) -> tuple[Param, ...]:
    """The recipe's own form schema — what the installer/editor fills in.

    This is the Airbyte spec-vs-config split: the recipe declares what it needs,
    the operator supplies it. Fields are `Param`s, so the Swift inspector renders
    a recipe's install form with the same code that renders windex's settings.
    """
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError("config must be a list of field declarations")
    if len(raw) > MAX_CONFIG_FIELDS:
        raise ValueError(f"config: at most {MAX_CONFIG_FIELDS} fields")
    out, seen = [], set()
    for i, f in enumerate(raw):
        if not isinstance(f, dict):
            raise ValueError(f"config[{i}] must be an object")
        key = f.get("key")
        if not isinstance(key, str) or not re.match(r"^[a-z][a-z0-9_]{0,63}$", key):
            raise ValueError(f"config[{i}].key must be a lowercase identifier")
        if key in seen:
            raise ValueError(f"config: duplicate key {key!r}")
        seen.add(key)
        kind = f.get("kind")
        try:
            out.append(Param(
                key=key, kind=kind,
                lo=f.get("lo"), hi=f.get("hi"),
                choices=tuple(f.get("choices") or ()),
                default=f.get("default"), label=f.get("label", ""),
                help=f.get("help", ""), required=bool(f.get("required", False)),
                stage=f.get("stage", "runtime"),
                editor=f.get("editor", ""), section=f.get("section", ""),
                unit=f.get("unit", ""), advanced=bool(f.get("advanced", False)),
                secret=bool(f.get("secret", False)),
                prefill=f.get("prefill"), pattern=f.get("pattern", ""),
                enum_titles=tuple(f.get("enum_titles") or ()),
                locked_reason=f.get("locked_reason", ""),
                depends_on=f.get("depends_on"),
                clamp_note=f.get("clamp_note", ""),
                enforce=f.get("enforce", "clamp"),
                # A recipe declares WHICH operator ceiling bounds it; it can never
                # declare the ceiling's value, and ceiling keys are absent from every
                # editable allowlist by construction.
                ceiling=f.get("ceiling"), floor=f.get("floor"),
                max_items=f.get("max_items"), max_len=f.get("max_len"),
                allow=tuple(f.get("allow") or ()),
            ))
        except ValueError as exc:
            raise ValueError(f"config[{i}] ({key}): {exc}")
    return tuple(out)


def _resolve_value(value, param: Param, cfg_keys: dict[str, Param],
                   settings: Settings, where: str):
    """One `with:` value. Literal, `@config.x`, or `@secret.x` — nothing else."""
    if isinstance(value, str):
        m = _CONFIG_REF.match(value)
        if m:
            key = m.group(1)
            ref = cfg_keys.get(key)
            if ref is None:
                raise ValueError(f"{where}: @config.{key} is not a declared config field")
            # Type must match, or an install-time string lands in an int field and
            # fails at run time instead of here.
            if ref.kind != param.kind:
                raise ValueError(
                    f"{where}: @config.{key} is {ref.kind}, but this field is "
                    f"{param.kind}")
            return value                      # resolved at run time, checked now
        m = _SECRET_REF.match(value)
        if m:
            if param.kind != "secret_ref":
                raise ValueError(f"{where}: a secret reference is only allowed on a "
                                 f"secret_ref field")
            # `@secret.github_tokens` is sugar for the bare name. Unwrap it before
            # coercing, or the allowlist check compares against the decorated form
            # and every reference fails — which is the one syntax the field type
            # exists to accept.
            value = m.group(1)
    try:
        return param.coerce(value, settings)
    except ValueError as exc:
        raise ValueError(f"{where}: {exc}")


def _parse_node(nid: str, raw, cfg_keys, settings, where: str) -> Node:
    if not isinstance(raw, dict):
        raise ValueError(f"{where}: node must be an object")
    kind = raw.get("kind")
    if kind not in ports.KINDS:
        raise ValueError(f"{where}: unknown kind {kind!r} "
                         f"(one of: {', '.join(ports.KINDS)})")
    uses = raw.get("uses")
    mod = registry.get(uses) if isinstance(uses, str) else None
    if mod is None:
        raise ValueError(f"{where}: unknown module {uses!r}. Recipes may only "
                         f"reference modules this windex ships.")
    if mod.kind != kind:
        raise ValueError(f"{where}: module {uses!r} is a {mod.kind} node, not {kind}")

    declared = {f.key: f for f in mod.fields}
    given = raw.get("with") or {}
    if not isinstance(given, dict):
        raise ValueError(f"{where}: `with` must be an object")
    # Unknown keys are a 422, NEVER ignored. Silently dropping a key the author
    # believed was doing something is how "the rate limit setting doesn't work"
    # bug reports happen that nobody can reproduce.
    for k in given:
        if k not in declared:
            raise ValueError(f"{where}: {uses} has no config field {k!r} "
                             f"(has: {', '.join(sorted(declared)) or 'none'})")
    config = {}
    for key, param in declared.items():
        if key in given:
            config[key] = _resolve_value(given[key], param, cfg_keys, settings,
                                         f"{where}.with.{key}")
        elif param.required:
            raise ValueError(f"{where}: {uses} requires {key!r}")
        elif param.default is not None:
            # Materialize the module's default rather than leaving the key absent.
            # The spec is FROZEN onto each run, so an omitted default would mean a
            # past run silently changes behaviour when the module's default changes
            # — which makes the freeze worthless. A complete spec is also what lets
            # a runner read its config without consulting the registry.
            #
            # Assigned directly, NOT through coerce(): a module's own default is
            # in-tree code, not caller input, so the security boundary does not
            # apply to it — and coerce() rightly refuses a locked field, which
            # would make a locked field unable to carry its own value.
            config[key] = param.default
    return Node(id=nid, kind=kind, uses=uses, config=config)


def _toposort(nodes: dict[str, Node], edges: list[tuple[str, str]],
              where: str) -> tuple[str, ...]:
    """Topological order, and the cycle check.

    Cycles are rejected outright rather than allowed through a 'feedback' port. A
    BFS back-edge is expressed as a STORE write instead — links go to a collect
    node, and the frontier discover reads them back on the next slice. That keeps
    every graph acyclic without losing anything, and it is what the existing
    crawler already does with `crawl_urls`.
    """
    incoming = {n: 0 for n in nodes}
    outgoing: dict[str, list[str]] = {n: [] for n in nodes}
    for a, b in edges:
        outgoing[a].append(b)
        incoming[b] += 1
    queue = sorted(n for n, c in incoming.items() if c == 0)
    order: list[str] = []
    while queue:
        n = queue.pop(0)
        order.append(n)
        for m in sorted(outgoing[n]):
            incoming[m] -= 1
            if incoming[m] == 0:
                queue.append(m)
    if len(order) != len(nodes):
        stuck = sorted(set(nodes) - set(order))
        raise ValueError(f"{where}: the graph has a cycle involving {', '.join(stuck)}. "
                         f"Express a back-edge as a store write, not an edge.")
    return tuple(order)


def _parse_flow(name: str, raw, cfg_keys, settings, stores: set[str]) -> Flow:
    where = f"flows.{name}"
    raw_nodes = _obj(raw, "nodes", f"{where}.")
    if not raw_nodes:
        raise ValueError(f"{where}: at least one node is required")
    if len(raw_nodes) > MAX_NODES:
        raise ValueError(f"{where}: at most {MAX_NODES} nodes")
    nodes = {nid: _parse_node(nid, n, cfg_keys, settings, f"{where}.nodes.{nid}")
             for nid, n in raw_nodes.items()}

    raw_edges = raw.get("edges") or []
    if not isinstance(raw_edges, list):
        raise ValueError(f"{where}.edges must be a list of [from, to] pairs")
    if len(raw_edges) > MAX_EDGES:
        raise ValueError(f"{where}: at most {MAX_EDGES} edges")
    edges: list[tuple[str, str]] = []
    for i, e in enumerate(raw_edges):
        if not (isinstance(e, (list, tuple)) and len(e) == 2
                and all(isinstance(x, str) for x in e)):
            raise ValueError(f"{where}.edges[{i}] must be [from, to]")
        a, b = e
        for end in (a, b):
            if end not in nodes:
                raise ValueError(f"{where}.edges[{i}]: unknown node {end!r}")
        if not ports.can_connect(nodes[a].kind, nodes[b].kind):
            ka, kb = ports.KINDS[nodes[a].kind], ports.KINDS[nodes[b].kind]
            raise ValueError(
                f"{where}.edges[{i}]: {a} ({ka.name} -> {ka.out or 'nothing'}) "
                f"cannot connect to {b} ({kb.name} expects {kb.inp or 'no input'})")
        if (a, b) in edges:
            raise ValueError(f"{where}.edges[{i}]: duplicate edge {a} -> {b}")
        edges.append((a, b))

    order = _toposort(nodes, edges, where)

    # Structural checks that catch a graph which parses but does nothing.
    has_out = {a for a, _ in edges}
    has_in = {b for _, b in edges}
    for nid, n in nodes.items():
        if n.kind not in ports.SINKS and nid not in has_out:
            raise ValueError(f"{where}.nodes.{nid}: a {n.kind} node produces output "
                             f"that nothing consumes")
        if n.kind not in ports.ROOTS and nid not in has_in:
            raise ValueError(f"{where}.nodes.{nid}: a {n.kind} node has no input")
    if not any(n.kind in ports.SINKS for n in nodes.values()):
        raise ValueError(f"{where}: no terminal node — the flow stages nothing")

    # A store a node writes to must be one this recipe declared. Store names are
    # namespaced by (recipe, store, key) in source_units, so this is what keeps a
    # recipe from writing into another recipe's state.
    for nid, n in nodes.items():
        for key in ("store", "into"):
            ref = n.config.get(key)
            if isinstance(ref, str) and ref and not ref.startswith("@") \
                    and ref not in stores:
                raise ValueError(f"{where}.nodes.{nid}: undeclared store {ref!r} "
                                 f"(declare it under `state:`)")
    return Flow(name=name, nodes=tuple(nodes[n] for n in order),
                edges=tuple(edges), order=order)


def parse(body: dict, settings: Settings, *, builtin: bool = False) -> Recipe:
    """Validate + normalize a recipe document. Raises ValueError (-> 422).

    `builtin` relaxes exactly one check: the reserved-name guard. That guard exists
    so a user-created source cannot shadow a built-in corpus (`news`, `github`, …),
    and the built-in recipes are precisely the things entitled to those names.

    It is a PARAMETER, never a field in the document. A recipe that could declare
    itself builtin could claim `news` and write into the news corpus, so the claim
    has to come from the caller — in-tree loading passes True, and every path that
    accepts a document from outside (the API, the marketplace) leaves it False.
    """
    if not isinstance(body, dict):
        raise ValueError("recipe must be an object")
    schema = body.get("schema", SCHEMA)
    if schema != SCHEMA:
        raise ValueError(f"unsupported schema {schema!r}; this windex speaks {SCHEMA}")

    name = body.get("name")
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise ValueError("name must match ^[a-z][a-z0-9_]{1,31}$")
    from windex.custom_source.registry import RESERVED
    if name in RESERVED and not builtin:
        raise ValueError(f"name {name!r} is reserved for a built-in source")

    corpus_raw = _obj(body, "corpus")
    source = corpus_raw.get("source") or name
    prefix = corpus_raw.get("id_prefix") or f"{source}:"
    if not isinstance(source, str) or not _NAME_RE.match(source):
        raise ValueError("corpus.source must match ^[a-z][a-z0-9_]{1,31}$")
    # The prefix must be namespaced to something THIS recipe owns — its own name or
    # its corpus source. Both, because the two legitimately differ: the `gh` recipe
    # feeds the `github` corpus and writes `gh:owner/repo` ids, and CLAUDE.md pins
    # those ids as the public API, so they cannot be renamed to match. What this
    # still forbids is the case that matters: a recipe claiming another source's
    # prefix and thereby its power to overwrite and tombstone.
    if not isinstance(prefix, str) or not (
            prefix.startswith(source) or prefix.startswith(name)):
        raise ValueError(
            f"corpus.id_prefix {prefix!r} must begin with the recipe name "
            f"({name!r}) or corpus.source ({source!r}), so a recipe cannot write "
            f"ids into another source's namespace")

    config = _parse_config_schema(body.get("config"))
    cfg_keys = {f.key: f for f in config}

    state = _obj(body, "state")
    if len(state) > MAX_STORES:
        raise ValueError(f"state: at most {MAX_STORES} stores")
    for key in state:
        if not re.match(r"^[a-z][a-z0-9_]{0,31}$", key):
            raise ValueError(f"state: invalid store name {key!r}")
    stores = set(state)

    flows_raw = _obj(body, "flows")
    if not flows_raw:
        raise ValueError("at least one flow is required")
    if len(flows_raw) > MAX_FLOWS:
        raise ValueError(f"at most {MAX_FLOWS} flows")
    flows = tuple(_parse_flow(fname, f, cfg_keys, settings, stores)
                  for fname, f in flows_raw.items())

    # A source is push or pull, not both: mixing them makes "what does refresh do"
    # unanswerable, and the two have different correctness rules for absent ids.
    kinds = {n.kind for f in flows for n in f.nodes}
    if "receive" in kinds and "discover" in kinds:
        raise ValueError("a recipe may have `receive` roots or `discover` roots, "
                         "not both — a source is push or pull")

    # An ABSENT refresh means "every flow"; an explicitly empty one means "nothing
    # to refresh", which is what a push source is. `or` collapses those two, and
    # collapsing them gives memory and custom a pull cycle they cannot service.
    raw_refresh = body.get("refresh")
    refresh = tuple(raw_refresh if raw_refresh is not None
                    else [f.name for f in flows])
    known = {f.name for f in flows}
    for r in refresh:
        if r not in known:
            raise ValueError(f"refresh names unknown flow {r!r}")

    return Recipe(
        name=name, version=int(body.get("version") or 1),
        title=body.get("title", ""), description=body.get("description", ""),
        corpus=Corpus(source=source, id_prefix=prefix,
                      collection=corpus_raw.get("collection") or source),
        config=config, state=state, flows=flows, refresh=refresh,
    )


def validate(body: dict, settings: Settings, *, builtin: bool = False) -> dict:
    """Parse and report, instead of raising. What `POST /recipes/validate` returns.

    Pure: no IO, no network, no database. That is what lets an editor call it on
    every keystroke — and what distinguishes it from `preview`, which fetches, and
    `dry-run`, which executes.
    """
    try:
        recipe = parse(body, settings, builtin=builtin)
    except ValueError as exc:
        return {"valid": False, "errors": [{"message": str(exc)}],
                "warnings": [], "normalized": None, "graph": None}
    warnings = []
    for f in recipe.flows:
        for n in f.nodes:
            given = ((body.get("flows") or {}).get(f.name, {})
                     .get("nodes", {}).get(n.id, {}).get("with") or {})
            for key, value in given.items():
                got = n.config.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool) \
                        and got != value:
                    warnings.append({
                        "path": f"flows.{f.name}.nodes.{n.id}.with.{key}",
                        "code": "clamped", "was": value, "now": got,
                        "message": f"{value} adjusted to {got} by the operator's bound",
                    })
    return {
        "valid": True, "errors": [], "warnings": warnings,
        "normalized": recipe.to_dict(),
        "graph": {f.name: {"order": list(f.order),
                           "edges": [list(e) for e in f.edges]} for f in recipe.flows},
    }
