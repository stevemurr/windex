#!/usr/bin/env python3
"""Collapse OpenAPI 3.1 nullable unions so swift-openapi-generator can read them.

FastAPI renders a `bool | None` field as::

    {"anyOf": [{"type": "boolean"}, {"type": "null"}]}

which is valid OpenAPI 3.1. swift-openapi-generator (1.13) does not support
`type: "null"`, and its failure mode is the dangerous one: rather than erroring,
it warns and **drops the whole property**. Measured against windex's control
plane that silently discarded 145 of 199 properties — 19 of the 22 on
`SettingsField` alone, which is the schema the entire form layer renders from.
A client generated from the raw document compiles, looks plausible, and is
missing almost everything.

Collapsing the union to its non-null branch is exactly equivalent here: the
affected keys are absent from `required`, so the generator emits them as Swift
optionals, which is what `X | None` meant in the first place.

This runs at GENERATION time only, on a copy. The checked-in
`openapi-admin.json` is the server's own artifact and is never modified — and
the build itself stays free of any Python dependency.

    ./normalize_spec.py <in.json> <out.json>
"""

from __future__ import annotations

import json
import sys

# Schemas the DOMAIN model owns, removed before generation so there is no second
# decoder for them.
#
# These three describe the `Param` shape — one typed, bounded, self-describing
# form control. The spec types it in exactly ONE of the three places it occurs:
#
#   SettingsScope.fields   -> SettingsField          (typed)
#   JobInfo.params         -> {type: object}         (untyped)
#   Registry.modules[]     -> {type: object}         (untyped)
#
# So a generated `SettingsField` cannot be the single source of truth: the job
# dialog and the recipe node inspector both receive the same shape as raw JSON
# and need a hand-written decoder regardless. Generating it too would mean two
# decoders for one wire format, kept honest only by a test — which is what this
# removal exists to avoid. `Param` in Models/Param.swift is the one decoder, and
# it serves all three call sites.
#
# Removing them leaves the operations that returned them typed as open objects,
# which is what the untyped two already were.
DOMAIN_OWNED = {"SettingsField", "SettingsScope", "SettingsAll"}

_OPEN_OBJECT = {"type": "object", "additionalProperties": True}

_dropped = 0
_replaced = 0


def normalize(node):
    """Rewrite `anyOf: [X, null]` → `X` and drop refs to domain-owned schemas."""
    global _dropped, _replaced

    if isinstance(node, list):
        return [normalize(item) for item in node]
    if not isinstance(node, dict):
        return node

    ref = node.get("$ref")
    if isinstance(ref, str):
        name = ref.rsplit("/", 1)[-1]
        if name in DOMAIN_OWNED:
            _replaced += 1
            return dict(_OPEN_OBJECT)

    node = {key: normalize(value) for key, value in node.items()}

    alts = node.get("anyOf")
    if isinstance(alts, list) and any(
        isinstance(a, dict) and a.get("type") == "null" for a in alts
    ):
        keep = [a for a in alts if not (isinstance(a, dict) and a.get("type") == "null")]
        _dropped += 1
        # Sibling keys like `title`/`description` live alongside `anyOf` and must
        # survive the collapse.
        siblings = {k: v for k, v in node.items() if k != "anyOf"}
        if len(keep) == 1:
            # The common case: one real type. Merge it up, letting the branch's
            # own keys win over the wrapper's.
            return {**siblings, **keep[0]}
        if keep:
            # A genuine multi-type union that merely included null.
            return {**siblings, "anyOf": keep}
        # Nothing but null — no representable type. Leave it for the generator
        # to skip rather than inventing one.
        return node
    return node


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    src, dst = sys.argv[1], sys.argv[2]

    with open(src) as fh:
        spec = json.load(fh)

    out = normalize(spec)

    # Drop the domain-owned definitions themselves; every reference to them was
    # rewritten to an open object above.
    schemas = out.get("components", {}).get("schemas", {})
    removed = [name for name in DOMAIN_OWNED if schemas.pop(name, None) is not None]

    leftover = [n for n in DOMAIN_OWNED if f'"{n}"' in json.dumps(out)]
    if leftover:
        sys.exit(f"error: dangling references to {leftover} after removal")

    with open(dst, "w") as fh:
        json.dump(out, fh, indent=2)

    print(f"normalized {_dropped} nullable unions; "
          f"removed {len(removed)} domain-owned schemas "
          f"({_replaced} refs inlined) -> {dst}")


if __name__ == "__main__":
    main()
