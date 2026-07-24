#!/usr/bin/env python
"""Write the OpenAPI schema to a file, for client generation and diffing.

The native client is generated from this document, so it has to be checked in:
building the macOS app must not require a Python environment. Committing it also
turns "a Python-side change broke the client" into a reviewable diff in the same
commit, rather than a build failure in Xcode days later.

    uv run python scripts/dump-openapi.py                 # print to stdout
    uv run python scripts/dump-openapi.py -o openapi.json # write
    uv run python scripts/dump-openapi.py --check         # fail if stale

`--check` is the CI gate: it exits 1 when the committed file no longer matches
the live app, and prints the paths and operations that moved.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

DEFAULT_OUT = Path("clients/macos/Packages/WindexKit/openapi.json")


def _schema() -> dict:
    from windex.api.app import app

    return app.openapi()


def _summarize(doc: dict) -> set[str]:
    """`METHOD path -> operationId` for every operation, for a readable diff."""
    return {
        f"{method.upper()} {path} -> {op.get('operationId', '?')}"
        for path, item in doc.get("paths", {}).items()
        for method, op in item.items()
        if isinstance(op, dict)
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-o", "--out", type=Path, default=None,
                    help=f"output path (default: stdout; CI uses {DEFAULT_OUT})")
    ap.add_argument("--check", action="store_true",
                    help="compare against the file instead of writing it")
    args = ap.parse_args()

    doc = _schema()
    # sort_keys so an unrelated dict-ordering change never shows up as a diff.
    text = json.dumps(doc, indent=2, sort_keys=True) + "\n"

    if args.check:
        path = args.out or DEFAULT_OUT
        if not path.exists():
            print(f"{path} does not exist; run: "
                  f"uv run python scripts/dump-openapi.py -o {path}", file=sys.stderr)
            return 1
        if path.read_text() == text:
            return 0
        old, new = _summarize(json.loads(path.read_text())), _summarize(doc)
        print(f"{path} is stale. Regenerate it and commit the result.", file=sys.stderr)
        for line in sorted(old - new):
            print(f"  - {line}", file=sys.stderr)
        for line in sorted(new - old):
            print(f"  + {line}", file=sys.stderr)
        if old == new:
            print("  (no operations moved — a schema//model detail changed)",
                  file=sys.stderr)
        return 1

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
        print(f"wrote {args.out} ({len(doc.get('paths', {}))} paths)", file=sys.stderr)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
