#!/usr/bin/env python3
"""Regenerate the WindexKit test fixtures from windex's own schema.

The Swift integration tests decode these, so they must be what the server
actually emits — a hand-written fixture drifts silently and the first sign is a
form that renders wrong against the real backend. Running this from the repo is
the only step that needs a Python environment; the fixtures it writes are checked
in, so Xcode and CI never do.

    uv run python clients/macos/Packages/WindexKit/Tests/WindexKitTests/Fixtures/generate_fixtures.py

Re-run it whenever `settings_schema.SCHEMA` or `Param.describe()` changes; the
`schemaMatchesServer` test is what catches a stale fixture, but only if the
fixture was regenerated.
"""

from __future__ import annotations

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[6]
sys.path.insert(0, str(ROOT / "src"))

from windex import settings_schema as schema  # noqa: E402
from windex.config import Settings  # noqa: E402

HERE = pathlib.Path(__file__).parent


def settings_payload() -> dict:
    """`GET /admin/v1/settings` as the server builds it (api/service.py:
    `{**field.describe(), "value": ..., "origin": ...}`).

    Values come from a default `Settings()` and every origin is "default", which
    is the state a fresh box is in. Two rows are then rewritten to `db` so the
    revert affordance has something to exercise.
    """
    base = Settings()
    scopes = []
    for scope in schema.scopes():
        fields = []
        for field in schema.fields_for(scope):
            described = field.describe()
            described["value"] = getattr(base, field.key, None)
            described["origin"] = "default"
            fields.append(described)
        scopes.append({"scope": scope, "fields": fields})

    for scope in scopes:
        if scope["scope"] == schema.GLOBAL:
            for field in scope["fields"]:
                if field["key"] == "embed_concurrency":
                    field["value"] = 12
                    field["origin"] = "db"
                if field["key"] == "embed_order":
                    field["value"] = "newest"
                    field["origin"] = "env"
    return {"scopes": scopes}


def main() -> None:
    payload = settings_payload()
    (HERE / "settings.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n")

    # The flat key list the Swift side asserts against, so a schema change that
    # adds or drops a setting fails a test rather than silently going unrendered.
    keys = {
        scope["scope"]: sorted(f["key"] for f in scope["fields"])
        for scope in payload["scopes"]
    }
    (HERE / "settings-keys.json").write_text(
        json.dumps(keys, indent=2, sort_keys=True) + "\n")

    n = sum(len(s["fields"]) for s in payload["scopes"])
    print(f"wrote settings.json ({len(payload['scopes'])} scopes, {n} fields)")


if __name__ == "__main__":
    main()
