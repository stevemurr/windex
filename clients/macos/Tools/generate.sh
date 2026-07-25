#!/usr/bin/env bash
# Regenerate WindexKit's admin DTOs from the checked-in OpenAPI document.
#
# The OUTPUT is checked in, so this is only run when the control plane changes —
# not as part of a build. That is what keeps `swift build` offline and keeps
# WindexKit's own dependency list down to swift-openapi-runtime.
#
#   clients/macos/Tools/generate.sh
#
# Review the diff afterwards: it is the control plane's contract changing, and a
# surprise in there is worth understanding before it reaches the UI.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KIT="$HERE/../Packages/WindexKit"
OUT="$KIT/Sources/WindexKit/Generated"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$OUT"

# Collapse `anyOf: [X, null]` first — the generator drops such properties
# outright rather than making them optional, which silently discarded 145 of
# 199 properties when run against the raw document. See normalize_spec.py.
# The checked-in spec is the server's artifact and is left untouched.
python3 "$HERE/normalize_spec.py" "$KIT/openapi-admin.json" "$WORK/admin.json"

echo "generating admin DTOs…"
swift run --package-path "$HERE" swift-openapi-generator generate \
    "$WORK/admin.json" \
    --config "$KIT/openapi-generator-config.yaml" \
    --output-directory "$OUT"

# The generator always writes Client.swift and Server.swift stubs even in
# types-only mode; they are empty and only add noise to the diff.
rm -f "$OUT/Client.swift" "$OUT/Server.swift"

echo "wrote $OUT/Types.swift"
echo
echo "next: cd $KIT && swift build && swift test"
