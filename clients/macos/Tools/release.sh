#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="${1:-0.1.0}"
BUILD="${2:-1}"
OUTPUT="${3:-$ROOT/build/release}"
ARCHIVE="$OUTPUT/Windex.xcarchive"
EXPORT="$OUTPUT/export"
ZIP="$OUTPUT/Windex-$VERSION.zip"

case "$OUTPUT/" in
  "$ROOT/"*) ;;
  *)
    echo "Output must be inside $ROOT" >&2
    exit 2
    ;;
esac

if [[ -z "${APPLE_TEAM_ID:-}" ]]; then
  echo "APPLE_TEAM_ID is required for a Developer ID release." >&2
  exit 2
fi

mkdir -p "$OUTPUT"
rm -rf "$ARCHIVE" "$EXPORT" "$ZIP"

xcodebuild \
  -project "$ROOT/Windex.xcodeproj" \
  -scheme Windex \
  -configuration Release \
  -destination "generic/platform=macOS" \
  -archivePath "$ARCHIVE" \
  MARKETING_VERSION="$VERSION" \
  CURRENT_PROJECT_VERSION="$BUILD" \
  DEVELOPMENT_TEAM="$APPLE_TEAM_ID" \
  archive

xcodebuild \
  -exportArchive \
  -archivePath "$ARCHIVE" \
  -exportPath "$EXPORT" \
  -exportOptionsPlist "$ROOT/ExportOptions.plist"

ditto -c -k --sequesterRsrc --keepParent "$EXPORT/Windex.app" "$ZIP"

if [[ -n "${NOTARY_PROFILE:-}" ]]; then
  xcrun notarytool submit "$ZIP" \
    --keychain-profile "$NOTARY_PROFILE" \
    --wait
  xcrun stapler staple "$EXPORT/Windex.app"
  rm -f "$ZIP"
  ditto -c -k --sequesterRsrc --keepParent "$EXPORT/Windex.app" "$ZIP"
else
  echo "NOTARY_PROFILE is unset; created a signed but unnotarized archive." >&2
fi

echo "$ZIP"
