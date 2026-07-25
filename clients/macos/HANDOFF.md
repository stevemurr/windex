# windex macOS — handoff

The original Phase 7 branch has been merged. Do not switch back to it.

Read `DESIGN.md` (visual language) and `Packages/WindexKit/README.md` (how the
client is built) before writing code. This file is just state + next steps.

---

## Done

`Packages/WindexKit` — transport + models. **91 tests, all passing.**

| | |
|---|---|
| Pairing | `health` (open) → `whoami` (gated), token proven before it's saved |
| Search + docs | all filters, heterogeneous results, degraded-mode detection |
| Control plane | **all 58 admin operations** |
| SSE | dashboard stream (6 typed feeds) + per-crawl-run stream |
| Registry | ETag-cached to Application Support, falls back when backend blinks |
| DTOs | 50 schema components generated from `openapi-admin.json`, checked in |
| Recipes | create/update/validate, placement and executor availability |
| Runs | list/detail/create/cancel, event pages and typed SSE |
| Marketplace | list/install/update with schema-driven install config |

`Packages/WindexKit/Sources/WindexUI` — design system + form renderer.

| | |
|---|---|
| Tokens | palette, typography, 8pt space enum, radius, motion, theme |
| Status | the §5.2 vocabulary (glyph + word + colour) |
| SchemaForm | every editor in §5.1; `FormModel` holds the logic, SwiftUI-free |

```sh
cd Packages/WindexKit && swift test    # nothing needs to be running
```

The app target, Keychain, fonts, planned screens, app icon, CI, and
archive/notarization tooling are implemented. The Xcode app suite has 12 tests.

---

## External gates

1. Install a Developer ID Application identity and set `APPLE_TEAM_ID`.
2. Create a `notarytool` Keychain profile and set `NOTARY_PROFILE`.
3. Run `Tools/release.sh <version> <build>`.
4. Pair the signed build directly to the LAN server and confirm the macOS local
   network prompt. Unsigned builds cannot prove that TCC path.
5. Rotate the production write token after the previously shared token has been
   replaced server-side.

---

## Things that will bite

**Execution availability is not validation.** Recipes can be installed, opened,
edited, and type-checked while their modules are migrating. Registry responses
report `implemented`, placement reports `executable`, and run creation returns
409 rather than queueing a graph that cannot execute.

**The marketplace is server-owned and inert.** Bundled entries and
`WINDEX_RECIPE_CATALOG_DIRS` contain YAML only. The admin API never fetches a
caller-chosen URL. Local edits block upstream updates instead of being
overwritten.

**`/admin` is a mount prefix.** `openapi-admin.json` paths are mount-relative:
its `/v1/health` is `/admin/v1/health` on the wire. `WindexSurface.admin` adds
it. Never send the token to `/v1` — that surface is open by design.

**Clamp vs reject.** Settings clamp (server silently adjusts, form previews it);
job params reject (server 422s, form blocks). Getting it backwards either
submits a value the operator didn't type or blocks input the server accepts.
Always treat the PATCH **response** as the truth — a clamped value comes back
different from what was sent.

**Regenerating.** After any change in `src/windex/api/`:

```sh
uv run python scripts/dump-openapi.py -o clients/macos/Packages/WindexKit/openapi.json
uv run python scripts/dump-openapi.py --which admin -o clients/macos/Packages/WindexKit/openapi-admin.json
clients/macos/Tools/generate.sh
```

`normalize_spec.py` runs first and is **not optional** — the generator drops
`anyOf: [X, null]` properties outright rather than erroring (145 of 199 the
first time). It also removes `SettingsField`/`SettingsScope`/`SettingsAll` so
`Param` stays the single decoder for that shape. Tests guard both.

**Design rules that erode one component at a time**

- Colour means *something needs you*. Healthy is `paper`/`graphite`, no green
  ticks. This is the most important line in `DESIGN.md`.
- Tables, not cards. Zero radius, hairline rules, no zebra.
- `dependsOn` and `lockedReason` **dim and disable, never hide**.
- Paused is normal. Degraded search is normal. Neither is an error state.
- Motion only where something is actually moving (4 places, §3.4).
