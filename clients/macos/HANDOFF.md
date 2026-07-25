# windex macOS — handoff

Branch `feat/macos-app-phase-7`, 1 commit behind `main` (merge before starting).

Read `DESIGN.md` (visual language) and `Packages/WindexKit/README.md` (how the
client is built) before writing code. This file is just state + next steps.

---

## Done

`Packages/WindexKit` — transport + models. **102 tests, all passing.**

| | |
|---|---|
| Pairing | `health` (open) → `whoami` (gated), token proven before it's saved |
| Search + docs | all filters, heterogeneous results, degraded-mode detection |
| Control plane | **all 43 admin operations** |
| SSE | dashboard stream (6 typed feeds) + per-crawl-run stream |
| Registry | ETag-cached to Application Support, falls back when backend blinks |
| DTOs | 38 generated from `openapi-admin.json`, checked in |

`Packages/WindexKit/Sources/WindexUI` — design system + form renderer.

| | |
|---|---|
| Tokens | palette, typography, 8pt space enum, radius, motion, theme |
| Status | the §5.2 vocabulary (glyph + word + colour) |
| SchemaForm | every editor in §5.1; `FormModel` holds the logic, SwiftUI-free |

```sh
cd Packages/WindexKit && swift test    # nothing needs to be running
```

## Not done

1. **App target.** No `.xcodeproj`, no `WindexApp`, no `Info.plist`.
2. **Keychain.** `WindexClient` takes a token; nothing persists it.
3. **Every screen.** Nothing is wired to a view yet.
4. **Fonts.** Archivo + IBM Plex Mono aren't bundled (see below).

---

## Next steps

### 1. App target + Keychain

Create the Xcode project, depend on both libraries. Two requirements:

- `NSAllowsLocalNetworking` in Info.plist — the API is plain HTTP on the LAN.
  Do **not** disable ATS globally.
- Token in Keychain, `kSecAttrAccessibleWhenUnlocked`. Never `UserDefaults`.

Window minimum 960×600; three-column collapses to two below 1100 (§8).

### 2. Fonts

`DESIGN.md` §3.2 wants **Archivo Condensed** + **IBM Plex Mono**; both SIL OFL,
neither bundled. Until added, `Typography` silently falls back to SF Pro
Condensed / SF Mono — legible but generic, so **the app won't look like the
design**. `Typography.missingFonts()` reports what's absent; the PostScript names
it looks up are in that file. Worth doing before screen work so the identity is
real from the first screen.

### 3. Screens, in `DESIGN.md` §9 order

1. Pairing + **the Colophon** (Overview) — proves connection and identity
2. Sources list → detail
3. Runs → **the Galley** (Run Monitor) — hardest; do once the language settles
4. Settings, Logs, Search — all `SchemaForm` and tables by then
5. Recipe editor — last

---

## Things that will bite

**Backend readiness doesn't match the build order.** Recipe CRUD, runs and the
marketplace don't exist server-side yet (main is actively building them). Ready
now: search, settings, jobs, loops, logs, schedule, freshness, stats, registry.

**Search returns nothing on the live box.** `/v1/stats` reports `vectors: {}` —
no Qdrant aliases — so every query is empty despite ~10M embedded docs.
Indexing is also globally paused. The Search screen will look correct and find
nothing. Independent of client work, but fix before step 4.

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
