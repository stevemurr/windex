# windex macOS — handoff

The original Phase 7 branch is merged. The Source/Pipeline redesign is a
coordinated breaking cutover; backward compatibility and database migration are
explicitly out of scope. The canonical plan is
`../../docs/source-pipeline-implementation-plan.md`.

Read `DESIGN.md` (visual language) and `Packages/WindexKit/README.md` (how the
client is built) before writing code. This file is just state + next steps.

---

## Done

`Packages/WindexKit` — transport + models. **93 tests, all passing.**

| | |
|---|---|
| Pairing | `health` (open) → `whoami` (gated), token proven before it's saved |
| Search + docs | all filters, heterogeneous results, degraded-mode detection |
| Control plane | **all 58 admin operations** |
| SSE | dashboard stream (6 typed feeds) + per-crawl-run stream |
| Registry | ETag-cached to Application Support, falls back when backend blinks |
| DTOs | 50 schema components generated from `openapi-admin.json`, checked in |
| Pipelines | typed revision, Flow, Node, Edge, layout, registry, draft, and local validation models |
| Sources | typed deployment, configuration, corpus counts, and Run summary models |
| Runs | canonical frozen-revision summaries; transport awaits the new contract |

`Packages/WindexKit/Sources/WindexUI` — design system + form renderer.

| | |
|---|---|
| Tokens | palette, typography, 8pt space enum, radius, motion, theme |
| Status | the §5.2 vocabulary (glyph + word + colour) |
| SchemaForm | every editor in §5.1; `FormModel` holds the logic, SwiftUI-free |

```sh
cd Packages/WindexKit && swift test    # nothing needs to be running
```

The app target, Keychain, fonts, app icon, CI, and archive/notarization tooling
are implemented. The active frontend branch adds a shared `BackendSession`,
canonical navigation, Source deployment workspace, shared Overview/Run
projections, a bounded structured Console, and an interactive Pipeline composer:
registry palette, draggable Nodes, Flow and boundary editing, typed connections,
undo/redo, local validation, auto-layout, draft recovery, and schema-driven Node
configuration.

The handwritten app and client surface no longer exposes Recipe or Marketplace.
The checked-in generated OpenAPI artifacts still reflect the old server and must
not be hand-edited; the backend cutover will replace them. Pipeline/Source
transport, publication, layout synchronization, live shared state, Run actions,
and Console history/streaming deliberately wait on the backend's canonical
contract epoch and regenerated OpenAPI surfaces.

### Backend integration checklist

Once the canonical server contract lands:

1. Regenerate both OpenAPI artifacts and replace the temporary registry adapter
   with typed Kind, Port, Module, capability, role, and field DTO projections.
2. Decode and enforce `contract_epoch` during pairing before constructing
   `BackendSession` or enabling mutation.
3. Add Pipeline/Source/Run/Overview/Event transports and make each shared store
   reconcile on connect, foreground, reconnect, and mutation responses.
4. Wire revision publication, independent layout ETags, Source create/upgrade/
   settings/lifecycle, and distinct Re-run/Run latest actions.
5. Connect the low-volume control SSE to Overview, Sources, Pipelines, and Runs;
   connect cursor-based history and the independently bounded high-volume stream
   to Console.
6. Add the local Python/Bash Module editor only after the secure scoped Module
   lifecycle API is available.

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

**Execution availability is not validation.** Pipeline drafts can be opened,
edited, and type-checked while their Modules are migrating. Registry responses
report `implemented`; publication and Run creation must refuse unavailable or
unapproved implementations.

**Source and Pipeline are intentionally different.** Editing a Pipeline publishes
a new immutable revision and never silently upgrades a Source. Editing Source
configuration affects future Runs only.

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
