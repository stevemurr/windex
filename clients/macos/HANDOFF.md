# windex macOS — epoch-2 handoff

The macOS client is an epoch-2-only application. There are no compatibility
shims, migrations, or active Recipe, Marketplace, crawl, job, loop, or legacy
dashboard surfaces. Database replacement is acceptable for this cutover.

The product model and implementation plan live in
`../../docs/source-pipeline-implementation-plan.md`. Read `DESIGN.md` for the
visual language and `Packages/WindexKit/README.md` for package mechanics.

## Contract authority

The backend owns both checked-in OpenAPI documents. `Generated/Types.swift` is
derived, never hand-edited:

```sh
clients/macos/Tools/generate.sh
swift test --package-path clients/macos/Packages/WindexKit
```

Pairing reads `/admin/v1/health` first and refuses any `contract_epoch` other
than `2` before authenticating or enabling mutations. `/admin` is a mount
prefix; the write token is sent only to the authenticated agent surface.

## Implemented frontend surfaces

- Registry, health, pairing, and strict epoch enforcement.
- Pipeline validation, creation, immutable revision publication and history,
  task preview transport, independent layout ETags, archive, and explicit
  revision generic Runs.
- Visual Pipeline graph composer with typed connections, registry palette,
  schema-backed node configuration, layout persistence, undo/redo, auto-layout,
  and local draft recovery.
- Source validation and creation, schema-backed settings, upgrade preview and
  confirmation, pause/resume, reset preview and confirmation, archive,
  schedules, Run latest, push contract instructions, and in-app push ingestion.
- Shared Run list and detail with task/unit progress, Run Events, typed boundary
  outputs, artifact download, cancel, frozen historic Re-run, and Source
  Run latest as distinct actions.
- Canonical Overview projection: corpus totals, searchable/vector counts,
  indexed-last-hour, Run pressure, active/recent Runs, worker lanes and blocked
  preconditions, Source schedules, recent documents, and service health.
- Global and Source settings with independent ETags. A 412 presents the
  operator’s edits beside current server values and offers reload or reapply.
- Cursor-based control and log SSE reconnect using `Last-Event-ID`. If either
  stream degrades, REST reconciliation runs at a bounded 2–15 second cadence
  and stops when both streams are live.
- Console history/facets and independently bounded live log stream.

Every connect, foreground, reconnect, and successful mutation reconciles the
Registry, Pipelines, Sources, Runs, Overview, and logs. Run list requests are
bounded to the server maximum of 200.

## Concurrency invariants

- Pipeline publication supplies the parent version and semantic hash.
- Generic Pipeline Runs always pin an explicit immutable revision unless a
  caller deliberately supplies the head ETag.
- Canvas layout writes use the layout’s independent ETag and the backend
  `layout.nodes` wire key.
- Source settings writes and deletes use the settings projection ETag.
- HTTP 409, 412, and 428 remain distinct typed errors.
- Historic Re-run uses the frozen historic revision/configuration. Run latest
  uses the Source’s current revision/configuration.

## Verification gates

Run both gates from a native Mac:

```sh
swift test --package-path clients/macos/Packages/WindexKit

cd clients/macos
xcodebuild \
  -project Windex.xcodeproj \
  -scheme Windex \
  -destination 'platform=macOS' \
  CODE_SIGNING_ALLOWED=NO \
  test
```

The active-code legacy scan must be empty:

```sh
rg -n 'Recipe|Marketplace|/v1/(jobs|crawl|stats|schedule|loops|recipes|marketplace)' \
  clients/macos/WindexApp clients/macos/Packages/WindexKit/Sources
```

For a live epoch-2 acceptance pass: pair, load every store, publish a Pipeline
revision, save a layout, create a Source, edit its settings, add/toggle a
schedule, preview/confirm an upgrade and reset only against disposable data,
queue/cancel/re-run a Run, reconnect both SSE streams, and confirm Overview and
Console update without manual refresh.

## Deferred deliberately

Custom Module authoring is not part of this cutover. The current backend
supports Python only and requires separate module-admin authentication plus
HTTPS. Marketplace remains out of scope.

## Release-only external gates

1. Install a Developer ID Application identity and set `APPLE_TEAM_ID`.
2. Create a `notarytool` Keychain profile and set `NOTARY_PROFILE`.
3. Run `Tools/release.sh <version> <build>`.
4. Pair the signed build directly to the LAN backend and confirm the macOS
   Local Network prompt.
5. Rotate any write token that has previously been shared outside the target
   deployment.
