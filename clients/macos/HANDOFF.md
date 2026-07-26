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
than `2` before authenticating, constructing a session, or enabling mutations.
`/admin` is a mount prefix, not part of the paths represented in the OpenAPI
document.

## Implemented frontend surfaces

- Registry, health, pairing, and strict epoch enforcement.
- Pipeline validation, creation, immutable revision publication and history,
  task preview transport, independent layout ETags, archive, and explicit
  revision generic Runs.
- Visual Pipeline graph composer with typed connections, registry palette,
  Pipeline parameter-definition editing, literal/Pipeline-parameter/secret Node
  bindings, Flow rename, Flow-input/Node/Flow-output connections, compatible
  target highlighting, bounded canvas zoom, diagnostic focus, debounced server
  validation, undo/redo, auto-layout, and local draft recovery. Published
  revisions are semantically read-only until the operator explicitly starts a
  new revision.
- Revision- and Flow-specific layout editing, including Node positions, groups,
  and annotations. All open layout object fields round-trip without changing
  array shapes. Layouts remain editable on immutable revisions and use their
  independent ETags.
- Generic Pipeline Runs select an explicit immutable revision and Flow, render
  declared Pipeline parameters with `SchemaForm`, collect one JSON value per
  typed boundary input, and preserve Source Run latest as a different action.
- Source-capable Pipeline revisions expose `Use as Source`, carrying that exact
  immutable revision into Source creation.
- Source validation and creation, schema-backed settings, upgrade preview and
  confirmation, enable/disable, pause/resume, reset preview with typed
  confirmation, archive, schedule/event triggers, Run latest, push contract
  instructions, and in-app push ingestion. Interval, cron, and event triggers
  can be created and edited independently from enable/disable. Pipeline values
  are rendered from the selected revision schema; secret-reference controls use
  configured secret names.
- Upgrade can target any eligible Source-capable revision and renders retained,
  defaulted, removed, clamped, missing, installation-stage, and state-impact
  details. An invalid candidate can be edited and checked locally. The current
  epoch-2 upgrade request and validation routes do not carry an edited target
  candidate, so the client can re-check its parameter schema but visibly blocks
  server validation and confirmation after such an edit instead of submitting
  a non-atomic or misleading request.
- Shared Run list and detail with task/unit progress, Run Events, typed boundary
  outputs, artifact download, cancel, frozen historic Re-run, and Source
  Run latest as distinct actions. Each Source workspace pages its canonical Run
  history and routes every row to shared Run detail.
- Canonical Overview projection: corpus totals, searchable/vector counts,
  indexed-last-hour, Run pressure, active/recent Runs, worker lanes and blocked
  preconditions, Source schedules, recent documents, and service health.
- Global and Source settings with independent ETags. One session-owned
  configuration draft per Source is shared by Source detail and global
  Settings, including unsaved edits. A 412 presents the operator’s edits beside
  current server values and offers reload or reapply.
- Cursor-based control and log SSE reconnect using `Last-Event-ID`. If either
  stream degrades, REST reconciliation runs at a bounded 2–15 second cadence
  and stops when both streams are live.
- Console facets for time, level, component, Source, Pipeline, Run, Node,
  Module, and text; persisted saved presets; server-filtered cursor history;
  contextual deep links; and an independently bounded, deduplicated live
  stream with follow/pause behavior.
- Source, Run, and Overview links preserve exact pinned Pipeline revision
  context. Activity and failure links open a prefiltered Console.

Every connect, foreground, reconnect, and successful mutation reconciles the
Registry, Pipelines, Sources, Runs, Overview, and logs. Run list requests are
bounded to the server maximum of 200.

## Concurrency invariants

- Pipeline publication supplies the parent version and semantic hash.
- Generic Pipeline Runs always pin an explicit immutable revision unless a
  caller deliberately supplies the head ETag.
- Canvas layout writes use the layout’s independent ETag and preserve the
  backend `layout.nodes`, `layout.groups`, and `layout.annotations` shapes.
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
revision, save layouts across multiple Flows, create a Source, edit its
settings, enable/disable it, pause/resume it, add/toggle a trigger,
preview/confirm an upgrade and reset only against disposable data,
edit an event trigger, queue/cancel/re-run a Run, reconnect both SSE streams, and confirm Overview and
Console update without manual refresh.

## Deferred deliberately

Custom Module authoring is not part of this cutover. The current backend
supports Python only and requires separate module-admin authentication plus
HTTPS. Marketplace remains out of scope.

## Current external acceptance blockers

- The available LAN server is still pre-epoch-2. Strict contract enforcement
  intentionally prevents pairing, so the end-to-end mutation/SSE acceptance
  pass must wait for an epoch-2 deployment.
- Server validation and atomic submission of an operator-edited upgrade
  candidate require the authoritative validation/upgrade request schemas to
  accept the target revision plus candidate values. The client supports
  preview, editing, and schema-local checking, but only confirms the unchanged
  server-generated candidate.

## Release-only external gates

1. Install a Developer ID Application identity and set `APPLE_TEAM_ID`.
2. Create a `notarytool` Keychain profile and set `NOTARY_PROFILE`.
3. Run `Tools/release.sh <version> <build>`.
4. Pair the signed build directly to the LAN backend and confirm the macOS
   Local Network prompt.
5. Rotate any write token that has previously been shared outside the target
   deployment.
