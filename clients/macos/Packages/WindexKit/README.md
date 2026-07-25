# WindexKit / WindexUI

Two libraries: `WindexKit` is the transport and model layer, `WindexUI` is the
design system and the generic form renderer. They're split so the transport
stays importable from anything that doesn't draw and testable headlessly.

Read `../../README.md` first for the two API contracts, and `../../DESIGN.md`
before writing any UI.

## Status

| Area | State |
| --- | --- |
| Pairing (`/admin/v1/health` → `/admin/v1/whoami`) | done |
| Search + document fetch, all filters | done |
| Settings, and the generic `Param` behind `SchemaForm` | done |
| All 43 control-plane operations | done |
| SSE: dashboard stream + per-crawl-run stream | done |
| Module registry, ETag-cached; recipe validate | done |
| Design tokens (§3) and the status vocabulary (§5.2) | done |
| `SchemaForm` — every editor in §5.1 | done |
| Fonts — Archivo and IBM Plex Mono are not bundled | **falls back** |
| Keychain token storage | not started |
| App target, and every screen | not started |

102 tests.

## Layout

```
Packages/WindexKit/
  openapi.json          # /v1 agent API — checked in
  openapi-admin.json    # control plane — checked in
  Sources/WindexKit/
    Core/               # client, errors, SSE, JSONValue, date parsing
    Models/             # Param, Settings, Search, Pairing, Wire (aliases)
    Generated/          # 38 control-plane DTOs — regenerate, don't edit
    API/                # per-surface call sites
  Sources/WindexUI/
    Tokens/             # palette, typography, layout, motion, theme
    Components/         # status vocabulary
    SchemaForm/         # FormModel (the logic) + the renderer
  Tests/WindexKitTests/
    MockServer/         # a real localhost HTTP server + fixtures
    Fixtures/           # GENERATED from windex's own schema
  Tests/WindexUITests/
```

## The fonts are not bundled

`DESIGN.md` §3.2 calls for **Archivo Condensed** (display) and **IBM Plex Mono**
(data). Neither is in the bundle yet, so `Typography` resolves each style to a
documented fallback — SF Pro Condensed and SF Mono. The app is legible but
generic until the real faces land; both are SIL OFL. `Typography.missingFonts()`
reports what's absent. The substitution is silent on purpose: a missing font
should degrade a screen, not crash it.

## SchemaForm

The most reused component in the app: settings, job argument dialogs, and later
— unchanged — recipe install params and the graph node inspector. It switches on
`editor`, never on `key`.

`FormModel` holds the logic and has no SwiftUI in it, so the rules that actually
matter are tested without rendering:

- **`dependsOn` dims and disables, never hides.** Hiding a setting an operator
  knows exists means they simply can't find it. A disabled field is also excluded
  from the patch — a gated-off value isn't the operator's intent.
- **`lockedReason` renders disabled with the reason**, never hidden.
- **Clamp vs reject.** A `clamp` param previews its adjustment ("Will save as
  3 s") and never blocks submit; a `reject` param errors. Getting this backwards
  either submits a value the operator didn't type or blocks input the server
  would have accepted.
- **The response is the truth.** `apply(_:)` adopts the server's returned values
  as the new baseline and records a notice wherever they differ from what was
  sent — "Adjusted to 3 s — the operator's floor."
- **Only changed keys are submitted.** A PATCH merges, so sending everything
  would turn every untouched default into an explicit override and the origin
  column would go all-`db` after one save.

No SPM dependencies, so there is no package graph to resolve and `swift build`
works offline. The mock server is built on Network.framework, which is in the SDK.

## Generated vs hand-written

Both, on a deliberate line.

**Generated** — `Sources/WindexKit/Generated/Types.swift`, the 38 control-plane
DTOs, checked in. This is where the churn is: `LoopState`, `JobInfo`,
`ScheduleEntry`, `ActivityItem`, `LogTail`, `CrawlRun`, `Registry`,
`TimeseriesPoint` and the rest. Regenerate when the control plane changes:

```sh
clients/macos/Tools/generate.sh
```

**Hand-written** — the transport, the SSE client, everything touching
`/v1/search`, and the `Param` form model:

- **Search** isn't in `openapi-admin.json` at all (it's the agent API), and its
  results are deliberately sparse-and-additive. `SearchHit` gives typed accessors
  over the known fields and preserves everything unrecognised in `additional`, so
  a source gaining a field needs no client change.
- **`Param`** is a domain model, not a DTO — see below.

### Exactly one decoder per wire shape

The rule: **generated where the spec owns the shape, hand-written only where the
spec leaves it untyped or doesn't describe it at all.** Where a generated type
would merely duplicate a hand-written one, the hand-written one is deleted;
`Health` and `WhoAmI` are aliases of the generated types in `Wire.swift`, with
`isWindex`/`needsToken` hung off them as extensions.

`Param` is the one shape that goes the other way, because the spec types it in
only **one** of the three places it occurs:

| Call site | Spec |
| --- | --- |
| `SettingsScope.fields` | `SettingsField` — typed |
| `JobInfo.params` | `{type: object}` — untyped |
| `Registry.modules[]` | `{type: object}` — untyped |

All three carry the same `Param.describe()` payload. A generated `SettingsField`
therefore could not be the single source of truth: the job dialog and the recipe
node inspector both receive that shape as raw JSON and need a hand-written
decoder regardless. Generating it too would mean two decoders for one format,
kept honest only by a test.

So `Tools/normalize_spec.py` **removes** `SettingsField`, `SettingsScope` and
`SettingsAll` before generation (`DOMAIN_OWNED`), and `Param` serves all three
call sites. It also carries the behaviour the form layer needs and a DTO can't:
enums instead of `String?`, clamp preview, `dependsOn` gating, pre-submit
validation. See `DESIGN.md` §5.1.

`domainOwnedSchemasStayRemoved` greps the generated source and fails if any of
the three comes back — a reintroduced twin would otherwise compile fine under a
different namespace and silently restore the drift.

### Why the generator isn't a dependency of this package

`swift-openapi-generator` pulls a seven-package graph (OpenAPIKit, Yams,
swift-algorithms, swift-numerics, argument-parser). Declaring it here would make
every clean checkout resolve all of it just to compile. It lives in
`../../Tools/` instead, the output is checked in, and this package depends only
on `swift-openapi-runtime`.

### The normalization step, and why it isn't optional

`Tools/normalize_spec.py` rewrites `anyOf: [X, {"type": "null"}]` to `X` on a
**copy** of the spec before generating.

FastAPI renders every `X | None` field that way. swift-openapi-generator 1.13
doesn't support `type: "null"`, and its failure mode is the dangerous one: it
warns and **drops the whole property** rather than erroring. Run against the raw
document it silently discarded **145 of 199 properties** — 19 of the 22 on
`SettingsField`, and `enabled`/`running`/`state` off `LoopState`. The generated
code compiled, looked plausible, and was missing almost everything.

Collapsing the union is exactly equivalent, because those keys are absent from
`required`, so they generate as Swift optionals — which is what `X | None` meant.
`nullableUnionsBecameOptionals` in the conformance suite fails if this step is
ever lost.

### Generated types find server bugs

Typed DTOs turn a contract mismatch into a compile-or-decode failure instead of a
field that silently reads as nil. Three surfaced the first time the generated
types met real payloads, all in `src/windex/api/models.py`:

| Field | Declared | Actually sent |
| --- | --- | --- |
| `ActivityItem.error` | `str` | `bool` — it's a flag: `not running and errored(key)` |
| `WorkersState.active` | `int` | `bool` — `True`/`False` in both branches |
| `TimeseriesPoint.t` | `float` | `str` — an ISO-8601 minute, `m.isoformat()` |

All three are fixed. The remaining response models were then audited against live
payloads from a running backend and validate clean.

When a decode fails after regenerating, suspect the model before the client: the
handler is the truth, and `_Loose` models don't validate their own output.

After changing anything in `src/windex/api/`, regenerate the specs and the DTOs:

```sh
uv run python scripts/dump-openapi.py -o clients/macos/Packages/WindexKit/openapi.json
uv run python scripts/dump-openapi.py --which admin -o clients/macos/Packages/WindexKit/openapi-admin.json
clients/macos/Tools/generate.sh
```

`uv run python scripts/dump-openapi.py --check` is the CI gate for the first two.

## Running the tests

```sh
cd clients/macos/Packages/WindexKit
swift test
```

Nothing needs to be running. The suite starts a real HTTP server on a loopback
port and points the client at it.

### Why a real socket and not a `URLProtocol` stub

The tests exist so that swapping in the real backend changes exactly one thing —
the base URL. A `URLProtocol` stub short-circuits `URLSession` above the network
stack, so it would pass while the things most likely to break went untested:
query-string encoding on the wire, whether `Authorization` is really attached to
`/admin` requests and really absent from `/v1` ones, status mapping, and SSE
framing across arbitrary buffer splits.

That is not hypothetical. Two bugs surfaced on the first run and neither would
have shown up against a stub:

* `URLSession.bytes.lines` **silently drops empty lines**, and in SSE the blank
  line is the event terminator — every event after the first merged into its
  predecessor. The parser now splits bytes itself.
* Assigning to `URLComponents.path` re-encodes an already-escaped string, so the
  doc id `gh:qdrant/qdrant` went out as `%253A` and 404'd. Fixed by assigning
  `percentEncodedPath`.

### Regenerating fixtures

The settings fixtures are generated from `windex.settings_schema` so the tests
decode what the server actually emits:

```sh
uv run python clients/macos/Packages/WindexKit/Tests/WindexKitTests/Fixtures/generate_fixtures.py
```

Re-run after changing `settings_schema.SCHEMA` or `Param.describe()`. The
`decodesRealSchema` test asserts the Swift model covers every scope and field the
server declares, so a schema change the client hasn't caught up with fails a test
instead of quietly rendering an incomplete form.

## Things a newcomer gets wrong

**The `/admin` mount prefix.** `openapi-admin.json` describes a *mounted* sub-app,
so its paths are mount-relative: the spec's `/v1/health` is `/admin/v1/health` on
the wire. `WindexSurface.admin` adds the prefix — don't hardcode paths around it.

**Never send the token to `/v1`.** The agent surface is open by design; attaching
the bearer there leaks the admin credential to routes that never need it. There's
a test for this.

**Clamp vs reject.** A numeric `Param` with `enforce: "clamp"` is *silently pulled*
to its bound by the server; one with `enforce: "reject"` is refused. Settings are
`clamp`, job params are `reject`. A client that clamps a `reject` param submits
something other than what the operator typed; one that validates a `clamp` param
locally blocks input the server would have accepted. Branch on `enforce`, and
treat the PATCH **response** as the truth — a clamped value comes back different
from what was sent. `DESIGN.md` §5.1 specifies how to surface the adjustment.

**An unknown `enforce` decodes to `reject`, not `clamp`.** Failing safe costs a
visible error; guessing `clamp` would submit a value the operator never typed.

**`Param.Editor` is open.** `EDITOR_FOR_KIND` server-side covers the built-in
kinds, but `recipe/parse.py` lets a module declare any `editor` string, so the
enum carries the full `DESIGN.md` §5.1 vocabulary plus an `.unknown` case that
falls back to a text field. A newer server can add a control without breaking the
form.
