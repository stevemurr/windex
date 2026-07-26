# windex for macOS

The native control plane. Build this on a Mac — it does not compile on the Spark.

## Read in this order

1. **`DESIGN.md`** (here) — the visual language. Tokens, the two signature screens,
   component specs, empty/error states, copy rules. Read before writing UI code.
2. **`HANDOFF.md`** — current implementation state and external release gates.
3. **`Packages/WindexKit/openapi-admin.json`** — the control-plane schema, checked
   in so the build needs no Python environment. `openapi.json` is the agent-facing
   `/v1` contract (search + docs), which is deliberately a separate document.

## Two contracts, on purpose

| | |
|---|---|
| `/v1` | The agent API: search, docs, push. Additive-only forever. Open — no token. |
| `/admin/v1` | The control plane. Churns. Token on everything except `/v1/health`. |

Generate the admin DTOs with `swift-openapi-generator`; **hand-write** the
transport, the SSE client, and anything touching `/v1/search`. Search results are
deliberately sparse-and-additive (`RESULT_FIELDS` server-side), so freezing them
into a generated struct means a client regen every time a source gains a field.
Model a result as `{ id, score, extras: [String: JSONValue] }` with typed accessors.

## Connecting

```
GET  http://<host>:8100/admin/v1/health     open — probe before pairing
GET  http://<host>:8100/admin/v1/whoami     gated — validate the token at setup
```

`health` reports `auth_required`, so the app knows whether to ask for a token
before it asks. Auth is `Authorization: Bearer <WINDEX_WRITE_TOKEN>` on everything
under `/admin`.

Plain HTTP on the LAN — no TLS by design (a self-signed cert would mean an ATS
exception, which is a worse posture than HTTP + token on a trusted network). Set
`NSAllowsLocalNetworking` in Info.plist rather than disabling ATS globally. If the
backend is ever fronted by Caddy or reached over Tailscale, it gets TLS for free
and the app should accept an `https://` base URL unchanged.

## The endpoint that matters most

`GET /admin/v1/registry` — Port types, Node kinds, and every Module's config
schema. The graph editor renders its palette, its connection rules and every node
inspector from this one document. **Hardcode no vocabulary.** It is ETag'd; cache
it to Application Support and revalidate with `If-None-Match`, which is what keeps
the editor usable when the backend blinks.

Connection validity is one function the server hands you the data for:
an output type connects to an input type iff they are equal. Read it off `kinds[]`.

## Status

The app target and both packages are implemented. The current breaking-cutover
track replaces Recipes with two explicit concepts:

- A **Pipeline** is a reusable graph with immutable, content-addressed revisions.
- A **Source** deploys one pinned Pipeline revision with origin, configuration,
  durable state, and searchable corpus identity.

Navigation is Overview, Sources, Pipelines, Runs, Logs, Search, and Settings.
The Pipeline composer is registry-driven; Sources never become a second graph
editor. Marketplace work is intentionally deferred.

```sh
cd Packages/WindexKit && swift test
cd ../.. && xcodebuild -project Windex.xcodeproj -scheme Windex \
  -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO test
```

See `RELEASING.md` for Developer ID archive and notarization.
