# windex for macOS — design direction

Read this before writing UI code. It fixes the visual language so the app reads as
one designed thing rather than an assembly of SwiftUI defaults. Architecture,
screen inventory and API contracts live in
`~/.claude/plans/i-want-to-look-cheeky-muffin.md` §F — this document is only about
how it looks, feels and speaks.

---

## 1. The thesis

**windex is a library and this is its engine room.**

Not a SaaS dashboard, not a developer tool. It is an instrument panel for a large,
slow machine that one person owns, which runs continuously and reads the web. The
dominant activity is *watching* — is it healthy, what is flowing — punctuated by
*authoring* a new source and *intervening* when something is wrong.

The subject's own materials are the design's materials. windex's vocabulary is
already print: documents, corpus, outlets, publication dates, extraction, staging,
imposition of one thing onto another. So the app borrows from **press and
typesetting production**, not from analytics. Type does the work a gauge usually
does. Numbers are *set*, not plotted. Rules and folios organize. The only thing
that looks like machinery is the one screen where machinery is genuinely running.

**What this deliberately is not:** a grid of rounded cards each holding a stat and
a sparkline. That layout says "metrics product". This one should say "the press is
running."

### The audience is one person who lives here

Steven, on his own hardware, daily, for long sessions. That justifies committing to
a single dark identity rather than tracking system appearance — the same call Logic
and DaVinci make, and for the same reason: long dwell, dense data, and an identity
you recognize instantly. Light mode is specified in §3.5 as a courtesy, not as a
co-equal target.

---

## 2. Direction in one line

Ink-dark ground, warm paper-toned type, process cyan as the only accent, condensed
grotesque for anything set large, typewriter mono for anything the machine produced.

---

## 3. Tokens

### 3.1 Colour

Named for the metaphor, because that is what keeps usage honest — you reach for
`rule` when you want a hairline, not for "gray-700".

| Token | Hex | Use |
|---|---|---|
| `ink` | `#10131A` | the ground. A blue-black, never `#000` — pure black on an OLED-less display reads as a hole, and the blue cast is what makes the paper tone look warm |
| `plate` | `#171B24` | raised surfaces: panels, sidebar, popovers |
| `rule` | `#262C38` | hairlines, table dividers, input borders. 1px, never 2 |
| `graphite` | `#79808F` | secondary text, labels, disabled |
| `paper` | `#E9E5DB` | primary text. Warm off-white, not `#FFF` — the whole point of the palette |
| `cyan` | `#35B4D8` | **the single accent.** Process cyan, from a printer's registration mark |

Semantic, used *only* for state and never decoratively:

| Token | Hex | Meaning |
|---|---|---|
| `amber` | `#D99A2B` | attention: paused, clamped, degraded, stale |
| `rust` | `#C4553D` | fault: failed, unreachable, refused |
| `moss` | `#6E9B7A` | healthy — used **sparingly**, mostly absent |

**Why no green accent:** health is the default state, and a design that colours the
default state spends its loudest signal on its least informative moment. Healthy is
`paper` and `graphite`. Colour means *something needs you*. That single rule is
what keeps the interface calm, and it is the most important thing in this document.

Contrast floor: `paper` on `ink` is ~14:1; `graphite` on `ink` is ~4.8:1 (body-size
minimum, so never use `graphite` below 12pt); `cyan` on `ink` is ~7:1.

### 3.2 Type

Three roles, three faces. Bundle Archivo and IBM Plex Mono in the app; SF Pro is
the system.

| Role | Face | Where |
|---|---|---|
| Display | **Archivo Condensed** (or Archivo Expanded for the wordmark) | set numbers, screen titles, section mastheads |
| UI | **SF Pro** | every label, button, menu, body sentence |
| Data | **IBM Plex Mono** | doc ids, URLs, paths, counts, timestamps, log lines, Pipeline definition |

Using SF for UI chrome is deliberate, not a cop-out: a native app that fights the
platform's UI face feels like a web page in a window. The character comes from
Archivo set large and Plex Mono set small, both of which are unmistakably not-SF.

Chose Archivo because it descends from newsprint/highway signage — functional
editorial type, exactly the register of a press room. Chose Plex Mono over the
usual JetBrains/SF Mono for its typewriter lineage: it makes machine output look
*typed*, which is the metaphor.

**Scale** (macOS points):

| Token | Size / line | Face | Use |
|---|---|---|---|
| `set-xl` | 56 / 52, -2% tracking | Archivo Condensed Medium | the one number on a screen that matters |
| `set-lg` | 34 / 34 | Archivo Condensed Medium | secondary set numbers |
| `masthead` | 19 / 24, +6% tracking, uppercase | Archivo Condensed Semibold | screen titles, section heads |
| `eyebrow` | 10 / 12, +12% tracking, uppercase | SF Pro Semibold, `graphite` | field groups, table headers |
| `body` | 13 / 18 | SF Pro Regular | everything conversational |
| `label` | 12 / 16 | SF Pro Medium | controls |
| `data` | 12 / 17 | IBM Plex Mono Regular | ids, paths, counts |
| `data-sm` | 11 / 15 | IBM Plex Mono Regular | log and unit-feed lines |

Numerals: **tabular everywhere**, no exceptions. A count that reflows as it ticks
is the single most common way a live dashboard feels cheap.

### 3.3 Space, shape, depth

- **8pt base.** Steps: 4, 8, 12, 16, 24, 32, 48. Nothing between.
- **Corner radius 4** on controls and panels. **0** on tables, rules and the run
  graph. Rounded-everything is what makes an app read as generic; the flat table
  edges are what make it read as printed.
- **No shadows.** Depth is a value step (`ink` → `plate`) plus a `rule` hairline.
  Shadows on a dark ground produce mud.
- **Measure:** prose caps at ~68 characters. Description fields in the Pipeline
  inspector are the only long-form text and they must not run the full pane width.

### 3.4 Motion

Motion is reserved for things that are *actually moving*. Four places, no others:

1. Throughput figure — animated counter, 400ms, `easeOut`.
2. Unit feed — new rows insert from the top, 180ms fade + 4pt offset.
3. Run-graph node status — 250ms colour crossfade.
4. Sidebar/detail transitions — the system default. Do not customize.

No scroll reveals, no hover lifts, no shimmer skeletons, no spring bounces.
`accessibilityReduceMotion` disables 1–3 and shows final values immediately.

### 3.5 Light mode

Not the designed target, and not to be spent effort on until asked. If enabled:
`ink`→`#F2EFE7`, `plate`→`#FFFFFF`, `paper`→`#171B24`, `rule`→`#DAD5C9`,
`graphite`→`#6B7280`, `cyan`→`#1B87A8` (darkened for contrast on light). Semantics
darken by ~12%.

---

## 4. The two signature moments

Spend the boldness here. Everything else is quiet.

### 4.1 The Colophon — the Overview screen

A printed colophon rather than a dashboard. One large set figure, a real table with
typographic hierarchy, hairline rules, and a running head. No cards, no sparklines,
no gauges.

```
┌────────────────────────────────────────────────────────────────────────┐
│  WINDEX                                        running · 4 h 12 m      │  ← running head, eyebrow
│  ──────────────────────────────────────────────────────────────────────│
│                                                                        │
│   1,542                          17,493,416 documents                  │  ← set-xl / set-lg
│   embeds per minute              across 11 sources                     │     tabular, ticking
│                                                                        │
│  ──────────────────────────────────────────────────────────────────────│
│  SOURCE          INDEXED      PENDING    LAST RUN        STATE         │  ← eyebrow row
│  news         2,432,790      118,204     14 min ago      ·             │  ← data mono numerals
│  wiki         2,106,024            0     3 h ago         ·             │
│  arxiv        2,711,611       12,880     1 h ago         ◐ running     │  ← cyan only when live
│  hn             376,148            0     yesterday       ⚠ stale       │  ← amber only when wrong
│  ──────────────────────────────────────────────────────────────────────│
│  gateway ok · qdrant ok · storage 1.6 TB free                          │  ← colophon footer
└────────────────────────────────────────────────────────────────────────┘
```

The state column is **blank** for healthy rows — a middot at most. Only a source
that is running or needs attention earns a glyph and a colour. A column of green
ticks carries no information and costs the interface its calm.

### 4.2 The Galley — the Run Monitor

The one screen where the machine is visible. A small imposition diagram of the DAG
above, the unit feed streaming below like type coming off a press.

```
┌────────────────────────────────────────────────────────────────────────┐
│  claude_docs · run 4412                        [ Stop ]  [ Re-run ]    │
│  ──────────────────────────────────────────────────────────────────────│
│   01 seed ──▶ 02 get ─┬─▶ 03 links ──▶ 04 front                        │
│                       └─▶ 05 text ──▶ 06 stage                         │  ← nodes numbered:
│                                                                        │     execution order is
│   ████████████████████████░░░░░░░░  412 / 500 pages · 3 m 20 s left    │     real sequence, so
│  ──────────────────────────────────────────────────────────────────────│     the numbering is
│  ok      /cookbook/agents/router          1,284 chars                  │     honest, not decor
│  ok      /cookbook/agents/memory            902 chars                  │
│  skip    /cookbook/assets/logo.svg        scope                        │  ← graphite, recedes
│  fail    /cookbook/legacy/v1              http 502                     │  ← rust
└────────────────────────────────────────────────────────────────────────┘
```

Feed rows are `data-sm`. Status is a lowercase word in a fixed 8-character column,
not a badge — badges at this density become confetti. Cap at 500 rows, newest
first, matching the server's own bound.

**Node numbering is justified**: the topological order is real, it is what the
executor follows, and knowing "05 runs after 02" is information the reader needs.
Do not number anything that is not genuinely sequential.

---

## 5. Components

### 5.1 SchemaForm

The most reused thing in the app: it renders the Node inspector, Source and
operator settings, and Run dialogs from the same `Param` JSON. Build it once,
generically, driven entirely by `GET /admin/v1/registry` and `/admin/v1/settings`.
**Hardcode no field.**

| `editor` | Control |
|---|---|
| `textfield` / `url` | `TextField`, `data` face for url |
| `textarea` | `TextEditor`, `data` face |
| `number` | `TextField` + `Stepper`, tabular, `unit` as a trailing `graphite` suffix |
| `checkbox` | `Toggle` |
| `select` / `multiselect` | `Picker` / menu of toggles, using `enumTitles` |
| `stringList` / `regexList` | editable list, add/remove/reorder; regex rows validate per keystroke |
| `keyValue` | two-column table |
| `json` | `TextEditor`, parse-on-type, inline error |
| `datepicker` / `duration` | `DatePicker` / value+unit pair |
| `secret` | `SecureField`, write-only, shows "set" never the value |
| `hidden` | not rendered |

Layout: label above control, not beside — labels vary in length and a side-by-side
grid ragged-rights badly. `eyebrow` for `section` headings. `advanced: true` fields
go in a collapsed `DisclosureGroup` labelled "Advanced".

Three attributes carry real meaning and must be rendered, not dropped:

- **`clamp` + `clampNote`** — show the note as `graphite` helper text under the
  field. After a server validate returns a `clamped` warning, annotate the field:
  *"Adjusted to 1.0 — the operator's floor."* Without this, the author types 0.1,
  gets 1.0, and has no idea why.
- **`lockedReason`** — render disabled with the reason as helper text. Never hide a
  locked field; showing it disabled teaches what the system will not do.
- **`dependsOn`** — dim and disable when the condition fails. Never hide, for the
  same reason.

### 5.2 Status

One vocabulary across the whole app. Word, not icon-only; colour only when it is
not the happy path.

| State | Glyph | Colour | Word |
|---|---|---|---|
| healthy / idle | `·` | `graphite` | (none) |
| running | `◐` | `cyan` | running |
| attention | `⚠` | `amber` | paused / stale / clamped |
| fault | `■` | `rust` | failed |

### 5.3 Tables

Zero radius, `rule` hairline between rows, no zebra striping. Header row is
`eyebrow`. Numeric columns right-aligned and tabular. Row height 28. A table is
the default way to show a list here — resist turning any of them into cards.

### 5.4 The Pipeline composer

The Pipeline workspace is an interactive, registry-driven graph canvas. A
published revision is semantically read-only; “New revision” creates a mutable
draft. Manual Node positions, groups, and annotations synchronize independently
and never change the semantic hash.

Node boxes use the same treatment as the Galley so a Pipeline and a Run read as
the same graph in definition and execution states. Node boxes: `plate` ground,
`rule` border, radius 0, name in `label`, Module id and Port types in `data-sm`
`graphite`. The Module palette and Node inspector hardcode no Module vocabulary.

Validation surfaces in two registers: local checks annotate the field instantly;
the debounced server `validate` populates a footer strip — `⚠ 1 warning · 0 errors`
— that expands to the list. Server wins on conflict.

---

## 6. Emptiness, loading, failure

These are the moments that decide whether an app feels finished.

**Empty is an invitation, never a shrug.** No "No data available." State what the
screen is for and offer the action:

> **No sources yet.**
> A Source binds a pinned Pipeline revision to an origin, runtime state, and
> searchable corpus.
> [ Choose a Pipeline ]

**Loading never blocks what is already known.** `Loadable<T>` carries `stale`, so a
refresh dims existing content by ~40% rather than replacing it with a spinner. A
spinner appears only on genuinely first load, centred, with no text. **No skeleton
shimmer** — it is animation spent on the least meaningful moment.

**Failure says what happened and what to do**, in the interface's voice. Never
apologize, never be vague, never surface a raw exception:

> **Can't reach windex at `spark.local:8100`.**
> The backend may be down, or this Mac may be off the network.
> [ Retry ]  [ Change backend ]

Specific cases worth designing rather than generalizing:

- **401** → a persistent banner, not an alert: *"This token was rejected. Pair again
  to continue."* with [ Pair ]. Never a modal — modals interrupt; this can wait.
- **503 admin disabled** → surface the server's own fix-it text verbatim. It names
  the exact env var, which is more useful than anything we could paraphrase.
- **Live updates lost** → a small `graphite` chip in the toolbar, *"live updates
  unavailable — refreshing every 5s"*. Degradation is information, not an error.

---

## 7. Words

- **Sentence case everywhere.** No Title Case buttons.
- **Name things as the person controls them.** "Sources", not "deployment rows".
  "Pause indexing", not "set control flag".
- **An action keeps its name through the whole flow.** The button says *Run now*,
  the toast says *Run queued*, the row says *running*.
- **Be specific over clever.** *"412 of 500 pages"* beats *"Crawling…"*.
- **Numbers get units and context.** *"1,542 embeds per minute"*, not *"1542"*.
- **Destructive actions name the consequence**, matching what the CLI already does:
  *"Delete claude_docs and its 85 documents?"* — never *"Are you sure?"*.

---

## 8. Quality floor

Not optional, and not to be announced in the UI:

- Full keyboard navigation; visible focus ring in `cyan` at 2pt offset.
- VoiceOver labels on every control; status conveyed by text, never colour alone —
  which is why every state in §5.2 carries a word.
- `accessibilityReduceMotion` honoured (§3.4).
- Dynamic Type respected for `body` and `label`; `set-*` and `data` may stay fixed.
- Window minimum 960×600; the three-column split collapses to two below 1100.
- Every destructive action confirmable and every long action cancellable.

---

## 9. Build order

1. **Tokens + `SchemaForm`.** Everything else depends on them, and `SchemaForm` is
   four screens' worth of UI in one component.
2. **Pairing + Overview (the Colophon).** Proves the connection and the identity in
   one screen.
3. **Sources list → source detail.**
4. **Runs list → Run Monitor (the Galley).** The hardest screen; do it once the
   language is settled.
5. **Settings, Logs, Search.** All `SchemaForm` and tables by this point.
6. **Pipeline composer.** Build the registry palette, canvas, and inspector
   before publication and Source-creation flows are connected.

---

## 10. The one risk, stated

Committing to type-as-instrumentation instead of charts. If the Overview screen
feels *sparse* rather than *composed* once real numbers are in it, the fix is
typographic — tighten the scale, raise the set figure, add a second tier to the
table — **not** adding cards or sparklines. Those are available to any project;
this direction is not, and diluting it back toward a dashboard costs the whole
identity.

The place a chart is genuinely earned is the run progress bar and, later, a
throughput history on the Metrics screen. Everywhere else, set the number.
