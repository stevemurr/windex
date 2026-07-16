# Programming docs source — DevDocs bundles (researched 2026-07-16)

Verified live; decision: **DevDocs pre-built bundles as primary** (effort ~2/5).

## The source
- Manifest `https://devdocs.io/docs.json` (follow 302; 363KB) → **819 docsets**, per-set
  `slug`, `release` (version), **`mtime` (the freshness watermark)**, `db_size`,
  `attribution` (upstream license HTML — store and surface it).
- Per set: `documents.devdocs.io/<slug>/index.json` (entries with real upstream `#anchor`s)
  and `db.json` (`{path: cleaned HTML}` — page-level).
- Note: their CDN filters some client UAs (a 403 just means UA — plain curl works).
- Doc unit: **one db.json page** (section-split via anchors available later if agents need
  finer targets). Freshness verified real: rust 2026-07-12, javascript 2026-07-09, etc.

## Canonical URLs (hard requirement — agents link to official docs)
`canonical = base_url + path (+ ".html" for sphinx-family) + "#anchor"`, where base_url
comes from the open-source scraper defs (`lib/docs/scrapers/<name>.rb`) — NOT the manifest
`home`. Maintained as a small `{slug → (base_url, suffix_rule)}` table (~20 lines for the
seed set; rules group by scraper family: sphinx=+.html, MDN=no suffix). Verified live for
python (docs.python.org/3.14/... #anchor → 200) and MDN.

### Build findings (2026-07-16, implemented in `src/windex/docs_source/`)
- **Per-page exact upstream URL comes free**: DevDocs' attribution filter appends
  `<a href="<scraped url>" class="_attribution-link">` to every page scraped over HTTP
  (verified 59/59 flask, 81/81 vue~3 pages). This is the PRIMARY canonical source — it
  survives the fact that DevDocs **lowercases all page paths** (`normalize_paths.rb`),
  which breaks table-based reconstruction against case-sensitive upstreams
  (doc.rust-lang.org, react.dev, docs.ruby-lang.org, gnu.org all 404 on lowercased
  mixed-case pages; MDN 301-redirects them, verified). Locally-scraped docsets (go)
  carry no link (`base_url.host == 'localhost'` guard in their filter) → table fallback.
- The rule table gained a third family, `"dir"`, for dirhtml-style upstreams where
  DevDocs' trailing-slash pages became `…/index` paths (flask, django, docker,
  kubernetes, go→pkg.go.dev): strip the trailing `index` segment. Sixteen fallback
  constructions verified live (200/301/302); known-imperfect fallbacks (bash, ruby
  mixed-case pages — both file-scraped) are documented in `canonical.py`.
- Full-replace per slug means the staging parquet (`docs/clean/<slug>.parquet`) is
  rewritten with the FULL live page set each refresh — unchanged pages must stay
  readable at their `text_ref` — while the text-hash-guarded ledger upsert keeps
  re-embedding to the changed delta and vanished pages are tombstoned.

## Refresh pattern
Re-fetch the 363KB manifest; re-pull `db.json` only for slugs whose `mtime` advanced;
full-replace that slug's staging partition (no per-page deltas upstream). Exactly the
watermark + idempotent pattern.

## Licensing
Per-docset upstream licenses (PSF, MDN CC-BY-SA 2.5, Go CC-BY, Rust Apache/MIT, PG license…).
Index + snippet + attribution + link-out is fine across the board; never republish full
CC-BY-SA page bodies without attribution/ShareAlike. Store the attribution string per set.

## Seed set (~300MB total JSON, excluding whales)
python~3.14, javascript, typescript, node, go, rust, cpp*, c, react, vue~3, html, css,
http, postgresql~18, git, bash, php, ruby~3.4, django~6.1, flask, tailwindcss, docker,
kubernetes. (*whales — openjdk 120MB, dom 63MB, cpp 42MB — defer or include per need.)

## Rejected / fallback
- Official per-language archives (e.g. docs.python.org tarballs incl. a 3.3MB plaintext
  variant): authoritative but a bespoke-format treadmill per language — fallback for top ~10.
- MDN content repo: only for MDN pages DevDocs lacks (DevDocs already ships the MDN family).
- Dash/Zeal user-contributed docsets: SQLite+HTML tree, heavier, same upstreams — gaps only.
- HF datasets: all stale 2023-2024 single-library dumps. Read the Docs: no bulk export.
