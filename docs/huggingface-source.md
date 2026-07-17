# Hugging Face source — crawling huggingface.co (researched 2026-07-16)

Everything below was verified live against huggingface.co on 2026-07-16, unauthenticated.
Scope: treating huggingface.co as a **website to crawl and index**, not as a metadata API.
(The Hub-metadata path was researched first under a mis-scoped brief; it survives only as
the *discovery/freshness* note and the reason model pages stay out — see the appendix.)

Decision: **build it, but only the text-rich third of the site** (effort ~2/5).
**~4,000 pages** — all of HF's docs, courses and blog. Not the 3.9M model/dataset/Space
pages, which are variously un-enumerable, un-extractable, or already covered.

## 1. robots.txt — fully permissive, no blocker

```
User-agent: *
Allow: /

Sitemap: https://huggingface.co/sitemap.xml
```

That is the entire file (68 bytes, HTTP 200). **No `Disallow`, no `Crawl-delay`, no
per-agent rules** — nothing we want is off-limits, and there is no declared delay to honor.
Politeness is therefore *our* obligation to define, and HF defines it for us in headers
instead (§5). robots.txt is re-checked per run via the existing `RobotsCache` regardless —
a permissive file today is not a licence to stop looking.

## 2. Sitemaps — a recency window, not a catalog

`sitemap.xml` is a sitemap index with 7 shards. **Every shard is small**, which is the
tell: measured URL counts and `lastmod` spans —

| shard | URLs | lastmod span | what it really is |
|---|---:|---|---|
| `sitemap-static.xml` | 10 | (none) | landing pages |
| `sitemap-doc.xml` | **52** | 2023-07-28 → 2026-07-16 | **complete** — doc *roots*, not pages |
| `sitemap-blog.xml` | **829** | **2020-02-14 → 2026-07-16** | **complete blog archive** |
| `sitemap-models.xml` | 6,026 | 2026-02-10 → 2026-07-17 | 0.2% of 2.9M — rolling |
| `sitemap-datasets.xml` | 6,729 | **2026-07-09 → 2026-07-17 (8 days)** | rolling window |
| `sitemap-spaces.xml` | 9,957 | 2021-12-26 → 2026-07-17 | ~10k cap |
| `sitemap-papers.xml` | **10,000** | 2025-02-18 → 2026-07-15 | exactly capped |

**The models/datasets/spaces/papers shards cannot enumerate the site.** `datasets` spans
eight days; `papers` is a round 10,000; `models` holds 6,026 of 2,914,786 and is topped by
repos modified minutes ago (`thekuntalpal/Kimi-K2.6`, `lastmod` 2026-07-17T00:37 — junk,
sorted by recency, not value). These are **"what changed lately" feeds**, and treating one
as a frontier would silently index a random recent slice of the Hub.

**The `doc` and `blog` shards are different in kind**: `blog` spans from 2020-02-14 (the
first post) to today — that is the whole archive. `doc` is the complete set of doc
products. Those two are real, completable units of work.

## 3. Which page types are worth linking to

Measured with **trafilatura** (`include_tables=True`) — the extractor windex actually uses,
not a browser — on live pages:

| page type | HTML | trafilatura text | verdict |
|---|---:|---:|---|
| `/docs/transformers/main_classes/pipelines` | 1,304,702 | **141,121** | **IN** |
| `/docs/transformers/quicktour` | 265,381 | **8,686** | **IN** |
| `/learn/agents-course` | 126,894 | **6,810** | **IN** |
| `/blog/<slug>` | 160,456 | **5,056** | **IN** |
| `/papers/2607.13921` | 151,725 | 2,882 | out — duplicate |
| `/<owner>/<model>` | 380,568 | 19,066 | out — un-enumerable |
| `/datasets/<owner>/<name>` | 539,194 | 54,576 | out — **junk text** |
| `/spaces/<owner>/<name>` | 54,285 | **220** | **out — nothing there** |
| `/<org>` | 407,797 | 1,277 | out — thin |

**The useful text is server-rendered — no JS needed** for docs, blog, papers, or model
pages. Two exceptions decide the scope:

- **Spaces extract to 220 characters.** They are client-rendered Gradio/Streamlit apps; the
  served HTML is a mount point. There is no text to index. Excluded on evidence, not taste.
- **Dataset pages extract the *viewer table*, not prose** — 54,576 chars beginning
  `Parameters Size / question stringlengths 42 985 | answer stringlengths 50 1.23k`. That is
  data rows, and embedding it would poison the index with table noise.

**In scope: `/docs/*` + `/learn/*` (3,175 pages), `/blog/*` (829), `/static` (10) ≈ 4,014
pages.**

**Out, with reasons:**
- **`/papers/*`** — the id *is* the arXiv id (`/papers/2607.13921` → arXiv 2607.13921), and
  **windex already indexes arXiv abstracts**. Indexing it would duplicate an existing source
  against itself. (The paper page's community-discussion layer is the only novel part —
  not worth 10k pages.)
- **Model / dataset pages** — the sitemap covers a rolling 0.2%, so a crawl cannot enumerate
  them; and if we ever want them, the metadata dump is strictly better than crawling
  (appendix). Crawling is the wrong tool here, independent of whether the content is wanted.
- **Spaces, org pages** — measured above.

### The prize: `llms.txt` + `.md`, and there is no scraping to do

HF publishes, per doc root, a **standard `llms.txt`** — a titled index of every page as a
`.md` link — and serves every doc page as **clean markdown**:

```
$ curl https://huggingface.co/docs/transformers/llms.txt        # 200, 65,309 B, 727 .md links
# Transformers
## Docs
- [Quickstart](https://huggingface.co/docs/transformers/v5.14.0/quicktour.md)
...
$ curl https://huggingface.co/docs/transformers/quicktour.md    # 200, text/markdown
# Quickstart
Transformers is designed to be fast and easy to use ...
```

Swept all 52 roots: **`llms.txt` present on 47/52, totalling 3,175 `.md` pages.**

| root | pages | | root | pages |
|---|---:|---|---|---:|
| `docs/transformers` | 727 | | `docs/peft` | 72 |
| `docs/diffusers` | 363 | | `docs/inference-providers` | 65 |
| `docs/hub` | 268 | | `docs/trl` | 63 |
| `learn/deep-rl-course` | 114 | | `docs/accelerate` | 58 |
| `docs/lerobot` | 97 | | `docs/datasets` | 58 |
| `learn/computer-vision-course` | 91 | | `docs/huggingface_hub` | 52 |
| `docs/huggingface.js` | 81 | | `docs/smolagents` | 22 |
| `learn/agents-course` | 77 | | *(+31 more)* | |

The 5 roots without `llms.txt` (404): `docs/evaluate`, `docs/optimum-executorch`,
`docs/setfit`, `docs/simulate`, `docs/tokenizers` — legacy/small. Fall back to HTML
extraction or skip; they are ~2% of the corpus.

**This matters because the docs nav is client-rendered.** A page's served HTML exposes only
21 internal links, 17 of which are SvelteKit asset chunks — the sidebar toctree is not in
the HTML. **BFS link-crawling would not enumerate a doc tree.** `llms.txt` is the only
enumeration path, and it happens to be the one HF publishes *for exactly this consumer*.

This is **DevDocs' shape, better**: manifest → per-set page list → clean content, except
the content arrives as markdown so **trafilatura is not needed for docs at all**, and —

### Canonical URLs come free (unlike DevDocs)

`llms.txt` links are version-pinned (`/v5.14.0/quicktour.md`), but the unversioned URL
serves byte-identical content (13,126 B both ways, verified), and the page itself declares:

```html
<link rel="canonical" href="https://huggingface.co/docs/transformers/quicktour">
```

So `canonical = strip /vX.Y.Z/ and strip .md` — **authoritative, from the page**. No
`{slug → base_url}` rule table, no lowercasing trap, none of `docs_source/canonical.py`'s
hard-won pain. Version-pin stripping must be deliberate: we want the evergreen URL, and
re-crawling a bumped version at the same canonical id is a clean upsert.

**Blog has no `.md`** (`/blog/<slug>.md` → 404) → blog stays HTML + trafilatura, which
extracts cleanly (5,056 chars).

## 4. Is this "smallweb pointed at one host"? **No — the load model inverts**

`src/windex/smallweb/` has the right *parts*, but its central premise is the opposite of
ours. From `docs/smallweb-source.md`: *"~37.6k hosts spread load naturally, so a simple
async pool + per-host last-hit map suffices."* And `poll.py:468`:

> *"Poll a batch of feeds, up to the global concurrency cap. Distinct hosts run in parallel;
> same-host page fetches serialize on the per-host interval."*

**HF is one host.** So:

- **`smallweb_concurrency: int = 12` is inert.** Twelve workers all serialize behind one
  host's limiter. The parallelism smallweb depends on does not exist here.
- **`smallweb_host_interval: float = 10.0` would cost 11 hours** for 4,014 pages. That
  interval is calibrated for hitting a stranger's personal blog, not for a host that
  publishes its own quota. HF needs **its own interval (3s, §5)** — do not reuse this value.
- **`PageFetcher.fetch()` rejects our best content outright**:
  ```python
  if "html" not in resp.headers.get("content-type", "").lower():
      return None
  ```
  `.md` serves `text/markdown`, `llms.txt` serves `text/plain` (both verified). Reusing
  `PageFetcher` unchanged silently drops **every doc page**. It needs an allowed-content-type
  parameter.

**Verdict: reuse the machinery, not the configuration.**

| component | reuse? |
|---|---|
| `RobotsCache` | **as-is** — correct even though HF's robots is `Allow: /` |
| `HostRateLimiter` | **mechanism yes, interval no** — 3s from HF's published quota, not 10s |
| `PageFetcher` | **needs a content-type allowlist** (`text/markdown`, `text/plain`) |
| trafilatura extraction | **blog only** — docs arrive as markdown |
| spaCy/FineWeb quality filters | **skip** — see below |
| staging / ledger / dedup / embed / serving | **as-is** |

**The spaCy thread-safety constraint still applies but mostly dissolves**: `extract_items()`
runs extraction on the main thread while network I/O fans out to workers. For HF that split
survives for the blog, and for docs there is nothing to extract — markdown goes straight to
staging. The quality filters should be **skipped entirely** for docs: `docs/smallweb-source.md`
already warns the FineWeb filters *"over-reject short/idiosyncratic"* text and *"false
rejection is the main quality risk"*. Official API reference pages are exactly the shape
those filters mis-handle, and this corpus is curated by construction — the same call DevDocs
made. **The quality filter here is the scope decision (§3), not a text classifier.**

## 5. Crawl cost and politeness — a third rate-limit bucket

HF publishes its limits in response headers. There are **three separate buckets**, and the
one governing page routes is by far the tightest — this is the cap that must be designed for,
the HN/Algolia lesson applied:

| bucket | routes | policy | effective |
|---|---|---|---|
| `"api"` | `/api/*` | `q=500;w=300` | 500 / 5 min |
| `"resolvers"` | `/raw/*`, `/resolve/*` | `q=3000;w=300` | 10 / s |
| **`"pages"`** | **`/docs/*`, `/blog/*`, `.md`** | **`q=100;w=300`** | **1 req / 3 s** |

Verified live: `ratelimit-policy: "fixed window";"pages";q=100;w=300`, with a live
`ratelimit: "pages";r=60;t=206` counter on every response. **`.md` fetches ride the `pages`
bucket**, not resolvers — I checked, because the fast path would have been a convenient
assumption.

**So the interval is 3s, and HF chose it, not us.** That happens to match arXiv's mandated
1-req/3s exactly, so `harvest.py`'s existing shape carries over. Better still, the harvester
should **read its own `ratelimit: r=/t=` header and self-throttle** rather than open-loop
sleep — the budget is published on every response.

- **Cold backfill: 4,014 pages ≈ 3.3 hours** (40 × 5-min windows at 100/window). One time.
- **Refresh is far cheaper than that, and must not be a re-sweep**: re-fetch the 52
  `llms.txt` files (52 requests ≈ 3 min), hash each, and re-pull `.md` only for roots whose
  `llms.txt` changed; use `sitemap-blog.xml`'s `lastmod` as the blog cursor. A quiet day
  costs ~60 requests. `ETag`/`If-None-Match` is present on `.md` (verified `W/"3346-..."`)
  as a second-level guard — but note a 304 still spends a request against the bucket, so
  the llms.txt-hash gate is what actually keeps refresh cheap, not the 304.
- Nightly cron alongside the other sources; there is no burst and no reason to hurry.

## 6. Where the API still helps (secondary)

Not as the product — as a **freshness signal we don't otherwise have**:

- **Not needed for docs/blog.** `llms.txt` hashes and blog `lastmod` are cheaper and more
  precise than any API call. Don't add the dependency.
- **Useful if model pages are ever revisited**: `cfahlgren1/hub-stats` (Apache-2.0, daily
  parquet, `lastModified` per repo) enumerates all 2,914,786 models — the thing the sitemap
  cannot do — for a ~173MB column-projected read. That is the *only* sane way to know which
  of 2.9M pages changed, and it argues for the dump over a crawl rather than for a crawl.

## Document model

- Two sources: **`hfdocs`** (docs + courses) and **`hfblog`**, own collections behind
  `hfdocs_current` / `hfblog_current`. Different text shapes (markdown vs extracted HTML)
  and different filters; blending them would serve both worse.
- Ids: **`hf:docs/<root>/<path>`** (`hf:docs/transformers/quicktour`) and
  **`hf:blog/<slug>`** — namespaced and stable, matching `docs:<slug>/<path>`. The
  **version is deliberately not in the id**: a version bump must upsert the same doc, not
  fork a new one.
- Canonical URL: from `rel="canonical"` (unversioned). Text: the `.md` body / extracted
  blog HTML. Title: from `llms.txt`'s link text (already human-written) or the blog `<title>`.
- Payload/filters: `root` (`transformers`, `agents-course`…), `kind` (`docs`|`learn`|`blog`),
  `version` (recorded, not in the id), `published_at` (blog), `last_modified` (sitemap/ETag).

## Licensing

- **Docs content mirrors the OSS library repos** — `transformers`, `diffusers`, `peft`,
  `trl` etc. are Apache-2.0, and the doc sources live in those repos. Courses vary
  (Apache-2.0/MIT typical). **Per-root licenses differ, so record one per root** exactly as
  the DevDocs source stores `attribution`; don't assume one blanket license.
- **Blog posts are authored by HF and by community/org accounts with no blanket license.**
  829 posts, mixed authorship.
- Standard windex posture covers all of it and is unchanged: **store text for snippets +
  embeddings, surface a snippet, always link to the canonical URL, never republish bodies.**
  `llms.txt` is a published invitation for machine consumption; it is not a copyright grant,
  so it changes the politeness calculus, not the licensing one.

## Rejected / alternatives

| Option | Why not |
|---|---|
| Crawl model/dataset pages from `sitemap-models/-datasets` | Rolling windows (0.2% of models; 8 days of datasets). Cannot enumerate; would index a random recent slice. |
| BFS link-crawl the doc trees | Sidebar toctree is client-rendered — only 21 links per page, 17 of them JS chunks. `llms.txt` is the enumeration. |
| Index Spaces | 220 chars extracted; client-rendered apps. Nothing to index. |
| Index `/papers/*` | The id *is* the arXiv id; **windex already indexes arXiv**. Self-duplication. |
| Reuse `smallweb` config as-is | `concurrency=12` inert on one host; `host_interval=10.0` → 11h; `PageFetcher` drops `text/markdown`. |
| Headless browser | Unnecessary — everything in scope is server-rendered, and docs are markdown. |
| DevDocs (existing source) covering HF | No overlap: DevDocs' seed set is python/js/rust/go/react. `transformers`/`diffusers`/`peft`/`trl` are **additive**. |
| Hub metadata dump as the product | Different question (appendix). Retained only as the model-page freshness answer. |

## Sketch

```
src/windex/hf/
  __init__.py      # USER_AGENT (shared honest UA)
  sync.py          # sitemap.xml → sitemap-doc/-blog → hf_roots + hf_blog watermarks
  crawl.py         # per root: llms.txt → hash-gate → .md fetch; blog: HTML + trafilatura.
                   # RobotsCache reused; HF-specific 3s interval driven off `ratelimit: r=`
  embed_index.py   # two SourceSpecs (hfdocs, hfblog)
```

```sql
CREATE TABLE IF NOT EXISTS hf_roots (          -- mirrors `docsets` exactly
    root          text PRIMARY KEY,            -- docs/transformers | learn/agents-course
    kind          text NOT NULL,               -- docs | learn
    llms_hash     text,                        -- freshness watermark (llms.txt sha1)
    ingested_hash text,                        -- hash last fully ingested
    pages         integer,
    version       text,                        -- observed vX.Y.Z (recorded, not in doc id)
    license       text,                        -- per-root upstream license/attribution
    status        text NOT NULL DEFAULT 'pending',
    processed_at  timestamptz
);
```

```
uv run windex hf sync     # sitemap → 52 roots + blog cursor
uv run windex hf crawl    # llms.txt hash-gated .md pull + blog delta (~3.3h cold, ~minutes warm)
uv run windex hf embed    # embed staged pages into hfdocs / hfblog
```

Which roots to index is config (`WINDEX_HF_ROOTS`), like `WINDEX_DOCS_SLUGS` — all 52 is
only 3,175 pages, so the default can simply be "all".

## Honest cost/benefit

**For:** robots.txt is permissive and there is no scraping to do — HF publishes `llms.txt`
and serves markdown, so the extraction problem that makes crawling expensive **does not
exist here**. ~4,000 pages is the smallest corpus of any windex source; canonical URLs are
declared by the pages themselves; refresh is 52 hashed requests. The content is the
canonical documentation for the ML stack (transformers, diffusers, peft, trl, timm,
smolagents) plus 14 courses, none of it in DevDocs — a real gap in what agents ask for.

**Against:** the `pages` bucket is genuinely tight (1 req/3s) — 3.3h cold, and a naive
re-sweep would cost 3.3h *every* night, so the llms.txt hash gate is load-bearing, not an
optimization. Five roots lack `llms.txt`. `.md` is undocumented as a contract and could
change (mitigation: HTML + trafilatura already works on the same pages — verified — so a
`.md` regression degrades to the DevDocs path rather than breaking the source). The blog's
829 posts are mixed-authorship marketing-to-technical and will vary in value.

**Verdict: build the docs/courses/blog crawl; leave the other 3.9M pages alone.** The site
is ~4,000 pages worth indexing and several million that are not, and the difference is
measurable rather than a matter of taste.

---

## Appendix — Hub metadata (from the earlier mis-scoped pass)

Kept because it answers §6 and justifies excluding model pages. Not the recommendation.

- **`cfahlgren1/hub-stats`** (Apache-2.0, updated daily): `models.parquet` **2,914,786 rows**
  / 1,307MB, `datasets.parquet` **958,541 rows** / 376MB. Complete Hub metadata — the
  enumeration the sitemap lacks. Column-projected ranged reads: **173MB** (models) / ~109MB
  (datasets); `siblings` 468MB + `config` 302MB dominate the file and aren't needed.
- Models carry **no prose** (only `cardData` frontmatter); datasets ship a ~600-char
  `description`. Card text needs per-repo `/raw/main/README.md` hydration — **7.5 req/s
  measured** on the loose `resolvers` bucket.
- **The Hub is mostly dead**: median model has **0 downloads and 0 likes**; **52.66% are
  zero-download *and* zero-like**; only 33.77% have a `pipeline_tag`. Any use of this data
  needs a filter — `likes>=5 OR downloads_30d>=1000` keeps **58,263 models (2.00%)** and
  **28,076 datasets (2.93%)**. Likes are far rarer on HF than GitHub stars (7.77% of models
  have one) so a likes-only port of the ≥10-stars rule would drop the used-but-unloved tail.
- `librarian-bots/model_cards_with_metadata` (667,424 rows with full card text, daily) looks
  like the shortcut but **covers only 45.9% of those survivors** — missing `01-ai/Yi-1.5-9B-Chat`
  and most of `01-ai/*` — and carries **no license tag**. Not a source of truth.
- Gated repos (e.g. `meta-llama/Llama-3.1-8B-Instruct`): card `/raw/` → **401**, metadata
  stays public.
- Single-maintainer risk on the dump, same as the open-index HN mirror.
