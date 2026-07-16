# Kagi Small Web source — feasibility (researched 2026-07-16)

Verified live; decision: **build it, sequenced after arXiv** (effort ~3/5 overall).

## The corpus
- `github.com/kagisearch/smallweb` (**MIT**), actively maintained (community PRs daily).
- `smallweb.txt`: **38,467 RSS/Atom feed URLs** (one per line; ~37.6k unique hosts,
  essentially one personal blog per domain). Blog posts claiming "~6,000 sites" are stale.
- Ignore `smallyt.txt` (video) and `smallcomic.txt` (images).
- Curation rules: personal, English, no ads/affiliate/LLM spam, post within 12 months.

## Why it's lighter than "crawl the web"
- Entries are **feed URLs** → ingestion is feed polling, not domain discovery.
- Sampled feeds average ~32 items; many are **full-text feeds** (body inline in
  `content:encoded`) → no page fetch at all for those.
- Bootstrap ≈ 5–6GB of feeds + ~20–60GB of page fetches (summary-only feeds), one-time.
- Freshness: conditional GET (ETag/304) daily — personal blogs post rarely; near-free.
- ~1 in 10 feeds dead/blocked (measured) → liveness pruning with fail counts.

## Reuse vs new
Reused directly: trafilatura extraction, quality filters (**must be re-tuned — FineWeb
filters over-reject short/idiosyncratic blog posts; false rejection is the main quality
risk**), both dedup tiers (feeds re-serve items every poll — the exact-dedup ledger is
essential), embed, staging, serving. New: list sync (trivial), feed poller (feedparser +
conditional GET), and the one real lift — a **polite per-domain HTML fetcher** (robots.txt
honored, descriptive UA, per-host min-interval, backoff). ~37.6k hosts spread load
naturally, so a simple async pool + per-host last-hit map suffices.

The fetcher is a reusable asset: once built, ooh.directory (~2.4k), blogroll.org (~1.1k)
and similar lists are ~effort-1 additions (combined <10% extra coverage).

## Ethics/licensing
List MIT; content is public personal blogs; windex links out (traffic to the small web —
on-mission and friendly). Honor robots + honest User-Agent (a default UA already drew a
403 in sampling). Attribute the Kagi list.
