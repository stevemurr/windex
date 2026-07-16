# arXiv data source decision (researched 2026-07-16)

All facts verified against the live OAI-PMH endpoint on 2026-07-16, not memory. A
`verb=Identify` smoke test re-confirms the endpoint before any harvest.

## Decision: OAI-PMH metadata harvest (primary)

`https://oaipmh.arxiv.org/oai` — **not** the old `export.arxiv.org/oai2` host
(tutorials describing that host are stale). `metadataPrefix=arXiv`.

- **Endpoint / policy** (from `Identify`): `earliestDatestamp` 2005-09-16,
  `granularity` YYYY-MM-DD (day-level), `deletedRecord=persistent`. Metadata is
  CC0 and harvesting is permitted; **full-content harvesting is not** — windex
  harvests metadata only (title + abstract), never PDFs/source.
- **Rate limit** (arXiv Terms of Use): 1 request / 3 seconds, a single
  connection, and a descriptive `User-Agent` with a contact URL (the shared
  windex UA). Enforced in `harvest.py` (`arxiv_request_interval = 3.0`).
- **Record shape** (`metadataPrefix=arXiv`): `id`, `created`, `updated`,
  `authors` (each `keyname` + `forenames` [+ `suffix`]), `title`, `categories`
  (space-separated, **first = primary**), `abstract`, and optional `doi`,
  `journal-ref`, `license`, `comments`. The OAI header `identifier` is
  `oai:arXiv.org:<paper_id>`; old-style ids keep their slash
  (`oai:arXiv.org:hep-th/9901001`).
- **datestamp = metadata modification date, not submission date.** A harvest from
  `earliestDatestamp` (2005-09-16) therefore returns the **complete** corpus
  (~3.1M records) because pre-2005 papers carry their true `created` inside the
  record. Verified live: a one-day window (`from=until=2024-01-02`) returned 752
  records whose `created` dates span 2010→2024.
- **Pagination**: `ListRecords` returns ~1,300 records/page; the
  `resumptionToken` is skip-offset style (`&skip=N`). Tokens **expire at the next
  00:00 UTC** (observed `expirationDate` = next midnight Z), so a single token
  chain cannot safely span a day boundary.

## Why per-year date windows (the `arxiv_windows` watermark)

Because a token chain expires at 00:00 UTC, the backfill is chunked into
**independently restartable per-year windows** (`from=YYYY-01-01`,
`until=YYYY-12-31`) rather than one 2–3h chain. A window is only marked `done`
once its token chain completes; an interrupted window is re-harvested from its
start (safe — the `documents.text_hash` ledger over title+abstract keeps
re-harvests to the changed-paper delta, exactly like the news/wiki ledgers). The
rolling incremental window (`--days N`) is re-armed on each freshness run so
metadata updates are picked up.

Full backfill: ~3.1M records ≈ 2,389 requests ≈ 2–3h at the 3s rate. Run it as
`windex arxiv harvest --from-year 2005`; the per-year windows make it resumable.

## Tombstones (`deletedRecord=persistent`)

Harvests can return `<header status="deleted">` tombstones (no metadata body).
These mark the ledger row `status='deleted'` and drop the corresponding Qdrant
point (best-effort — a down index leaves the ledger tombstoned, and the point is
dropped on the next reindex).

## Rejected / alternatives

| Source | Why not |
|---|---|
| `export.arxiv.org/oai2` | **Stale host** in older tutorials; the live endpoint is `oaipmh.arxiv.org/oai` |
| `metadataPrefix=oai_dc` | Dublin Core loses the structured arXiv fields (primary category, per-author names, doi) |
| `metadataPrefix=arXivRaw` / `arXivOld` | Raw submission history / legacy schema — more fields than needed, no plain abstract convenience |
| arXiv bulk PDF/source on S3 (requester-pays) | Full text — not permitted for harvest, and outside the metadata-only scope |
| Kaggle `arXiv` JSON snapshot | A periodic dump, not a freshness feed; OAI-PMH gives day-level incremental + tombstones |

## Operational caveat

`deletedRecord=persistent` means tombstones are retained, so a re-harvest of any
window reconciles deletions. There is no upstream retention limit on the OAI feed
(unlike the Wikipedia dumps' 4-week window), so a long outage is recovered by
re-harvesting the affected per-year windows.
