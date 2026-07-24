"""The crawl driver: BFS over a persisted frontier into a custom source.

Shape of one run:

    seed → crawl_urls(pending) ─┐
                                ├─ fetch → extract text + links
                                │      ├─ links: scope-filter → enqueue (depth+1)
                                │      └─ text : boilerplate guard → batch
                                └─ batch full → upsert_docs → mark rows staged

Two properties are load-bearing:

**Resumable.** The frontier lives in ``crawl_urls``, not in memory, so a killed
worker resumes where it stopped instead of re-fetching from the seed. Rows move
pending → staged/skipped/failed only after the batch containing them is durably
staged, so a crash re-does at most one batch — and re-doing it is a ``text_hash``
no-op in the ledger, exactly like every other source's retry.

**Bounded.** ``max_pages`` and ``max_depth`` are hard stops. Hitting the page
budget sets ``truncated`` in the run stats rather than reporting a clean finish,
because a crawl that silently stopped early is indistinguishable from a small
cluster — and the difference matters when you are deciding whether search results
are missing something.

BOILERPLATE. Some sites answer HTTP 200 with a shell page for *every* unknown
path (platform.claude.com does; see docs). Such a crawl would otherwise stage
dozens of distinct doc ids carrying identical text, and ``upsert_docs``' text_hash
guard would not catch it — that guard is per-id. Two defences here: any page whose
extracted text matches a SEED's text is dropped immediately, and any text seen
``crawl_boilerplate_repeat`` times in one run is dropped from then on.
"""

from __future__ import annotations

import time
from datetime import datetime
from urllib.parse import urljoin

import psycopg
from psycopg.types.json import Jsonb
from rich.console import Console

from windex import db
from windex.ccnews.dedup import text_hash
from windex.config import Settings
from windex.crawl import fetch as cfetch
from windex.crawl.links import extract_links
from windex.crawl.recipe import Recipe
from windex.crawl.scope import canonicalize, in_scope
from windex.custom_source.ingest import apply_tombstones

console = Console()

# Terminal states a frontier row can reach. 'pending' is the only non-terminal.
STAGED, SKIPPED, FAILED = "staged", "skipped", "failed"


def doc_suffix(url: str, seed: str) -> str:
    """The ``<source>:<suffix>`` doc-id tail for a crawled page.

    The URL's path+query with the leading slash dropped — human-readable, stable
    across re-crawls, and unique within a host. Bounded to 200 chars to satisfy
    ``custom_source.ingest.SUFFIX_RE``; over-long URLs fall back to a hash tail so
    two deep URLs cannot collide after truncation.
    """
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    tail = parts.path.lstrip("/") + (f"?{parts.query}" if parts.query else "")
    tail = tail or "index"
    if len(tail) > 200:
        tail = f"{tail[:160]}~{text_hash(url)[:16]}"
    return tail


# --- frontier ---------------------------------------------------------------

def enqueue(cur: psycopg.Cursor, run_id: int, urls: list[tuple[str, int]]) -> int:
    """Add (url, depth) pairs to the frontier. ON CONFLICT DO NOTHING makes a
    re-discovered URL free — the primary key is the dedup, so the driver never
    needs an in-memory 'seen' set that a restart would lose."""
    if not urls:
        return 0
    cur.executemany(
        "INSERT INTO crawl_urls (run_id, url, depth) VALUES (%s, %s, %s) "
        "ON CONFLICT (run_id, url) DO NOTHING",
        [(run_id, u, d) for u, d in sorted(set(urls))],
    )
    return cur.rowcount or 0


def claim_batch(conn: psycopg.Connection, run_id: int, limit: int) -> list[tuple[str, int]]:
    """Next pending URLs, shallowest first so the traversal is breadth-first.

    Not FOR UPDATE SKIP LOCKED: one run is processed by exactly one worker (the
    run itself is what workers contend for), so rows within a run have no second
    claimant to lose a race against.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT url, depth FROM crawl_urls WHERE run_id = %s AND status = 'pending' "
            "ORDER BY depth, seq LIMIT %s",
            (run_id, limit),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def mark(cur: psycopg.Cursor, run_id: int, rows: list) -> None:
    """Set terminal status+reason for frontier rows.

    Rows are ``(url, status, reason)`` or ``(url, status, reason, doc_id)`` — the
    doc_id is recorded for pages that produced a document so ``prune_missing``
    can tell what this run covered even across a resume.
    """
    if not rows:
        return
    cur.executemany(
        "UPDATE crawl_urls SET status = %s, reason = %s, doc_id = %s "
        "WHERE run_id = %s AND url = %s",
        [(r[1], r[2] or None, r[3] if len(r) > 3 else None, run_id, r[0]) for r in rows],
    )


def prune_missing(conn: psycopg.Connection, settings: Settings, run_id: int,
                  source: str, stats: dict) -> int:
    """Tombstone live docs in ``source`` that this run did not produce.

    This is the one operation in the crawler that DELETES content, so it refuses
    to act unless the run is a trustworthy census of the cluster. A crawl that
    stopped early saw a *subset*, and pruning against a subset would delete
    everything it merely failed to reach — turning a transient network blip or a
    max_pages cap into silent corpus loss. The guard is therefore:

      * ``truncated`` — hit the page budget with URLs still queued ⇒ incomplete.
      * ``failed > 0`` — some page did not come back ⇒ we cannot prove it is gone.
      * no docs produced at all ⇒ almost certainly a broken run (bad scope, dead
        host), and pruning would wipe the whole source on a typo.

    Returns the number tombstoned (0 when the guard blocks it, with the reason
    recorded in ``stats['prune_skipped']`` so the UI can say why nothing happened).
    """
    if stats.get("truncated"):
        stats["prune_skipped"] = "truncated"
        return 0
    if stats.get("failed"):
        stats["prune_skipped"] = "failed_pages"
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM documents WHERE source = %s AND status <> 'deleted' "
            "AND id NOT IN (SELECT doc_id FROM crawl_urls "
            "               WHERE run_id = %s AND doc_id IS NOT NULL)",
            (source, run_id),
        )
        orphans = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT count(*) FROM crawl_urls WHERE run_id = %s AND doc_id IS NOT NULL",
                    (run_id,))
        covered = cur.fetchone()[0]
    if not covered:
        stats["prune_skipped"] = "no_pages"
        return 0
    return apply_tombstones(conn, settings, source, orphans)


# --- extraction -------------------------------------------------------------

# Re-exported so `run.extract_page` stays the driver's single extraction entry
# point; the implementation (trafilatura + the structural fallback for the pages
# it declines) lives in crawl.extract.
from windex.crawl.extract import declared_canonical, extract_page  # noqa: E402


def _published(value: str) -> datetime | None:
    from windex.dateparse import parse_and_clamp

    return parse_and_clamp(value) if value else None


# --- the driver -------------------------------------------------------------

def execute(conn: psycopg.Connection, settings: Settings, run_id: int, source: str,
            recipe: Recipe, *, fetcher=None, client=None,
            should_stop=None, on_progress=None, pause_poll_seconds: float = 5.0) -> dict:
    """Run one crawl to completion (or to its budget/cancel). Returns stats.

    ``should_stop`` is polled between batches for cancellation; ``on_progress`` is
    called with the running stats so the worker can heartbeat and the SSE stream
    has something to report.
    """
    from windex.custom_source.ingest import upsert_docs

    stats = {"found": 0, "fetched": 0, "staged": 0, "skipped": 0, "failed": 0,
             "boilerplate": 0, "truncated": False}
    own_client = client is None
    client = client or cfetch.build_client(recipe)
    fetcher = fetcher or cfetch.build_fetcher(client, settings, recipe)
    seed = recipe.seeds[0]

    # Text fingerprints that mean "this is chrome, not a document".
    seed_hashes: set[str] = set()
    hash_counts: dict[str, int] = {}
    repeat_cap = max(2, settings.crawl_boilerplate_repeat)

    try:
        with conn.cursor() as cur:
            stats["found"] += enqueue(cur, run_id, [(s, 0) for s in recipe.seeds])
        conn.commit()

        while True:
            if should_stop is not None and should_stop():
                stats["cancelled"] = True
                break
            # Pause is honoured BETWEEN batches, never mid-batch: a batch stages in
            # one transaction and there is nothing half-applied to wait on.
            while db.get_control(conn, "indexing", "running") == "paused":
                if should_stop is not None and should_stop():
                    stats["cancelled"] = True
                    return stats
                time.sleep(pause_poll_seconds)

            remaining = recipe.limits.max_pages - stats["fetched"]
            if remaining <= 0:
                # Budget exhausted with work still queued ⇒ genuinely truncated.
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM crawl_urls WHERE run_id = %s AND "
                                "status = 'pending' LIMIT 1", (run_id,))
                    stats["truncated"] = cur.fetchone() is not None
                break
            batch = claim_batch(conn, run_id, min(settings.crawl_batch, remaining))
            if not batch:
                break

            docs: list[dict] = []
            marks: list[tuple[str, str, str]] = []
            discovered: list[tuple[str, int]] = []

            for url, depth in batch:
                body, final_url, reason = fetcher.fetch(url)
                if body is None:
                    # A blocked/robots/http failure is terminal for this URL but
                    # not for the run: one bad page must not sink a cluster.
                    is_skip = reason in ("robots", "scope", "private_ip", "scheme",
                                         "no_host", "dns")
                    marks.append((url, SKIPPED if is_skip else FAILED, reason))
                    stats["skipped" if is_skip else "failed"] += 1
                    continue
                stats["fetched"] += 1

                if depth < recipe.limits.max_depth:
                    for link in extract_links(body, final_url):
                        ok, _why = in_scope(link, recipe, seed)
                        if ok:
                            discovered.append((link, depth + 1))

                page = extract_page(body, final_url, recipe)
                if page is None:
                    marks.append((url, SKIPPED, "no_text"))
                    stats["skipped"] += 1
                    continue

                thash = text_hash(page["text"])
                if depth == 0:
                    seed_hashes.add(thash)
                elif recipe.dedup.drop_boilerplate:
                    hash_counts[thash] = hash_counts.get(thash, 0) + 1
                    if thash in seed_hashes or hash_counts[thash] >= repeat_cap:
                        # The soft-404 shell, or any other page-shaped chrome.
                        marks.append((url, SKIPPED, "boilerplate"))
                        stats["skipped"] += 1
                        stats["boilerplate"] += 1
                        continue

                # Index the URL the page claims as its own, when it declares one
                # and that claim stays inside the cluster. Guarded on scope: a
                # canonical pointing off-host (or outside the crawl) must not
                # silently change what we index — we only honour a page
                # renaming itself WITHIN the cluster we were asked to crawl.
                doc_url = final_url
                declared = declared_canonical(body)
                if declared:
                    canon = canonicalize(urljoin(final_url, declared))
                    if in_scope(canon, recipe, seed)[0] or canon in recipe.seeds:
                        doc_url = canon

                docs.append({
                    "id": doc_suffix(doc_url, seed),
                    "url": doc_url,
                    "title": page["title"],
                    "text": page["text"],
                    "published_at": _published(page["published_at"]),
                    "extra": {"crawl_run": run_id, "depth": depth, "seed": seed},
                    "_frontier_url": url,
                })

            if docs:
                result = upsert_docs(conn, settings, source,
                                     [{k: v for k, v in d.items()
                                       if not k.startswith("_")} for d in docs])
                stats["staged"] += result["staged"]
                # Carry the documents id onto the frontier row: `prune` needs to
                # know which docs THIS run accounts for, including work done
                # before a restart.
                marks.extend((d["_frontier_url"], STAGED, "", f"{source}:{d['id']}")
                             for d in docs)

            with conn.cursor() as cur:
                mark(cur, run_id, marks)
                stats["found"] += enqueue(cur, run_id, discovered)
            conn.commit()

            if on_progress is not None:
                on_progress(stats)

        # Prune AFTER the loop and only on a clean exit. Reaching here means the
        # frontier drained (or the budget/cancel path already recorded why), so
        # `stats` is the census prune_missing gates on. A cancelled run leaves
        # via should_stop and is NOT a census — the `cancelled` flag below is what
        # keeps it from pruning against a half-crawled cluster.
        if recipe.dedup.prune and not stats.get("cancelled"):
            stats["pruned"] = prune_missing(conn, settings, run_id, source, stats)
    finally:
        if own_client:
            client.close()
    return stats


# --- run lifecycle (used by the worker and the API) -------------------------

def create_run(conn: psycopg.Connection, source: str, recipe: Recipe) -> int:
    """Queue a crawl. The recipe is frozen into the row so a later edit of the
    source's recipe never rewrites what this run did."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO crawl_runs (source, recipe) VALUES (%s, %s) RETURNING id",
            (source, Jsonb(recipe.to_dict())),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def claim_run(conn: psycopg.Connection) -> dict | None:
    """Claim the oldest pending run. FOR UPDATE SKIP LOCKED so two workers (or a
    worker and a restarted worker) never take the same run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE crawl_runs SET status = 'running', started_at = now(),
                   heartbeat_at = now()
            WHERE id = (SELECT id FROM crawl_runs WHERE status = 'pending'
                        ORDER BY requested_at FOR UPDATE SKIP LOCKED LIMIT 1)
            RETURNING id, source, recipe
            """
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return {"id": row[0], "source": row[1], "recipe": row[2]}


def heartbeat(conn: psycopg.Connection, run_id: int, stats: dict) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE crawl_runs SET heartbeat_at = now(), stats = %s WHERE id = %s",
                    (Jsonb(stats), run_id))
    conn.commit()


def finish(conn: psycopg.Connection, run_id: int, status: str, stats: dict,
           error: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE crawl_runs SET status = %s, finished_at = now(), stats = %s, "
            "error = %s WHERE id = %s",
            (status, Jsonb(stats), error, run_id),
        )
    conn.commit()


def is_cancelled(conn: psycopg.Connection, run_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM crawl_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
    return bool(row) and row[0] == "cancelled"


def reclaim_stale(conn: psycopg.Connection, settings: Settings) -> int:
    """Return runs whose worker died back to 'pending'.

    Mirrors the reclaim functions the other sources have. Safe because the
    frontier is persisted: a reclaimed run resumes from its remaining pending
    rows rather than restarting, and re-staging an already-staged page is a
    text_hash no-op.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE crawl_runs SET status = 'pending' WHERE status = 'running' "
            "AND (heartbeat_at IS NULL OR heartbeat_at < now() - make_interval(mins => %s))",
            (settings.crawl_stale_minutes,),
        )
        n = cur.rowcount or 0
    conn.commit()
    return n
