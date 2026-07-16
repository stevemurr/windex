"""Batch driver: the daily job and the backfill are this same loop, differing
only in window depth and batch count. Each batch is download → extract/filter →
dedup → mark done → (optionally) delete WARCs. Idempotent: every step keys off
state tables, so a crashed run resumes where it left off."""

import hashlib
import shutil
import time
from pathlib import Path

import psycopg
from rich.console import Console

from windex import db
from windex.ccnews import dedup as dd
from windex.ccnews import download, pipeline, sync
from windex.config import Settings

console = Console()


def batch_id_for(paths: list[str]) -> str:
    digest = hashlib.sha1("\n".join(paths).encode()).hexdigest()[:8]
    return f"{sync.path_date(paths[0]):%Y%m%d}-{digest}"


def run_batches(
    conn: psycopg.Connection,
    settings: Settings,
    batch_size: int = 16,
    max_batches: int | None = None,
    keep_warcs: bool = False,
    workers: int = 0,
    max_consecutive_failures: int = 3,
    pause_poll_seconds: float = 10.0,
) -> int:
    """Process pending WARC files in batches. Returns docs staged for embedding.

    A failed batch is marked and skipped so long unattended runs survive one bad
    file; repeated back-to-back failures (systemic problem) still abort."""
    staged = 0
    batches_done = 0
    consecutive_failures = 0
    while max_batches is None or batches_done < max_batches:
        # dashboard pause: wait between batches, never mid-batch
        paused_notice = False
        while db.get_control(conn, "indexing", "running") == "paused":
            if not paused_notice:
                console.print("[yellow]paused via control flag — waiting[/yellow]")
                paused_notice = True
            time.sleep(pause_poll_seconds)
        paths = sync.pending_paths(conn, batch_size)
        if not paths:
            break
        bid = batch_id_for(paths)
        console.print(f"[bold]batch {bid}[/bold]: {len(paths)} WARCs")
        sync.mark(conn, paths, "processing")
        try:
            local = download.download_batch(paths, settings.ccnews_downloads_dir)
            sizes = {p: lp.stat().st_size for p, lp in zip(paths, local)}
            extracted_dir = settings.news_staging_dir / "extracted" / bid
            logging_dir = settings.news_staging_dir / "logs" / bid
            pipeline.process_batch(
                warc_dir=settings.ccnews_downloads_dir,
                local_names=[p.name for p in local],
                out_dir=extracted_dir,
                logging_dir=logging_dir,
                language=settings.news_language,
                workers=workers,
            )
            text_ref = f"news/clean/{bid}.parquet"
            stats = dd.run_dedup(
                conn,
                extracted_dir=extracted_dir,
                clean_path=settings.staging_dir / text_ref,
                text_ref=text_ref,
                day=sync.path_date(paths[0]),
            )
            sync.mark(conn, paths, "done", {"batch_id": bid, **stats}, sizes=sizes)
            console.print(f"  {stats}")
            staged += stats["clean_out"]
            if not keep_warcs:
                for p in local:
                    Path(p).unlink(missing_ok=True)
                shutil.rmtree(extracted_dir, ignore_errors=True)
            consecutive_failures = 0
        except Exception as exc:
            conn.rollback()
            sync.mark(conn, paths, "failed")
            consecutive_failures += 1
            console.print(f"[red]batch {bid} failed[/red] ({exc}); continuing")
            if consecutive_failures >= max_consecutive_failures:
                raise
        batches_done += 1
    return staged
