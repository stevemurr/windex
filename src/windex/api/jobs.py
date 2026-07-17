"""Dashboard job manager: launch and stop the windex CLI jobs from the API.

Strictly whitelisted — fixed argv templates plus typed, bounded parameters.
The API listens on the LAN; nothing here may ever compose arbitrary commands.
Jobs are detached processes (they survive API restarts); running state is
derived from the process table, logs live under ~/.windex/logs/<job>.log.
"""

import os
import signal
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

LOG_DIR = Path.home() / ".windex" / "logs"
VENV_BIN = Path(sys.executable).parent
PROJECT_ROOT = VENV_BIN.parent.parent


@dataclass(frozen=True)
class Param:
    flag: str
    kind: str  # int | date | choice
    lo: int = 0
    hi: int = 0
    choices: tuple[str, ...] = ()
    default: object = None


@dataclass(frozen=True)
class Job:
    name: str
    argv: tuple[str, ...]           # after the windex binary
    pattern: str                    # pgrep -f pattern (must be unambiguous)
    title: str
    description: str
    category: str
    params: dict[str, Param] = field(default_factory=dict)
    confirm: bool = False           # UI asks before starting


JOBS: dict[str, Job] = {j.name: j for j in [
    Job("ccnews-sync", ("ccnews", "sync"), "windex ccnews sync",
        "Find new shards", "Check Common Crawl for new WARC files in the window",
        "news", {"days": Param("--days", "int", 1, 365, default=90)}),
    Job("ccnews-run", ("ccnews", "run", "--no-embed"), "windex ccnews run",
        "Process WARCs", "Download → extract → filter → dedup pending shards",
        "news", {"batch_size": Param("--batch-size", "int", 1, 64, default=16),
                 "max_batches": Param("--max-batches", "int", 1, 500, default=23)}),
    Job("embed-loop", ("embed-loop", "ccnews"), "windex embed-loop ccnews",
        "Embed loop", "Continuously embed the news backlog into the index",
        "news"),
    Job("ccnews-retry-failed", ("ccnews", "retry-failed"), "windex ccnews retry-failed",
        "Retry failed shards", "Requeue WARC files that failed processing",
        "news"),
    Job("gh-scan", ("gh", "scan"), "windex gh scan",
        "Scan event hours", "Stream GH Archive hours, counting star events",
        "github", {"max_files": Param("--max-files", "int", 1, 20000, default=48)}),
    Job("gh-discover", ("gh", "discover"), "windex gh discover",
        "Discover repos", "Search API sweep for new 10★+ repositories",
        "github", {"created_from": Param("--created-from", "date", default="2025-10-01")}),
    Job("gh-hydrate", ("gh", "hydrate", "--min-star-events", "0"), "windex gh hydrate",
        "Hydrate repos", "Fetch metadata + READMEs for candidate repositories",
        "github", {"limit": Param("--limit", "int", 1, 500000, default=100000)}),
    Job("gh-embed", ("embed-loop", "gh"), "windex embed-loop gh",
        "Embed repos", "Continuously embed hydrated repositories into the index",
        "github"),
    Job("wiki-sync", ("wiki", "sync"), "windex wiki sync",
        "Find latest dump", "Discover the newest complete Wikipedia CirrusSearch snapshot",
        "wiki"),
    Job("wiki-ingest", ("wiki", "ingest"), "windex wiki ingest",
        "Ingest articles", "Stream pending Wikipedia shards → clean parquet + ledger",
        "wiki", {"max_files": Param("--max-files", "int", 1, 64, default=64)}),
    Job("wiki-embed", ("embed-loop", "wiki"), "windex embed-loop wiki",
        "Embed articles", "Continuously embed staged Wikipedia articles into the index",
        "wiki"),
    Job("arxiv-harvest", ("arxiv", "harvest"), "windex arxiv harvest --days",
        "Harvest recent", "Incremental OAI-PMH harvest of the last N days of arXiv metadata",
        "arxiv", {"days": Param("--days", "int", 1, 3650, default=7)}),
    Job("arxiv-backfill", ("arxiv", "harvest"), "windex arxiv harvest --from-year",
        "Backfill corpus", "Plan + harvest per-year windows (whole corpus from --from-year)",
        "arxiv", {"from_year": Param("--from-year", "int", 2005, 2100, default=2005),
                  "to_year": Param("--to-year", "int", 2005, 2100, default=2026)},
        confirm=True),
    Job("arxiv-embed", ("embed-loop", "arxiv"), "windex embed-loop arxiv",
        "Embed papers", "Embed staged arXiv papers into the index",
        "arxiv"),
    Job("smallweb-sync", ("smallweb", "sync"), "windex smallweb sync",
        "Sync feed list", "Reconcile the feeds table against Kagi's smallweb.txt",
        "smallweb"),
    Job("smallweb-poll", ("smallweb", "poll"), "windex smallweb poll",
        "Poll feeds", "Conditional-GET active feeds, fetch + stage new posts (polite)",
        "smallweb", {"max_feeds": Param("--max-feeds", "int", 1, 100000, default=1000)}),
    Job("smallweb-embed", ("embed-loop", "smallweb"), "windex embed-loop smallweb",
        "Embed posts", "Embed staged Small Web posts into the index",
        "smallweb"),
    Job("docs-sync", ("docs", "sync"), "windex docs sync",
        "Sync manifest", "Fetch the DevDocs manifest and update the docsets watermark",
        "docs"),
    Job("docs-ingest", ("docs", "ingest"), "windex docs ingest",
        "Ingest docsets", "Fetch pending docsets → clean parquet + ledger (changed-page delta)",
        "docs", {"max_docsets": Param("--max-docsets", "int", 1, 819, default=25)}),
    Job("docs-embed", ("embed-loop", "docs"), "windex embed-loop docs",
        "Embed pages", "Embed staged documentation pages into the index",
        "docs"),
    Job("hn-harvest", ("hn", "harvest"), "windex hn harvest --days",
        "Harvest recent", "Trailing-window Algolia re-pull: new stories + points refresh",
        "hn", {"days": Param("--days", "int", 1, 365, default=2)}),
    Job("hn-backfill", ("hn", "backfill"), "windex hn backfill",
        "Backfill corpus", "Plan + drain per-month windows from the open-index parquet mirror",
        "hn", {"from_year": Param("--from-year", "int", 2006, 2100, default=2006),
               "to_year": Param("--to-year", "int", 2006, 2100, default=2026)},
        confirm=True),
    Job("hn-embed", ("embed-loop", "hn"), "windex embed-loop hn",
        "Embed stories", "Embed staged Hacker News stories into the index",
        "hn"),
    Job("hf-sync", ("hf", "sync"), "windex hf sync",
        "Sync roots + blog", "Sitemap → doc roots + blog posts, then re-hash every llms.txt",
        "hf"),
    Job("hf-crawl", ("hf", "crawl"), "windex hf crawl",
        "Crawl pages", "Pull .md for changed doc roots + new blog posts (polite: 1 req/3s)",
        "hf", {"max_roots": Param("--max-roots", "int", 1, 52, default=52),
               "max_posts": Param("--max-posts", "int", 1, 2000, default=829)}),
    Job("hf-embed", ("embed-loop", "hf"), "windex embed-loop hf",
        "Embed pages", "Embed staged Hugging Face docs/courses/blog into the index",
        "hf"),
    Job("daily", ("daily",), "windex daily",
        "Daily job", "The full freshness cycle (news + github), idempotent",
        "maintenance"),
    Job("reindex", ("reindex",), "windex reindex",
        "Rebuild index", "Drop vectors and re-embed everything from staged text",
        "maintenance",
        {"source": Param("", "choice",
                         choices=("news", "repos", "wiki", "arxiv", "smallweb", "docs",
                                  "hn", "hf", "all"),
                         default="all")},
        confirm=True),
]}


def build_argv(job: Job, params: dict) -> list[str]:
    argv = [str(VENV_BIN / "windex"), *job.argv]
    for key, spec in job.params.items():
        value = params.get(key, spec.default)
        if value is None:
            continue
        if spec.kind == "int":
            value = int(value)
            if not (spec.lo <= value <= spec.hi):
                raise ValueError(f"{key} out of range [{spec.lo}, {spec.hi}]")
            argv += [spec.flag, str(value)]
        elif spec.kind == "date":
            value = date.fromisoformat(str(value)).isoformat()
            argv += [spec.flag, value]
        elif spec.kind == "choice":
            if value not in spec.choices:
                raise ValueError(f"{key} must be one of {spec.choices}")
            argv += ([value] if not spec.flag else [spec.flag, str(value)])
    if job.name == "reindex":
        argv.append("--yes")
    unknown = set(params) - set(job.params)
    if unknown:
        raise ValueError(f"unknown params: {sorted(unknown)}")
    return argv


def _pids(pattern: str) -> list[int]:
    out = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
    return [int(p) for p in out.stdout.split()] if out.returncode == 0 else []


def _log_tail(name: str, nbytes: int = 400) -> str:
    log = LOG_DIR / f"{name}.log"
    if not log.exists():
        return ""
    with open(log, "rb") as f:
        f.seek(max(f.seek(0, 2) - nbytes, 0))
        lines = f.read().decode(errors="replace").strip().splitlines()
    return lines[-1] if lines else ""


def list_jobs() -> list[dict]:
    out = []
    for job in JOBS.values():
        pids = _pids(job.pattern)
        out.append({
            "name": job.name, "title": job.title, "description": job.description,
            "category": job.category, "running": bool(pids), "pids": pids,
            "confirm": job.confirm,
            "params": {k: {"kind": p.kind, "default": p.default,
                           "choices": list(p.choices)} for k, p in job.params.items()},
            "last_log": _log_tail(job.name),
        })
    return out


def start(name: str, params: dict) -> dict:
    job = JOBS.get(name)
    if job is None:
        raise KeyError(name)
    if _pids(job.pattern):
        raise RuntimeError(f"{name} is already running")
    argv = build_argv(job, params or {})
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{job.name}.log"
    # rotate-on-start guard: detached children write raw stdout no logging
    # handler can cap, so bound growth here (newsyslog covers the interval)
    if log_path.exists() and log_path.stat().st_size > 10_485_760:
        log_path.replace(log_path.with_suffix(".log.1"))
    log = open(log_path, "ab")
    proc = subprocess.Popen(
        argv, stdout=log, stderr=log, cwd=PROJECT_ROOT, start_new_session=True
    )
    return {"started": name, "pid": proc.pid}


def stop(name: str) -> dict:
    job = JOBS.get(name)
    if job is None:
        raise KeyError(name)
    pids = _pids(job.pattern)
    for pid in pids:
        try:
            # Only nuke the process group when this pid LEADS it. start() uses
            # start_new_session=True, so a job we launched owns its group and
            # killpg cleanly takes its children too. But a job started any other
            # way (shell loop, script, cron) can share its parent's group with
            # unrelated siblings — killpg there stops every other embed loop as
            # collateral. Reported 2026-07-17: "stopping one embed job stopped
            # them all". _pids already matches this job's own processes, so
            # killing them directly is the correct fallback.
            pgid = os.getpgid(pid)
            if pgid == pid:
                os.killpg(pgid, signal.SIGTERM)
            else:
                os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    return {"stopped": name, "pids": pids}
