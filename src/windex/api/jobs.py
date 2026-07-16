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
    Job("embed-loop", ("ccnews", "embed-loop"), "windex ccnews embed-loop",
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
    Job("gh-embed", ("gh", "embed"), "windex gh embed",
        "Embed repos", "Embed hydrated repositories into the index",
        "github", {"limit": Param("--limit", "int", 1, 500000, default=100000)}),
    Job("wiki-sync", ("wiki", "sync"), "windex wiki sync",
        "Find latest dump", "Discover the newest complete Wikipedia CirrusSearch snapshot",
        "wiki"),
    Job("wiki-ingest", ("wiki", "ingest"), "windex wiki ingest",
        "Ingest articles", "Stream pending Wikipedia shards → clean parquet + ledger",
        "wiki", {"max_files": Param("--max-files", "int", 1, 64, default=64)}),
    Job("wiki-embed", ("wiki", "embed"), "windex wiki embed",
        "Embed articles", "Embed staged Wikipedia articles into the index",
        "wiki", {"limit": Param("--limit", "int", 1, 10000000, default=100000)}),
    Job("daily", ("daily",), "windex daily",
        "Daily job", "The full freshness cycle (news + github), idempotent",
        "maintenance"),
    Job("reindex", ("reindex",), "windex reindex",
        "Rebuild index", "Drop vectors and re-embed everything from staged text",
        "maintenance",
        {"source": Param("", "choice", choices=("news", "repos", "wiki", "all"), default="all")},
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
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    return {"stopped": name, "pids": pids}
