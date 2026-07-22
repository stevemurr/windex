"""Dashboard job manager: launch and stop the windex CLI jobs from the API.

Strictly whitelisted — fixed argv templates plus typed, bounded parameters.
The API listens on the LAN; nothing here may ever compose arbitrary commands.
Jobs are detached processes (they survive API restarts); running state is
derived from the process table, logs live under ~/.windex/logs/<job>.log.
"""

import contextlib
import fcntl
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
    Job("memory-embed", ("embed-loop", "memory"), "windex embed-loop memory",
        "Embed chat memory", "Embed pushed chat-history chunks", "memory"),
    Job("daily", ("daily",), "windex daily",
        "Daily job", "The full freshness cycle (news + github), idempotent",
        "maintenance"),
    Job("reindex", ("reindex",), "windex reindex",
        "Rebuild index", "Drop vectors and re-embed everything from staged text",
        "maintenance",
        {"source": Param("", "choice",
                         choices=("news", "repos", "wiki", "arxiv", "smallweb", "docs",
                                  "hn", "hf", "memory", "all"),
                         default="all")},
        confirm=True),
]}


# Push-based sources: they have a supervised embed loop (so they appear in
# embed_loop_jobs, freshness, /v1/loops) but NO pull ingest — content arrives via
# a write endpoint, not a fetch. This frozenset is what the seed/schedule guards
# subtract so a push source never seeds a broken `ingest-<src>` schedule row or
# becomes an editable ingest target (both would be undispatchable).
PUSH_SOURCES = frozenset({"memory"})


# serve is a MANAGED process but deliberately NOT in JOBS: JOBS is the
# LAN-exposed start/stop whitelist (/v1/jobs), so it must never offer a control
# that stops the server hosting the API. `windex up`/`down`/`status` and the
# watchdog manage serve through the helpers below. The pattern is the flagged
# form the manager always launches ("windex serve --host …"), which a literal
# pgrep/`in` match distinguishes from `windex serve-mcp`.
SERVE = Job("serve", ("serve",), "windex serve --host",
            "API server", "REST API + dashboard + /metrics on :8100", "system")

# The scheduler is a MANAGED process like serve — supervised by up/status/the
# watchdog but deliberately NOT in JOBS (the LAN-exposed whitelist can't be
# allowed to stop the timer that drives ingest). Its pattern is a literal
# "windex scheduler", which won't cross-match serve/serve-mcp/embed-loop.
SCHEDULER = Job("scheduler", ("scheduler",), "windex scheduler",
                "Job scheduler", "Editable schedule timer loop (fires due ingest/command jobs)",
                "system")


def embed_loop_jobs() -> list[Job]:
    """The supervised embed-loop jobs, one per source — the same predicate the
    exporter uses to exclude loops from windex_job_up. The single source of
    truth for 'which loops should be running', shared by `windex up`/`status`,
    the watchdog, and the metrics exporter — never a hardcoded source list."""
    return [j for j in JOBS.values() if j.argv[0] == "embed-loop"]


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
    # `pgrep` only sees this process's own PID namespace and may be absent from a
    # slim container image. In the containerized (split serve/loops) deployment the
    # loops run in separate containers, so cross-process liveness comes from Postgres
    # heartbeats (see service.loop_states), not pgrep. Missing pgrep => "can't tell",
    # not a 500.
    try:
        out = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
    except FileNotFoundError:
        return []
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


def _spawn(log_name: str, argv: list[str]) -> int:
    """Detach a windex subprocess, its stdout+stderr appended to
    ~/.windex/logs/<log_name>.log. start_new_session makes the child lead its
    own process group so stop() can killpg it without catching siblings (the
    2026-07-17 'stopping one loop stopped them all' fix). Shared by the
    dashboard job starts and the serve manager."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{log_name}.log"
    # rotate-on-start guard: detached children write raw stdout no logging
    # handler can cap, so bound growth here (newsyslog covers the interval)
    if log_path.exists() and log_path.stat().st_size > 10_485_760:
        log_path.replace(log_path.with_suffix(".log.1"))
    log = open(log_path, "ab")
    proc = subprocess.Popen(
        argv, stdout=log, stderr=log, cwd=PROJECT_ROOT, start_new_session=True
    )
    return proc.pid


@contextlib.contextmanager
def _spawn_lock(name: str):
    """Serialize check-and-spawn for `name` across processes. The API server AND
    the scheduler can both try to start the same job (a human clicks 'run now'
    while the scheduler fires it), so a threading.Lock is not enough — an flock on
    a per-job lockfile makes the _pids() check and the spawn atomic, closing the
    TOCTOU that let two concurrent starts both pass the check and double-spawn."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fd = os.open(LOG_DIR / f".{name}.spawn.lock", os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)  # releases the flock (and on process death)


def start(name: str, params: dict) -> dict:
    job = JOBS.get(name)
    if job is None:
        raise KeyError(name)
    with _spawn_lock(name):
        if _pids(job.pattern):
            raise RuntimeError(f"{name} is already running")
        argv = build_argv(job, params or {})
        return {"started": name, "pid": _spawn(job.name, argv)}


def _stop_pattern(name: str, pattern: str) -> dict:
    pids = _pids(pattern)
    for pid in pids:
        try:
            # Only nuke the process group when this pid LEADS it. _spawn uses
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
            except (ProcessLookupError, PermissionError):
                # The retry can hit the SAME PermissionError (ownership hasn't
                # changed) — swallow it too, else a stop request 500s instead of
                # returning cleanly. A pid we can't signal is left for the OS.
                pass
    return {"stopped": name, "pids": pids}


def stop(name: str) -> dict:
    job = JOBS.get(name)
    if job is None:
        raise KeyError(name)
    return _stop_pattern(name, job.pattern)


def serve_running(port: int = 8100) -> bool:
    """True if the API is up: something accepts a TCP connection on `port` (the
    real 'is it serving' signal); a pgrep on the serve pattern is the fallback
    used when checking whether there's a process to stop."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        if s.connect_ex(("127.0.0.1", port)) == 0:
            return True
    return bool(_pids(SERVE.pattern))


def start_serve(host: str = "127.0.0.1", port: int = 8100) -> dict:
    """Launch `windex serve` detached (reusing the job spawn machinery). Refuses
    if the port is already served."""
    with _spawn_lock("serve"):
        if serve_running(port):
            raise RuntimeError("serve is already running")
        argv = [str(VENV_BIN / "windex"), "serve", "--host", host, "--port", str(port)]
        # Raw stdout/stderr → serve.out.log, NOT serve.log: `windex serve` installs
        # its own RotatingFileHandler on serve.log (cli.py), and pointing the
        # detached process's fds at the same file would fight that handler.
        return {"started": "serve", "pid": _spawn("serve.out", argv)}


def stop_serve() -> dict:
    """Stop the managed API server (SIGTERM its process group)."""
    return _stop_pattern("serve", SERVE.pattern)


def scheduler_running() -> bool:
    """True if the job scheduler timer loop is running (pgrep on its pattern)."""
    return bool(_pids(SCHEDULER.pattern))


def start_scheduler() -> dict:
    """Launch `windex scheduler` detached (reusing the job spawn machinery).
    Refuses if one is already running. Logs to ~/.windex/logs/scheduler.log."""
    with _spawn_lock("scheduler"):
        if scheduler_running():
            raise RuntimeError("scheduler is already running")
        argv = [str(VENV_BIN / "windex"), "scheduler"]
        return {"started": "scheduler", "pid": _spawn("scheduler", argv)}


def stop_scheduler() -> dict:
    """Stop the managed scheduler (SIGTERM its process group)."""
    return _stop_pattern("scheduler", SCHEDULER.pattern)
