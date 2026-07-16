"""Dashboard log viewer: whitelisted log registry + safe tailing.

Same security model as jobs.py — the client only ever supplies a registry key,
never a path. Every line is redacted before leaving the process (the API is
LAN-exposed and unauthenticated): configured secret values are scrubbed exactly,
plus token-shaped patterns as a backstop. Sources on the external drive are
"available: false" when the mount is gone, never an error.
"""

import logging
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from windex.api import jobs
from windex.config import get_settings

LOG_DIR = jobs.LOG_DIR
MAX_WINDOW_BYTES = 256 * 1024

_ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
_SHAPES = [
    re.compile(r"gh[pousr]_\w{20,}"),
    re.compile(r"sk-[\w-]{20,}"),
    re.compile(r"(?i)(authorization:\s*(?:bearer|token)\s+)\S+"),
    re.compile(r"(?i)(api[-_]?key[\"']?\s*[:=]\s*)\S+"),
]


class QuietAccess(logging.Filter):
    """Drops uvicorn access lines for the dashboard's own polling endpoints —
    they would otherwise dominate serve.log with zero information."""

    NOISY = ("/v1/events", "/v1/workers", "/v1/stats", "/v1/jobs", "/v1/logs",
             "/v1/recent", "/v1/timeseries", "favicon", "apple-touch")

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not any(p in message for p in self.NOISY)


@dataclass(frozen=True)
class LogSource:
    name: str
    title: str
    description: str
    category: str            # server | news | github | maintenance | infra
    kind: str = "file"       # file | container
    path: Path | None = None
    container: str | None = None


_JOB_LOGS = [
    LogSource(j.name, j.title, j.description, j.category,
              path=LOG_DIR / f"{j.name}.log")
    for j in jobs.JOBS.values()
]

LOGS: dict[str, LogSource] = {s.name: s for s in [
    LogSource("server", "Server", "REST API — uvicorn access + errors", "server",
              path=LOG_DIR / "serve.log"),
    LogSource("watchdog", "Watchdog", "Service health + mount monitor", "server",
              path=Path.home() / ".windex" / "watchdog.log"),
    *_JOB_LOGS,
    LogSource("backfill", "News backfill (manual)", "Manually launched backfill runs", "news",
              path=LOG_DIR / "backfill.log"),
    LogSource("hydrate", "GitHub hydrate (manual)", "Manually launched hydrate runs", "github",
              path=LOG_DIR / "hydrate.log"),
    LogSource("postgres", "Postgres", "Database server log", "infra",
              kind="container", container="windex-postgres"),
    LogSource("qdrant", "Qdrant", "Vector store log", "infra",
              kind="container", container="windex-qdrant"),
]}


def _secret_values() -> list[str]:
    s = get_settings()
    return [v for v in [s.embed_api_key, *s.github_token_list()] if v]


def redact(line: str) -> str:
    for value in _secret_values():
        line = line.replace(value, "•••")
    for pattern in _SHAPES:
        line = pattern.sub(
            lambda m: (m.group(1) if m.groups() else "") + "•••", line
        )
    return line


def _clean(text: str) -> list[str]:
    text = _ANSI.sub("", text).replace("\r", "\n")
    return [ln.rstrip() for ln in text.splitlines() if ln.strip()]


def _read_file_tail(path: Path) -> str | None:
    if not path.exists():
        return None
    with open(path, "rb") as f:
        size = f.seek(0, 2)
        f.seek(max(size - MAX_WINDOW_BYTES, 0))
        return f.read().decode(errors="replace")


def _read_container_tail(container: str) -> str | None:
    try:
        out = subprocess.run(
            ["container", "logs", container],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if out.returncode != 0:
        return None
    return out.stdout[-MAX_WINDOW_BYTES:]


def tail(name: str, lines: int = 200, grep: str | None = None) -> dict:
    source = LOGS.get(name)
    if source is None:
        raise KeyError(name)
    if source.kind == "container":
        raw = _read_container_tail(source.container)
    else:
        try:
            raw = _read_file_tail(source.path)
        except OSError:  # detached external mount and similar
            raw = None
    if raw is None:
        return {"name": name, "available": False, "lines": []}
    cleaned = _clean(raw)
    if grep:
        needle = grep.lower()
        cleaned = [ln for ln in cleaned if needle in ln.lower()]
    return {
        "name": name,
        "available": True,
        "truncated": len(raw) >= MAX_WINDOW_BYTES,
        "lines": [redact(ln) for ln in cleaned[-lines:]],
    }


def list_logs() -> list[dict]:
    out = []
    for s in LOGS.values():
        size = mtime = None
        available = False
        if s.kind == "file":
            try:
                stat = s.path.stat()
                size, mtime, available = stat.st_size, int(stat.st_mtime), True
            except OSError:
                pass
        else:
            available = True  # container logs resolve at read time
        out.append({"name": s.name, "title": s.title, "description": s.description,
                    "category": s.category, "kind": s.kind,
                    "size": size, "mtime": mtime, "available": available})
    return out
