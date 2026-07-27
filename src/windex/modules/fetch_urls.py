"""URL derivation and fixed-host allowlisting shared by fetch runners."""

from __future__ import annotations

from urllib.parse import urlsplit

from windex.pipeline.ports import WorkUnit
from windex.worker.protocol import PermanentTaskError, TaskContext


def hosts(raw) -> set[str]:
    values = raw if isinstance(raw, list) else str(raw or "").split(",")
    return {str(value).strip().lower() for value in values if str(value).strip()}


def assert_host(url: str, allowed: set[str]) -> None:
    host = (urlsplit(url).hostname or "").lower()
    if not host or (allowed and host not in allowed):
        raise PermanentTaskError(
            f"fetch target host {host or '<missing>'!r} is not in the allowlist")


def unit_url(ctx: TaskContext, unit: WorkUnit) -> str:
    # A root's stored URL is its human-facing landing page. The enumeration
    # contract is llms.txt; page children carry `path` plus their own URL and
    # must still take the ordinary payload branch below.
    if (ctx.search_name == "hf" and unit.ref.store == "root"
            and not unit.payload.get("path")):
        key = unit.ref.key.strip("/")
        return f"https://huggingface.co/{key}/llms.txt"
    if unit.payload.get("url"):
        return str(unit.payload["url"])
    if ctx.search_name == "hf" and unit.ref.key == "sitemap":
        return "https://huggingface.co/sitemap.xml"
    if ctx.search_name == "hf":
        key = unit.ref.key.strip("/")
        if unit.ref.store == "post":
            return f"https://huggingface.co/blog/{key}"
    if unit.ref.key.startswith(("http://", "https://")):
        return unit.ref.key
    raise PermanentTaskError(
        f"{ctx.module} cannot derive a URL for unit {unit.ref.key!r}")


__all__ = ["assert_host", "hosts", "unit_url"]
