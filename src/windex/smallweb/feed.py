"""Small Web feedparser accessors shared by epoch-2 Modules."""

from datetime import UTC, datetime


def entry_link(entry) -> str | None:
    return ((entry.get("link") or "").strip()) or None


def entry_title(entry) -> str | None:
    return ((entry.get("title") or "").strip()) or None


def entry_published(entry) -> str | None:
    """Return a normalized entry publication/update timestamp."""
    for key in ("published_parsed", "updated_parsed"):
        timestamp = entry.get(key)
        if timestamp:
            return datetime(
                *timestamp[:6],
                tzinfo=UTC,
            ).isoformat()
    return None


def item_body(entry, inline_summary_min: int) -> tuple[str | None, bool]:
    """Return an inline full body, or signal that the page must be fetched."""
    for content in entry.get("content") or []:
        value = content.get("value")
        if value:
            return value, True
    summary = entry.get("summary") or ""
    if len(summary) >= inline_summary_min:
        return summary, True
    return None, False


def newest_entries(parsed, limit: int) -> list:
    """Return at most ``limit`` entries, newest first when dates exist."""
    entries = list(getattr(parsed, "entries", []) or [])

    def sort_key(entry):
        return (
            entry.get("published_parsed")
            or entry.get("updated_parsed")
            or ()
        )

    if any(sort_key(entry) for entry in entries):
        entries.sort(key=sort_key, reverse=True)
    return entries[:limit]
