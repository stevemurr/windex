"""The ``custom_sources`` registry: name validation + CRUD over the row.

A registry row records a custom source's title/description and optional stored
refresh recipe. Doc counts (total live + pending-embed) are computed on read from
the shared documents ledger, so ``IndexInfo`` is self-describing without a second
bookkeeping table. Name validation is the security boundary that keeps a custom
source from shadowing a built-in corpus source or the search-side ``all``.
"""

from __future__ import annotations

import re

import psycopg
from psycopg.types.json import Jsonb

from windex.index import qdrant as qidx

# ^[a-z][a-z0-9_]{1,31}$ — lowercase, starts with a letter, 2..32 chars. This is
# what makes <name>:<suffix> ids, the <name> Qdrant collection base, and the
# loop_<name>/heartbeat control-flag suffixes all safe without escaping.
NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,31}$")

# Names a custom source may never take. The built-in corpus sources (qidx.SOURCES)
# so a custom collection can't shadow news/wiki/memory/…; the search pseudo-source
# `all`; the github CLI/label aliases (github/gh) and ccnews (the corpus↔CLI
# vocabulary split); and `custom` itself — the aggregate embed-loop name and
# PUSH_SOURCES member (jobs.py), never a real source.
RESERVED = set(qidx.SOURCES) | {"all", "github", "gh", "ccnews", "custom"}


class DuplicateSource(Exception):
    """Raised by ``create`` when a source with that name already exists. The
    route maps it to HTTP 409 (distinct from a 422 name-validation failure)."""


def validate_name(name: str) -> str:
    """Return ``name`` if it is a legal, non-reserved custom-source name; raise
    ValueError otherwise (the route maps that to 422)."""
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ValueError(
            f"invalid source name {name!r}: must match {NAME_RE.pattern} "
            "(lowercase, start with a letter, 2-32 chars)"
        )
    if name in RESERVED:
        raise ValueError(f"reserved source name: {name!r}")
    return name


def _counts(cur: psycopg.Cursor, names: list[str]) -> dict[str, dict[str, int]]:
    """{name: {status: count}} over the documents ledger for the given sources,
    in one grouped scan. Empty input ⇒ ``{}`` (no query)."""
    if not names:
        return {}
    cur.execute(
        "SELECT source, status, count(*) FROM documents "
        "WHERE source = ANY(%s) GROUP BY source, status",
        (names,),
    )
    out: dict[str, dict[str, int]] = {}
    for source, status, n in cur.fetchall():
        out.setdefault(source, {})[status] = n
    return out


def _info(row: tuple, by_status: dict[str, int]) -> dict:
    """Assemble the IndexInfo shape the API returns. ``doc_count`` is live docs
    (anything not tombstoned); ``pending`` is docs awaiting a vector (deduped)."""
    live = sum(n for st, n in by_status.items() if st != "deleted")
    return {
        "name": row[0],
        "title": row[1],
        "description": row[2],
        "recipe": row[3],  # jsonb → psycopg hands back a dict/list/None
        "doc_count": live,
        "pending": by_status.get("deduped", 0),
        "created_at": row[4].isoformat() if row[4] else None,
        "updated_at": row[5].isoformat() if row[5] else None,
    }


_COLUMNS = "name, title, description, recipe, created_at, updated_at"


def create(conn: psycopg.Connection, name: str, title: str = "",
           description: str = "", recipe: dict | None = None) -> dict:
    """Register a new custom source. Raises ValueError for an invalid/reserved
    name, DuplicateSource if it already exists. Returns its IndexInfo."""
    validate_name(name)
    with conn.cursor() as cur:
        try:
            cur.execute(
                "INSERT INTO custom_sources (name, title, description, recipe) "
                "VALUES (%s, %s, %s, %s)",
                (name, title or "", description or "",
                 Jsonb(recipe) if recipe is not None else None),
            )
        except psycopg.errors.UniqueViolation:
            conn.rollback()
            raise DuplicateSource(f"source already exists: {name}")
    conn.commit()
    return get(conn, name)


def get(conn: psycopg.Connection, name: str) -> dict | None:
    """IndexInfo for one source, or None if unknown."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLUMNS} FROM custom_sources WHERE name = %s", (name,))
        row = cur.fetchone()
        if row is None:
            return None
        counts = _counts(cur, [name]).get(name, {})
    return _info(row, counts)


def list_all(conn: psycopg.Connection) -> list[dict]:
    """Every registered custom source (IndexInfo, name-sorted), with doc counts
    from a single grouped ledger scan."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_COLUMNS} FROM custom_sources ORDER BY name")
        rows = cur.fetchall()
        counts = _counts(cur, [r[0] for r in rows])
    return [_info(r, counts.get(r[0], {})) for r in rows]


_UNSET = object()


def update(conn: psycopg.Connection, name: str, title=_UNSET, description=_UNSET,
           recipe=_UNSET) -> dict | None:
    """Partial update of a source's title/description/recipe — only the arguments
    actually passed are changed (the route passes exactly the client-set fields).
    Returns the updated IndexInfo, or None if the source is unknown."""
    sets, params = [], []
    if title is not _UNSET:
        sets.append("title = %s")
        params.append(title or "")
    if description is not _UNSET:
        sets.append("description = %s")
        params.append(description or "")
    if recipe is not _UNSET:
        sets.append("recipe = %s")
        params.append(Jsonb(recipe) if recipe is not None else None)
    if not sets:
        return get(conn, name)
    sets.append("updated_at = now()")
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE custom_sources SET {', '.join(sets)} WHERE name = %s",
            (*params, name),
        )
        updated = cur.rowcount
    conn.commit()
    return get(conn, name) if updated else None


def delete_row(conn: psycopg.Connection, name: str) -> bool:
    """Drop just the registry row (True if it existed). Full teardown — tombstone
    the docs, remove staging — lives in custom_source.ingest.delete_source."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM custom_sources WHERE name = %s", (name,))
        deleted = cur.rowcount
    conn.commit()
    return bool(deleted)
