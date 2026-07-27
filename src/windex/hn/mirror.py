"""Normalize open-index Hacker News parquet rows."""

from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc

from windex.ccnews.identity import text_hash
from windex.hn.algolia import clean_text, clean_title, doc_id, item_url

_COLUMNS = (
    "id",
    "type",
    "dead",
    "deleted",
    "by",
    "time",
    "text",
    "url",
    "score",
    "title",
    "descendants",
)


def _flag_is_zero(column: pa.ChunkedArray) -> pa.ChunkedArray:
    return pc.equal(pc.cast(column, pa.int64()), 0)


def filter_stories(table: pa.Table) -> pa.Table:
    """Keep live story rows across the mirror's observed schema variants."""
    kind = table["type"]
    keep = (
        pc.equal(kind, 1)
        if pa.types.is_integer(kind.type)
        else pc.equal(kind, "story")
    )
    for name in ("dead", "deleted"):
        if name in table.column_names:
            keep = pc.and_(keep, _flag_is_zero(table[name]))
    return table.filter(keep)


def stories_from_table(
    table: pa.Table,
    from_ts: int,
    until_ts: int,
) -> list[dict]:
    """Normalize mirror rows to the same shape as Algolia hits."""
    selected = table.select([
        name for name in _COLUMNS if name in table.column_names
    ])
    rows = []
    for row in filter_stories(selected).to_pylist():
        timestamp = row.get("time")
        epoch = (
            int(timestamp.timestamp())
            if isinstance(timestamp, datetime)
            else int(timestamp or 0)
        )
        if not from_ts <= epoch < until_ts:
            continue
        title = clean_title(row.get("title"))
        text = clean_text(row.get("text"))
        rows.append({
            "id": doc_id(row["id"]),
            "url": item_url(row["id"]),
            "target_url": row.get("url") or None,
            "title": title,
            "story_text": text,
            "author": row.get("by") or "",
            "points": int(row.get("score") or 0),
            "num_comments": int(row.get("descendants") or 0),
            "created_at": (
                datetime.fromtimestamp(epoch, tz=UTC)
                .isoformat()
                .replace("+00:00", "Z")
            ),
            "thash": text_hash(title + "\n\n" + text),
        })
    return rows
