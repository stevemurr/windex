"""Two-tier dedup over an extracted batch, then staging of clean docs.

Tier 1 (exact): stable id from canonical URL + normalized text hash, checked
within the batch and against the documents ledger — catches re-crawls across
daily dumps. Tier 2 (near-dup): MinHash band collisions against a rolling
window (news syndication crosses days). Duplicates are kept as documents rows
pointing at their canonical via duplicate_of, never deleted.

The whole batch is one transaction; the clean parquet is renamed into place at
commit so text_ref never points at a partial file.
"""

import hashlib
import re
from datetime import date, datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from windex.ccnews.minhash import band_hashes, signature

CHUNK = 2048
_TRACKING_PARAMS = re.compile(r"^(utm_|fbclid|gclid|mc_cid|mc_eid|ref$)")
_WS = re.compile(r"\s+")

CLEAN_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("url", pa.string()),
        ("canonical_url", pa.string()),
        ("title", pa.string()),
        ("published_at", pa.string()),
        ("lang", pa.string()),
        ("text", pa.string()),
    ]
)


def canonical_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = [(k, v) for k, v in parse_qsl(parts.query) if not _TRACKING_PARAMS.match(k.lower())]
    return urlunsplit(
        (
            parts.scheme.lower() or "https",
            parts.netloc.lower(),
            parts.path.rstrip("/") or "/",
            urlencode(query),
            "",
        )
    )


def doc_id(canon: str) -> str:
    return "news:" + hashlib.sha1(canon.encode()).hexdigest()[:20]


def text_hash(text: str) -> str:
    return hashlib.sha1(_WS.sub(" ", text.lower()).strip().encode()).hexdigest()


def _parse_ts(value: str | None) -> datetime | None:
    from windex.dateparse import parse_and_clamp

    return parse_and_clamp(value)


def _iter_extracted(extracted_dir: Path):
    for f in sorted(extracted_dir.glob("*.parquet")):
        pf = pq.ParquetFile(f)
        for rb in pf.iter_batches(batch_size=CHUNK):
            yield rb.to_pylist()


def _existing(cur: psycopg.Cursor, column: str, values: list[str]) -> dict[str, str]:
    if not values:
        return {}
    cur.execute(
        f"SELECT {column}, id FROM documents WHERE {column} = ANY(%s)",  # noqa: S608
        (values,),
    )
    return dict(cur.fetchall())


def _band_collisions(cur: psycopg.Cursor, pairs: list[tuple[int, int, int]]) -> dict[int, str]:
    """pairs: (doc_ordinal, band_idx, band_hash) → first colliding doc per ordinal."""
    if not pairs:
        return {}
    cur.execute(
        """
        SELECT DISTINCT ON (v.ord) v.ord, mb.doc_id
        FROM (SELECT * FROM unnest(%s::int[], %s::smallint[], %s::bigint[]) AS t(ord, band_idx, band_hash)) v
        JOIN minhash_bands mb ON mb.band_idx = v.band_idx AND mb.band_hash = v.band_hash
        ORDER BY v.ord
        """,
        ([p[0] for p in pairs], [p[1] for p in pairs], [p[2] for p in pairs]),
    )
    return dict(cur.fetchall())


def run_dedup(
    conn: psycopg.Connection,
    extracted_dir: Path,
    clean_path: Path,
    text_ref: str,
    day: date,
) -> dict:
    stats = {
        "extracted_in": 0,
        "dup_batch_exact": 0,
        "dup_db_exact": 0,
        "dup_near": 0,
        "already_indexed": 0,
        "clean_out": 0,
    }
    seen_ids: set[str] = set()
    seen_hashes: dict[str, str] = {}
    seen_bands: dict[tuple[int, int], str] = {}

    clean_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = clean_path.with_suffix(".parquet.tmp")
    writer = pq.ParquetWriter(tmp_path, CLEAN_SCHEMA)

    doc_rows: list[tuple] = []
    try:
        with conn.cursor() as cur:
            for rows in _iter_extracted(extracted_dir):
                stats["extracted_in"] += len(rows)
                chunk = []
                for row in rows:
                    meta = row.get("metadata") or {}
                    url = meta.get("url") or ""
                    canon = canonical_url(url) if url else ""
                    if not canon:
                        continue
                    did = doc_id(canon)
                    thash = text_hash(row["text"])
                    if did in seen_ids or thash in seen_hashes:
                        stats["dup_batch_exact"] += 1
                        continue
                    seen_ids.add(did)
                    seen_hashes[thash] = did
                    published = _parse_ts(meta.get("date"))
                    chunk.append(
                        {
                            "id": did,
                            "url": url,
                            "canon": canon,
                            "thash": thash,
                            "title": meta.get("title"),
                            "published": published,
                            "lang": meta.get("language"),
                            "text": row["text"],
                        }
                    )

                # exact dedup against the ledger
                existing_ids = _existing(cur, "id", [c["id"] for c in chunk])
                existing_hashes = _existing(cur, "text_hash", [c["thash"] for c in chunk])
                survivors = []
                for c in chunk:
                    if c["id"] in existing_ids:
                        stats["already_indexed"] += 1
                    elif c["thash"] in existing_hashes:
                        stats["dup_db_exact"] += 1
                        doc_rows.append(
                            (c["id"], "news", c["url"], c["canon"], c["title"], c["published"],
                             c["lang"], c["thash"], "duplicate", existing_hashes[c["thash"]], None)
                        )
                    else:
                        survivors.append(c)

                # near-dup via minhash bands (in-batch, then rolling window)
                pairs, doc_bands = [], {}
                for i, c in enumerate(survivors):
                    sig = signature(c["text"])
                    if sig is None:
                        continue
                    doc_bands[i] = band_hashes(sig)
                    for b_idx, b_hash in enumerate(doc_bands[i]):
                        pairs.append((i, b_idx, b_hash))
                db_hits = _band_collisions(cur, pairs)

                clean_chunk, band_rows = [], []
                for i, c in enumerate(survivors):
                    bands = doc_bands.get(i)
                    dup_of = None
                    if bands is not None:
                        dup_of = db_hits.get(i) or next(
                            (seen_bands[(bi, bh)] for bi, bh in enumerate(bands)
                             if (bi, bh) in seen_bands),
                            None,
                        )
                    if dup_of:
                        stats["dup_near"] += 1
                        doc_rows.append(
                            (c["id"], "news", c["url"], c["canon"], c["title"], c["published"],
                             c["lang"], c["thash"], "duplicate", dup_of, None)
                        )
                        continue
                    if bands is not None:
                        for bi, bh in enumerate(bands):
                            seen_bands[(bi, bh)] = c["id"]
                            band_rows.append((bi, bh, c["id"], day))
                    stats["clean_out"] += 1
                    clean_chunk.append(c)
                    doc_rows.append(
                        (c["id"], "news", c["url"], c["canon"], c["title"], c["published"],
                         c["lang"], c["thash"], "deduped", None, text_ref)
                    )

                if clean_chunk:
                    writer.write_batch(
                        pa.record_batch(
                            [
                                pa.array([c["id"] for c in clean_chunk]),
                                pa.array([c["url"] for c in clean_chunk]),
                                pa.array([c["canon"] for c in clean_chunk]),
                                pa.array([c["title"] for c in clean_chunk]),
                                pa.array(
                                    [c["published"].isoformat() if c["published"] else None
                                     for c in clean_chunk]
                                ),
                                pa.array([c["lang"] for c in clean_chunk]),
                                pa.array([c["text"] for c in clean_chunk]),
                            ],
                            schema=CLEAN_SCHEMA,
                        )
                    )
                if band_rows:
                    with cur.copy(
                        "COPY minhash_bands (band_idx, band_hash, doc_id, day) FROM STDIN"
                    ) as copy:
                        for r in band_rows:
                            copy.write_row(r)

            cur.executemany(
                """
                INSERT INTO documents
                    (id, source, url, canonical_url, title, published_at, lang, text_hash,
                     status, duplicate_of, text_ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                doc_rows,
            )
        writer.close()
        tmp_path.rename(clean_path)
        conn.commit()
    except Exception:
        writer.close()
        tmp_path.unlink(missing_ok=True)
        conn.rollback()
        raise
    return stats


def prune_bands(conn: psycopg.Connection, window_days: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM minhash_bands WHERE day < (SELECT max(day) FROM minhash_bands) - %s",
            (window_days,),
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted
