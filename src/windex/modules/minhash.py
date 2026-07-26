"""Source-scoped near-duplicate detection."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

from windex.modules.transform import _doc_id, _run
from windex.pipeline.ports import ExtractedDoc
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext


def dedup_minhash(ctx: TaskContext) -> SliceResult:
    from windex.ccnews.minhash import band_hashes, signature

    if ctx.source_id is None:
        raise PermanentTaskError("dedup.minhash requires a source-bound Pipeline Run")
    source_id = ctx.source_id
    window = int(ctx.config.get("window_days", 30))

    def transform(docs: list[ExtractedDoc]) -> list[ExtractedDoc]:
        with ctx.conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM minhash_bands
                 WHERE source_id = %s AND day < current_date - %s
                """,
                (source_id, window),
            )
        local: dict[tuple[int, int], str] = {}
        outputs = []
        for doc in docs:
            signature_value = signature(doc.text)
            if signature_value is None:
                outputs.append(doc)
                continue
            doc_id = _doc_id(ctx, doc)
            bands = band_hashes(signature_value)
            duplicate = next(
                (
                    local[(index, value)]
                    for index, value in enumerate(bands)
                    if (index, value) in local and local[(index, value)] != doc_id
                ),
                None,
            )
            if duplicate is None:
                with ctx.conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT doc_id
                          FROM minhash_bands
                         WHERE source_id = %s
                           AND doc_id <> %s
                           AND (band_idx, band_hash) IN (
                               SELECT * FROM unnest(%s::smallint[], %s::bigint[]))
                         ORDER BY day DESC LIMIT 1
                        """,
                        (
                            source_id,
                            doc_id,
                            list(range(len(bands))),
                            bands,
                        ),
                    )
                    row = cur.fetchone()
                    duplicate = row[0] if row and row[0] != doc_id else None
            if duplicate:
                outputs.append(
                    replace(
                        doc,
                        fields={**doc.fields, "_duplicate_of": duplicate},
                    )
                )
                continue
            published = doc.published_at.date() if doc.published_at else date.today()
            with ctx.conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO minhash_bands
                           (source_id, band_idx, band_hash, doc_id, day)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        (source_id, index, value, doc_id, published)
                        for index, value in enumerate(bands)
                    ],
                )
            for index, value in enumerate(bands):
                local[(index, value)] = doc_id
            outputs.append(doc)
        return outputs

    return _run(ctx, transform)
