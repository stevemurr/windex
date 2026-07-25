"""Document transforms: ExtractedDoc -> ExtractedDoc."""

from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import replace
from datetime import date

from windex.ccnews.dedup import canonical_url as news_canonical
from windex.ccnews.dedup import text_hash
from windex.modules.common import (
    finish_batch,
    pending_batches,
    require_type,
)
from windex.recipe.ports import ExtractedDoc
from windex.sanitize import strip_smuggled
from windex.textguard import is_empty_text
from windex.worker.protocol import PermanentTaskError, SliceResult, TaskContext

_INPUT_BATCH = 20


def _prefix(ctx: TaskContext) -> str:
    return str((ctx.spec.get("corpus") or {}).get("id_prefix") or f"{ctx.source}:")


def _doc_id(ctx: TaskContext, doc: ExtractedDoc) -> str:
    return _prefix(ctx) + doc.suffix


def _run(ctx: TaskContext, transform, *, limit: int = _INPUT_BATCH) -> SliceResult:
    batches, more = pending_batches(ctx, limit=limit)
    processed = []
    input_count = output_count = 0
    for batch in batches:
        docs = [
            require_type(value, ExtractedDoc, ctx.module)
            for value in batch.values
        ]
        coverage = [doc for doc in docs if doc.fields.get("_coverage_only")]
        outputs = transform(
            [doc for doc in docs if not doc.fields.get("_coverage_only")])
        outputs.extend(coverage)
        finish_batch(ctx, batch, outputs=outputs)
        input_count += len(docs)
        output_count += len(outputs)
        processed.append(batch)
        if ctx.should_yield():
            break
    ctx.conn.commit()
    done = len(processed)
    if done:
        ctx.heartbeat(done, 0, {
            "last": processed[-1].key,
            "input_docs": input_count,
            "output_docs": output_count,
        })
    return SliceResult(
        units_done=done,
        exhausted=not more and done == len(batches),
        stats={"inputs": input_count, "outputs": output_count,
               "dropped": input_count - output_count},
    )


def canonical_url(ctx: TaskContext) -> SliceResult:
    strategy = str(ctx.config.get("strategy", ""))
    if strategy not in {"sha1_of_canonical", "path_suffix", "field"}:
        raise PermanentTaskError(
            f"canonical.url has unknown strategy {strategy!r}")

    def transform(docs: list[ExtractedDoc]) -> list[ExtractedDoc]:
        outputs = []
        for doc in docs:
            canonical = news_canonical(doc.canonical_url or doc.url)
            if strategy == "sha1_of_canonical":
                suffix = hashlib.sha1(canonical.encode()).hexdigest()[:20]
            elif strategy == "field":
                suffix = str(doc.fields.get("id") or doc.suffix)
            else:
                # Extractors already derive the source's stable path/hash suffix.
                suffix = doc.suffix
            outputs.append(replace(
                doc, suffix=suffix, canonical_url=canonical))
        return outputs

    return _run(ctx, transform)


def dedup_exact(ctx: TaskContext) -> SliceResult:
    scope = str(ctx.config.get("scope", "both"))
    if scope not in {"batch", "ledger", "both"}:
        raise PermanentTaskError(f"dedup.exact has unknown scope {scope!r}")

    def transform(docs: list[ExtractedDoc]) -> list[ExtractedDoc]:
        hashes = [text_hash(doc.title + "\n\n" + doc.text) for doc in docs]
        existing = {}
        if scope in {"ledger", "both"} and hashes:
            with ctx.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (text_hash) text_hash, id
                      FROM documents
                     WHERE text_hash = ANY(%s) AND status <> 'deleted'
                     ORDER BY text_hash, created_at, id
                    """,
                    (hashes,),
                )
                existing = dict(cur.fetchall())
        seen = {}
        outputs = []
        for doc, digest in zip(docs, hashes):
            duplicate = existing.get(digest)
            if duplicate is None and scope in {"batch", "both"}:
                duplicate = seen.get(digest)
            fields = {**doc.fields, "_text_hash": digest}
            if duplicate and duplicate != _doc_id(ctx, doc):
                fields["_duplicate_of"] = duplicate
            else:
                seen[digest] = _doc_id(ctx, doc)
            outputs.append(replace(doc, fields=fields))
        return outputs

    return _run(ctx, transform)


def dedup_minhash(ctx: TaskContext) -> SliceResult:
    from windex.ccnews.minhash import band_hashes, signature

    window = int(ctx.config.get("window_days", 30))

    def transform(docs: list[ExtractedDoc]) -> list[ExtractedDoc]:
        with ctx.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM minhash_bands WHERE day < current_date - %s",
                (window,),
            )
        local: dict[tuple[int, int], str] = {}
        outputs = []
        for doc in docs:
            signature_value = signature(doc.text)
            if signature_value is None:
                outputs.append(doc)
                continue
            bands = band_hashes(signature_value)
            duplicate = next(
                (local[(index, value)] for index, value in enumerate(bands)
                 if (index, value) in local),
                None,
            )
            if duplicate is None:
                with ctx.conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT doc_id
                          FROM minhash_bands
                         WHERE (band_idx, band_hash) IN (
                               SELECT * FROM unnest(%s::smallint[], %s::bigint[]))
                         ORDER BY day DESC LIMIT 1
                        """,
                        (list(range(len(bands))), bands),
                    )
                    row = cur.fetchone()
                    duplicate = row[0] if row else None
            if duplicate:
                outputs.append(replace(
                    doc,
                    fields={**doc.fields, "_duplicate_of": duplicate},
                ))
                continue
            doc_id = _doc_id(ctx, doc)
            published = doc.published_at.date() if doc.published_at else date.today()
            with ctx.conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO minhash_bands
                           (band_idx, band_hash, doc_id, day)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        (index, value, doc_id, published)
                        for index, value in enumerate(bands)
                    ],
                )
            for index, value in enumerate(bands):
                local[(index, value)] = doc_id
            outputs.append(doc)
        return outputs

    return _run(ctx, transform)


def dedup_boilerplate(ctx: TaskContext) -> SliceResult:
    cap = int(ctx.config.get("repeat_cap", 2))

    def transform(docs: list[ExtractedDoc]) -> list[ExtractedDoc]:
        counts: Counter[str] = Counter()
        outputs = []
        for doc in docs:
            digest = text_hash(doc.text)
            counts[digest] += 1
            if counts[digest] <= cap:
                outputs.append(doc)
        return outputs

    return _run(ctx, transform)


def filter_quality(ctx: TaskContext) -> SliceResult:
    from datatrove.data import Document
    from datatrove.pipeline.filters import (
        C4QualityFilter,
        FineWebQualityFilter,
        GopherQualityFilter,
        GopherRepetitionFilter,
    )

    filters = (
        GopherRepetitionFilter(),
        GopherQualityFilter(),
        C4QualityFilter(filter_no_terminal_punct=False),
        FineWebQualityFilter(),
    )

    def transform(docs: list[ExtractedDoc]) -> list[ExtractedDoc]:
        outputs = []
        for doc in docs:
            candidate = Document(
                text=doc.text, id=_doc_id(ctx, doc),
                metadata={"url": doc.url},
            )
            if all(
                (result[0] if isinstance(result := gate.filter(candidate), tuple)
                 else bool(result))
                for gate in filters
            ):
                outputs.append(replace(doc, text=candidate.text))
        return outputs

    return _run(ctx, transform)


def filter_lang(ctx: TaskContext) -> SliceResult:
    allowed = {
        value.strip().lower()
        for value in str(ctx.config.get("languages", "en")).split(",")
        if value.strip()
    }

    def transform(docs: list[ExtractedDoc]) -> list[ExtractedDoc]:
        undecided = [doc for doc in docs if not doc.lang]
        detected = {}
        if undecided:
            from datatrove.data import Document
            from datatrove.pipeline.filters import LanguageFilter

            gate = LanguageFilter(languages=sorted(allowed))
            for doc in undecided:
                candidate = Document(
                    text=doc.text, id=_doc_id(ctx, doc), metadata={})
                verdict = gate.filter(candidate)
                keep = verdict[0] if isinstance(verdict, tuple) else bool(verdict)
                detected[id(doc)] = (
                    keep, candidate.metadata.get("language"))
        outputs = []
        for doc in docs:
            if doc.lang:
                if doc.lang.lower().split("-", 1)[0] in allowed:
                    outputs.append(doc)
            else:
                keep, language = detected[id(doc)]
                if keep:
                    outputs.append(replace(doc, lang=language or "en"))
        return outputs

    return _run(ctx, transform)


def sanitize_documents(ctx: TaskContext) -> SliceResult:
    """Mandatory last transform, callable even though recipes do not name it."""

    def transform(docs: list[ExtractedDoc]) -> list[ExtractedDoc]:
        outputs = []
        for doc in docs:
            title = strip_smuggled(doc.title)
            text = strip_smuggled(doc.text)
            if doc.deleted or not is_empty_text(title + "\n\n" + text):
                outputs.append(replace(doc, title=title, text=text))
        return outputs

    return _run(ctx, transform)
