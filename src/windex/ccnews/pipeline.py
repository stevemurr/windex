"""Extraction + quality filtering for a batch of downloaded CC-News WARCs.

FineWeb recipe via datatrove blocks, with one substitution: stock Trafilatura
drops article titles, so NewsExtractor uses trafilatura.bare_extraction to keep
title/date metadata alongside the text.
"""

import os
from pathlib import Path

from datatrove.pipeline.base import PipelineStep


class NewsExtractor(PipelineStep):
    type = "🛢 - EXTRAC"
    name = "news-trafilatura"

    def run(self, data, rank: int = 0, world_size: int = 1):
        import trafilatura

        for doc in data:
            res = None
            with self.track_time():
                try:
                    res = trafilatura.bare_extraction(
                        doc.text,
                        url=doc.metadata.get("url"),
                        include_comments=False,
                        with_metadata=True,
                    )
                except Exception:
                    res = None
            if res is not None and not isinstance(res, dict):  # trafilatura >= 1.9 Document
                res = res.as_dict() if hasattr(res, "as_dict") else vars(res)
            text = (res or {}).get("text") or ""
            if len(text) < 200:
                self.stat_update("dropped_no_text")
                continue
            doc.text = text
            # always set every key: ParquetWriter needs a uniform metadata schema
            for key in ("title", "date", "author", "sitename"):
                value = (res or {}).get(key)
                doc.metadata[key] = str(value) if value else ""
            self.stat_update("extracted")
            yield doc


def build_pipeline(warc_dir: Path, rel_paths_file: Path, out_dir: Path, language: str):
    from datatrove.pipeline.filters import (
        C4QualityFilter,
        FineWebQualityFilter,
        GopherQualityFilter,
        GopherRepetitionFilter,
        LanguageFilter,
        URLFilter,
    )
    from datatrove.pipeline.readers import WarcReader
    from datatrove.pipeline.writers import ParquetWriter

    return [
        WarcReader(str(warc_dir), paths_file=str(rel_paths_file)),
        URLFilter(),
        NewsExtractor(),
        LanguageFilter(languages=[language]),
        GopherRepetitionFilter(),
        GopherQualityFilter(),
        C4QualityFilter(filter_no_terminal_punct=False),
        FineWebQualityFilter(),
        ParquetWriter(str(out_dir), output_filename="${rank}.parquet"),
    ]


def process_batch(
    warc_dir: Path,
    local_names: list[str],
    out_dir: Path,
    logging_dir: Path,
    language: str,
    workers: int = 0,
) -> None:
    """Run the extraction pipeline over the given WARC files (one datatrove task
    per file). Raises on failure; datatrove resumes completed tasks on retry."""
    import shutil

    from datatrove.executor import LocalPipelineExecutor

    # Clean slate per attempt: partial parquet/completions from a crashed run
    # would otherwise leak stale rows (or corrupt files) into dedup.
    shutil.rmtree(out_dir, ignore_errors=True)
    shutil.rmtree(logging_dir, ignore_errors=True)
    logging_dir.mkdir(parents=True, exist_ok=True)
    rel_paths_file = logging_dir / "input_files.txt"
    rel_paths_file.write_text("\n".join(local_names))

    executor = LocalPipelineExecutor(
        pipeline=build_pipeline(warc_dir, rel_paths_file, out_dir, language),
        tasks=len(local_names),
        workers=workers or max((os.cpu_count() or 4) - 2, 1),
        logging_dir=str(logging_dir),
    )
    executor.run()
