from pathlib import Path

from windex.pipeline import wire
from windex.pipeline.ports import PartitionRef, RawBlob
from windex.pipeline.run_store import _remove_download_outputs


def _stored(path: Path):
    return wire.encode_many([
        RawBlob(
            ref=PartitionRef(store="ccnews", key=path.name),
            uri=f"https://example.test/{path.name}",
            path=path,
        ),
    ])


def test_terminal_download_cleanup_is_bounded_and_prunes_empty_dirs(tmp_path):
    downloads = tmp_path / "downloads"
    managed = downloads / "_pipeline_runs" / "7" / "11" / "news.warc.gz"
    managed.parent.mkdir(parents=True)
    managed.write_bytes(b"warc")
    outside = tmp_path / "do-not-delete.warc.gz"
    outside.write_bytes(b"outside")

    removed = _remove_download_outputs(
        [_stored(managed), _stored(managed), _stored(outside)],
        downloads_dir=downloads,
    )

    assert removed == 1
    assert not managed.exists()
    assert not (downloads / "_pipeline_runs" / "7" / "11").exists()
    assert outside.read_bytes() == b"outside"


def test_terminal_download_cleanup_ignores_inline_blobs(tmp_path):
    stored = wire.encode_many([
        RawBlob(
            ref=PartitionRef(store="ccnews", key="inline"),
            uri="https://example.test/inline",
            body=b"small",
        ),
    ])

    assert _remove_download_outputs(
        [stored], downloads_dir=tmp_path / "downloads") == 0
