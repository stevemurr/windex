"""Settings validation: fields with a fixed vocabulary must reject typos at
load time rather than silently falling back to default behavior."""

import pytest
from pydantic import ValidationError

from windex.config import Settings


def test_embed_order_rejects_invalid_values(tmp_path):
    # A typo like WINDEX_EMBED_ORDER=Newest must fail loudly, not load and then
    # no-op the intended freshness push (pipeline.py compares == "newest" exactly).
    for bad in ("Newest", "newest ", "descending", "oldest_first"):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, data_root=tmp_path, embed_order=bad)


def test_embed_order_accepts_the_two_valid_values(tmp_path):
    assert Settings(_env_file=None, data_root=tmp_path, embed_order="oldest").embed_order == "oldest"
    assert Settings(_env_file=None, data_root=tmp_path, embed_order="newest").embed_order == "newest"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pipeline_gc_interval_seconds", 0),
        ("pipeline_gc_terminal_retention_seconds", -1),
        ("pipeline_gc_min_file_age_seconds", -1),
        ("pipeline_gc_max_files_per_tick", 0),
        ("pipeline_gc_max_bytes_per_tick", 0),
    ],
)
def test_pipeline_gc_rejects_unbounded_or_negative_policy(
    tmp_path, field, value,
):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, data_root=tmp_path, **{field: value})
