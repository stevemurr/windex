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
