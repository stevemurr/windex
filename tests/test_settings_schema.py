"""The editable-settings allowlist and the override resolution.

Most of these are about what must be REFUSED. The allowlist is the security
boundary between a LAN-exposed API and the process's credentials, and the
resolution layer sits on the path of every request and job — so its failure
modes matter more than its happy path.
"""

import pytest

from windex import settings_schema as schema
from windex.config import Settings


def test_every_allowlisted_key_exists_on_settings():
    """A typo'd key would be silently unsettable — it would validate, store, and
    then be dropped on read because Settings has no such field."""
    base = Settings(_env_file=None)
    bogus = [f.key for scope in schema.scopes() for f in schema.fields_for(scope)
             if not hasattr(base, f.key)]
    assert bogus == []


@pytest.mark.parametrize("key", [
    "pg_dsn", "write_token", "embed_api_key", "embed_bulk_api_key",
    "embed_query_api_key", "github_tokens", "judge_api_key", "rerank_api_key",
    "data_root", "serve_host",
])
def test_secrets_and_infrastructure_are_not_editable(key):
    """The allowlist exists so these are unreachable. If one ever becomes
    editable, a LAN caller can read it back out of GET /v1/settings."""
    with pytest.raises(ValueError, match="not editable"):
        schema.coerce(schema.GLOBAL, key, "anything")


@pytest.mark.parametrize("key", ["embed_model", "embed_dim"])
def test_vector_space_keys_are_not_editable(key):
    """Changing these is a re-embed + Qdrant alias flip, not a settings edit.
    Offering them here would silently corrupt search."""
    with pytest.raises(ValueError, match="not editable"):
        schema.coerce(schema.GLOBAL, key, 1)


def test_ceilings_are_not_themselves_editable():
    """crawl_*_ceiling bounds what a crawl policy may request; editing it through the
    same API it constrains would defeat it."""
    with pytest.raises(ValueError, match="not editable"):
        schema.coerce(schema.GLOBAL, "crawl_max_pages_ceiling", 10 ** 9)


def test_key_belongs_to_exactly_one_scope():
    with pytest.raises(ValueError, match="belongs to scope"):
        schema.coerce("wiki", "embed_concurrency", 8)
    assert schema.coerce(schema.GLOBAL, "embed_concurrency", 8) == 8


def test_numbers_clamp_rather_than_reject():
    """A caller may ask to be gentler; the ceiling is silently honoured so a form
    submit doesn't fail over a typo."""
    assert schema.coerce("hf", "hf_request_interval", 10 ** 6) == 60
    assert schema.coerce("hf", "hf_request_interval", 0.001) == 3.0   # arXiv-style floor
    assert schema.coerce("hf", "hf_blog_batch", -5) == 1


def test_type_errors_are_rejected():
    with pytest.raises(ValueError, match="expected a number"):
        schema.coerce("hf", "hf_blog_batch", "not-a-number")
    with pytest.raises(ValueError, match="expected a number"):
        schema.coerce("hf", "hf_blog_batch", True)      # bool is an int in Python
    with pytest.raises(ValueError, match="expected a string"):
        schema.coerce("wiki", "wiki_dump", 7)


def test_choice_is_constrained():
    assert schema.coerce(schema.GLOBAL, "embed_order", "newest") == "newest"
    with pytest.raises(ValueError, match="must be one of"):
        schema.coerce(schema.GLOBAL, "embed_order", "sideways")


def test_csv_is_normalized():
    """csv is stored as the raw comma string Settings' *_list() helpers parse,
    so stray whitespace/empties must not survive into those helpers."""
    assert schema.coerce("hf", "hf_roots", " transformers , , diffusers ") == \
        "transformers,diffusers"


def test_coerce_all_is_all_or_nothing():
    """One bad key rejects the batch so a form submit never half-applies."""
    with pytest.raises(ValueError):
        schema.coerce_all("hf", {"hf_blog_batch": 10, "pg_dsn": "x"})
