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
    """crawl_*_ceiling bounds what a recipe may request; editing it through the
    same API it constrains would defeat it."""
    with pytest.raises(ValueError, match="not editable"):
        schema.coerce(schema.GLOBAL, "crawl_max_pages_ceiling", 10 ** 9)


def test_key_belongs_to_exactly_one_scope():
    with pytest.raises(ValueError, match="belongs to scope"):
        schema.coerce("wiki", "embed_concurrency", 8)
    assert schema.coerce(schema.GLOBAL, "embed_concurrency", 8) == 8


def test_numbers_clamp_rather_than_reject():
    """A caller may ask to be gentler; the ceiling is silently honoured so a form
    submit doesn't fail over a typo. Same call crawl/recipe.py makes."""
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


# --- resolution -------------------------------------------------------------

def _write(pg, scope, values):
    from psycopg.types.json import Jsonb
    with pg.cursor() as cur:
        cur.execute("INSERT INTO source_config (scope, settings) VALUES (%s, %s) "
                    "ON CONFLICT (scope) DO UPDATE SET settings = EXCLUDED.settings",
                    (scope, Jsonb(values)))
    pg.commit()


@pytest.fixture(autouse=True)
def _clean_overrides(pg):
    from windex.config import invalidate_overrides
    with pg.cursor() as cur:
        cur.execute("DELETE FROM source_config")
    pg.commit()
    invalidate_overrides(clear=True)
    yield
    with pg.cursor() as cur:
        cur.execute("DELETE FROM source_config")
    pg.commit()
    invalidate_overrides(clear=True)


def test_db_override_beats_env(pg, settings):
    from windex.config import effective_settings, invalidate_overrides

    _write(pg, "hf", {"hf_blog_batch": 7})
    invalidate_overrides(clear=True)
    assert effective_settings("hf", dsn=settings.pg_dsn).hf_blog_batch == 7


def test_untouched_keys_fall_through(pg, settings):
    """Sparse rows are the whole point: an install that edits one key must not
    have every other key frozen at whatever the default was that day."""
    from windex.config import Settings as S
    from windex.config import effective_settings, invalidate_overrides

    _write(pg, "hf", {"hf_blog_batch": 7})
    invalidate_overrides(clear=True)
    eff = effective_settings("hf", dsn=settings.pg_dsn)
    assert eff.hf_request_interval == S().hf_request_interval


def test_override_is_scoped(pg, settings):
    from windex.config import Settings as S
    from windex.config import effective_settings, invalidate_overrides

    _write(pg, "hf", {"hf_blog_batch": 7})
    invalidate_overrides(clear=True)
    # Resolving a DIFFERENT source must not pick up hf's override.
    assert effective_settings("wiki", dsn=settings.pg_dsn).hf_blog_batch == \
        S().hf_blog_batch


def test_global_applies_to_every_scope(pg, settings):
    from windex.config import effective_settings, invalidate_overrides

    _write(pg, schema.GLOBAL, {"embed_order": "newest"})
    invalidate_overrides(clear=True)
    assert effective_settings("wiki", dsn=settings.pg_dsn).embed_order == "newest"
    assert effective_settings(None, dsn=settings.pg_dsn).embed_order == "newest"


def test_hand_edited_bad_value_is_clamped_on_read(pg, settings):
    """A row can be written by psql, restored from a backup, or left by an older
    schema. Values are re-validated on READ, because a bad one would otherwise be
    applied to every job."""
    from windex.config import effective_settings, invalidate_overrides

    _write(pg, "hf", {"hf_request_interval": 10 ** 9})
    invalidate_overrides(clear=True)
    assert effective_settings("hf", dsn=settings.pg_dsn).hf_request_interval == 60


def test_unknown_key_in_db_is_ignored_not_fatal(pg, settings):
    """A key removed from the allowlist in a later version must not break config
    resolution for everything else."""
    from windex.config import effective_settings, invalidate_overrides

    _write(pg, "hf", {"hf_blog_batch": 9, "some_retired_key": "x"})
    invalidate_overrides(clear=True)
    assert effective_settings("hf", dsn=settings.pg_dsn).hf_blog_batch == 9


DEAD_DSN = "postgresql://windex:windex@127.0.0.1:59999/nope"


def test_cold_start_with_unreachable_database_falls_back_to_env(settings):
    """Config resolution is on the path of every request and job; a Postgres blip
    must never raise. With nothing cached there is nothing to fall back to but
    env."""
    from windex.config import Settings as S
    from windex.config import effective_settings, invalidate_overrides

    invalidate_overrides(clear=True)
    assert effective_settings("hf", dsn=DEAD_DSN).hf_blog_batch == S().hf_blog_batch


def test_blip_retains_last_known_config(pg, settings):
    """Deliberate: reverting a running fleet to env values mid-outage would
    silently undo whatever was tuned — a worse failure than carrying on with the
    last good config."""
    from windex.config import effective_settings, invalidate_overrides

    _write(pg, "hf", {"hf_blog_batch": 9})
    invalidate_overrides(clear=True)
    assert effective_settings("hf", dsn=settings.pg_dsn).hf_blog_batch == 9  # warms

    invalidate_overrides()                     # expire, but keep the safety net
    assert effective_settings("hf", dsn=DEAD_DSN).hf_blog_batch == 9
