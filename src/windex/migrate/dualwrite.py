"""Keep `source_units` current as the legacy pipeline keeps running.

Phase 2 proved the backfill matches *today*. This is what keeps it matching
*tomorrow*, for the week or two the two models run side by side before reads flip
per source.

WHY TRIGGERS AND NOT A PYTHON SHIM. The obvious implementation is a helper called
from each source's `mark()`. It is also the wrong one: there are ten sources with
a dozen write sites between them — `mark`, `reclaim_stale`, `retry-failed`, the
per-source status updates, the ad-hoc psql fix someone runs at 2am — and missing a
single one produces exactly the failure this whole phase exists to prevent, a unit
that quietly looks done. Application code can forget a call; a trigger cannot be
bypassed, and it is transactional by construction, so the mirror commits with the
write that caused it or not at all.

The cost is that the projection has to exist as SQL inside a trigger function. That
is paid by GENERATING the triggers from `watermarks.MAPPINGS` — the same `{WHERE}`
-marked INSERT the bulk backfill runs, filtered to the changed row. One definition
of what a unit means, used three ways (backfill, mirror, verify), so they cannot
drift apart.

This is deliberately reversible and inert: `disable()` drops every trigger and the
legacy tables are untouched throughout. Nothing reads `source_units` yet.
"""

from __future__ import annotations

import psycopg

from windex.migrate.watermarks import MAPPINGS, Mapping

# Prefix for everything this module creates, so `disable()` can find its own work
# and nothing else. Never DROP by pattern outside this namespace.
_PREFIX = "windex_mirror_"


def _fn(m: Mapping) -> str:
    return f"{_PREFIX}{m.table}"


def _row_filter(m: Mapping, alias: str) -> str:
    """`WHERE pk = NEW.pk` — the per-row restriction of the bulk projection."""
    return "WHERE " + " AND ".join(f"{m.table}.{c} = {alias}.{c}" for c in m.pk)


def _statements(m: Mapping) -> list[str]:
    """The trigger function + trigger for one legacy table."""
    insert = m.insert.replace("{WHERE}", _row_filter(m, "NEW"))
    # A legacy DELETE must remove the mirrored unit too, or a deleted row would
    # linger as permanently-pending work. gh_shards is the one that actually does
    # this today (the sweep prunes shards), but every table gets it: a mirror that
    # only ever grows is a slow-motion divergence.
    key_for_old = m.key_expr
    for col in m.pk:
        key_for_old = key_for_old.replace(col, f"OLD.{col}")
    delete = (f"DELETE FROM source_units WHERE source = '{m.recipe}' "
              f"AND store = '{m.store}' AND unit_key = ({key_for_old});")
    return [
        f"""
        CREATE OR REPLACE FUNCTION {_fn(m)}() RETURNS trigger
        LANGUAGE plpgsql AS $windex$
        BEGIN
            IF (TG_OP = 'DELETE') THEN
                {delete}
                RETURN OLD;
            END IF;
            {insert};
            RETURN NEW;
        END
        $windex$;
        """,
        f"DROP TRIGGER IF EXISTS {_fn(m)}_trg ON {m.table};",
        f"""
        CREATE TRIGGER {_fn(m)}_trg
        AFTER INSERT OR UPDATE OR DELETE ON {m.table}
        FOR EACH ROW EXECUTE FUNCTION {_fn(m)}();
        """,
    ]


def enable(conn: psycopg.Connection) -> list[str]:
    """Install the mirror triggers. Idempotent (CREATE OR REPLACE + DROP IF EXISTS)."""
    done = []
    with conn.cursor() as cur:
        for m in MAPPINGS:
            cur.execute(f"SELECT to_regclass('{m.table}')")
            if cur.fetchone()[0] is None:
                continue
            for stmt in _statements(m):
                cur.execute(stmt)
            done.append(m.table)
    conn.commit()
    return done


def disable(conn: psycopg.Connection) -> list[str]:
    """Remove every trigger this module installed. The legacy tables are left
    exactly as they were — dual-write is additive and this is its undo."""
    done = []
    with conn.cursor() as cur:
        for m in MAPPINGS:
            cur.execute(f"SELECT to_regclass('{m.table}')")
            if cur.fetchone()[0] is None:
                continue
            cur.execute(f"DROP TRIGGER IF EXISTS {_fn(m)}_trg ON {m.table}")
            cur.execute(f"DROP FUNCTION IF EXISTS {_fn(m)}()")
            done.append(m.table)
    conn.commit()
    return done


def status(conn: psycopg.Connection) -> list[dict]:
    """Which legacy tables currently mirror. Reported per table rather than as one
    boolean: a partially-installed state is the one worth seeing."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
            "WHERE NOT t.tgisinternal AND t.tgname LIKE %s", (_PREFIX + "%",))
        live = {r[0] for r in cur.fetchall()}
    return [{"table": m.table, "recipe": m.recipe, "store": m.store,
             "mirroring": m.table in live} for m in MAPPINGS]
