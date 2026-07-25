"""Firing, against the live Postgres. The properties here are the ones that can
only be checked with a real transaction and a real partial unique index.

`compile_tasks` is a fake throughout — the recipe engine is a parallel workstream,
and the scheduler takes the node list as a callable precisely so it does not have
to wait for it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import time
from zoneinfo import ZoneInfo

import psycopg
import pytest
from psycopg.types.json import Jsonb

from windex.scheduler import fire as fr
from windex.scheduler.fire import (
    BadNodes,
    RecipeMissing,
    TransactionInFlight,
    emit_event,
    fire_trigger,
    tick,
)
from windex.scheduler.loop import try_scheduler_lock

UTC = timezone.utc
NOW = datetime(2026, 7, 24, 3, 0, tzinfo=UTC)


# --- fakes and helpers -------------------------------------------------------

def fake_nodes(spec: dict) -> list[dict]:
    """A three-node DAG: one root plus two dependents. The shape matters for the
    fan-out assertions — exactly one node should land `ready`."""
    return [
        {"node": "discover", "kind": "discover", "module": "test.discover",
         "lane": "io", "weight": 0.1},
        {"node": "fetch", "kind": "fetch", "module": "http.get", "lane": "net",
         "depends_on": ["discover"], "preconditions": ["gateway"], "weight": 0.7},
        {"node": "embed", "kind": "load", "module": "embed.queue", "lane": "gpu",
         "depends_on": ["fetch"], "weight": 0.2},
    ]


def duplicate_nodes(spec: dict) -> list[dict]:
    """Two nodes with the same id. The first INSERT succeeds, the second violates
    `UNIQUE (run_id, node)` — a failure that lands strictly *between* the `runs`
    row and a complete fan-out, which is the exact window the old
    dispatch/_mark_ran pair could not survive."""
    return [
        {"node": "discover", "kind": "discover", "module": "test.discover"},
        {"node": "discover", "kind": "fetch", "module": "http.get"},
    ]


def exploding_compiler(spec: dict) -> list[dict]:
    raise RuntimeError("compiler blew up")


def make_recipe(pg, name="wiki", source=None, spec=None, version=3):
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO recipes (name, source, spec, spec_hash, version)
               VALUES (%s, %s, %s, %s, %s) ON CONFLICT (name) DO NOTHING""",
            (name, source or name, Jsonb(spec if spec is not None else {}),
             f"sha1:{name}", version))
    pg.commit()


def make_trigger(pg, name="nightly", recipe="wiki", **kw):
    cols = {"type": "cron", "cron": "0 3 * * *", "interval_seconds": None,
            "timezone": "UTC", "event": None, "priority": 50, "jitter_seconds": 0,
            "catch_up": False, "enabled": True, "next_fire_at": None,
            "last_fired_at": None}
    cols.update(kw)
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO triggers (name, recipe, type, cron, interval_seconds,
                                     timezone, event, priority, jitter_seconds,
                                     catch_up, enabled, next_fire_at, last_fired_at)
               VALUES (%(n)s, %(r)s, %(type)s, %(cron)s, %(interval_seconds)s,
                       %(timezone)s, %(event)s, %(priority)s, %(jitter_seconds)s,
                       %(catch_up)s, %(enabled)s, %(next_fire_at)s, %(last_fired_at)s)""",
            {"n": name, "r": recipe, **cols})
    pg.commit()


def pause(pg, scope, reason="operator", expires_at=None):
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO pauses (scope, reason, paused_by, expires_at)
               VALUES (%s, %s, 'pytest', %s)
               ON CONFLICT (scope) DO UPDATE SET reason = EXCLUDED.reason,
                                                 expires_at = EXCLUDED.expires_at""",
            (scope, reason, expires_at))
    pg.commit()


def q(pg, sql, args=()):
    """Read helper. Always ends the implicit transaction it opens, so the next
    scheduler call sees an idle connection (see fire.unit)."""
    with pg.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()
    pg.rollback()
    return rows


def trigger_row(pg, name="nightly"):
    r = q(pg, "SELECT next_fire_at, last_fired_at, last_run_id FROM triggers WHERE name = %s",
          (name,))[0]
    return {"next_fire_at": r[0], "last_fired_at": r[1], "last_run_id": r[2]}


def events(pg, event=None):
    if event:
        return q(pg, "SELECT event, level, data FROM run_events WHERE event = %s ORDER BY seq",
                 (event,))
    return q(pg, "SELECT event, level, data FROM run_events ORDER BY seq")


def finish_runs(pg, state="succeeded"):
    """Move every live run to a terminal state, releasing the dedupe key. Used to
    isolate catch-up behaviour from `runs_dedupe_live_uniq`, which would otherwise
    make every one of these tests pass for the wrong reason."""
    with pg.cursor() as cur:
        cur.execute("UPDATE runs SET state = %s, finished_at = now()", (state,))
    pg.commit()


# --- the atomic fire ---------------------------------------------------------

def test_fire_writes_run_tasks_and_watermark_in_one_transaction(pg):
    make_recipe(pg, "wiki", source="wiki", spec={"nodes": []}, version=7)
    make_trigger(pg, next_fire_at=NOW)

    fired = fire_trigger(pg, "nightly", compile_tasks=fake_nodes, now=NOW, advance=True)

    assert fired.run_id is not None and fired.tasks == 3 and not fired.coalesced

    run = q(pg, """SELECT recipe, recipe_version, source, spec, spec_hash, trigger,
                          trigger_by, mode, priority, dedupe_key, state
                     FROM runs WHERE id = %s""", (fired.run_id,))[0]
    assert run[:3] == ("wiki", 7, "wiki")
    assert run[3] == {"nodes": []}          # the FROZEN spec, copied onto the run
    assert run[4] == "sha1:wiki"
    assert run[5:] == ("schedule", "nightly", "run", 50, "wiki", "queued")

    tasks = q(pg, """SELECT node, kind, module, lane, state, depends_on, preconditions,
                            weight, priority, source
                       FROM run_tasks WHERE run_id = %s ORDER BY node""", (fired.run_id,))
    assert [t[0] for t in tasks] == ["discover", "embed", "fetch"]
    # Exactly the roots are claimable; the claim index is partial on 'ready', so a
    # run whose roots stayed 'pending' would queue and never start.
    assert {t[0]: t[4] for t in tasks} == {
        "discover": "ready", "fetch": "pending", "embed": "pending"}
    by_node = {t[0]: t for t in tasks}
    assert by_node["fetch"][3] == "net" and by_node["fetch"][6] == ["gateway"]
    assert by_node["embed"][5] == ["fetch"]
    assert all(t[9] == "wiki" for t in tasks)      # source denormalized onto every task

    row = trigger_row(pg)
    assert row["last_fired_at"] == NOW
    assert row["last_run_id"] == fired.run_id
    assert row["next_fire_at"] == NOW + timedelta(days=1)

    ev = events(pg, "run.queued")
    assert len(ev) == 1 and ev[0][2]["trigger"] == "nightly" and ev[0][2]["tasks"] == 3


def test_fire_materializes_saved_config_and_flow_for_richer_compiler(pg):
    spec = {
        "config": [
            {"key": "batch", "kind": "int", "default": 10},
            {"key": "host", "kind": "str"},
        ],
    }
    make_recipe(pg, "wiki", spec=spec)
    make_trigger(pg)
    with pg.cursor() as cur:
        cur.execute(
            """INSERT INTO recipe_config (recipe, values)
               VALUES ('wiki', '{"batch": 25, "host": "saved.example"}')""")
    pg.commit()
    seen = {}

    def richer(document, *, values=None, flow=None):
        seen.update({"spec": document, "values": values, "flow": flow})
        return fake_nodes(document)

    fired = fire_trigger(
        pg,
        "nightly",
        compile_tasks=richer,
        params={
            "batch": 50,
            "flow": "refresh",
            "dedupe_key": "wiki:refresh",
            "scheduler_note": "not recipe config",
        },
    )

    assert fired.run_id is not None
    assert seen == {
        "spec": spec,
        "values": {"batch": 50, "host": "saved.example"},
        "flow": "refresh",
    }
    run_params = q(
        pg, "SELECT params FROM runs WHERE id = %s", (fired.run_id,))[0][0]
    assert run_params["scheduler_note"] == "not recipe config"
    assert run_params["host"] == "saved.example"


def test_a_failure_mid_fan_out_persists_nothing(pg):
    """The property the old `dispatch_entry` + `_mark_ran` pair could not have.

    The `runs` row and the first `run_tasks` row are already written when the
    duplicate node violates `UNIQUE (run_id, node)`. Everything must vanish —
    including the watermark advance, because a half-fired trigger that *looks*
    fired is how a nightly job silently stops running.
    """
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    before = trigger_row(pg)

    with pytest.raises(psycopg.errors.UniqueViolation):
        fire_trigger(pg, "nightly", compile_tasks=duplicate_nodes, now=NOW, advance=True)

    assert q(pg, "SELECT count(*) FROM runs")[0][0] == 0
    assert q(pg, "SELECT count(*) FROM run_tasks")[0][0] == 0
    assert q(pg, "SELECT count(*) FROM run_events")[0][0] == 0
    assert trigger_row(pg) == before          # watermark untouched: it will retry


def test_a_compiler_that_raises_persists_nothing(pg):
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    before = trigger_row(pg)

    with pytest.raises(RuntimeError, match="compiler blew up"):
        fire_trigger(pg, "nightly", compile_tasks=exploding_compiler, now=NOW, advance=True)

    assert q(pg, "SELECT count(*) FROM runs")[0][0] == 0
    assert trigger_row(pg) == before


def test_an_empty_node_list_is_refused(pg):
    """A run with no tasks can never start, never fail, and holds the dedupe key
    against every future fire — an infinitely stuck recipe."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    with pytest.raises(BadNodes, match="no nodes"):
        fire_trigger(pg, "nightly", compile_tasks=lambda spec: [], now=NOW)
    assert q(pg, "SELECT count(*) FROM runs")[0][0] == 0


def test_unknown_node_keys_are_refused(pg):
    """A compiler that renames `lane` to `queue` would otherwise put every task in
    the default `io` lane — a fleet-wide fairness bug presenting as 'everything
    got slower'."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    bad = lambda spec: [{"node": "a", "kind": "fetch", "module": "http.get",  # noqa: E731
                         "queue": "net"}]
    with pytest.raises(BadNodes, match="unknown key"):
        fire_trigger(pg, "nightly", compile_tasks=bad, now=NOW)


def test_manual_fire_does_not_consume_tonights_occurrence(pg):
    """`advance=False` is the "Run now" path: an extra run, not a rescheduled one."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW + timedelta(hours=15))
    fired = fire_trigger(pg, "nightly", compile_tasks=fake_nodes,
                         now=NOW, trigger_by="operator@console")
    row = trigger_row(pg)
    assert row["next_fire_at"] == NOW + timedelta(hours=15)
    assert row["last_fired_at"] == NOW and row["last_run_id"] == fired.run_id
    assert q(pg, "SELECT trigger_by FROM runs WHERE id = %s",
             (fired.run_id,))[0][0] == "operator@console"


# --- dedupe ------------------------------------------------------------------

def test_a_double_fire_is_coalesced_not_duplicated(pg):
    """`runs_dedupe_live_uniq`: a human clicking Run at the same second the timer
    fires must not produce two ingests of the same corpus."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)

    first = fire_trigger(pg, "nightly", compile_tasks=fake_nodes, now=NOW, advance=True)
    second = fire_trigger(pg, "nightly", compile_tasks=fake_nodes, now=NOW, advance=True)

    assert second.coalesced and second.run_id is None
    assert second.blocked_by_run_id == first.run_id
    assert q(pg, "SELECT count(*) FROM runs")[0][0] == 1
    assert q(pg, "SELECT count(*) FROM run_tasks")[0][0] == 3

    ev = events(pg, "trigger.coalesced")
    assert len(ev) == 1 and ev[0][1] == "warn"
    assert ev[0][2]["blocking_run_id"] == first.run_id

    # The occurrence is still spent — otherwise it re-attempts every 10 s for as
    # long as the blocking run lives.
    assert trigger_row(pg)["next_fire_at"] == NOW + timedelta(days=1)


def test_dedupe_releases_once_the_run_reaches_a_terminal_state(pg):
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    first = fire_trigger(pg, "nightly", compile_tasks=fake_nodes, now=NOW)
    finish_runs(pg)
    second = fire_trigger(pg, "nightly", compile_tasks=fake_nodes, now=NOW)
    assert not second.coalesced and second.run_id != first.run_id


# --- pauses ------------------------------------------------------------------

@pytest.mark.parametrize("scope", ["global", "source:wiki", "recipe:wiki"])
def test_a_paused_scope_is_not_fired_into_and_says_why(pg, scope):
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    pause(pg, scope, reason="GPU freed for interactive search")

    result = tick(pg, compile_tasks=fake_nodes, now=NOW)

    assert not result.fired and len(result.skipped) == 1
    assert q(pg, "SELECT count(*) FROM runs")[0][0] == 0

    ev = events(pg, "trigger.skipped")
    assert len(ev) == 1 and ev[0][1] == "warn"
    data = ev[0][2]
    assert data["scope"] == scope
    assert data["reason"] == "GPU freed for interactive search"
    assert data["trigger"] == "nightly" and data["recipe"] == "wiki"
    # The occurrence is spent: catch_up=false means a paused window is gone.
    assert trigger_row(pg)["next_fire_at"] == NOW + timedelta(days=1)


def test_a_pause_on_a_different_source_does_not_suppress(pg):
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    pause(pg, "source:ccnews")
    assert len(tick(pg, compile_tasks=fake_nodes, now=NOW).fired) == 1


def test_a_lane_pause_does_not_suppress_the_fire(pg):
    """`lane:gpu` means 'do not execute this kind of work', not 'do not plan it'.
    The run should queue and drain when the lane reopens; suppressing the fire
    would turn a throttle into data loss."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    pause(pg, "lane:gpu")
    assert len(tick(pg, compile_tasks=fake_nodes, now=NOW).fired) == 1


def test_an_expired_pause_no_longer_suppresses(pg):
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    pause(pg, "source:wiki", expires_at=NOW - timedelta(minutes=1))
    assert len(tick(pg, compile_tasks=fake_nodes, now=NOW).fired) == 1


def test_a_recipe_feeding_a_paused_source_under_another_name_is_suppressed(pg):
    """Pausing a *source* must stop every recipe that writes into it, not just the
    one that happens to share its name."""
    make_recipe(pg, "wiki_backfill", source="wiki")
    make_trigger(pg, recipe="wiki_backfill", next_fire_at=NOW)
    pause(pg, "source:wiki")
    assert not tick(pg, compile_tasks=fake_nodes, now=NOW).fired


def test_the_skip_notice_is_written_once_per_pause_episode(pg):
    """A paused 30 s interval drain would otherwise write 2,880 identical audit
    rows a day."""
    make_recipe(pg, "embed_drain", source="wiki")
    make_trigger(pg, name="drain", recipe="embed_drain", type="interval",
                 cron=None, interval_seconds=30, next_fire_at=NOW)
    pause(pg, "global", reason="maintenance window")

    announced: dict = {}
    t = NOW
    for _ in range(5):
        tick(pg, compile_tasks=fake_nodes, now=t, announced=announced)
        t += timedelta(seconds=30)
    assert len(events(pg, "trigger.skipped")) == 1

    # A NEW pause episode announces again.
    pause(pg, "global", reason="second maintenance window")
    tick(pg, compile_tasks=fake_nodes, now=t, announced=announced)
    ev = events(pg, "trigger.skipped")
    assert len(ev) == 2 and ev[1][2]["reason"] == "second maintenance window"


def test_catch_up_defers_through_a_pause_and_fires_exactly_once_on_resume(pg):
    """The 'pause a week, unpause, 84 runs stampede' hazard."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW, catch_up=True)
    pause(pg, "source:wiki", reason="disk full")

    announced: dict = {}
    t = NOW
    for _ in range(4):          # four nights paused
        r = tick(pg, compile_tasks=fake_nodes, now=t, announced=announced)
        assert not r.fired and r.deferred
        # Deferral leaves the due time exactly where it was.
        assert trigger_row(pg)["next_fire_at"] == NOW
        t += timedelta(days=1)

    deferred = events(pg, "trigger.deferred")
    assert len(deferred) == 1 and deferred[0][2]["reason"] == "disk full"

    with pg.cursor() as cur:
        cur.execute("DELETE FROM pauses")
    pg.commit()

    r = tick(pg, compile_tasks=fake_nodes, now=t, announced=announced)
    assert len(r.fired) == 1
    finish_runs(pg)
    # ONCE. Not once per missed night.
    for _ in range(3):
        assert not tick(pg, compile_tasks=fake_nodes, now=t, announced=announced).fired
    assert q(pg, "SELECT count(*) FROM runs")[0][0] == 1
    assert trigger_row(pg)["next_fire_at"] > t


# --- catch-up ----------------------------------------------------------------

def test_catch_up_false_drops_missed_windows_but_records_them(pg):
    """Today a nine-hour outage over 03:00 means the nightly job never runs and
    nothing anywhere says so."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW - timedelta(days=7))

    r = tick(pg, compile_tasks=fake_nodes, now=NOW)

    assert not r.fired and len(r.missed) == 1
    assert q(pg, "SELECT count(*) FROM runs")[0][0] == 0
    ev = events(pg, "trigger.missed")
    assert len(ev) == 1 and ev[0][1] == "warn"
    assert ev[0][2]["late_seconds"] == 7 * 86400
    assert trigger_row(pg)["next_fire_at"] > NOW


def test_catch_up_true_fires_once_for_n_missed_windows(pg):
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW - timedelta(days=7), catch_up=True)

    assert len(tick(pg, compile_tasks=fake_nodes, now=NOW).fired) == 1
    finish_runs(pg)
    for _ in range(5):
        assert not tick(pg, compile_tasks=fake_nodes, now=NOW).fired
    assert q(pg, "SELECT count(*) FROM runs")[0][0] == 1


def test_ordinary_tick_latency_is_not_a_misfire(pg):
    """A due time a few seconds in the past is the normal case for a 10 s tick,
    and must fire in both modes rather than being written off as a miss."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW - timedelta(seconds=8))
    r = tick(pg, compile_tasks=fake_nodes, now=NOW)
    assert len(r.fired) == 1 and not r.missed


# --- next_fire_at is absolute ------------------------------------------------

def test_next_fire_at_round_trips_absolute_across_a_dst_boundary(pg):
    """Stored as an instant, not as wall-clock text: 03:00 Europe/London is 03:00Z
    the night before the switch and 02:00Z the night after, and both come back out
    of Postgres meaning exactly that."""
    make_recipe(pg, "wiki")
    make_trigger(pg, timezone="Europe/London", next_fire_at=None)
    london = ZoneInfo("Europe/London")

    fr.plan_unarmed(pg, datetime(2026, 3, 27, 12, 0, tzinfo=UTC))
    got = trigger_row(pg)["next_fire_at"]
    assert got.tzinfo is not None
    assert got == datetime(2026, 3, 28, 3, 0, tzinfo=UTC)       # GMT
    assert got.astimezone(london).hour == 3

    fired = fire_trigger(pg, "nightly", compile_tasks=fake_nodes,
                         now=datetime(2026, 3, 28, 3, 0, tzinfo=UTC), advance=True)
    assert fired.run_id
    got = trigger_row(pg)["next_fire_at"]
    assert got == datetime(2026, 3, 29, 2, 0, tzinfo=UTC)       # BST — same wall clock
    assert got.astimezone(london).hour == 3


# --- interval ----------------------------------------------------------------

def test_interval_trigger_fires_on_its_period(pg):
    make_recipe(pg, "embed_drain", source="wiki")
    make_trigger(pg, name="drain", recipe="embed_drain", type="interval", cron=None,
                 interval_seconds=30, next_fire_at=NOW)

    assert len(tick(pg, compile_tasks=fake_nodes, now=NOW).fired) == 1
    assert trigger_row(pg, "drain")["next_fire_at"] == NOW + timedelta(seconds=30)
    finish_runs(pg)

    assert not tick(pg, compile_tasks=fake_nodes, now=NOW + timedelta(seconds=15)).fired
    assert len(tick(pg, compile_tasks=fake_nodes,
                    now=NOW + timedelta(seconds=30)).fired) == 1


def test_interval_trigger_is_armed_from_null(pg):
    make_recipe(pg, "embed_drain", source="wiki")
    make_trigger(pg, name="drain", recipe="embed_drain", type="interval", cron=None,
                 interval_seconds=60, next_fire_at=None)
    assert fr.plan_unarmed(pg, NOW) == ["drain"]
    assert trigger_row(pg, "drain")["next_fire_at"] == NOW + timedelta(seconds=60)


# --- arming ------------------------------------------------------------------

def test_event_and_manual_triggers_are_disarmed(pg):
    """A planned instant on a trigger nothing plans is a value the due-index would
    happily act on."""
    make_recipe(pg, "memory")
    make_trigger(pg, name="on-push", recipe="memory", type="event", cron=None,
                 event="source.pushed:memory", next_fire_at=NOW)
    fr.plan_unarmed(pg, NOW)
    assert trigger_row(pg, "on-push")["next_fire_at"] is None
    assert not tick(pg, compile_tasks=fake_nodes, now=NOW).fired


def test_a_disabled_trigger_never_fires(pg):
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW, enabled=False)
    assert not tick(pg, compile_tasks=fake_nodes, now=NOW).fired


def test_an_unevaluatable_row_is_reported_not_raised(pg):
    """`triggers` carries no CHECK constraints, so a row hand-edited in psql
    reaches the tick unvalidated. One bad row must not stop the good ones."""
    make_recipe(pg, "wiki")
    make_recipe(pg, "hn")
    make_trigger(pg, name="broken", recipe="wiki", cron="99 3 * * *")
    make_trigger(pg, name="good", recipe="hn", next_fire_at=NOW)

    r = tick(pg, compile_tasks=fake_nodes, now=NOW)
    assert [f.trigger for f in r.fired] == ["good"]
    ev = events(pg, "trigger.failed")
    assert len(ev) == 1 and ev[0][2]["trigger"] == "broken"


# --- failure isolation -------------------------------------------------------

def test_a_dangling_recipe_reference_is_data_not_a_crash(pg):
    """`triggers.recipe` is deliberately not a foreign key, so a dangling
    reference is reachable and has to be reported as a row."""
    make_trigger(pg, recipe="uninstalled", next_fire_at=NOW)

    r = tick(pg, compile_tasks=fake_nodes, now=NOW)
    assert not r.fired and len(r.failed) == 1

    ev = events(pg, "trigger.failed")
    assert len(ev) == 1 and ev[0][1] == "error"
    assert "RecipeMissing" in ev[0][2]["error"]
    # Advanced, so it does not re-attempt (and re-log) every 10 s forever.
    assert trigger_row(pg)["next_fire_at"] > NOW


def test_a_disabled_recipe_does_not_fire(pg):
    make_recipe(pg, "wiki")
    with pg.cursor() as cur:
        cur.execute("UPDATE recipes SET enabled = false WHERE name = 'wiki'")
    pg.commit()
    make_trigger(pg, next_fire_at=NOW)
    with pytest.raises(RecipeMissing):
        fire_trigger(pg, "nightly", compile_tasks=fake_nodes, now=NOW)


def test_one_bad_trigger_does_not_abort_the_tick(pg):
    make_recipe(pg, "wiki")
    make_trigger(pg, name="a-broken", recipe="ghost", next_fire_at=NOW)
    make_trigger(pg, name="b-fine", recipe="wiki", next_fire_at=NOW)
    r = tick(pg, compile_tasks=fake_nodes, now=NOW)
    assert [f.trigger for f in r.fired] == ["b-fine"]
    assert [n for n, _ in r.failed] == ["a-broken"]


# --- events ------------------------------------------------------------------

def test_event_trigger_chains_off_a_finished_run(pg):
    """Replaces the `&&` in `cli.REFRESH_CHAINS`: a chain becomes a row with a
    `chain` provenance instead of an exit code in refresh.log."""
    make_recipe(pg, "wiki_ingest", source="wiki")
    make_trigger(pg, name="after-sync", recipe="wiki_ingest", type="event", cron=None,
                 event="run.succeeded:wiki_sync")

    r = emit_event(pg, "run.succeeded:wiki_sync", compile_tasks=fake_nodes, now=NOW)

    assert len(r.fired) == 1
    run = q(pg, "SELECT trigger, trigger_by FROM runs WHERE id = %s", (r.fired[0].run_id,))[0]
    assert run == ("chain", "run.succeeded:wiki_sync")


def test_push_and_boot_events_are_plain_events(pg):
    make_recipe(pg, "memory")
    make_recipe(pg, "maintain")
    make_trigger(pg, name="on-push", recipe="memory", type="event", cron=None,
                 event="source.pushed:memory")
    make_trigger(pg, name="on-boot", recipe="maintain", type="event", cron=None,
                 event="boot")

    r = emit_event(pg, "source.pushed:memory", compile_tasks=fake_nodes, now=NOW)
    assert len(r.fired) == 1
    assert q(pg, "SELECT trigger FROM runs WHERE id = %s",
             (r.fired[0].run_id,))[0][0] == "event"

    r = emit_event(pg, "boot", compile_tasks=fake_nodes, now=NOW)
    assert len(r.fired) == 1


def test_an_unknown_event_raises_rather_than_matching_nothing(pg):
    """A chain that silently never fires is invisible — the downstream recipe just
    has stale data and no error anywhere says why."""
    with pytest.raises(ValueError, match="unknown event kind"):
        emit_event(pg, "deploy.finished:wiki", compile_tasks=fake_nodes, now=NOW)


def test_an_event_into_a_paused_scope_is_skipped(pg):
    make_recipe(pg, "memory")
    make_trigger(pg, name="on-push", recipe="memory", type="event", cron=None,
                 event="source.pushed:memory")
    pause(pg, "source:memory", reason="reindexing")

    r = emit_event(pg, "source.pushed:memory", compile_tasks=fake_nodes, now=NOW)
    assert not r.fired and len(r.skipped) == 1
    assert events(pg, "trigger.skipped")[0][2]["event"] == "source.pushed:memory"


def test_an_event_does_not_disturb_a_timed_trigger_for_the_same_recipe(pg):
    make_recipe(pg, "wiki")
    make_trigger(pg, name="nightly", recipe="wiki", next_fire_at=NOW + timedelta(hours=10))
    make_trigger(pg, name="on-push", recipe="wiki", type="event", cron=None,
                 event="source.pushed:wiki")
    emit_event(pg, "source.pushed:wiki", compile_tasks=fake_nodes, now=NOW)
    assert trigger_row(pg, "nightly")["next_fire_at"] == NOW + timedelta(hours=10)


# --- the transaction guard ---------------------------------------------------

def test_a_connection_with_pending_writes_is_refused(pg):
    """psycopg nests `transaction()` as a SAVEPOINT when one is already open,
    which would silently make the fire non-atomic. Loud beats silent."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    with pg.cursor() as cur:
        cur.execute("INSERT INTO control (key, value) VALUES ('probe', '1')")
    with pytest.raises(TransactionInFlight, match="uncommitted writes"):
        fire_trigger(pg, "nightly", compile_tasks=fake_nodes, now=NOW)
    pg.rollback()


def test_a_read_only_open_transaction_is_tolerated(pg):
    """The common case: the caller ran a SELECT, which opens a transaction
    implicitly. It has written nothing, so ending it is provably lossless."""
    make_recipe(pg, "wiki")
    make_trigger(pg, next_fire_at=NOW)
    with pg.cursor() as cur:
        cur.execute("SELECT count(*) FROM runs")
        cur.fetchone()
    assert fire_trigger(pg, "nightly", compile_tasks=fake_nodes, now=NOW).run_id


# --- the singleton lock ------------------------------------------------------

def test_only_one_session_holds_the_scheduler_lock(pg, pg_dsn):
    """Two workers, one ticker. A file lock (jobs._spawn_lock's flock) cannot do
    this across containers; a Postgres advisory lock can, and it is released
    automatically when the holding session ends — including on SIGKILL."""
    a = psycopg.connect(pg_dsn)
    b = psycopg.connect(pg_dsn)
    try:
        assert try_scheduler_lock(a) is True
        assert try_scheduler_lock(b) is False
        assert try_scheduler_lock(a) is True      # re-entrant for the holder
        a.close()                                 # holder dies -> lock released
        # close() is client-side; the server frees the lock when it PROCESSES the
        # disconnect. Asserting immediately is a race that only passed because
        # earlier tests in this file added enough latency to hide it — so poll for
        # the property rather than for the scheduler's timing.
        deadline = time.monotonic() + 5.0
        while not try_scheduler_lock(b):
            assert time.monotonic() < deadline, \
                "lock not released within 5s of the holding session ending"
            time.sleep(0.05)
    finally:
        for c in (a, b):
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass
