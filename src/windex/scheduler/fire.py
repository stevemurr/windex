"""Firing a trigger, and one tick of the scheduler.

The single most important property in this file is that **firing is one
transaction**:

    BEGIN
      SELECT … FROM triggers WHERE name = %s FOR UPDATE      -- exclusive, this row
      INSERT INTO runs …                ON CONFLICT DO NOTHING
      INSERT INTO run_tasks …           -- the whole fan-out, from the FROZEN spec
      UPDATE triggers SET last_fired_at, next_fire_at, last_run_id
      INSERT INTO run_events …
    COMMIT

What that replaces: `service.dispatch_entry` spawns a detached process and
`service._mark_ran` then issues a separate `UPDATE schedule SET last_run`. Those
are two round trips with no transaction around them. Anything that interrupts the
process between them — a SIGKILL, an OOM, or the transient host↔container TCP drop
that `db.Reconnecting` was written for after it took out a whole sweep on
2026-07-17 — leaves the entry looking un-run. The next tick is still inside the
same minute, `_is_due` says yes, and the job runs twice. Nobody sees it, because
it happens at 03:00 and both runs "succeed".

Here there is no gap. Either the run row, all of its tasks and the advanced
watermark are visible together, or none of them are.

Two more properties fall out of doing it this way:

**`FOR UPDATE` on the trigger row makes concurrency safe even without the
advisory lock.** `loop.py` guarantees a single ticker, but the guarantee is a
performance one, not a correctness one — during a rolling restart, or when
somebody runs `windex scheduler2 --once` by hand against a live box, two tickers
briefly overlap. The row lock means the loser blocks and then re-reads a row whose
`next_fire_at` has already moved past `now`, so it declines.

**A double-fire that still gets through is harmless.** `runs_dedupe_live_uniq`
(a partial UNIQUE on `dedupe_key` over the live states) turns the second INSERT
into a no-op, and the trigger is recorded as `trigger.coalesced` rather than
silently doing nothing. That is the layer that survives the case the row lock
cannot cover: a human clicking "Run now" at the same second the timer fires.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from windex.scheduler import events as ev
from windex.scheduler import pauses as pz
from windex.scheduler import triggers as tg
from windex.scheduler.triggers import CRON, EVENT, INTERVAL, Trigger

log = logging.getLogger("windex.scheduler")

# A node compiler: recipe spec (jsonb) -> the DAG's nodes. Supplied by the caller
# so this package never imports `windex.recipe.*` — the recipe engine is built in
# parallel, and a scheduler that cannot be tested until the compiler lands is a
# scheduler that gets tested by production.
CompileTasks = Callable[[dict], list[dict]]

# The live states `runs_dedupe_live_uniq` covers. Written out rather than
# interpolated so the ON CONFLICT predicate is textually identical to the index
# predicate in schema.sql — Postgres infers the index by matching the predicate,
# and a mismatch is a runtime "no unique or exclusion constraint matches", not a
# silent fallback.
LIVE_STATES = "('queued','running','blocked')"

# Keys `compile_tasks` may return for a node, with their defaults. A returned key
# outside this set is refused: silently dropping it would mean a node config that
# looks applied in the recipe and is absent in the row that actually runs.
_NODE_DEFAULTS: dict[str, Any] = {
    "lane": "io",
    "config": {},
    "depends_on": [],
    "preconditions": [],
    "weight": 1.0,
    "max_attempts": 3,
    "lease_seconds": 300,
}
_NODE_REQUIRED = ("node", "kind", "module")


class SchedulerError(Exception):
    """Base for the failures a tick isolates to one trigger instead of dying on."""


class UnknownTrigger(SchedulerError, KeyError):
    pass


class RecipeMissing(SchedulerError):
    """The trigger names a recipe that is not installed.

    Deliberately not a crash: `triggers.recipe` is intentionally not a foreign key
    (run history has to outlive an uninstalled recipe, and the same reasoning
    applies to the trigger that fed it), so a dangling reference is reachable and
    has to be reported as data, not as an exception in a log nobody tails.
    """


class BadNodes(SchedulerError):
    """`compile_tasks` returned something that is not a fannable node list."""


class TransactionInFlight(SchedulerError, RuntimeError):
    """The caller handed over a connection with uncommitted writes on it."""


# --- the atomicity guard ----------------------------------------------------

@contextmanager
def unit(conn: psycopg.Connection):
    """Open the **outermost** transaction on `conn`, and refuse to be nested.

    This exists because of a psycopg3 behaviour that is silent, correct by its own
    lights, and exactly wrong here: `Connection.transaction()` starts a real
    `BEGIN`/`COMMIT` only when the connection has no transaction open. If one is
    already open — and with `autocommit=False`, a *single previous `SELECT`* opens
    one implicitly — the block becomes a `SAVEPOINT`/`RELEASE` instead. Nothing
    raises, nothing logs, and the work is still uncommitted when the block exits.
    Measured on PG 16 + psycopg 3: rows written inside such a block were invisible
    to a second connection and vanished on the next `rollback()`.

    Which would quietly reintroduce the very defect this module was written to
    remove. "Firing is one transaction" is not a property you can assert in a
    docstring and leave to a library's nesting rules.

    So: if a transaction is open, decide by whether it has written anything.
    Postgres assigns an XID lazily, so `pg_current_xact_id_if_assigned()` is NULL
    for a transaction that has only read — that is provably safe to end, and it is
    the ordinary case (a scan that opened one implicitly). A transaction holding
    writes is the caller's, and discarding it silently would be worse than
    failing, so that raises.
    """
    status = conn.info.transaction_status
    if status == TransactionStatus.INERROR:
        conn.rollback()          # aborted: nothing to preserve, and nothing works until reset
    elif status not in (TransactionStatus.IDLE, TransactionStatus.UNKNOWN):
        with conn.cursor() as cur:
            cur.execute("SELECT pg_current_xact_id_if_assigned()")
            xid = cur.fetchone()[0]
        if xid is not None:
            raise TransactionInFlight(
                "the scheduler owns its connection's transaction boundaries: it was "
                f"handed one with uncommitted writes (xid {xid}). Commit or roll back "
                "before calling in, or give the scheduler its own connection — "
                "otherwise a fire would nest as a savepoint and never commit.")
        conn.rollback()          # read-only so far: provably nothing to lose
    with conn.transaction():
        yield


@dataclass(frozen=True)
class Fired:
    """The outcome of one fire attempt."""

    trigger: str
    recipe: str
    run_id: int | None      # None when coalesced — no new run was created
    tasks: int
    coalesced: bool = False
    blocked_by_run_id: int | None = None


@dataclass
class TickResult:
    """What one tick did, in the four categories an operator asks about."""

    fired: list[Fired] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)   # (trigger, why)
    deferred: list[tuple[str, str]] = field(default_factory=list)
    missed: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.fired or self.skipped or self.deferred
                    or self.missed or self.failed)

    def summary(self) -> str:
        parts = []
        if self.fired:
            parts.append("fired " + ", ".join(
                f"{f.trigger}→{f.run_id}" if f.run_id else f"{f.trigger} (coalesced)"
                for f in self.fired))
        for label, rows in (("skipped", self.skipped), ("deferred", self.deferred),
                            ("missed", self.missed), ("failed", self.failed)):
            if rows:
                parts.append(f"{label} " + ", ".join(f"{n} ({why})" for n, why in rows))
        return "; ".join(parts)


# --- events -----------------------------------------------------------------

def write_event(cur: psycopg.Cursor, event: str, *, run_id: int | None = None,
                task_id: int | None = None, level: str = "info",
                message: str = "", data: dict | None = None) -> None:
    """Append one `run_events` row.

    `ts` is left to the column default (the *database's* now()), never the
    caller's logical `now`. That is not fussiness: `run_events` is RANGE
    partitioned by `ts` with no DEFAULT partition (schema.sql explains why — a row
    in DEFAULT makes the next month's CREATE fail, converting a retention problem
    into an outage). A test or a backfill driving a logical clock outside the live
    partition window would otherwise get "no partition of relation found" from an
    *audit* write and lose the whole transaction it was recording. The logical
    instant goes in `data` where it costs nothing.

    The savepoint covers the other half of the same hazard: using the database's
    clock keeps a write inside the window, but the window itself runs out if
    `init-db` has not rolled it forward in months. An audit row must never be able
    to abort the transaction it exists to record, so the event is dropped and the
    fire commits.
    """
    cur.execute("SAVEPOINT windex_event")
    try:
        cur.execute(
            """INSERT INTO run_events (run_id, task_id, level, event, message, data)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (run_id, task_id, level, event, message, Jsonb(data or {})),
        )
    except psycopg.Error as exc:
        cur.execute("ROLLBACK TO SAVEPOINT windex_event")
        log.warning("run_events insert failed (%s); event %r dropped. Run "
                    "`windex init-db` if a partition is missing.", exc, event)
    else:
        cur.execute("RELEASE SAVEPOINT windex_event")


# --- reading ----------------------------------------------------------------

def load_trigger(cur: psycopg.Cursor, name: str, *, lock: bool = False) -> Trigger:
    """Read one trigger. `lock=True` takes a row-level exclusive lock that is held
    to the end of the enclosing transaction — the serialization point for two
    tickers racing over the same trigger."""
    cur.execute(
        f"SELECT {Trigger.COLUMNS} FROM triggers WHERE name = %s"
        + (" FOR UPDATE" if lock else ""),
        (name,),
    )
    row = cur.fetchone()
    if row is None:
        raise UnknownTrigger(name)
    return Trigger.from_row(row)


def due_triggers(cur: psycopg.Cursor, now: datetime, limit: int = 200) -> list[str]:
    """Names of the timed triggers whose planned instant has arrived.

    Names only, not rows: the row is re-read under `FOR UPDATE` inside the fire
    transaction, so a trigger edited (or already fired by another ticker) between
    the scan and the fire is evaluated against what is true at fire time, not
    against a snapshot the tick took while it was thinking.
    """
    cur.execute(
        """SELECT name FROM triggers
            WHERE enabled AND type IN ('cron','interval')
              AND next_fire_at IS NOT NULL AND next_fire_at <= %s
            ORDER BY next_fire_at
            LIMIT %s""",
        (now, limit),
    )
    return [r[0] for r in cur.fetchall()]


def plan_unarmed(conn: psycopg.Connection, now: datetime) -> list[str]:
    """Give every enabled timed trigger with a NULL `next_fire_at` one.

    Runs at the top of each tick because that is the only place that can: a
    trigger inserted by hand, by the marketplace installer, or by an operator
    re-enabling a disabled row has no planned instant, and `due_triggers`'
    `next_fire_at IS NOT NULL` predicate (which is what lets it use
    `triggers_due_idx`) would leave it dark forever. Arming here rather than in
    the INSERT means there is exactly one implementation of "when does this next
    run", so a hand-written row cannot disagree with a scheduled one.

    Also nulls out `next_fire_at` on `event`/`manual` triggers: a planned instant
    on a trigger nothing plans is a value the due-index would happily act on.
    """
    armed: list[str] = []
    with unit(conn):
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT {Trigger.COLUMNS} FROM triggers
                     WHERE enabled AND next_fire_at IS NULL AND type IN ('cron','interval')
                       FOR UPDATE SKIP LOCKED""")
            rows = [Trigger.from_row(r) for r in cur.fetchall()]
            for trig in rows:
                try:
                    tg.validate(trig)
                    nxt = tg.plan_next_fire(trig, now)
                except ValueError as exc:
                    # An invalid row must not stall the arming of the valid ones,
                    # and must not be retried silently every 10 s forever.
                    log.warning("trigger %s: cannot arm: %s", trig.name, exc)
                    write_event(cur, "trigger.failed", level="error",
                                message=str(exc), data={"trigger": trig.name})
                    continue
                cur.execute(
                    "UPDATE triggers SET next_fire_at = %s, updated_at = now() WHERE name = %s",
                    (nxt, trig.name))
                armed.append(trig.name)
            cur.execute(
                """UPDATE triggers SET next_fire_at = NULL, updated_at = now()
                    WHERE type IN ('event','manual') AND next_fire_at IS NOT NULL""")
    return armed


# --- the fan-out ------------------------------------------------------------

def _normalize_node(raw: Any, index: int) -> dict:
    """Validate one node dict from `compile_tasks` and fill its defaults.

    Strict about unknown keys. A compiler that renames `lane` to `queue` and gets
    silence would produce a run whose tasks all sit in the default `io` lane —
    a fleet-wide fairness bug presenting as "everything got slower", which is the
    hardest kind of bug to attribute. Better to refuse the fire.
    """
    if not isinstance(raw, dict):
        raise BadNodes(f"node #{index} is {type(raw).__name__}, expected dict")
    missing = [k for k in _NODE_REQUIRED if not raw.get(k)]
    if missing:
        raise BadNodes(f"node #{index} is missing required key(s): {', '.join(missing)}")
    unknown = set(raw) - set(_NODE_REQUIRED) - set(_NODE_DEFAULTS)
    if unknown:
        raise BadNodes(f"node {raw['node']!r}: unknown key(s) {', '.join(sorted(unknown))}")
    node = {k: raw[k] for k in _NODE_REQUIRED}
    for key, default in _NODE_DEFAULTS.items():
        node[key] = raw.get(key, default)
    return node


def _fan_out(cur: psycopg.Cursor, run_id: int, source: str, priority: int,
             nodes: list[dict]) -> int:
    """Insert the run's `run_tasks` rows. Returns the count.

    Root nodes (no `depends_on`) are inserted `ready`; everything else `pending`.
    The claim index is partial on `state = 'ready'`, so *something* has to promote
    the roots or the run queues and never starts. Doing it here, in the fire
    transaction, means a run is either fully absent or immediately claimable —
    there is no "queued but unstartable" state for a crash to leave behind.
    Dependency-driven promotion of the rest is the worker pool's job.
    """
    if not nodes:
        raise BadNodes("compile_tasks returned no nodes — a run with no work is a bug, "
                       "not an empty success")
    rows = []
    for node in nodes:
        rows.append((
            run_id, source, node["node"], node["kind"], node["module"], node["lane"],
            Jsonb(node["config"]), list(node["depends_on"]), list(node["preconditions"]),
            "ready" if not node["depends_on"] else "pending",
            priority, float(node["weight"]), int(node["max_attempts"]),
            int(node["lease_seconds"]),
        ))
    # executemany, not execute_values: the node count per run is ~3-10 and the
    # UNIQUE (run_id, node) violation on a duplicate node id has to abort the
    # whole transaction, which it does either way.
    cur.executemany(
        """INSERT INTO run_tasks
               (run_id, source, node, kind, module, lane, config, depends_on,
                preconditions, state, priority, weight, max_attempts, lease_seconds)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        rows,
    )
    return len(rows)


def _load_recipe(cur: psycopg.Cursor, name: str) -> tuple[str, int, dict, str]:
    cur.execute(
        "SELECT source, version, spec, spec_hash FROM recipes WHERE name = %s AND enabled",
        (name,))
    row = cur.fetchone()
    if row is None:
        raise RecipeMissing(name)
    source, version, spec, spec_hash = row
    return source, version, spec or {}, spec_hash or ""


# --- the primitive ----------------------------------------------------------

def fire_trigger(conn: psycopg.Connection, name: str, *, compile_tasks: CompileTasks,
                 now: datetime | None = None, advance: bool = False,
                 trigger_by: str = "", run_trigger: str | None = None,
                 params: dict | None = None) -> Fired:
    """Fire one trigger, unconditionally, in a single transaction.

    This is the primitive: it applies **no** policy. Due-ness, pauses and
    catch-up all live in `tick`; "Run now" from the console and an event fan-out
    call straight through. Keeping the policy out of here is what makes the
    transaction easy to read and impossible to half-apply.

    `advance=True` recomputes `next_fire_at` — the tick's behaviour. It defaults
    to False so that a manual fire at 14:00 does not move tonight's 03:00 run: an
    operator pressing "Run now" is asking for an *extra* run, not a rescheduled
    one, and quietly consuming the night's occurrence is the kind of surprise
    that gets noticed a week later as missing data.

    Raises UnknownTrigger, RecipeMissing, BadNodes, or ValueError (invalid row).
    Every one of those rolls the transaction back whole.
    """
    now = now or datetime.now(timezone.utc)
    params = params or {}
    with unit(conn):
        with conn.cursor() as cur:
            trig = load_trigger(cur, name, lock=True)
            tg.validate(trig)
            source, version, spec, spec_hash = _load_recipe(cur, trig.recipe)

            # compile_tasks runs INSIDE the transaction on purpose. It is pure and
            # fast (it walks a spec dict), and having it here means a compiler that
            # raises on a malformed spec aborts the fire rather than leaving a
            # `runs` row with no tasks — a run that can never start, never fail,
            # and holds the dedupe key against every future fire.
            nodes = [_normalize_node(n, i) for i, n in enumerate(compile_tasks(spec))]

            merged = {**trig.params, **params}
            dedupe_key = str(merged.get("dedupe_key") or trig.recipe)
            run_trigger = run_trigger or _runs_trigger_for(trig)

            cur.execute(
                f"""INSERT INTO runs (recipe, recipe_version, source, spec, spec_hash,
                                      trigger, trigger_by, params, mode, priority, dedupe_key)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'run', %s, %s)
                    ON CONFLICT (dedupe_key) WHERE state IN {LIVE_STATES} DO NOTHING
                    RETURNING id""",
                (trig.recipe, version, source, Jsonb(spec), spec_hash, run_trigger,
                 trigger_by or trig.name, Jsonb(merged), trig.priority, dedupe_key),
            )
            row = cur.fetchone()

            if row is None:
                # Coalesced by runs_dedupe_live_uniq. Still advance the watermark:
                # leaving next_fire_at in the past would re-attempt every 10 s for
                # as long as the blocking run lives, and each attempt writes an
                # event. The occurrence is spent either way — a run for this
                # recipe IS live, which is what the trigger was asking for.
                cur.execute(
                    f"SELECT id FROM runs WHERE dedupe_key = %s AND state IN {LIVE_STATES} "
                    f"ORDER BY id DESC LIMIT 1", (dedupe_key,))
                blocking = cur.fetchone()
                blocking_id = blocking[0] if blocking else None
                _advance(cur, trig, now, run_id=None, advance=advance)
                write_event(
                    cur, "trigger.coalesced", run_id=blocking_id, level="warn",
                    message=(f"{trig.name}: {trig.recipe} is already live as run "
                             f"{blocking_id} — this occurrence was coalesced"),
                    data={"trigger": trig.name, "recipe": trig.recipe,
                          "dedupe_key": dedupe_key, "blocking_run_id": blocking_id,
                          "at": now.isoformat()})
                return Fired(trig.name, trig.recipe, None, 0, coalesced=True,
                             blocked_by_run_id=blocking_id)

            run_id = row[0]
            n_tasks = _fan_out(cur, run_id, source, trig.priority, nodes)
            _advance(cur, trig, now, run_id=run_id, advance=advance)
            write_event(
                cur, "run.queued", run_id=run_id,
                message=f"{trig.recipe} queued by trigger {trig.name} ({trig.type})",
                data={"trigger": trig.name, "trigger_type": trig.type,
                      "recipe": trig.recipe, "source": source, "tasks": n_tasks,
                      "priority": trig.priority, "dedupe_key": dedupe_key,
                      "at": now.isoformat()})
            return Fired(trig.name, trig.recipe, run_id, n_tasks)


def _runs_trigger_for(trig: Trigger) -> str:
    """Map a trigger type onto the `runs.trigger` vocabulary
    (manual|schedule|event|chain|system)."""
    if trig.type in (CRON, INTERVAL):
        return "schedule"
    if trig.type == EVENT and trig.event:
        return ev.runs_trigger_column(trig.event)
    return "manual"


def _advance(cur: psycopg.Cursor, trig: Trigger, now: datetime,
             *, run_id: int | None, advance: bool) -> None:
    """Stamp `last_fired_at`/`last_run_id`, and re-plan `next_fire_at` when asked.

    The re-plan bases on `now` (the actual fire instant), not on the stored
    `next_fire_at`. That is what makes catch-up "once, not N times": after a week
    of downtime the stored value is 7 days stale, and planning from it would walk
    forward one occurrence, still be in the past, and fire again on the next tick
    — seven times, which is exactly the stampede `catch_up` exists to prevent.
    """
    nxt = tg.plan_next_fire(trig, now) if advance and trig.is_timed else None
    if advance and trig.is_timed:
        cur.execute(
            """UPDATE triggers SET last_fired_at = %s, next_fire_at = %s,
                      last_run_id = COALESCE(%s, last_run_id), updated_at = now()
                WHERE name = %s""",
            (now, nxt, run_id, trig.name))
    else:
        cur.execute(
            """UPDATE triggers SET last_fired_at = %s,
                      last_run_id = COALESCE(%s, last_run_id), updated_at = now()
                WHERE name = %s""",
            (now, run_id, trig.name))


def _rearm(cur: psycopg.Cursor, trig: Trigger, now: datetime) -> datetime | None:
    """Move `next_fire_at` past `now` WITHOUT firing — the skip/miss path."""
    nxt = tg.plan_next_fire(trig, now)
    cur.execute("UPDATE triggers SET next_fire_at = %s, updated_at = now() WHERE name = %s",
                (nxt, trig.name))
    return nxt


# --- the policy layer -------------------------------------------------------

def tick(conn: psycopg.Connection, *, compile_tasks: CompileTasks,
         now: datetime | None = None,
         grace_seconds: float = tg.DEFAULT_MISFIRE_GRACE,
         announced: dict[str, tuple] | None = None,
         limit: int = 200) -> TickResult:
    """One scheduler tick: arm anything unplanned, then evaluate everything due.

    Each trigger is its own transaction, so one bad recipe, one dangling
    reference or one compiler crash costs exactly that trigger — the rest of the
    night still runs. The 2026-07-17 post-mortem is the reason this is stated
    rather than assumed: a single component failing took every source down with
    it because nothing isolated it.

    `announced` is the caller's dict, carried across ticks by `loop.run_loop`, and
    it is a spam guard, not state the correctness depends on. Without it a paused
    30-second `interval` drain writes a `trigger.skipped` row every 30 s — 2,880
    audit rows a day saying the same thing — and a paused `catch_up` trigger
    writes one every 10 s. Passing None gives a throwaway dict, i.e. always
    announce, which is what tests and one-shot runs want.
    """
    now = now or datetime.now(timezone.utc)
    announced = {} if announced is None else announced
    result = TickResult()

    plan_unarmed(conn, now)

    with unit(conn), conn.cursor() as cur:
        names = due_triggers(cur, now, limit=limit)

    for name in names:
        try:
            _tick_one(conn, name, compile_tasks=compile_tasks, now=now,
                      grace_seconds=grace_seconds, announced=announced, result=result)
        except psycopg.OperationalError:
            # NOT a per-trigger failure — the connection itself is gone. Swallowing
            # it here would leave `run_loop` believing it still has a working
            # session, so it would keep "ticking" against a corpse and logging one
            # failure per trigger forever instead of reconnecting. Let it out.
            raise
        except Exception as exc:  # noqa: BLE001 — isolation is the whole point
            log.warning("trigger %s: fire failed: %s", name, exc)
            result.failed.append((name, str(exc)))
            _record_failure(conn, name, exc)
    return result


def _tick_one(conn: psycopg.Connection, name: str, *, compile_tasks: CompileTasks,
              now: datetime, grace_seconds: float, announced: dict,
              result: TickResult) -> None:
    """Decide what to do with one due trigger, then do it.

    The decision (pause / misfire / fire) is taken inside a transaction holding
    `FOR UPDATE` on the row, and the skip and miss branches complete inside that
    same transaction. The fire branch releases and re-enters via `fire_trigger`,
    which re-locks and re-reads: that costs one extra round trip and buys a single
    implementation of the fire transaction, used identically by the tick, by
    events and by "Run now". Two copies of that transaction is how they drift.
    """
    with unit(conn), conn.cursor() as cur:
        trig = load_trigger(cur, name, lock=True)
        # Re-check under the lock. Between the scan and here, another ticker may
        # have fired this trigger and pushed next_fire_at into the future, or an
        # operator may have disabled it.
        if not trig.enabled or trig.next_fire_at is None or trig.next_fire_at > now:
            return
        tg.validate(trig)

        source = _recipe_source(cur, trig.recipe)
        pause = pz.active_pause(cur, pz.scopes_for(trig.recipe, source), now)
        if pause is not None:
            _handle_paused(cur, trig, pause, now, announced, result)
            return

        if tg.is_misfire(trig, now, grace_seconds) and not trig.catch_up:
            # Downtime (or a pause that has since lifted) swallowed one or more
            # windows. catch_up=false means "the window is gone, do not make it
            # up" — but the miss is recorded, because today the equivalent is
            # total silence and the operator's only clue is stale data.
            late = (now - trig.next_fire_at).total_seconds()
            nxt = _rearm(cur, trig, now)
            why = f"missed by {int(late)}s (catch_up off)"
            write_event(cur, "trigger.missed", level="warn",
                        message=f"{trig.name}: due {trig.next_fire_at.isoformat()}, "
                                f"{int(late)}s late — re-armed for "
                                f"{nxt.isoformat() if nxt else 'never'}",
                        data={"trigger": trig.name, "recipe": trig.recipe,
                              "due_at": trig.next_fire_at.isoformat(),
                              "late_seconds": int(late),
                              "rearmed_to": nxt.isoformat() if nxt else None})
            result.missed.append((trig.name, why))
            return

    # Outside the transaction above: fire_trigger opens its own. A catch_up
    # trigger that missed N windows reaches here exactly once, because the fire
    # advances next_fire_at from `now` rather than from the stale value — N
    # missed windows collapse into one run, which is the whole point.
    fired = fire_trigger(conn, name, compile_tasks=compile_tasks, now=now, advance=True)
    announced.pop(name, None)   # a real fire clears any standing pause notice
    result.fired.append(fired)


def _handle_paused(cur: psycopg.Cursor, trig: Trigger, pause: pz.Pause,
                   now: datetime, announced: dict, result: TickResult) -> None:
    """A due trigger whose scope is paused. Two behaviours, chosen by `catch_up`.

    `catch_up=false` — **skip**. Re-arm past `now` so the occurrence is spent, and
    record `trigger.skipped` with the scope and the reason. That row is the
    feature: today a paused source shows an unexplained gap in the freshness
    panel, and "paused" and "broken" look identical from the console.

    `catch_up=true` — **defer**. Leave `next_fire_at` where it is, so the trigger
    stays due and fires the moment the pause lifts. Exactly once: the misfire
    branch never runs for a catch_up trigger, and the fire re-plans from `now`.
    """
    first = announced.get(trig.name) != pause.stamp
    announced[trig.name] = pause.stamp

    if trig.catch_up:
        result.deferred.append((trig.name, pause.describe()))
        if first:
            write_event(cur, "trigger.deferred", level="info",
                        message=f"{trig.name}: {pause.describe()} — deferring until resume",
                        data={"trigger": trig.name, "recipe": trig.recipe,
                              "scope": pause.scope, "reason": pause.reason,
                              "paused_by": pause.paused_by,
                              "due_at": trig.next_fire_at.isoformat()
                              if trig.next_fire_at else None,
                              "at": now.isoformat()})
        return

    due_at = trig.next_fire_at
    nxt = _rearm(cur, trig, now)
    result.skipped.append((trig.name, pause.describe()))
    if first:
        write_event(cur, "trigger.skipped", level="warn",
                    message=f"{trig.name}: not fired — {pause.describe()}",
                    data={"trigger": trig.name, "recipe": trig.recipe,
                          "scope": pause.scope, "reason": pause.reason,
                          "paused_by": pause.paused_by,
                          "due_at": due_at.isoformat() if due_at else None,
                          "rearmed_to": nxt.isoformat() if nxt else None,
                          "at": now.isoformat()})


def _recipe_source(cur: psycopg.Cursor, recipe: str) -> str:
    """The `documents.source` a recipe feeds, for pause-scope resolution. Falls
    back to the recipe name when the row is missing — the fire will raise
    RecipeMissing a moment later, and a pause on `source:<name>` should still be
    honoured in the meantime rather than being bypassed by a data error."""
    cur.execute("SELECT source FROM recipes WHERE name = %s", (recipe,))
    row = cur.fetchone()
    return (row[0] if row and row[0] else recipe)


def _record_failure(conn: psycopg.Connection, name: str, exc: Exception) -> None:
    """Write `trigger.failed` and push `next_fire_at` forward.

    Advancing on failure is deliberate. A dangling recipe reference or a compiler
    that raises would otherwise be re-attempted every 10 s forever, writing an
    error row each time — the failure buries the log it is reported in, and the
    box does measurable work doing it. One row per occurrence is enough to notice.
    Best-effort: if this write also fails the tick simply moves on, because the
    only thing worse than an unrecorded failure is a scheduler that dies recording
    one.
    """
    try:
        with unit(conn), conn.cursor() as cur:
            trig = load_trigger(cur, name, lock=True)
            if trig.is_timed:
                try:
                    _rearm(cur, trig, datetime.now(timezone.utc))
                except ValueError:
                    # The row itself is unevaluatable (bad cron/zone); disarming
                    # stops the hot loop and plan_unarmed will report it again.
                    cur.execute("UPDATE triggers SET next_fire_at = NULL, updated_at = now() "
                                "WHERE name = %s", (name,))
            write_event(cur, "trigger.failed", level="error",
                        message=f"{name}: {type(exc).__name__}: {exc}",
                        data={"trigger": name, "recipe": trig.recipe,
                              "error": f"{type(exc).__name__}: {exc}"})
    except Exception:  # noqa: BLE001 — recording a failure must never raise
        log.exception("trigger %s: could not record failure", name)


# --- events -----------------------------------------------------------------

def emit_event(conn: psycopg.Connection, event: str, *, compile_tasks: CompileTasks,
               now: datetime | None = None, trigger_by: str = "",
               params: dict | None = None,
               announced: dict[str, tuple] | None = None) -> TickResult:
    """Fire every enabled trigger listening for `event`.

    The event name is validated first and hard — this is the boundary described in
    `events.py`. An unknown name raises rather than matching nothing, because a
    chain that silently never fires is invisible: the downstream recipe just has
    stale data and no error anywhere says why.

    Pause is honoured the same way the tick honours it, minus the catch-up
    branch: an event is a *moment*, not a window, so there is nothing to defer to.
    A pushed document that arrives during a pause is not lost — the pushed rows
    are already durable and the next fire picks them up.
    """
    ev.validate_event(event)
    now = now or datetime.now(timezone.utc)
    announced = {} if announced is None else announced
    result = TickResult()

    with unit(conn), conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM triggers WHERE enabled AND type = 'event' AND event = %s "
            "ORDER BY name", (event,))
        names = [r[0] for r in cur.fetchall()]

    for name in names:
        try:
            with unit(conn), conn.cursor() as cur:
                trig = load_trigger(cur, name, lock=True)
                source = _recipe_source(cur, trig.recipe)
                pause = pz.active_pause(cur, pz.scopes_for(trig.recipe, source), now)
                if pause is not None:
                    if announced.get(name) != pause.stamp:
                        announced[name] = pause.stamp
                        write_event(cur, "trigger.skipped", level="warn",
                                    message=f"{name}: not fired on {event} — "
                                            f"{pause.describe()}",
                                    data={"trigger": name, "recipe": trig.recipe,
                                          "event": event, "scope": pause.scope,
                                          "reason": pause.reason,
                                          "at": now.isoformat()})
                    result.skipped.append((name, pause.describe()))
                    continue
            result.fired.append(fire_trigger(
                conn, name, compile_tasks=compile_tasks, now=now,
                trigger_by=trigger_by or event, params=params))
            announced.pop(name, None)
        except psycopg.OperationalError:
            raise            # the connection, not the trigger — see `tick`
        except Exception as exc:  # noqa: BLE001 — one listener must not kill the rest
            log.warning("trigger %s: event %s failed: %s", name, event, exc)
            result.failed.append((name, str(exc)))
            _record_failure(conn, name, exc)
    return result
