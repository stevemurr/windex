"""Pause scopes: which ones cover a recipe, and whether one is live right now.

Two independent mechanisms enforce a pause, on purpose:

1. The claim predicate in the worker pool refuses to lease a task whose scope is
   paused. That is the *correctness* layer, and it does not depend on the
   scheduler behaving.
2. The scheduler declines to create the run at all (this module). That is the
   *hygiene* layer — without it a week of pausing accumulates a week of queued
   runs that all become claimable the instant the pause lifts.

`runs_dedupe_live_uniq` bounds the damage of skipping layer 2 to one run per
recipe rather than 84, but "one surprise ingest per recipe on resume" is still
the wrong behaviour, and worse, the console shows a gap with no explanation. So
every suppressed fire writes an event carrying the scope and the reason, and the
question "why did nothing run last night" has an answer in the same stream as
everything else.

`lane:*` scopes are deliberately NOT consulted here. A lane pause (`lane:gpu`,
to free the GPU for interactive queries) means "do not *execute* this kind of
work", not "do not plan it" — the run should queue and drain when the lane
reopens. Suppressing the fire would turn a throttle into data loss.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg

GLOBAL = "global"


@dataclass(frozen=True)
class Pause:
    scope: str
    reason: str
    paused_by: str
    paused_at: datetime
    expires_at: datetime | None

    def describe(self) -> str:
        """One line for a log or an event message. Always names the scope, because
        "paused" without "by what" is the message that sends an operator to the
        wrong console screen."""
        return f"{self.scope}" + (f" ({self.reason})" if self.reason else "")

    @property
    def stamp(self) -> tuple:
        """Identity of this pause *episode*, for the once-per-episode announcement
        guard in `fire`.

        Covers exactly what the announcement would say — scope, reason, and when
        it started. Keying on `paused_at` alone is not enough: an operator who
        edits the reason of a standing pause ("disk full" → "disk full, waiting on
        the replacement") has changed the only sentence the UI shows, and
        suppressing that as a duplicate would leave the stale wording on screen
        indefinitely.
        """
        return (self.scope, self.paused_at, self.reason)


def scopes_for(recipe: str, source: str) -> list[str]:
    """The pause scopes that suppress this recipe, most general first.

    `source` and `recipe` are usually equal (a recipe is named for the source it
    feeds), but not always — the system recipes (`_embed`, `_reclaim`) and any
    recipe with a `source` override break the identity, and pausing a *source*
    must stop every recipe that writes into it, not just the one that shares its
    name. Order is precedence order, so the reported reason is the broadest one
    in force.
    """
    scopes = [GLOBAL]
    if source:
        scopes.append(f"source:{source}")
    if recipe and recipe != source:
        scopes.append(f"recipe:{recipe}")
    elif recipe:
        scopes.append(f"recipe:{recipe}")
    # dict.fromkeys: order-preserving dedupe for the recipe == source case.
    return list(dict.fromkeys(scopes))


def active_pause(cur: psycopg.Cursor, scopes: list[str],
                 now: datetime) -> Pause | None:
    """The live pause covering any of `scopes`, or None.

    Expiry is evaluated against the caller's `now` rather than the database's,
    so a test can drive an auto-resume without waiting for it and so one tick
    sees one consistent instant across every trigger it evaluates. An expired row
    is left in place — it is the record of *why* something was paused, and
    deleting it on read would race a concurrent unpause and lose that.
    """
    if not scopes:
        return None
    cur.execute(
        """SELECT scope, reason, paused_by, paused_at, expires_at
             FROM pauses
            WHERE scope = ANY(%s)
              AND (expires_at IS NULL OR expires_at > %s)
            ORDER BY array_position(%s::text[], scope)
            LIMIT 1""",
        (scopes, now, scopes),
    )
    row = cur.fetchone()
    return Pause(*row) if row else None
