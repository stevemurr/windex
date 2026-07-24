"""The event vocabulary — bounded, and validated because it is a boundary.

`type='event'` triggers are woken by a string. If that string is unconstrained,
then whatever can reach `emit_event` can name any trigger it likes, and the
"which recipes can this input start" question has no answer short of reading the
whole `triggers` table. So the vocabulary is closed: four kinds, three of which
take exactly one argument, and the argument is a recipe/source name under the
same `^[a-z][a-z0-9_]{1,31}$` rule that already stops a custom source from
shadowing a built-in corpus (`custom_source/registry.py:22`).

The four kinds and what each replaces:

``run.succeeded:<recipe>``
    Chaining. Today this is `&&` inside a bash string built by
    `cli._refresh_script`, which has two failure modes worth naming: a
    non-zero exit anywhere in the chain silently drops the rest of that source's
    steps into `refresh.log` where nobody reads it, and the chain is *hardcoded*
    in `REFRESH_CHAINS` so it can only ever express the eight built-ins. As an
    event it is a row with an error, and any recipe can chain off any other.

``source.pushed:<name>``
    A push source (memory, custom) actually becoming push-driven. Today a pushed
    document waits up to one embed-loop interval (30 s) for a poll to notice it.

``unit.failed_threshold:<source>``
    A source crossing its failure threshold — the hook for a repair/backoff
    recipe. Emitted by the worker pool, not by anything here.

``boot``
    Fired once when the pool first sees a healthy mount + gateway. This one is
    directly the power-outage story: the CIFS mount races the app units at boot
    and loses, so recovery work must be driven by "the mount came back", not by a
    clock that already ticked past it while the mount was missing.

Nothing here emits — `fire.emit_event` does. This module only decides what is a
legal event name and how it maps onto `runs.trigger`.
"""

from __future__ import annotations

import re

# The argument grammar, shared with custom_source.registry.NAME_RE. Duplicated as
# a local constant rather than imported: importing registry drags in the qdrant
# client, and the scheduler must stay loadable on a box where the vector store is
# down (that is precisely when you want the scheduler still running).
ARG_RE = re.compile(r"^[a-z][a-z0-9_]{1,31}$")

SEP = ":"

# kind -> takes an argument. Adding a kind is a one-line edit here plus a decision
# in `runs_trigger_column` about whether it reads as a chain or a plain event.
EVENT_KINDS: dict[str, bool] = {
    "run.succeeded": True,
    "source.pushed": True,
    "unit.failed_threshold": True,
    "boot": False,
}


def parse_event(event: str) -> tuple[str, str | None]:
    """Split a validated event into (kind, arg). Raises ValueError otherwise.

    Split on the FIRST separator only: `run.succeeded` contains a dot but no
    colon, so `kind:arg` stays unambiguous, and an arg containing a colon is
    rejected by ARG_RE rather than quietly truncated.
    """
    if not isinstance(event, str) or not event:
        raise ValueError("event must be a non-empty string")
    kind, sep, arg = event.partition(SEP)
    if kind not in EVENT_KINDS:
        raise ValueError(
            f"unknown event kind {kind!r} — the vocabulary is closed: "
            f"{', '.join(sorted(EVENT_KINDS))}")
    takes_arg = EVENT_KINDS[kind]
    if takes_arg and not sep:
        raise ValueError(f"event {kind!r} requires an argument: {kind}{SEP}<name>")
    if not takes_arg and sep:
        raise ValueError(f"event {kind!r} takes no argument, got {event!r}")
    if takes_arg and not ARG_RE.match(arg):
        raise ValueError(
            f"event argument {arg!r} must match {ARG_RE.pattern} "
            f"(the recipe/source name rule)")
    return kind, (arg if takes_arg else None)


def validate_event(event: str) -> str:
    """Validate and return the event unchanged. Raises ValueError if illegal.

    Returns the input so it composes as `event = validate_event(event)` at the
    top of a write path — the shape that makes "was this validated" visible in
    the code rather than a comment.
    """
    parse_event(event)
    return event


def run_succeeded_event(recipe: str) -> str:
    """The event name a completed run emits. One constructor so the caller can
    never build `run.succeeded/<recipe>` or `run_succeeded:<recipe>` and have it
    silently match nothing — a chain that never fires is invisible, unlike one
    that fires wrongly."""
    if not ARG_RE.match(recipe or ""):
        raise ValueError(f"recipe name {recipe!r} must match {ARG_RE.pattern}")
    return f"run.succeeded{SEP}{recipe}"


def runs_trigger_column(event: str) -> str:
    """What to write into `runs.trigger` for a run started by this event.

    `run.succeeded:*` is `chain`, everything else is `event`. The distinction is
    not cosmetic: `chain` is the successor of the `&&` in `REFRESH_CHAINS`, and
    "show me every run that was a consequence of another run" is a question the
    freshness UI asks and cannot ask if both collapse to one word.
    """
    kind, _ = parse_event(event)
    return "chain" if kind == "run.succeeded" else "event"
