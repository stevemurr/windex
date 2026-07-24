"""What flows along an edge, and which kinds may be wired to which.

The payoff of a CLOSED node-kind vocabulary is here: every kind has exactly one
input port and one output port, and their types are fixed by the kind. So wiring
is checkable by table lookup, and a mis-wire (`fetch -> collect`) is a 422 at
install rather than a TypeError inside a worker at 3am. That is the whole reason
this is a fixed vocabulary rather than free-form nodes declaring their own I/O.

The types themselves are not arbitrary either — each one is a shape that already
exists in the eleven hand-written sources, named. `WorkUnit` is what a pending
watermark row becomes; `RawBlob` is what a fetch returns; `ExtractedDoc` is what
`upsert_docs` takes. Naming them is most of what makes the graph checkable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class PartitionRef:
    """Which unit of upstream work a value came from.

    `id_scope` is the ledger id prefix this partition OWNS (`docs:python~3.14/`,
    `hf:docs/transformers/`). It is what makes partition-scoped replace correct:
    a batch may tombstone ids under its own scope that it did not write, and may
    not touch anything else. Three functions named `_ledger_ids_for_<thing>`
    collapse into that one rule.
    """

    store: str
    key: str
    id_scope: str | None = None


@dataclass(frozen=True)
class WorkUnit:
    """One claimable piece of upstream work. What `discover` emits."""

    ref: PartitionRef
    payload: dict = field(default_factory=dict)   # url, from/until, stars, depth…
    upstream: dict = field(default_factory=dict)  # the freshness token it was claimed at
    attempt: int = 0
    epoch: int = 0                                # the run id, carried end to end


@dataclass(frozen=True)
class RawBlob:
    """Bytes plus provenance. What `fetch` emits.

    `path` is set when the payload was staged to disk (a 1GB WARC), `body` when it
    was small enough to hold. Exactly one is set; a module that assumes the wrong
    one is the bug this split exists to make obvious.
    """

    ref: PartitionRef
    uri: str
    media_type: str = ""
    path: Path | None = None
    body: bytes | None = None
    meta: dict = field(default_factory=dict)      # status, etag, final_url, bytes
    epoch: int = 0


@dataclass(frozen=True)
class PartitionRecord:
    """An assertion about a unit of work. What `catalog` emits and `collect` writes.

    `delta` is additive (`star_events += n`) where `payload` is replacing. Both
    exist because GH Archive counts watch events incrementally while the Search
    API reports absolute stars, and merging them needs both semantics.
    """

    store: str
    key: str
    upstream: dict = field(default_factory=dict)
    stage: str | None = None
    payload: dict = field(default_factory=dict)
    delta: dict = field(default_factory=dict)
    absent_ok: bool = True


@dataclass(frozen=True)
class ExtractedDoc:
    """A document, before it is staged. What `extract` emits and `load` consumes.

    `suffix` not `id`: the full id is `corpus.id_prefix + suffix`, forced by the
    loader. A recipe cannot name an id outside its own namespace, so it cannot
    overwrite or tombstone another source's documents.
    """

    ref: PartitionRef
    suffix: str
    url: str
    text: str
    title: str = ""
    canonical_url: str | None = None
    published_at: datetime | None = None
    lang: str | None = None
    fields: dict = field(default_factory=dict)    # extra staged parquet columns
    payload: dict = field(default_factory=dict)   # extra Qdrant payload fields
    deleted: bool = False                         # in-stream tombstone (arxiv)
    epoch: int = 0


@dataclass(frozen=True)
class Coverage:
    """What `load` is allowed to treat as a complete census.

    The guard on every destructive operation. `prune`/replace may only act when a
    batch actually saw everything it claims to have seen — a truncated or partly
    failed run deleting "missing" documents is data loss dressed as tidying.
    """

    refs: tuple[PartitionRef, ...] = ()
    failures: int = 0
    truncated: bool = False

    @property
    def complete(self) -> bool:
        return self.failures == 0 and not self.truncated


# --- the closed kind vocabulary ---------------------------------------------
# in/out are port TYPE names; None means the kind is a root (no input) or a sink
# (no output). `stateful` records which kinds may touch a store, which is what
# lets the compiler refuse a graph that writes from somewhere it shouldn't.

@dataclass(frozen=True)
class Kind:
    name: str
    inp: str | None
    out: str | None
    stateful: bool
    title: str
    help: str


KINDS: dict[str, Kind] = {k.name: k for k in (
    Kind("discover", None, "WorkUnit", True, "Discover",
         "Pull root: selects pending units from a store. The only kind that claims."),
    Kind("receive", None, "ExtractedDoc", False, "Receive",
         "Push root: documents arrive over HTTP rather than being fetched."),
    Kind("fetch", "WorkUnit", "RawBlob", False, "Fetch",
         "The only kind allowed to touch the network."),
    Kind("catalog", "RawBlob", "PartitionRecord", False, "Catalog",
         "Reads a listing and asserts what work exists."),
    Kind("extract", "RawBlob", "ExtractedDoc", False, "Extract",
         "Turns bytes into documents."),
    Kind("transform", "ExtractedDoc", "ExtractedDoc", False, "Transform",
         "Filters, enriches or deduplicates a document stream."),
    Kind("collect", "PartitionRecord", None, True, "Collect",
         "Writes assertions into a store. A fan-in point."),
    Kind("load", "ExtractedDoc", None, True, "Load",
         "Stages parquet and writes the ledger delta. Terminal, and a fan-in point."),
)}

ROOTS = tuple(k.name for k in KINDS.values() if k.inp is None)
SINKS = tuple(k.name for k in KINDS.values() if k.out is None)

# Port types, published to clients so a graph editor can validate a connection
# without hardcoding the lattice. Deliberately NOMINAL and flat: `extends` exists
# for future widening, but nothing uses it yet, and "any" is not a member — an
# untyped port would defeat the reason the vocabulary is closed.
PORT_TYPES: dict[str, dict] = {
    "WorkUnit": {"title": "Unit of work",
                 "fields": ["ref", "payload", "upstream", "attempt"]},
    "RawBlob": {"title": "Fetched bytes",
                "fields": ["ref", "uri", "media_type", "path", "body", "meta"]},
    "PartitionRecord": {"title": "Work assertion",
                        "fields": ["store", "key", "upstream", "stage", "payload", "delta"]},
    "ExtractedDoc": {"title": "Document",
                     "fields": ["ref", "suffix", "url", "title", "text",
                                "published_at", "lang", "fields", "payload"]},
}


def can_connect(from_kind: str, to_kind: str) -> bool:
    """Whether an edge between two kinds type-checks.

    One table lookup, because each kind has exactly one port per direction. This
    is the function the Swift editor reimplements in ~10 lines from `PORT_TYPES`
    and the kind table it is served.
    """
    a, b = KINDS.get(from_kind), KINDS.get(to_kind)
    if a is None or b is None or a.out is None or b.inp is None:
        return False
    return a.out == b.inp


def describe_kinds() -> list[dict]:
    """The palette a client renders node types from."""
    return [{"id": k.name, "title": k.title, "help": k.help,
             "in": k.inp, "out": k.out, "stateful": k.stateful}
            for k in KINDS.values()]
