"""Memory push identity.

Regression cover for the epoch-2 cutover: `IngestRequest` is strict and carries
no batch-level conversation id, so `push.docs` used to resolve every memory push
to the literal partition "push". Because the memory Source replaces by id scope,
that made every conversation collide in one scope and every push wipe the
previous chat. These tests pin identity to the documents themselves.

Pure functions only — no postgres, no qdrant.
"""

from datetime import datetime, timezone

import pytest

from windex.modules.receive import memory_identity, memory_partition
from windex.worker.protocol import PermanentTaskError

CONVERSATION = "0f9d2a41-3c7e-4b18-9a05-6d1f8c2e4b77"
OTHER = "7b2c5e90-1a44-4f63-8e21-3d9a0b6c5f18"


def _doc(conversation=CONVERSATION, index=0, **overrides):
    document = {
        "id": f"{conversation}/{index:05d}",
        "url": f"llmchat://chat/{conversation}?chunk={index}",
        "title": "Design review",
        "text": "User: what did we decide?",
        "published_at": "2026-07-26T09:15:00Z",
        "fields": {
            "conversation_id": conversation,
            "chunk_index": index,
            "message_range": [0, 4],
        },
    }
    document.update(overrides)
    return document


# --- partition resolution ---------------------------------------------------

def test_partition_comes_from_document_fields():
    assert memory_partition({}, [_doc(index=0), _doc(index=1)]) == CONVERSATION


def test_partition_falls_back_to_the_document_id_prefix():
    document = _doc()
    document["fields"] = {"chunk_index": 0}
    assert memory_partition({}, [document]) == CONVERSATION


def test_explicit_batch_partition_wins_and_carries_an_empty_delete():
    """The delete path pushes zero documents, so the batch key is the only
    thing naming the conversation being emptied."""
    assert memory_partition({"partition": CONVERSATION}, []) == CONVERSATION


def test_two_conversations_in_one_batch_are_rejected():
    """The failure this whole change exists to prevent: collapsing a mixed
    batch to one partition tombstones an entire unrelated chat."""
    with pytest.raises(PermanentTaskError, match="mixes conversations"):
        memory_partition({}, [_doc(), _doc(conversation=OTHER)])


def test_a_nameless_batch_is_rejected_rather_than_defaulted():
    with pytest.raises(PermanentTaskError, match="names no conversation"):
        memory_partition({}, [])

    with pytest.raises(PermanentTaskError, match="names no conversation"):
        memory_partition({}, [{"text": "orphan", "fields": {}}])


def test_conversations_do_not_share_a_partition():
    """Two separate pushes must resolve to two separate replace scopes."""
    first = memory_partition({}, [_doc(conversation=CONVERSATION)])
    second = memory_partition({}, [_doc(conversation=OTHER)])
    assert first != second


# --- per-document identity --------------------------------------------------

def test_identity_is_read_from_the_document():
    identity = memory_identity(_doc(index=3), 3, CONVERSATION)
    assert identity.suffix == f"{CONVERSATION}/00003"
    assert identity.url == f"llmchat://chat/{CONVERSATION}?chunk=3"
    assert identity.title == "Design review"
    assert identity.published_at == datetime(
        2026, 7, 26, 9, 15, tzinfo=timezone.utc)
    assert identity.fields == {
        "conversation_id": CONVERSATION,
        "chunk_index": 3,
        "message_range": [0, 4],
    }


def test_message_range_survives():
    """It was hardcoded to None before the fix."""
    identity = memory_identity(_doc(), 0, CONVERSATION)
    assert identity.fields["message_range"] == [0, 4]


def test_chunk_index_beats_enumeration_order():
    """Positional index is a fallback, not the identity: a retried slice must
    not renumber the chunks it re-sends."""
    identity = memory_identity(_doc(index=7), 0, CONVERSATION)
    assert identity.fields["chunk_index"] == 7
    assert identity.suffix.endswith("/00007")


def test_a_document_outside_its_conversation_scope_is_rejected():
    stray = _doc()
    stray["id"] = f"{OTHER}/00000"
    with pytest.raises(PermanentTaskError, match="lies outside"):
        memory_identity(stray, 0, CONVERSATION)


def test_legacy_document_shape_still_maps():
    """Pre-epoch-2 clients sent index/ended_at and a top-level message_range."""
    identity = memory_identity(
        {
            "index": 2,
            "text": "…",
            "ended_at": "2026-07-26T09:15:00Z",
            "message_range": [2, 6],
        },
        2,
        CONVERSATION,
        batch_title="Design review",
    )
    assert identity.suffix == f"{CONVERSATION}/00002"
    assert identity.url == f"llmchat://chat/{CONVERSATION}?chunk=2"
    assert identity.title == "Design review"
    assert identity.fields["chunk_index"] == 2
    assert identity.fields["message_range"] == [2, 6]
    assert identity.published_at == datetime(
        2026, 7, 26, 9, 15, tzinfo=timezone.utc)


def test_non_object_fields_are_rejected():
    with pytest.raises(PermanentTaskError, match="fields must be an object"):
        memory_identity(_doc(fields=["conversation_id"]), 0, CONVERSATION)
