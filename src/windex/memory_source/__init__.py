"""Push-based chat-memory source.

Unlike every other windex source, `memory` is not pulled from an upstream: the
LLMChat macOS app chunks each conversation's user/assistant transcript and POSTs
the full chunk list per conversation to `/v1/memory/conversations/{uuid}`. The
server treats each conversation as a DevDocs-style full-replace unit
(``docs_source.ingest.stage_docset`` is the template): the whole chunk set is
rewritten to ``memory/clean/<conversation_id>.parquet`` (tmp-file + rename), and
a ``text_hash``-guarded ledger upsert re-embeds only the changed delta — so the
append-only common case (a finished turn adds a trailing chunk) re-embeds just
that one chunk, while edits/deletes tombstone vanished chunk ids. The shared
embed driver (``windex.embed.pipeline``) then drains staged chunks exactly as it
does for every other parquet-backed source; the app never computes embeddings.
"""
