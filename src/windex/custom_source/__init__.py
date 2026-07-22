"""User-defined push-based custom sources.

A generalization of the `memory` source: instead of one hardcoded push index,
the API can register any number of them. Each custom source reuses the shared
machinery unchanged — the documents ledger (``documents.source = <name>``, ids
``<name>:<suffix>``), the per-source Qdrant collection behind ``<name>_current``,
and the shared embed driver (``windex.embed.pipeline``) — so nothing here
computes embeddings or invents a second storage path.

Where it deliberately DIFFERS from ``memory``: ingest is **upsert + explicit
delete**, not full-replace. Each ``POST /v1/sources/{name}/docs`` stages only the
changed delta (``text_hash`` dedup ledger) to its own per-batch parquet; docs are
never implicitly tombstoned by being absent from a later push — a caller removes
them explicitly via ``/docs/delete`` or drops the whole source. The registry row
(``custom_sources``) also stores an optional refresh ``recipe`` server-side, so a
scheduled refresh prompt can be a stateless one-liner.
"""
