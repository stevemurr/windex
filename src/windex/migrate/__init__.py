"""One-way, re-runnable migrations from the legacy per-source state tables to the
unified source-recipes model.

Nothing here deletes or rewrites a legacy table. Each command is a projection:
read the old shape, write the new one, and leave the old one authoritative. That
is what makes the whole migration reversible by `TRUNCATE source_units` right up
until reads are flipped, one source at a time.
"""
