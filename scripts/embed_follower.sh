#!/usr/bin/env bash
# Thin wrapper kept for compatibility — the loop lives in the CLI now
# (backoff + circuit breaker; see `windex ccnews embed-loop --help`).
cd "$(dirname "$0")/.."
exec .venv/bin/windex ccnews embed-loop "$@"
