#!/usr/bin/env bash
# Drains the embedding backlog while a `ccnews run --no-embed` processor is
# active, then exits once the processor is gone and the backlog is empty.
# Lets extraction and embedding overlap instead of serializing overnight runs.
set -uo pipefail
cd "$(dirname "$0")/.."

while :; do
  out=$(.venv/bin/windex ccnews embed 2>&1 | tail -1) || true
  echo "$(date '+%H:%M:%S') $out"
  if ! pgrep -f "ccnews run" >/dev/null; then
    if echo "$out" | grep -q "embedded 0 docs"; then
      echo "$(date '+%H:%M:%S') processor gone and backlog empty — done"
      break
    fi
  fi
  sleep 30
done
