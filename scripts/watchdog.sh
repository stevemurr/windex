#!/usr/bin/env bash
# Service watchdog: Apple `container` v0.4.1 has no restart policy, so this
# loop revives stopped windex containers (observed: qdrant SIGBUS under memory
# pressure took the overnight run down, 2026-07-16). dev.sh up is idempotent.
set -u
cd "$(dirname "$0")/.."

while :; do
  healthy=1
  curl -sf -m 5 http://127.0.0.1:6333/ >/dev/null 2>&1 || healthy=0
  container exec windex-postgres pg_isready -q -U windex >/dev/null 2>&1 || healthy=0
  if [ "$healthy" = 0 ]; then
    echo "$(date '+%F %T') service unhealthy — running dev.sh up"
    ./scripts/dev.sh up
  fi
  sleep 60
done
