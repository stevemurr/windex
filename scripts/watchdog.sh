#!/usr/bin/env bash
# Service watchdog v2. Lessons from the 2026-07-16 external-drive detaches:
# - logs live on the INTERNAL disk (v1 logged to the disk that failed)
# - a detach invalidates handles/mmaps inside the container VMs: on mount loss,
#   STOP containers immediately (limits corruption), wait, restart when back
# - pg_isready lies when handles are stale — use a real SELECT 1
set -u
cd "$(dirname "$0")/.."
LOG="$HOME/.windex/watchdog.log"
mkdir -p "$(dirname "$LOG")"
MOUNT=/Volumes/External/windex

say() { echo "$(date '+%F %T') $*" >> "$LOG"; }
mount_ok() { [ -d "$MOUNT/services/postgres" ]; }
pg_ok() { container exec windex-postgres psql -U windex -d windex -tAc 'SELECT 1' 2>/dev/null | grep -q 1; }
qd_ok() { curl -sf -m 5 http://127.0.0.1:6333/ >/dev/null 2>&1; }

say "watchdog v2 started"
while :; do
  if ! mount_ok; then
    say "EXTERNAL MOUNT MISSING — stopping containers to limit corruption"
    container stop windex-postgres windex-qdrant >/dev/null 2>&1
    while ! mount_ok; do sleep 10; done
    say "mount back — restarting services"
    ./scripts/dev.sh up >> "$LOG" 2>&1
  elif ! pg_ok || ! qd_ok; then
    say "health check failed (pg=$(pg_ok && echo ok || echo FAIL) qdrant=$(qd_ok && echo ok || echo FAIL)) — full restart"
    container stop windex-postgres windex-qdrant >/dev/null 2>&1
    sleep 2
    ./scripts/dev.sh up >> "$LOG" 2>&1
    sleep 20
  fi
  sleep 30
done
