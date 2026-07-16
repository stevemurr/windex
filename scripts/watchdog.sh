#!/usr/bin/env bash
# Service watchdog v3. Post-mortem lessons (2026-07-16, both incidents):
# - v2 probed postgres via `container exec` — the runtime's control channel —
#   which stays healthy when the HOST PORT-FORWARD (the path the API actually
#   uses) stalls. v3 probes 127.0.0.1:5432 over TCP like a real client.
# - One failed probe must not trigger a restart (restarts CAUSE outages and IP
#   churn): require 3 consecutive failures.
# - Silence was ambiguous: hourly heartbeat + container IPs logged, IP changes
#   called out (answers "what restarted them" forensics directly).
set -u
cd "$(dirname "$0")/.."
LOG="$HOME/.windex/watchdog.log"
mkdir -p "$(dirname "$LOG")"
MOUNT=/Volumes/External/windex
PY=.venv/bin/python

say() { echo "$(date '+%F %T') $*" >> "$LOG"; }
mount_ok() { [ -d "$MOUNT/services/postgres" ]; }
pg_tcp_ok() {
  "$PY" -c "import psycopg; psycopg.connect('postgresql://windex:windex@127.0.0.1:5432/windex', connect_timeout=5).close()" 2>/dev/null
}
qd_ok() { curl -sf -m 5 http://127.0.0.1:6333/ >/dev/null 2>&1; }
ips() { container ls 2>/dev/null | awk '/windex-/ {printf "%s=%s ", $1, $6}'; }

say "watchdog v3 started · $(ips)"
fails=0
last_ips="$(ips)"
last_beat=$(date +%s)

while :; do
  now_ips="$(ips)"
  if [ "$now_ips" != "$last_ips" ]; then
    say "container IPs changed: '$last_ips' → '$now_ips'"
    last_ips="$now_ips"
  fi
  if [ $(( $(date +%s) - last_beat )) -ge 3600 ]; then
    say "heartbeat ok · $(ips)"
    last_beat=$(date +%s)
  fi

  if ! mount_ok; then
    say "EXTERNAL MOUNT MISSING — stopping containers to limit corruption"
    container stop windex-postgres windex-qdrant >/dev/null 2>&1
    while ! mount_ok; do sleep 10; done
    say "mount back — restarting services"
    ./scripts/dev.sh up >> "$LOG" 2>&1
    fails=0
  elif ! pg_tcp_ok || ! qd_ok; then
    fails=$((fails + 1))
    say "health probe failed ($fails/3) pg_tcp=$(pg_tcp_ok && echo ok || echo FAIL) qdrant=$(qd_ok && echo ok || echo FAIL)"
    if [ "$fails" -ge 3 ]; then
      say "3 consecutive failures — full restart"
      container stop windex-postgres windex-qdrant >/dev/null 2>&1
      sleep 2
      ./scripts/dev.sh up >> "$LOG" 2>&1
      sleep 20
      fails=0
    fi
  else
    fails=0
  fi
  sleep 15
done
