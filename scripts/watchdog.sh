#!/usr/bin/env bash
# Service watchdog v4. Supervises the data plane AND the app processes.
#
# v3 lessons (2026-07-16) kept intact:
# - Probe postgres over the HOST port-forward (127.0.0.1:5432) like a real
#   client, not `container exec` — the port-forward is what stalls while the
#   runtime's control channel stays healthy.
# - One failed probe must not trigger a restart (restarts CAUSE outages and IP
#   churn): require 3 consecutive failures.
# - Silence was ambiguous: hourly heartbeat + container IPs logged, IP changes
#   called out (answers "what restarted them" forensics directly).
#
# v4 adds PROCESS supervision — the 2026-07-17 gap: a ~25-min gateway blip
# exited every embed loop by design and nothing restarted them, so a short blip
# became a ~36h stall. Now, when the data plane is healthy, `windex status
# --json` names the supervised members (serve + the 8 embed loops) that are
# absent and `windex up` — idempotent, the same start mechanism the CLI uses —
# brings the missing ones back. Storm guards: a 2-cycle debounce, a 5-per-600s
# restart rate cap (then alert-only, leaning on the Grafana LoopDown/
# EmbedsStalled rules), and a bounded serve backstop so a genuinely broken serve
# isn't hammered. Supervision lives in the data-plane-healthy branch ONLY, so it
# can never restart loops onto a dead postgres/qdrant.
#
# On start it waits for the external volume, then runs `windex up` once — so the
# launchd agent (deploy/com.windex.supervisor.plist) brings the whole stack up
# in order at boot.
set -u
cd "$(dirname "$0")/.."
LOG="$HOME/.windex/watchdog.log"
mkdir -p "$(dirname "$LOG")"
MOUNT=/Volumes/External/windex
PY=.venv/bin/python
WINDEX=.venv/bin/windex

say() { echo "$(date '+%F %T') $*" >> "$LOG"; }
mount_ok() { [ -d "$MOUNT/services/postgres" ]; }
pg_tcp_ok() {
  "$PY" -c "import psycopg; psycopg.connect('postgresql://windex:windex@127.0.0.1:5432/windex', connect_timeout=5).close()" 2>/dev/null
}
qd_ok() { curl -sf -m 5 http://127.0.0.1:6333/ >/dev/null 2>&1; }
ips() { container ls 2>/dev/null | awk '/windex-/ {printf "%s=%s ", $1, $6}'; }

# Supervised members (serve + the 8 loops) that `windex status` reports absent.
# A pure process-table read (windex status --json), so it works even when serve
# itself is down.
down_procs() {
  "$WINDEX" status --json 2>/dev/null | "$PY" -c '
import json, sys
try:
    print(" ".join(json.load(sys.stdin).get("down", [])))
except Exception:
    pass
' 2>/dev/null
}

# True if fewer than 5 restarts happened in the last 600s (also prunes the ring).
rate_ok() {
  local now cutoff kept="" t count=0
  now=$(date +%s); cutoff=$((now - 600))
  for t in $restart_times; do
    if [ "$t" -ge "$cutoff" ]; then kept="$kept $t"; count=$((count + 1)); fi
  done
  restart_times="$kept"
  [ "$count" -lt 5 ]
}

say "watchdog v4 started · $(ips)"

# Boot ordering: wait for the external volume before anything (a missing mount
# means postgres would init on the internal disk — corruption), then bring the
# whole stack up once. The loop below keeps it up.
while ! mount_ok; do say "waiting for $MOUNT to mount…"; sleep 10; done
say "mount present — windex up"
"$WINDEX" up >> "$LOG" 2>&1

fails=0
down_streak=0
serve_fails=0
restart_times=""
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
    # Data plane is healthy (mount + pg + qdrant), so it is safe to (re)start app
    # processes without racing a dead store. Supervise serve + the 8 loops.
    down="$(down_procs)"
    if [ -z "$down" ]; then
      down_streak=0; serve_fails=0
    else
      down_streak=$((down_streak + 1))
      if [ "$down_streak" -lt 2 ]; then
        # Debounce: a just-restarted process needs a cycle before pgrep sees it.
        say "supervise: down=[$down] (debounce $down_streak/2)"
      elif ! rate_ok; then
        say "supervise: down=[$down] but restart rate cap hit (>=5/600s) — alerting only"
      else
        case " $down " in *" serve "*) serve_fails=$((serve_fails + 1));; *) serve_fails=0;; esac
        if [ "$serve_fails" -ge 3 ]; then
          # A serve that won't stay up (bad bind, import error) must not be
          # hammered; keep supervising the loops, let a human fix serve.
          say "supervise: serve down ${serve_fails}× — restarting loops only (windex up --no-serve); investigate serve"
          "$WINDEX" up --no-serve >> "$LOG" 2>&1
        else
          say "supervise: down=[$down] — windex up"
          "$WINDEX" up >> "$LOG" 2>&1
        fi
        restart_times="$restart_times $(date +%s)"
        down_streak=0
        sleep 20  # settle: let freshly-spawned PIDs appear before the next probe
      fi
    fi
  fi
  sleep 15
done
