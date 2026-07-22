#!/usr/bin/env bash
# Service watchdog v5. Supervises the data plane AND the app processes.
#
# v3 lessons kept: probe over the host port-forward like a real client; debounce
# before acting; hourly heartbeat + container-IP-change logging.
# v4 added process supervision (restart down loops/serve via idempotent
# `windex up`), with a debounce, a rate cap, and a bounded-serve backstop.
# v5 fixes the 2026-07-20 THRASH: under heavy load qdrant went SLOW (not dead);
# v4 restarted BOTH containers on 3 failed probes with no backoff, kept nuking a
# HEALTHY postgres for qdrant's sake, and churned container IPs every ~45s at
# load 39 — which PREVENTED recovery. v5:
#   - checks postgres and qdrant SEPARATELY and restarts only the dead one;
#   - "dead" = the TCP port won't accept a connection — a slow-but-listening
#     service is ALIVE and is NEVER restarted merely for being slow;
#   - qdrant tolerates more consecutive misses than postgres (slow is normal for
#     it — int8 vectors mmap'd off the external disk);
#   - escalating backoff between restarts + a streak cap that stops restarting a
#     service that won't recover (alert-only), so a slow store can't thrash.
set -u
cd "$(dirname "$0")/.."
LOG="$HOME/.windex/watchdog.log"
mkdir -p "$(dirname "$LOG")"
MOUNT=/Volumes/External/windex
PY=.venv/bin/python
WINDEX=.venv/bin/windex

say() { echo "$(date '+%F %T') $*" >> "$LOG"; }
mount_ok() { [ -d "$MOUNT/services/postgres" ]; }
# Liveness = the port accepts a TCP connection. A busy/slow-but-listening service
# still passes, so we never restart it just for being slow (the v5 fix). A
# wedged/dead container refuses the connection and is caught.
port_alive() { "$PY" -c "import socket,sys; socket.create_connection(('127.0.0.1',int(sys.argv[1])),3).close()" "$1" 2>/dev/null; }
ips() { container ls 2>/dev/null | awk '/windex-/ {printf "%s=%s ", $1, $6}'; }

# supervised app members (serve + the 8 loops) that `windex status` reports absent
down_procs() {
  "$WINDEX" status --json 2>/dev/null | "$PY" -c '
import json, sys
try:
    print(" ".join(json.load(sys.stdin).get("down", [])))
except Exception:
    pass
' 2>/dev/null
}

# True if fewer than 5 process-restarts happened in the last 600s (prunes the ring).
rate_ok() {
  local now cutoff kept="" t count=0
  now=$(date +%s); cutoff=$((now - 600))
  for t in $restart_times; do
    if [ "$t" -ge "$cutoff" ]; then kept="$kept $t"; count=$((count + 1)); fi
  done
  restart_times="$kept"
  [ "$count" -lt 5 ]
}

say "watchdog v5 started · $(ips)"

# Boot ordering: wait for the external volume, then bring the whole stack up once.
while ! mount_ok; do say "waiting for $MOUNT to mount…"; sleep 10; done
say "mount present — windex up"
"$WINDEX" up >> "$LOG" 2>&1

pg_fails=0; pg_streak=0; pg_last=0
qd_fails=0; qd_streak=0; qd_last=0
down_streak=0; serve_fails=0; restart_times=""
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
    pg_fails=0; qd_fails=0
    sleep 15
    continue
  fi

  now=$(date +%s)
  port_alive 5432 && PG=1 || PG=0
  port_alive 6333 && QD=1 || QD=0

  # --- postgres: restart ONLY postgres, only when the port is genuinely dead ---
  if [ "$PG" = 1 ]; then
    pg_fails=0; pg_streak=0
  else
    pg_fails=$((pg_fails + 1))
    say "postgres port not answering ($pg_fails)"
    backoff=$(( 30 * (1 << (pg_streak < 4 ? pg_streak : 4)) ))
    if [ "$pg_fails" -ge 3 ]; then
      if [ "$pg_streak" -ge 5 ]; then
        say "postgres still dead after $pg_streak restarts — NOT restarting; investigate"
      elif [ $((now - pg_last)) -ge "$backoff" ]; then
        say "postgres dead — recreating (streak $pg_streak; backoff escalates)"
        container stop windex-postgres >/dev/null 2>&1; sleep 1
        ./scripts/dev.sh up >> "$LOG" 2>&1
        pg_last=$(date +%s); pg_streak=$((pg_streak + 1)); pg_fails=0; sleep 15
      fi
    fi
  fi

  # --- qdrant: separate, more tolerant (slow under load is expected, not dead) ---
  if [ "$QD" = 1 ]; then
    qd_fails=0; qd_streak=0
  else
    qd_fails=$((qd_fails + 1))
    say "qdrant port not answering ($qd_fails)"
    backoff=$(( 30 * (1 << (qd_streak < 4 ? qd_streak : 4)) ))
    if [ "$qd_fails" -ge 5 ]; then
      if [ "$qd_streak" -ge 5 ]; then
        say "qdrant still dead after $qd_streak restarts — NOT restarting; investigate"
      elif [ $((now - qd_last)) -ge "$backoff" ]; then
        say "qdrant dead — recreating (streak $qd_streak; backoff escalates)"
        container stop windex-qdrant >/dev/null 2>&1; sleep 1
        ./scripts/dev.sh up >> "$LOG" 2>&1
        qd_last=$(date +%s); qd_streak=$((qd_streak + 1)); qd_fails=0; sleep 15
      fi
    fi
  fi

  # --- process supervision only when BOTH data services are alive ---
  if [ "$PG" = 1 ] && [ "$QD" = 1 ]; then
    down="$(down_procs)"
    if [ -z "$down" ]; then
      down_streak=0; serve_fails=0
    else
      down_streak=$((down_streak + 1))
      if [ "$down_streak" -lt 2 ]; then
        say "supervise: down=[$down] (debounce $down_streak/2)"
      elif ! rate_ok; then
        say "supervise: down=[$down] but restart rate cap hit (>=5/600s) — alerting only"
      else
        case " $down " in *" serve "*) serve_fails=$((serve_fails + 1));; *) serve_fails=0;; esac
        if [ "$serve_fails" -ge 3 ]; then
          say "supervise: serve down ${serve_fails}× — restarting loops only (windex up --no-serve); investigate serve"
          "$WINDEX" up --no-serve >> "$LOG" 2>&1
        else
          say "supervise: down=[$down] — windex up"
          "$WINDEX" up >> "$LOG" 2>&1
        fi
        restart_times="$restart_times $(date +%s)"
        down_streak=0
        sleep 20
      fi
    fi
  fi

  sleep 15
done
