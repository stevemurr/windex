#!/usr/bin/env bash
# Dev services for windex, managed with Apple's `container` CLI (no Docker on this machine).
# Usage: scripts/dev.sh up|down|destroy|status|logs <name>|psql
set -euo pipefail

PG_NAME=windex-postgres
QD_NAME=windex-qdrant
PG_IMAGE=docker.io/library/postgres:16
QD_IMAGE=docker.io/qdrant/qdrant:latest
# Service data lives on the external drive via bind mounts: the internal disk
# has almost no free space, and Apple-container named volumes are sparse images
# under ~/Library that would grow onto it.
SERVICES_DIR="${WINDEX_SERVICES_DIR:-/Volumes/External/windex/services}"

# `container start` on an already-running container breaks it (v0.4.1), so
# only start when the container exists and is not running.
run_or_start() {
  local name=$1 info
  info=$(container inspect "$name" 2>/dev/null || true)
  if [ -z "$info" ] || [ "$info" = "[]" ]; then
    shift
    "$@" >/dev/null
    echo "$name: created"
  elif echo "$info" | grep -q '"status":"running"'; then
    # Don't trust "status":"running" — it lies. Observed 2026-07-16: postgres'
    # port went dead, `inspect` still said running, `exec` said "container is
    # not running", and `stop`/`kill`/`start` all no-op'd. The watchdog's
    # recovery was `stop` + `up`, so it printed "already running" and did
    # nothing, every 15s, while the whole pipeline was down. A container that
    # can't exec is wedged no matter what the status field claims: recreate it.
    # (Data is on bind mounts, so recreating costs nothing.)
    if container exec "$name" true >/dev/null 2>&1; then
      echo "$name: already running"
    else
      echo "$name: wedged (status=running but not execable) — recreating"
      container kill "$name" >/dev/null 2>&1 || true
      container stop "$name" >/dev/null 2>&1 || true
      container delete --force "$name" >/dev/null 2>&1 || true
      shift
      "$@" >/dev/null
      echo "$name: recreated"
    fi
  else
    container start "$name" >/dev/null
    echo "$name: started"
  fi
}

up() {
  container system start >/dev/null 2>&1 || true
  mkdir -p "$SERVICES_DIR/postgres" "$SERVICES_DIR/qdrant"
  # PGDATA subdir keeps initdb happy regardless of what the mount root contains.
  run_or_start "$PG_NAME" container run -d --name "$PG_NAME" \
    -e POSTGRES_USER=windex -e POSTGRES_PASSWORD=windex -e POSTGRES_DB=windex \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -v "$SERVICES_DIR/postgres:/var/lib/postgresql/data" \
    -c 2 -m 4G \
    -p 5432:5432 "$PG_IMAGE"
  run_or_start "$QD_NAME" container run -d --name "$QD_NAME" \
    -v "$SERVICES_DIR/qdrant:/qdrant/storage" \
    -c 4 -m 10G \
    -p 6333:6333 -p 6334:6334 "$QD_IMAGE"
  container ls
}

down() {
  container stop "$PG_NAME" "$QD_NAME" 2>/dev/null || true
  echo "stopped"
}

destroy() {
  container stop "$PG_NAME" "$QD_NAME" 2>/dev/null || true
  container rm "$PG_NAME" "$QD_NAME" 2>/dev/null || true
  echo "containers removed (data kept in $SERVICES_DIR)"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  destroy) destroy ;;
  status) container ls ;;
  logs) container logs "${2:?usage: dev.sh logs <container>}" ;;
  psql) container exec -it "$PG_NAME" psql -U windex -d windex ;;
  *) echo "usage: $0 up|down|destroy|status|logs <name>|psql" >&2; exit 1 ;;
esac
