#!/usr/bin/env bash
# Install the windex launchd agents:
#   - com.windex.supervisor : RunAtLoad + KeepAlive → scripts/watchdog.sh, which
#     waits for the mount, runs `windex up`, then supervises serve + the 8 loops.
#   - com.windex.<source>   : StartCalendarInterval agents for the recurring
#     ingest/harvest/maintain schedule (was README's hand-installed crontab).
#
# NOT run automatically — installing launchd agents (and, for a headless box,
# enabling auto-login) are machine-level decisions. Run it yourself:
#     bash deploy/install-launchd.sh              # install / re-install (idempotent)
#     bash deploy/install-launchd.sh --uninstall  # remove every windex agent
#
# A LaunchAgent only runs inside a logged-in GUI session. For the supervisor to
# come up at BOOT (not just at manual login), enable Automatic login:
#   System Settings → Users & Groups → Automatically log in as → <this user>.
#
# launchd calendar agents beat cron here: they run a MISSED job when the Mac
# wakes (cron silently skips), and don't depend on the deprecated macOS cron or
# its Full-Disk-Access need. Every windex job is idempotent, so a coalesced or
# re-run job is safe.
set -euo pipefail

REPO="/Users/murr/Code/github.com/stevemurr/windex"
PATHVAL="$REPO/.venv/bin:/usr/local/bin:/usr/bin:/bin"
LA="$HOME/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"
mkdir -p "$LA" "$HOME/.windex/logs"

# label | hour | minute | weekday("" = every day, 0 = Sunday) | command
# Mirrors the README schedule. The per-source `&& … embed` one-shots are kept:
# the pipeline is built for concurrent embed passes, so they compose safely with
# the always-on embed loops (a one-shot just drains faster right after ingest).
JOBS=(
  "com.windex.daily|2|15||windex daily"
  "com.windex.arxiv|3|30||windex arxiv harvest --days 7 && windex arxiv embed"
  "com.windex.smallweb|4|0||windex smallweb sync && windex smallweb poll && windex smallweb embed"
  "com.windex.hn|4|30||windex hn harvest --days 2 && windex hn embed"
  "com.windex.wiki|5|0|0|windex wiki sync && windex wiki ingest && windex wiki embed"
  "com.windex.docs|5|30||windex docs sync && windex docs ingest && windex docs embed"
  "com.windex.maintain|5|45||windex maintain"
  "com.windex.hf|6|0||windex hf sync && windex hf crawl && windex hf embed"
  "com.windex.maintain-reindex|6|15|0|windex maintain --reindex"
)

all_labels() {
  echo com.windex.supervisor
  for j in "${JOBS[@]}"; do echo "${j%%|*}"; done
}

if [ "${1:-}" = "--uninstall" ]; then
  for label in $(all_labels); do
    launchctl bootout "$DOMAIN/$label" 2>/dev/null || true
    rm -f "$LA/$label.plist"
    echo "removed $label"
  done
  exit 0
fi

cal_plist() {
  local label="$1" hour="$2" minute="$3" weekday="$4" cmd="$5"
  # XML-escape the shell command: the chained jobs contain `&&`, and a bare `&`
  # is invalid inside a plist <string> (must be &amp;), which launchctl rejects.
  local full="cd $REPO && $cmd"
  full="${full//&/&amp;}"; full="${full//</&lt;}"; full="${full//>/&gt;}"
  local sched="        <key>Hour</key><integer>$hour</integer>
        <key>Minute</key><integer>$minute</integer>"
  [ -n "$weekday" ] && sched="        <key>Weekday</key><integer>$weekday</integer>
$sched"
  cat > "$LA/$label.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>$label</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>-lc</string>
        <string>$full</string>
    </array>
    <key>WorkingDirectory</key><string>$REPO</string>
    <key>EnvironmentVariables</key>
    <dict><key>PATH</key><string>$PATHVAL</string></dict>
    <key>StartCalendarInterval</key>
    <dict>
$sched
    </dict>
    <key>StandardOutPath</key><string>$HOME/.windex/logs/$label.log</string>
    <key>StandardErrorPath</key><string>$HOME/.windex/logs/$label.log</string>
</dict>
</plist>
EOF
}

install_one() {
  local label="$1"
  launchctl bootout "$DOMAIN/$label" 2>/dev/null || true   # idempotent re-install
  launchctl bootstrap "$DOMAIN" "$LA/$label.plist"
  echo "installed $label"
}

# Supervisor (committed plist, copied verbatim).
cp "$REPO/deploy/com.windex.supervisor.plist" "$LA/com.windex.supervisor.plist"
install_one com.windex.supervisor

# Calendar agents (generated from the table above).
for j in "${JOBS[@]}"; do
  IFS='|' read -r label hour minute weekday cmd <<< "$j"
  cal_plist "$label" "$hour" "$minute" "$weekday" "$cmd"
  install_one "$label"
done

echo
echo "Installed $(all_labels | wc -l | tr -d ' ') agents."
echo "For the supervisor to start at BOOT (not just at manual login), enable"
echo "Automatic login: System Settings → Users & Groups → Automatically log in as."
