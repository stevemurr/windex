#!/usr/bin/env bash
# Install the unattended-boot fixes. Idempotent — safe to re-run.
#
# Run as your normal user (NOT under sudo): the system pieces are installed via
# sudo internally, while the systemd *user* drop-ins must be written as you.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ${EUID} -eq 0 ]]; then
    echo "error: run this as your normal user, not with sudo." >&2
    echo "       (the user-scope drop-ins would land in root's config)" >&2
    exit 1
fi

echo "==> system scope (sudo)"
sudo -v

# Keep a one-time backup of the hand-maintained spec before anything derives over
# it. Running containers already have their devices injected and are unaffected by
# a rewrite, but a bad spec would break the *next* container start — including an
# automatic one, since the GPU services are restart=unless-stopped.
if [[ -f /etc/cdi/nvidia.yaml && ! -f /etc/cdi/nvidia.yaml.pre-downconvert ]]; then
    sudo cp -a /etc/cdi/nvidia.yaml /etc/cdi/nvidia.yaml.pre-downconvert
    echo "    backed up /etc/cdi/nvidia.yaml -> .pre-downconvert"
fi

sudo install -m 0755 "${HERE}/nvidia-cdi-downconvert" /usr/local/sbin/nvidia-cdi-downconvert
sudo install -m 0644 "${HERE}/nvidia-cdi-downconvert.service" /etc/systemd/system/
sudo install -m 0644 "${HERE}/nvidia-cdi-downconvert.path"    /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nvidia-cdi-downconvert.path
# --now on a oneshot runs it immediately, which also proves it works and drops
# the readiness stamp for the models unit below.
sudo systemctl enable --now nvidia-cdi-downconvert.service

echo "==> user scope"
install -d "${HOME}/.config/systemd/user/windex-data.service.d"
install -d "${HOME}/.config/systemd/user/windex-models.service.d"

# Retire the CIFS mount guard (2026-07-24). The corpus moved to the local NVMe, so
# there is no mount to wait for — and leaving this in place costs up to 180s of
# boot latency on every boot, spent polling for a share nothing reads. Removed
# here rather than just deleted from the repo, so re-running this script cleans up
# an install that predates the move.
STALE_MOUNT_GUARD="${HOME}/.config/systemd/user/windex-data.service.d/10-wait-for-mount.conf"
if [[ -f ${STALE_MOUNT_GUARD} ]]; then
    rm -f "${STALE_MOUNT_GUARD}"
    echo "    removed stale mount guard (corpus is on local NVMe now)"
fi

STALE_MODEL_DROPIN="${HOME}/.config/systemd/user/windex-models.service.d/10-embeddings-only.conf"
if [[ -f ${STALE_MODEL_DROPIN} ]]; then
    rm -f "${STALE_MODEL_DROPIN}"
    echo "    removed stale embeddings-only model drop-in"
fi
install -m 0644 "${HERE}/windex-models.service.d/10-model-stack.conf" \
    "${HOME}/.config/systemd/user/windex-models.service.d/"
systemctl --user daemon-reload

echo
echo "==> verify"
systemctl is-enabled nvidia-cdi-downconvert.service nvidia-cdi-downconvert.path
echo -n "cdiVersion now: "; grep '^cdiVersion' /etc/cdi/nvidia.yaml
echo -n "readiness stamp: "; cat /run/windex-cdi-ready 2>/dev/null || echo "MISSING"
echo
echo "effective boot set for windex-models (must contain qwen3.6):"
systemctl --user show windex-models.service -p ExecStart --value \
    | grep -o 'up -d[^"]*' | grep 'qwen3\\.6' || {
        echo "  ERROR: qwen3.6 is absent from the effective boot set" >&2
        exit 1
    }
echo
echo "Done. Neither drop-in restarts anything now — they take effect on next boot"
echo "(or on an explicit: systemctl --user restart windex-data windex-models)."
