# Unattended boot

Until 2026-07-24 a reboot left the box in a state that looked healthy and indexed
nothing. Three independent faults, none of which announced itself:

| # | Fault | Symptom | Status |
|---|-------|---------|--------|
| 1 | App containers start ~19s **before** the CIFS mount | Stack "up", every embed cycle fails on a missing parquet path | **gone** — corpus moved to local NVMe, 2026-07-24 |
| 2 | `/etc/cdi/nvidia.yaml` pins a stale `/dev/dri/cardN` | `failed to stat CDI host device`; no GPU container starts | fixed here |
| 3 | `windex-models.service` runs a bare `up -d` | Boot membership changes whenever the compose file changes | fixed here |

`install.sh` fixes 2 and 3, and removes the now-obsolete guard for 1 if a previous
install left it behind. Run it as your normal user (it sudo's internally):

```sh
bash ops/reboot/install.sh
```

## 1. The mount race — eliminated rather than guarded

**This fault no longer exists.** On 2026-07-24 the corpus moved off the CIFS share
onto the local NVMe (`WINDEX_DATA_ROOT=/home/murr/windex-data`, bound to the same
path inside the container), so there is no mount to race. The
`10-wait-for-mount.conf` drop-in has been deleted from the repo and uninstalled.

Kept here because the reasoning is worth not relearning:

The mount was never broken. `mnt-windex\x2dexternal\x2drw.mount` is generated from
fstab and came up on its own — it was just **slower than podman**:

```
23:52:23  windex-data.service -> podman-compose up -d
23:52:42  /mnt/windex-external-rw mounted        <- 19s late
```

podman resolves a bind at container *start*, so a container that won this race
bound the empty pre-mount directory and kept it until restarted. Nothing crashed;
the loops just failed forever on paths that did not exist.

`RequiresMountsFor=` would have been the idiomatic fix, but `windex-data.service`
is a **user** unit and the `.mount` lives in the **system** manager — systemd will
not order across that boundary. So the drop-in polled, with a 180s cap.

**And the poll lost.** Its first real test was the 2026-07-24 power outage: a
house-wide outage reboots the *Mac* too, so the share did not exist until the Mac
finished booting — the mount landed ~11 minutes after the containers started. The
180s cap expired, every container bound the empty directory, and indexing was
silently dead with the stack fully "healthy". Raising the cap was never a real fix
either: the Mac's boot time is unbounded from the Spark's point of view.

That is the argument that moved the corpus. A guard that must wait an unbounded
time for another machine is not a dependency you can make reliable — it is one to
delete. The 180s poll also cost that latency on *every* boot, for a share only
windex read.

## 2. The CDI spec

`nvidia-cdi-refresh.service` regenerates `/var/run/cdi/nvidia.yaml` at
**cdiVersion 0.7.0** on every boot and driver change. podman 4.9.3 cannot parse
0.7.0, so the only usable spec is a 0.6.0 copy at `/etc/cdi/nvidia.yaml` — which,
maintained by hand, goes stale in two different ways:

* **driver upgrade** → stale `.so` paths (`failed to fulfil mount request`)
* **plain reboot** → stale `/dev/dri/cardN` (`failed to stat CDI host device`)

The second is the one people get wrong. DRI numbering is **not stable across
boots** — observed `card1` → `card0` on 2026-07-24 with the driver unchanged at
580.173.02 — so a hand-patched file cannot survive on its own.

`nvidia-cdi-downconvert` therefore **derives** `/etc/cdi/nvidia.yaml` from the
freshly generated `/var/run` copy on every boot, applying only the schema
downgrade (`cdiVersion` + dropping the 0.7.0-only `additionalGids` blocks). It
never edits device paths, which makes both classes of staleness structurally
impossible. It runs at boot (`After=nvidia-cdi-refresh.service`) and again
whenever the generated spec changes (`.path` unit).

Do **not** "fix" this with `nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml`
— that writes 0.7.0 and makes it worse. Note too that `nvidia-ctk cdi list`
reads 0.7.0 happily and reports healthy devices; it tells you nothing about what
podman can use.

The real fix is **podman 5.x**, which reads 0.7.0 natively — then this whole
service and `/etc/cdi/nvidia.yaml` can be deleted.

## 3. The production model set is explicit

The bare `up -d` made every compose declaration part of the boot contract by
accident. The boot set is now explicit, and includes qwen3.6 because it is a
production system component:

    litellm-db litellm qwen3.6 embeddings dcgm-exporter prometheus grafana

Adding an experimental service to the compose file therefore does not make it
start unattended; changing the production set requires changing and reviewing
the drop-in.

## Ordering

```
nvidia-cdi-refresh.service     writes /var/run/cdi/nvidia.yaml (0.7.0)
  └─ nvidia-cdi-downconvert    writes /etc/cdi/nvidia.yaml (0.6.0) + /run/windex-cdi-ready
                                 │
       (system scope) ──────────┼───────────── (user scope)
                                 │
   windex-data.service      brings up postgres/qdrant/app (no wait — storage is local)
   windex-models.service    waits for /run/windex-cdi-ready, then the model stack
```

The remaining cross-scope handshake is a poll, not `After=`, because systemd cannot
order a user unit against a system unit. `/run` is tmpfs, so the readiness stamp
cannot survive a reboot and go falsely positive.

## Verifying without rebooting

```sh
# CDI: re-run and confirm it is a no-op the second time
sudo systemctl start nvidia-cdi-downconvert.service
journalctl -u nvidia-cdi-downconvert.service -n 5 --no-pager

# The CDI guard, in isolation
timeout 5 /usr/bin/sh -c 'until test -e /run/windex-cdi-ready; do sleep 2; done'; echo "cdi guard: $?"

# Storage: local, so this is a plain existence + headroom check, not a mount check
curl -s localhost:8100/metrics | grep '^windex_storage_ok'   # both tiers must be 1

# What will actually run at boot
systemctl --user show windex-models.service -p ExecStart --value
```

A real reboot is still the only complete test. After one, the pass condition is:
all Windex services plus qwen3.6, embeddings, and dcgm-exporter are up,
`windex_embeds_per_minute` is non-zero, and `windex_storage_ok` is 1 for both
tiers.

Note the post-outage manual restart that used to be required (fault 1) is no longer
needed — there is no mount for the containers to miss.
