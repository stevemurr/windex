# Spark prep — getting the DGX Spark (192.168.1.237) ready for the windex cutover

Do these BEFORE the Phase 4 cutover (see `docs/search-overhaul-plan.md`). Steps
that need a password or `sudo` are for **you to run** — the Mac session won't
handle your password or make privileged system changes.

## 1. SSH key access (so the Mac session can reach the Spark passwordlessly)

On the Spark, add the Mac's public key (paste it into `authorized_keys`):

```bash
# on the Spark, as murr:
mkdir -p ~/.ssh && chmod 700 ~/.ssh
cat >> ~/.ssh/authorized_keys <<'KEY'
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDog7ylcNr5hgvyv4rfegtD8O5R51QyOWHUCxHlj4TL1LkpJqcnyOg0Pje5gX5lqeemVHRgjwt+eZCCKP9gBtmC2s9qoRLendT3SDXkZwsxTvC1rl5PAwhrLszy3AoyY9iIizwSTESDC/5jy/migdJA1rjXPEpuwyZOu9AGiDlc9qjW/Gdfs6o5M9Swc4Ek8surskdP6025vAUCW+A+uiV+EQ/kZDDdISsjx5ujtmppJGJsTuFMVMhPPMLZqxNSivkPG8Aj2raES0TQjCx69SJDFTbkeGn+RMien6OOMMRkkBULm0JcZCJRnqAMuYyFzyeWwBVcuF6u1KUU6jiPX2XwOlSYTz2jCo+KGLqeCbPh87RJEOdGTyrTk45CS0LrNtCYvHTi0T7oQHj4Ew/AOPSjhiWuDbEA238l8hP89gA6CY+8mtHQDL0KucdzRrbpwGQ14JXdnzdWW28GRiKV4dVFOTGT3aKEEqMNpmSkk52Nd5zY5r97rZTyUZbqlVzfVuE= murr@Stevens-Mac-mini.local
KEY
chmod 600 ~/.ssh/authorized_keys
```

Then `ssh murr@192.168.1.237 true` from the Mac should succeed with no password.

## 2. Share /Volumes/External from the Mac (SMB — recommended)

SMB is Apple's maintained/blessed sharing path and mounts cleanly from Linux;
macOS's `nfsd` is finicky (reserved ports, silent export rejection, `showmount`
lies), so prefer SMB. On the **Mac** (GUI is most reliable):

- System Settings → General → **Sharing** → **File Sharing = On**.
- Under *Shared Folders* click **+** and add **/Volumes/External**.
- Select it, click **Options…**, tick **Share files and folders using SMB**, and
  tick your account (**murr**) — enter your login password when prompted to store
  the SMB credential. The share name defaults to the volume name: **External**.

(CLI alternative, once File Sharing is on:
`sudo sharing -a /Volumes/External -S External -s 001`.)

## 3. Mount it on the Spark — run on the **Spark**

```bash
sudo apt-get install -y cifs-utils          # if not already present
sudo mkdir -p /mnt/windex-external
# a creds file keeps your password out of shell history and the mount command:
printf 'username=murr\npassword=<your-mac-login-password>\n' | sudo tee /etc/windex-smb.cred >/dev/null
sudo chmod 600 /etc/windex-smb.cred
sudo mount -t cifs //192.168.1.237/External /mnt/windex-external \
  -o credentials=/etc/windex-smb.cred,ro,uid=$(id -u),gid=$(id -g),iocharset=utf8,vers=3.0
ls /mnt/windex-external/windex/staging      # should show the parquet/staging dirs
```
Read-only (`ro`) — the Spark only READS parquet for the rebuild. To persist, add
an `/etc/fstab` line (`… cifs credentials=/etc/windex-smb.cred,ro,_netdev,uid=…`).
Then set `WINDEX_DATA_ROOT=/mnt/windex-external/windex`. (Move the disk to
Spark-attached storage later and just repoint the var.)

**NFS alternative** (if you'd rather): `/etc/exports` line
`/Volumes/External -alldirs -mapall=501 -network 192.168.1.0 -mask 255.255.255.0`,
then `sudo nfsd update`; mount `-t nfs -o resvport,ro`. `sudo nfsd checkexports`
prints why a line was rejected, and test the actual mount (macOS `showmount` is
unreliable).

## 4. De-risk Qdrant on aarch64 — run on the **Spark**

```bash
getconf PAGE_SIZE     # 4096 is fine; 65536 (64KB) needs a jemalloc-matched Qdrant build
podman --version      # install podman if absent
podman run --rm docker.io/qdrant/qdrant:latest --version   # arm64 image smoke test
```
If PAGE_SIZE is 65536, use a Qdrant build compiled for 64KB pages (or a
jemalloc-tuned image) — note it before the cutover.

## 5. Confirm the model endpoints (already on the Spark)

The gateway windex already calls for embeddings — confirm it also serves the
reranker + LLM, and note the exact model names for the `.env`:

```bash
curl -s http://192.168.1.237:4000/v1/models    # list served models (embed / rerank / llm)
# reranker smoke (adjust model name):
curl -s http://192.168.1.237:4000/rerank -H 'content-type: application/json' \
  -d '{"model":"<rerank-model>","query":"transformer","documents":["attention is all you need","a cooking blog"],"top_n":2}'
```
Fill these into the Spark's `.env`: `WINDEX_RERANK_ENDPOINT/MODEL`,
`WINDEX_JUDGE_ENDPOINT/MODEL` (see `.env.example`).

## 6. Clone windex on the Spark

```bash
cd /home/murr/Code    # alongside llm-inference-platform
git clone <windex remote> windex && cd windex
git checkout feat/search-overhaul
# copy .env from the Mac and edit: WINDEX_QDRANT_URL=http://localhost:6333,
# WINDEX_DATA_ROOT=/mnt/windex-external/windex, PG DSN → the Mac (until PG migrates),
# and the RERANK/JUDGE endpoints from step 5.
```

Once 1–6 are green, the Phase 4 cutover (Podman stack up, `pg_dump`→restore,
rebuild the RAM-resident index, backfill arxiv 2018+, move `serve`) is a
redeploy, not a scramble.
