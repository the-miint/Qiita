# First-deploy bootstrap

**Purpose.** Operator runbook for bringing up a fresh deployment: from
host provisioning through migrations against a clean database, the
first OIDC login, operator promotion to `system_admin`, data-plane
bootstrap, and orchestrator startup. End state: all three services
authenticated and ready to serve traffic. Each step is independent —
re-running it should be safe (idempotent) unless noted.

For the conceptual reference (principal model, scopes, endpoints), see
[`docs/auth.md`](../auth.md). The orchestrator authenticates to the CP
in two directions: the shared bearer (`/etc/qiita/cp-to-co.token`,
step 7) for inbound CP→CO calls, and a `compute-worker` service-account
PAT (`/etc/qiita/co-to-cp.token`, provisioned per
[`compute-service-account-provisioning.md`](compute-service-account-provisioning.md))
for outbound CO→CP callbacks like `POST /sequence-range`. Rotation of
the compute-worker PAT follows
[`orchestrator-token-rotation.md`](orchestrator-token-rotation.md).

> **Optional workflow tip.** If you have access to Claude (claude.ai/code
> or the CLI), pointing it at this runbook and asking it to walk you
> through one command at a time — pasting each command's output back —
> tends to catch failure modes the runbook doesn't yet anticipate
> (sudoer PATH quirks, cross-distro path conventions, conda shadowing,
> AuthRocket UI drift, etc.). The non-obvious gotchas captured below
> were surfaced this way. The runbook is meant to be self-contained
> without an AI assistant — every step has its own verification
> command; this tip is a convenience, not a prerequisite.

## Account model

This runbook uses two account labels on every command — both are *roles*,
not OS users, deliberately distinct from the `qiita-*` service-account
naming (`qiita-api`, `qiita-orch`, `qiita-data`, `qiita-job`):

| Label | What it means |
|---|---|
| `[operator]` | A shared, named operational account on the deploy host — typically a user literally named `qiita` (don't confuse with `qiita-api` etc.). **No sudo.** Owns the clone, holds shell env vars across steps, runs `make migrate`, owns `~/.qiita/token`. |
| `[admin]` | Your own personal account with sudo. Runs everything that touches `/etc/qiita/`, `/opt/qiita/`, `/etc/systemd/`, `/etc/nginx/`, and `systemctl`. |

The operator account (`qiita` in this runbook; substitute your site's
name) must **not** be a member of any of `qiita-services`,
`qiita-pipeline`, `qiita-data`, `qiita-fs`, or `qiita-job` — otherwise
the operator gains read access to service secrets and lake data, which
defeats the privilege separation the service users provide. Verify
with `id <operator>` in step 0.

Service start/stop authority on the qiita-* units is **root only**
(same model as `postgresql.service` / `nginx.service`). Operators run
`sudo systemctl restart qiita-control-plane` from their personal admin
account. There is no polkit rule or sudoers grant for `qiita`.

## Deploy mechanics: the two entry points

This runbook references two scripts under `deploy/`. They share the
same install logic; the difference is where the artifacts come from:

- **`deploy/activate.sh`** — installs whatever's already in
  `/opt/qiita/incoming/` (rsync target). Run by **CI** after CI builds
  on a separate host and rsyncs the artifacts over. CI invokes it
  directly: `sudo QIITA_HOSTNAME=... deploy/activate.sh`.
- **`deploy/local-deploy.sh`** — the **manual / CI-less** entry point.
  Run on the deploy host from a local git clone. It does the rsync
  (after `git pull` + `make build-data-plane` as the operator account)
  and then exec's `activate.sh`. First deploy uses this; ongoing manual
  deploys also use this. Until CI exists, this is the only deploy path.

When CI lands, ongoing deploys move to it automatically; `local-deploy.sh`
remains as the manual fallback (broken CI, hotfix from a feature
branch, etc.).

## 0. Prerequisites

This runbook assumes the host has been provisioned by your infra / DBA
team. Each prerequisite below comes with a one-line verification
command so you can confirm the state before running step 1.

### 0.1 System users + groups

System users (created once as non-login system accounts):

| User | Role |
|---|---|
| `qiita-api` | Control plane (FastAPI) |
| `qiita-orch` | Compute orchestrator |
| `qiita-data` | Data plane (Arrow Flight / Rust) |
| `qiita-job` | SLURM job-execution identity (workflow jobs run as this user on the cluster); on the deploy host, also owns the JWT-refresh timer |

System groups (composition matters):

| Group | Members | Purpose |
|---|---|---|
| `qiita-services` | `qiita-api`, `qiita-orch` | Read shared service secrets (`/etc/qiita/cp-to-co.token`). |
| `qiita-pipeline` | `qiita-api`, `qiita-data`, `qiita-job`, `qiita-orch` | Two adjacent jobs: (a) staging handoff — SLURM jobs write Parquet under the staging dir, DP reads via group membership; (b) orchestrator workspace — qiita-api (CP runner) writes per-ticket subdirs via group, `qiita-orch` (owner) writes per-step subdirs and *also* uses pipeline group-write to descend into CP-created parents, `qiita-job` writes outputs via the same group. Sites sometimes provision under a different name (e.g. `qiita-fs`) — what matters is the *membership*. |
| `qiita-data` | `qiita-data` (single member) | Locks the durable Parquet dir to the DP only. Required as a separate group so the data dir at mode `0750` does **not** also grant read to `qiita-job` (which would defeat the staging/parquet split). |

`qiita-api` belongs to **both** `qiita-services` and `qiita-pipeline`.
The pipeline membership is what lets the CP runner mkdir per-ticket
workspaces under the orchestrator workspace dir (see §0.3) — without
it, the first work-ticket dispatch fails at `workspace.mkdir(...)`
because the dir is owned `qiita-orch:qiita-pipeline 2770`. `qiita-orch`
also belongs to `qiita-pipeline` so it can descend into the per-ticket
subdirs the CP runner creates (resolved as the bundled fix for the
nested workspace dir perms issue described next).

> **Nested workspace dir perms — both layers shipped in this PR.** The
> per-ticket subdirs the CP runner creates inherit `qiita-pipeline` via
> setgid, but their *mode* depends on systemd's UMask. Default UMask
> 0022 lands them at `0755`, where `qiita-orch` is "other" → traverse
> but no write — the orchestrator's SlurmBackend then hits
> PermissionError on `input/output/logs/` mkdir under each attempt
> dir. The fix combines two halves:
>
> 1. `UMask=0007` systemd dropins on **both** `qiita-control-plane`
>    and `qiita-compute-orchestrator` (see
>    `deploy/systemd/*.service.d/umask.conf`). The CP's UMask makes
>    every per-ticket dir land at `2770`; the orchestrator's UMask
>    propagates the same posture to the nested `input/output/logs/`
>    dirs and to per-step attempt dirs.
> 2. `qiita-orch` is a member of `qiita-pipeline`. Mode `2770` only
>    helps if `qiita-orch` is *in* the group; without it, "group" still
>    excludes the orchestrator and the mode argument is moot. With
>    membership, `qiita-orch` writes via group both into CP-created
>    parents (which it doesn't own) and through to its own descendant
>    dirs (which inherit `qiita-pipeline` via setgid).
>
> Both halves ship in `deploy/activate.sh` and the runbook usermod
> below; no operator-side toggle.

Verification:
```bash
# [either] system users exist with nologin shell
getent passwd qiita qiita-api qiita-orch qiita-data qiita-job

# [either] groups exist with expected memberships
getent group qiita-services qiita-pipeline qiita-data

# [either] service users have the right primary / secondary groups
# (qiita-api must show BOTH qiita-services AND qiita-pipeline in `groups=`;
#  qiita-orch must show BOTH qiita-services AND qiita-pipeline; if any
#  service user is missing qiita-pipeline run `sudo usermod -aG
#  qiita-pipeline <user>` and `systemctl restart` the service.)
id qiita-api qiita-orch qiita-data qiita-job

# [either] operator is NOT in any service group
id qiita
```

### 0.2 Postgres databases + roles

Provided by your DBA:

- `qiita_miint` database with `qiita_miint_rw` role (used by the control plane)
- `qiita_miint_lake` database with `qiita_miint_lake_rw` role (used by the data plane as DuckLake catalog)
- Network reachability from the deploy host to the Postgres host
- **Postgres extensions enabled** in `qiita_miint`: `citext` (required by the auth migrations). Created by the DBA as superuser: `sudo -u postgres psql -d qiita_miint -c "CREATE EXTENSION IF NOT EXISTS citext;"`. The `qiita_miint_rw` role doesn't need CREATE-EXTENSION privilege; the extension being installed at the DB level is enough. Keep this list current by grepping `qiita-control-plane/db/migrations/` for `CREATE EXTENSION` before each deploy.
- **NTP** running on both the deploy host and the PG host. A `cli_login_code` CHECK constraint (`expires_at > created_at`) compares a Python-computed `expires_at` against a PG-computed `created_at`; clock drift larger than `CLI_LOGIN_CODE_TTL_SECONDS` (default 30s) violates it and produces a 500 on `/auth/handoff`. Standard `chronyd` setup on both hosts; verify with `chronyc tracking` and `timedatectl`.

Verification:
```bash
# [operator] interactive password prompt avoids putting the password in shell history
psql -h <pg-host> -U qiita_miint_rw -d qiita_miint -W \
    -c "SELECT current_user, current_database();"

psql -h <pg-host> -U qiita_miint_lake_rw -d qiita_miint_lake -W \
    -c "SELECT current_user, current_database();"

# [operator] confirm citext is enabled and clock skew is small
psql -h <pg-host> -U qiita_miint_rw -d qiita_miint -W -c "
  SELECT 'a'::citext = 'A'::citext AS citext_ok,
         extract(epoch from (now() AT TIME ZONE 'UTC' - now())) AS pg_now_utc_offset;"
date -u  # compare with the pg_now_utc_offset above — should agree within seconds
```

### 0.3 Shared filesystem

Qiita's filesystem paths derive from three **base roots** the operator
sets once per component env file; the services compute fixed subdirs from
them (no per-leaf env var). The data plane writes Parquet under a durable
mount (`PATH_PERSISTENT/ducklake`); SLURM upload staging lives under a
scratch mount (`PATH_SCRATCH/staging`), ideally on the same filesystem as
the lake for the atomic-rename fast path; the orchestrator stages
per-ticket workspaces under the same scratch mount (`PATH_SCRATCH/ticket`),
visible from every compute node (SLURM uses it as the job's
`current_working_directory`).

| Base root | Derived leaf | Owner | Group | Mode | Notes |
|---|---|---|---|---|---|
| `PATH_PERSISTENT` | `…/ducklake` | `qiita-data` | `qiita-data` | `0750` | DuckLake data path. DP-only. |
| `PATH_SCRATCH` | `…/staging` | `qiita-data` | `qiita-pipeline` | `2770` | DoPut upload staging. Setgid forces `qiita-pipeline` group on inherited files. |
| `PATH_SCRATCH` | `…/ticket` | `qiita-orch` | `qiita-pipeline` | `2770` | Per-ticket workspace trees (`<work_ticket_idx>/<step>/attempt-N/`). `qiita-orch` writes per-step subdirs as owner; `qiita-api` (CP runner) and `qiita-job` (SLURM job outputs) write via the `qiita-pipeline` group. Setgid carries the group to inherited files. |
| `PATH_DERIVED` | `…/images` | `qiita-orch` | `qiita-orch` | `0755` | Apptainer SIF tier (SLURM container steps). Created at the bcl-convert deploy, not here — see `DEPLOY_CHECKLIST.md`. |

The leaf dirs are always those fixed subdirs of the base roots — the
service derives them in code, so there's no subdir-vs-mount choice to
make. The mount points themselves stay infra-owned (`root:root 0755`);
the operator creates each derived leaf with the ownership above.

**Placeholder vocabulary used downstream.** `<persistent>` is the value
of `PATH_PERSISTENT` (the durable mount root); `<scratch>` is the value of
`PATH_SCRATCH` (the scratch mount root). The final paths are always
`<persistent>/ducklake`, `<scratch>/staging`, and `<scratch>/ticket`.
`PATH_SCRATCH` must be byte-identical across the control-plane,
data-plane, and compute-orchestrator env files (all three derive the same
`/ticket` and/or `/staging`).

Three places consume these placeholders, named here so renumbering
doesn't strand the reference:
- the data-plane dir-creation step creates `<persistent>/ducklake` and
  `<scratch>/staging`
- the orchestrator workspace dir-creation step creates `<scratch>/ticket`
- the env-file rendering steps wire `PATH_PERSISTENT=<persistent>` (data
  plane), `PATH_SCRATCH=<scratch>` (data plane, control plane, and compute
  orchestrator — the same value in all three)
- the end-to-end smoke references both `<persistent>/ducklake` and
  `<scratch>/ticket` paths.

Verification:
```bash
# [either]
stat -c '%n  owner=%U  group=%G  mode=%a  device=%d' <persistent>/ducklake <scratch>/staging <scratch>/ticket
```

Watch the **device numbers**: if `<persistent>/ducklake` and
`<scratch>/staging` are on different volumes, the DP's rename from staging
to final falls back to copy+delete. Works correctly, just slower. Flag to
infra if you want the fast path; not blocking otherwise.

### 0.4 nginx + TLS + DNS

Required:

- nginx is installed and `active` on the deploy host.
- DNS for the externally-resolvable hostname (e.g. `qiita.example.org`)
  resolves to this host.
- TLS cert + key are installed.

**Cert path convention varies by distro family**:

- Debian-family: typically `/etc/ssl/certs/...`, `/etc/ssl/private/...`
- RHEL-family (Rocky / RHEL / Alma): typically `/etc/pki/tls/certs/...`, `/etc/pki/tls/private/...`

The shipped nginx template (`deploy/nginx/qiita.conf`) hard-codes the
Debian paths at `/etc/ssl/certs/qiita.crt` and `/etc/ssl/private/qiita.key`.
On a RHEL-family host where the sysadmin installed certs under
`/etc/pki/tls/`, **symlink** the qiita-expected paths to where the
certs actually live:
```bash
# [admin]
sudo install -d -o root -g root -m 0755 /etc/ssl/certs
sudo install -d -o root -g root -m 0710 /etc/ssl/private
sudo ln -s /etc/pki/tls/certs/<cert>.cer    /etc/ssl/certs/qiita.crt
sudo ln -s /etc/pki/tls/private/<key>.key   /etc/ssl/private/qiita.key
```
Symlinks survive in-place cert rotation (sysadmin replaces the underlying
file; symlink target stays valid) and survive every redeploy. Editing
`qiita.conf` to point at the real paths is a trap: `activate.sh`
re-installs the upstream template on every deploy and silently
overwrites the edit.

Verification:
```bash
# [admin]
systemctl is-active nginx
sudo ls -la /etc/ssl/certs/qiita.crt /etc/ssl/private/qiita.key
sudo readlink -f /etc/ssl/certs/qiita.crt /etc/ssl/private/qiita.key
dig +short <fqdn>
hostname -I
```

If there's a pre-existing `/etc/nginx/conf.d/default.conf` (or any
non-qiita vhost) on the host, disable it before reloading nginx — it
can shadow `qiita.conf` and serve a different TLS chain that breaks
AuthRocket's URL validator on first realm setup:
```bash
# [admin]
sudo mv /etc/nginx/conf.d/default.conf /etc/nginx/conf.d/default.conf.disabled
```

### 0.5 Build + ops tooling

The deploy host needs the toolchain to compile the data-plane binary
and create the Python service venvs. Install system-wide so they're
available to both `[operator]` (for `make migrate`, `qiita-admin`) and
`[admin]` (for `deploy/local-deploy.sh`).

| Tool | Why needed | Rocky 10 install |
|---|---|---|
| `git`, `make`, `psql`, `curl`, `openssl`, `jq` | basic ops | usually pre-installed; if any are missing, `sudo dnf install -y <tool-name>` |
| `gcc`, `gcc-c++`, `cmake` | build duckdb (bundled) | `sudo dnf install -y gcc gcc-c++ cmake` |
| `rust`, `cargo` | build data plane | `sudo dnf install -y rust cargo` |
| `apptainer` | run workflow containers (optional for first deploy) | `sudo dnf install -y epel-release && sudo dnf install -y apptainer` |
| `uv` | Python venv management | not in dnf — install standalone (see below) |

uv install (standalone tarball, no `curl \| sh`-as-root):
```bash
# [admin]
cd /tmp && \
  curl -LsSf -o uv.tar.gz https://github.com/astral-sh/uv/releases/latest/download/uv-x86_64-unknown-linux-gnu.tar.gz && \
  tar -xzf uv.tar.gz && \
  sudo install -m 0755 uv-x86_64-unknown-linux-gnu/uv  /usr/local/bin/uv && \
  sudo install -m 0755 uv-x86_64-unknown-linux-gnu/uvx /usr/local/bin/uvx && \
  rm -rf uv-x86_64-unknown-linux-gnu uv.tar.gz
```

`dbmate` (used by `make migrate`) and `grpcurl` (used by `make
verify-health`) **do not** need a system-wide install — the Makefile
auto-fetches them into `~/.local/bin/` of whoever runs them.

Verification:
```bash
# [admin]
command -v uv cargo rustc apptainer cmake gcc g++ make rsync
```
All should resolve to `/usr/local/bin/...` or `/usr/bin/...`.

### 0.6 sudo's `secure_path`

RHEL-family `/etc/sudoers` ships with a `secure_path` that does **not**
include `/usr/local/bin`. So `sudo uv ...` fails with "command not found"
even though `uv` is at `/usr/local/bin/uv`. The shipped
`deploy/activate.sh` works around this by hard-coding `/usr/local/bin/uv`,
but for any other sudo'd commands you'd run that need `/usr/local/bin`
tools, either use the full path or have the sysadmin add the directory
to `secure_path`:
```
Defaults  secure_path = /sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin
```

### 0.7 Conda interference on shared operator accounts

If your operator account (e.g. `qiita`) is a shared cross-server
identity whose login dotfiles auto-activate a conda environment
(common on HPC sites), conda will shadow system binaries (`openssl`,
`jq`) and add noise. To keep the deploy host clean without disturbing
the account's setup elsewhere, append a host-scoped opt-out to
`~/.bash_profile` on the deploy host:
```bash
if [ "$(hostname -s)" = "<deploy-hostname>" ]; then
    while [ -n "${CONDA_DEFAULT_ENV:-}" ]; do conda deactivate 2>/dev/null || break; done
    [ -n "${PATH:-}" ] && export PATH=$(echo "$PATH" | awk -v RS=: -v ORS=: '!/miniconda3|conda/' | sed 's/:$//')
    unset -f conda 2>/dev/null
    unset CONDA_SHLVL CONDA_DEFAULT_ENV CONDA_PYTHON_EXE CONDA_EXE CONDA_PREFIX 2>/dev/null
fi
```
Reopen the shell to confirm `command -v python openssl jq` resolves
under `/usr/bin/`.

### 0.8 AuthRocket realm

Configured per [`authrocket-realm-setup.md`](authrocket-realm-setup.md).
The realm must be reachable (`curl -sSf
https://<realm>.loginrocket.com/connect/jwks` returns a JWKS JSON)
**before** step 1.

**Ordering caveat**: if AuthRocket's "App or login URL" field validation
fails, it's almost certainly because the validator probes the URL and
your nginx isn't serving cleanly yet. Complete steps 0.4 and the
bootstrap section below first (so `https://<fqdn>/` returns a 502 from
qiita's nginx), then revisit the realm field.

## Bootstrap (one-time, before step 1)

`/opt/qiita/` is empty on a fresh host. This section gets the code +
binaries onto the host so the rest of the runbook works. Subsequent
deploys after the first will use the same machinery (just re-run
`local-deploy.sh`).

### Create `/opt/qiita/` and `/etc/qiita/`

```bash
# [admin]
sudo install -d -o root -g root -m 0755 /opt/qiita
sudo install -d -o root -g root -m 0755 /etc/qiita
```

### Clone the repo as `qiita`

```bash
# [operator]
cd ~ && git clone <repo-url> qiita-miint
```

Pick `main` unless you have a specific reason to deploy a feature
branch. The clone path here (`~/qiita-miint`) is referenced by
`deploy/local-deploy.sh` as the default `QIITA_CLONE`.

### Run the first deploy

`deploy/local-deploy.sh` does the rest: pulls latest source, builds the
data-plane binary (slow, ~5-20 min on first run as DuckDB compiles from
source — use tmux/screen if SSH might drop), stages everything into
`/opt/qiita/incoming/`, and exec's `deploy/activate.sh`. Activate is
first-deploy-safe: it skips service restarts when env files are absent
and skips the nginx reload when TLS files are absent (both states are
resolved later by the steps below).

```bash
# [admin]
sudo QIITA_HOSTNAME=<fqdn> /home/qiita/qiita-miint/deploy/local-deploy.sh
```

Expected: build runs to completion; rsyncs land in `/opt/qiita/`; `uv
sync` populates each Python venv under `/opt/qiita/<service>/.venv/`;
data-plane binary is installed at `/opt/qiita/data-plane/qiita-data-plane`;
systemd units land in `/etc/systemd/system/`; nginx config installs at
`/etc/nginx/conf.d/qiita.conf` with `__QIITA_HOSTNAME__` substituted.

Verify the venvs use a world-traversable Python (not `/root/.local/`):
```bash
# [admin]
ls -la /opt/qiita/control-plane/.venv/bin/python /opt/qiita/compute-orchestrator/.venv/bin/python
sudo -u qiita-api  /opt/qiita/control-plane/.venv/bin/python --version
sudo -u qiita-orch /opt/qiita/compute-orchestrator/.venv/bin/python --version
```
Symlinks should resolve under `/opt/uv-python/` (not `/root/.local/...`),
and both service users should be able to execute their Python.

### Put `qiita-admin` on the operator's PATH

The `qiita-admin` CLI lives in the deployed CP venv at
`/opt/qiita/control-plane/.venv/bin/qiita-admin`. The operator runs it
from many steps below; symlink it into the operator's PATH once:

```bash
# [operator]
mkdir -p ~/.local/bin
ln -sf /opt/qiita/control-plane/.venv/bin/qiita-admin ~/.local/bin/qiita-admin
hash -r   # forget any stale lookup cache in this shell
command -v qiita-admin   # expect ~/.local/bin/qiita-admin (neutral against QIITA_USER overrides)
```

If `~/.local/bin` isn't on the operator's PATH, add it via `.bash_profile`
(`export PATH="$HOME/.local/bin:$PATH"`).

## 1. Write the control plane env file

systemd loads `/etc/qiita/control-plane.env` for the CP. This step
generates the shared HMAC secret and installs a rendered copy of the
committed template at `.env.control-plane.example`.

**Keep these shell-resident across steps 1-8b** (use tmux/screen or
otherwise ensure the shell doesn't close):

- `$HMAC_SECRET_KEY` — step 8b's data-plane env file must see the
  byte-identical value.
- `$DATABASE_URL` — step 2 (`make migrate`), step 3 (`verify_jwt.py`),
  and step 6 (`qiita-admin set-system-role` — direct DB, no HTTP) all
  read it. Sourced from `/tmp/control-plane.env` below; once `[admin]`
  installs the final copy as `root:qiita-api 0440`, the operator can
  no longer read it back from disk, so the in-shell copy is the only
  surviving access until step 8b is done.

If either var goes missing between steps, recover via
`sudo cat /etc/qiita/control-plane.env` (admin shell) and re-export
manually — or for the HMAC specifically, regenerate (in which case
you must also re-render the data-plane env file with the new value
and restart both services).

**The cross-account handoff.** The CP env file holds secrets (DB password,
HMAC). `[operator]` renders the working copy at `/tmp/control-plane.env`
(mode `0600` qiita-owned), `[operator]` sources it into their own shell
**before** `[admin]` shreds it (otherwise the operator can't read
`DATABASE_URL` for `make migrate` and the `verify_jwt.py` step), then
`[admin]` installs the final copy at `/etc/qiita/control-plane.env` with
`root:qiita-api 0440` and shreds the working copy.

Prerequisites: pg-host + password for `qiita_miint_rw` (DBA); AuthRocket
realm subdomain (from `authrocket-realm-setup.md`); externally-resolvable
FQDN; the path you've picked for `<scratch>` (the scratch mount root from
§0.3 — the `ticket` and `staging` leaves don't have to exist yet, they get
created in §8a/§9a). The CP boot fails fast if `PATH_SCRATCH` is unset, so
you're committing to the path here even though the leaf dirs show up a few
steps later.

```bash
# [operator] in a stable shell (use tmux/screen — HMAC_SECRET_KEY must survive
# into step 8b's data-plane env file)
cd ~/qiita-miint

# Shared HMAC. CP and DP must see the byte-identical value.
export HMAC_SECRET_KEY=$(openssl rand -base64 32)

# Render and tighten perms before any secrets land in the file
cp .env.control-plane.example /tmp/control-plane.env
chmod 0600 /tmp/control-plane.env

# Substitute every value we have. The password is filled by the editor pass below.
sed -i.bak \
    -e "s|^HMAC_SECRET_KEY=.*|HMAC_SECRET_KEY=$HMAC_SECRET_KEY|" \
    -e "s|^DATABASE_URL=.*|DATABASE_URL='postgresql://qiita_miint_rw:<password>@<pg-host>/qiita_miint?sslmode=prefer'|" \
    -e "s|^# AUTHROCKET_ISSUER=.*|AUTHROCKET_ISSUER='https://authrocket.com'|" \
    -e "s|^# AUTHROCKET_LOGINROCKET_URL=.*|AUTHROCKET_LOGINROCKET_URL='https://<realm>.loginrocket.com'|" \
    -e "s|^# AUTHROCKET_JWKS_URL=.*|AUTHROCKET_JWKS_URL='https://<realm>.loginrocket.com/connect/jwks'|" \
    -e "s|^# QIITA_ENDPOINT_URL=.*|QIITA_ENDPOINT_URL='https://<fqdn>'|" \
    -e "s|^# COMPUTE_ORCHESTRATOR_URL=.*|COMPUTE_ORCHESTRATOR_URL=http://127.0.0.1:8081|" \
    -e "s|^PATH_SCRATCH=.*|PATH_SCRATCH=<scratch>|" \
    /tmp/control-plane.env
rm /tmp/control-plane.env.bak

# Fill in the remaining placeholders interactively (DB password and any
# realm/fqdn values you didn't sed-substitute above)
${EDITOR:-vi} /tmp/control-plane.env

# Source into qiita's shell — DATABASE_URL is needed by `make migrate` in
# step 2 and the env vars are needed by scripts/verify_jwt.py in step 3.
# Once admin installs the file at /etc/qiita/control-plane.env, qiita can't
# read it back (root:qiita-api 0440), so source must happen first.
set -a && source /tmp/control-plane.env && set +a
```

```bash
# [admin] install the final copy and shred the working file
sudo install -m 0440 -o root -g qiita-api /tmp/control-plane.env /etc/qiita/control-plane.env
# sudo on shred because /tmp/control-plane.env is qiita-owned mode 0600 and admin can't write it
sudo shred -u /tmp/control-plane.env
```

`?sslmode=prefer` makes `dbmate` (which uses Go's `lib/pq` driver, defaulting
to `sslmode=require`) fall back to plain when the PG server doesn't speak SSL,
while still upgrading if SSL becomes available later. Without the explicit
setting, `make migrate` fails with:
```
Error: pq: SSL is not enabled on the server
```
`asyncpg` (used by the CP at runtime) defaults to `prefer` already, so the
explicit setting only changes `make migrate`'s behavior.

**If HMAC values disagree** later. Every Flight DoGet/DoAction will return
`Unauthenticated: invalid HMAC signature` from the data plane. The control
plane logs the signed ticket with no error — the failure shows up only at
the DP. Fix: confirm `HMAC_SECRET_KEY` is byte-identical in both env files,
then `sudo systemctl restart qiita-control-plane qiita-data-plane@*`.

See [`authrocket-realm-setup.md`](authrocket-realm-setup.md) for the
realm-side configuration.

## 2. Apply migrations

```bash
# [operator] DATABASE_URL is already in qiita's shell from the source in step 1
make migrate
```

`make migrate` runs `dbmate up` and auto-installs `dbmate` to
`~/.local/bin/` on first run. Seeds the system principal at `idx=1`
and creates the auth tables. After this, `qiita.principal` has exactly
one row (the `system` principal); no human or service-account principals
exist yet.

Verify:
```bash
# [operator]
psql -h <pg-host> -U qiita_miint_rw -d qiita_miint -W \
    -c "SELECT idx, display_name, system_role FROM qiita.principal;"
```
Expected: one row, `1 | system | system_admin`.

Note: the `principal` table has no `kind` column — the principal subtype
(human / service / system) surfaces in `qiita-admin whoami` output
(step 10b) via joins with `qiita.user` / `qiita.service_account`, but
isn't a column on `principal` itself. Don't add `kind` to ad-hoc queries.

## 3. One-shot JWT shape verify

Log into the AuthRocket realm via the hosted UI (the AuthRocket
[smoke test](authrocket-realm-setup.md#smoke-test) shows how to capture
the raw JWT). Then:

```bash
# [operator] env vars are still live from step 1's source. read -s keeps
# the token out of bash history.
read -s JWT
# (paste, Enter — no echo, that's normal)

# The repo has no top-level pyproject.toml; uv run needs to find one,
# so cd into qiita-control-plane first. The verifier imports from
# qiita_control_plane.* and reuses the production resolver path.
cd ~/qiita-miint/qiita-control-plane
uv run python ../scripts/verify_jwt.py "$JWT"
```

First `uv run` syncs the qiita-control-plane venv (~30s); subsequent
runs are fast.

On success it prints the parsed claims (`iss`, `sub`, `email`, etc.).
If anything fails — bad signature, wrong issuer, missing required claim
— fix the realm config before proceeding (cross-reference with
`authrocket-realm-setup.md`).

## 4. Start the control plane

```bash
# [admin]
sudo systemctl enable --now qiita-control-plane
```

systemd loads `/etc/qiita/control-plane.env`. `AuthRocketVerifier.from_settings`
runs at FastAPI lifespan — if any required env var is missing the boot
fails fast. Check `journalctl -u qiita-control-plane` for the specific
missing var.

## 5. Verify the reverse proxy is reachable

```bash
# [anywhere — qiita's shell, your laptop, etc.]
curl -sSf -o /dev/null -w "%{http_code}\n" https://<fqdn>/health
# expected: 200
```

The CP exposes `GET /health` at the root (not under `/api/v1/`); nginx's
catch-all `location /` proxies it to `qiita_control_plane` upstream.

Failure modes:

- **DNS / connection refused** — hostname doesn't resolve, or nginx
  isn't running.
- **`curl: (60) SSL certificate problem`** — cert/key mismatch, expired
  cert, or the symlinks at `/etc/ssl/{certs,private}/qiita.{crt,key}`
  don't resolve. `sudo readlink -f` them.
- **`404 Not Found` on `/health` but other paths return 502** — the
  `__QIITA_HOSTNAME__` substitution didn't run. Check `grep server_name
  /etc/nginx/conf.d/qiita.conf` for the placeholder still present.
- **`502 Bad Gateway`** — nginx is up but the CP isn't on `127.0.0.1:8080`.
  `systemctl status qiita-control-plane` and `journalctl -u qiita-control-plane`.

## 6. First human login (operator promotion + PAT mint)

`qiita-admin login` drives the LoginRocket Web flow end-to-end: spawns a
localhost loopback HTTP server, opens the browser to qiita's `/auth/login`,
captures the AuthRocket round-trip, exchanges the one-time code for a
PAT, and saves it to `~/.qiita/token` (mode 0600).

> **Before step 6 on SSH-tunneled deploys.** The cli_login_code TTL
> (default 30s, set by `CLI_LOGIN_CODE_TTL_SECONDS`) is way too short
> for a flow that involves manual URL copy + SSH tunnel + curl. For
> the duration of the SSH-tunneled flow, bump it:
>
> ```bash
> # [admin]
> sudo bash -c "echo 'CLI_LOGIN_CODE_TTL_SECONDS=300' >> /etc/qiita/control-plane.env"
> sudo systemctl restart qiita-control-plane
> ```
>
> **Set it back to the default once the local browser-loopback flow
> works on this deploy.** The 30s default deliberately keeps the
> intercept window short; a permanent 300s value is only justified if
> you'll keep doing the SSH-tunneled flow regularly. Roll back with:
>
> ```bash
> # [admin]
> sudo sed -i '/^CLI_LOGIN_CODE_TTL_SECONDS=/d' /etc/qiita/control-plane.env
> sudo systemctl restart qiita-control-plane
> ```

```bash
# [operator]
qiita-admin --base-url https://<fqdn> login
```

On the *first* login, the resolver creates a `principal` row
(`system_role='user'`), a `user` row keyed on that principal, and a
`user_identities(iss, sub)` link in one transaction. An
`oidc_create_principal` audit event is recorded. The freshly-minted PAT
is written to `~/.qiita/token`.

Promote yourself to `system_admin` (direct DB; no HTTP auth needed):

```bash
# [operator]
qiita-admin set-system-role --email <operator-email> --role system_admin
```

After the role change takes effect, the existing PAT carries `user`-level
scopes. Re-run `qiita-admin login` to mint a fresh PAT scoped to the
`system_admin` ceiling.

### Headless / no-browser fallback

For SSH'd remotes / CI / containers without DISPLAY, `qiita-admin login`
prints the URL it tried to open. Open it on a machine with a browser,
complete the AuthRocket login, and the redirect will land at
`http://127.0.0.1:<port>/?ot_code=<value>`. If that loopback isn't
reachable on the same host, capture the JWT manually via the AuthRocket
admin UI and use the legacy direct PAT mint:

```bash
curl -X POST https://<fqdn>/api/v1/auth/pat \
    -H "Authorization: Bearer $JWT" \
    -H "Content-Type: application/json" \
    -d '{"label":"my-laptop","ttl_days":90}'
```

Save the plaintext `qk_...` to `~/.qiita/token` (mode 0600).

## 7. Install the CP↔CO shared bearer

The control plane and the compute orchestrator authenticate to each other
on a private path via a constant-time string compare against a shared
bearer. Both services read the same file.

```bash
# [admin]
openssl rand -base64 32 | sudo tee /etc/qiita/cp-to-co.token > /dev/null
sudo chown root:qiita-services /etc/qiita/cp-to-co.token
sudo chmod 0440 /etc/qiita/cp-to-co.token
```

Owner `root`, group `qiita-services` (contains `qiita-api` and
`qiita-orch`), mode `0440`: both service users read via group membership;
only root can write.

The CO loads it via `CP_TO_CO_TOKEN_PATH` (default
`/etc/qiita/cp-to-co.token`); it will refuse to start if the file is
missing or unreadable. Dev / CI can opt into env-var fallback by setting
`QIITA_ALLOW_TOKEN_ENV=true` and providing `CP_TO_CO_TOKEN`; production
must use the file.

## 8. Bootstrap the data plane

Prerequisites from your DBA: `qiita_miint_lake` database,
`qiita_miint_lake_rw` role with login password, network reachability
from the data plane host to the Postgres host.

The data plane bootstraps its own DuckLake catalog on first start —
there's no `make migrate` equivalent. You only need to give it the
catalog DB credentials, a directory on the shared filesystem to hold
Parquet files, and an env file.

### 8a. Create the Parquet data and staging directories

The data plane derives `PATH_PERSISTENT/ducklake` (lake data) and
`PATH_SCRATCH/staging` (DoPut upload staging), so create exactly those
leaf dirs under the mount roots picked in §0.3. The mounts themselves stay
infra-owned.

```bash
# [admin]
sudo install -d -o qiita-data -g qiita-data -m 0750     <persistent>/ducklake
sudo install -d -o qiita-data -g qiita-pipeline -m 2770 <scratch>/staging
```

Then `PATH_PERSISTENT=<persistent>` and `PATH_SCRATCH=<scratch>` in step 8b
(the data plane appends `/ducklake` and `/staging` itself).

The setgid bit on the staging dir (`2770`) is load-bearing: files
written by `qiita-job` inherit the `qiita-pipeline` group regardless
of the writing process's primary group, so the data plane (also in
`qiita-pipeline`) can read them.

### 8b. Write the data plane env file

Render the committed template at `.env.data-plane.example`, substituting
the HMAC secret from step 1's shell.

```bash
# [operator]
cd ~/qiita-miint
cp .env.data-plane.example /tmp/data-plane.env
chmod 0600 /tmp/data-plane.env

sed -i.bak \
    -e "s|^HMAC_SECRET_KEY=.*|HMAC_SECRET_KEY=$HMAC_SECRET_KEY|" \
    -e "s|^DUCKLAKE_CATALOG_CONNSTR=.*|DUCKLAKE_CATALOG_CONNSTR='dbname=qiita_miint_lake host=<pg-host> user=qiita_miint_lake_rw password=<password> sslmode=prefer'|" \
    -e "s|^PATH_SCRATCH=.*|PATH_SCRATCH=<scratch>|" \
    -e "s|^# PATH_PERSISTENT=.*|PATH_PERSISTENT=<persistent>|" \
    /tmp/data-plane.env
rm /tmp/data-plane.env.bak

${EDITOR:-vi} /tmp/data-plane.env  # fill the lake DB password
```

```bash
# [admin]
sudo install -m 0440 -o root -g qiita-data /tmp/data-plane.env /etc/qiita/data-plane.env
sudo shred -u /tmp/data-plane.env
```

`DUCKLAKE_CATALOG_CONNSTR` is libpq format — space-separated `key=value`
pairs, NOT a `postgresql://` URL. (`DATABASE_URL` in the CP env is the
URL form because asyncpg accepts that; the data plane reads the raw
libpq form because DuckDB's postgres extension expects it.)

After this step the shared `$HMAC_SECRET_KEY` is no longer needed in
the operator's shell (it's persisted in `/etc/qiita/{control,data}-plane.env`).
Same for the `DATABASE_URL` / `AUTHROCKET_*` env vars we sourced in
step 1 — they're only used by `make migrate` (step 2) and `verify_jwt.py`
(step 3), both already done. Clear them to limit exposure:

```bash
# [operator]
unset HMAC_SECRET_KEY DATABASE_URL \
      AUTHROCKET_ISSUER AUTHROCKET_LOGINROCKET_URL AUTHROCKET_JWKS_URL \
      QIITA_ENDPOINT_URL COMPUTE_ORCHESTRATOR_URL
```

### 8c. Start the data plane

```bash
# [admin]
sudo systemctl enable --now qiita-data-plane@50051
```

The systemd template `qiita-data-plane@.service` reads
`LISTEN_ADDR=127.0.0.1:%i` — the instance number *is* the port.
`qiita-data-plane@50051` listens on `127.0.0.1:50051`, the upstream
nginx already routes to. For first deploy a single instance is fine;
add `qiita-data-plane@50052` (etc.) and the matching nginx upstream
entry when traffic warrants horizontal scaling.

On first start, the DP attaches DuckLake to `qiita_miint_lake` (creates
metadata tables) and runs `ensure_reference_tables`. All idempotent —
restarts are safe.

### 8d. Verify the data plane is up

```bash
# [admin]
make verify-health
```

Expected: `status: SERVING`. `Unauthenticated: invalid HMAC signature`
from later Flight calls means `HMAC_SECRET_KEY` differs between the CP
and DP env files (see step 1).

## 9. Start the orchestrator

> **Expect to coordinate with your HPC admin** through this section.
> Beyond the values they supply (URL, JWT, partition, account), the
> slurmrestd → slurmctld auth path on their side (MUNGE keys, clock
> sync between hosts) needs to be working before the orchestrator can
> actually dispatch jobs. The orchestrator itself will start fine
> regardless; the failure mode if HPC-side auth is broken is per-job,
> not boot-time. Validate against `/slurm/v0.0.40/nodes` (a real RPC
> path) rather than only `/ping` to catch the inter-daemon auth.
>
> The cluster-side prerequisites — the `qiita-job` identity, SLURM
> account / QOS, libjwt version — and the full SLURM gotcha list are in
> [`slurm-backend-setup.md`](slurm-backend-setup.md). This section
> covers only the deploy-host orchestrator config.

### 9a. Create the orchestrator workspace dir

Both the CP runner (`qiita-api`) and the CO (`qiita-orch`) need to
write here, plus SLURM jobs (`qiita-job`) write outputs underneath. The
workspace is the derived `PATH_SCRATCH/ticket`; per §0.3 it is owned
`qiita-orch:qiita-pipeline 2770`; qiita-api and qiita-job get write access
via the `qiita-pipeline` group (§0.1).

```bash
# [admin]
sudo install -d -o qiita-orch -g qiita-pipeline -m 2770 <scratch>/ticket
```

The setgid bit on `2770` is load-bearing: SLURM jobs writing outputs
inherit the `qiita-pipeline` group regardless of `qiita-job`'s primary
group, so the next workflow step (running as `qiita-orch` or `qiita-api`)
can read them. Same pattern as the data plane's staging dir (§8a).

**Did the CP env in §1 already set `PATH_SCRATCH`?** If not, edit
`/etc/qiita/control-plane.env` to set it now and restart the CP — boot
will fail-fast if it's unset. The value **must equal** the `PATH_SCRATCH`
§9b puts in the orchestrator env (both derive the same `/ticket`).

### 9b. Write the orchestrator env file

```bash
# [operator]
cd ~/qiita-miint
cp .env.compute-orchestrator.example /tmp/compute-orchestrator.env
chmod 0600 /tmp/compute-orchestrator.env

# Force the SLURM backend (the example defaults to 'local' for dev/smoke).
sed -i.bak \
    -e "s|^COMPUTE_BACKEND=.*|COMPUTE_BACKEND=slurm|" \
    -e "s|^# PATH_SCRATCH=.*|PATH_SCRATCH=<scratch>|" \
    -e "s|^# SLURMRESTD_URL=|SLURMRESTD_URL=|" \
    -e "s|^# SLURMRESTD_JWT_PATH=|SLURMRESTD_JWT_PATH=|" \
    -e "s|^# SLURMRESTD_USER_NAME=|SLURMRESTD_USER_NAME=|" \
    -e "s|^# SLURM_PARTITION=|SLURM_PARTITION=|" \
    -e "s|^# SLURM_ACCOUNT=|SLURM_ACCOUNT=|" \
    /tmp/compute-orchestrator.env
rm /tmp/compute-orchestrator.env.bak

${EDITOR:-vi} /tmp/compute-orchestrator.env  # fill SLURM endpoint values
```

```bash
# [admin]
sudo install -m 0440 -o root -g qiita-orch /tmp/compute-orchestrator.env /etc/qiita/compute-orchestrator.env
sudo shred -u /tmp/compute-orchestrator.env
```

The SLURM JWT at `SLURMRESTD_JWT_PATH` must be a token minted for the
`qiita-job` identity (`sun=qiita-job`) — minting needs root or
SlurmUser on the cluster, and SLURM JWTs expire. See
[`slurm-backend-setup.md`](slurm-backend-setup.md) for minting and the
rotation options; the orchestrator re-reads the file on a `401`.

### 9c. Start the orchestrator

```bash
# [admin]
sudo systemctl enable --now qiita-compute-orchestrator
```

The orchestrator validates its credentials at startup and refuses to
boot if any are missing: the CP↔CO bearer (`/etc/qiita/cp-to-co.token`,
step 7), the CO→CP compute-worker PAT (`/etc/qiita/co-to-cp.token`,
provisioned per
[`compute-service-account-provisioning.md`](compute-service-account-provisioning.md)),
and on the SLURM backend the five `SLURM*` env vars **and a non-empty
JWT file at `SLURMRESTD_JWT_PATH`** (the SLURM client is constructed in
the lifespan and reads the file). An *expired* JWT lets boot succeed
but the first dispatched step gets a `401`; the client's 401-retry
re-reads the file. So before `enable --now` on a SLURM-backend deploy,
set up the JWT-refresh timer per
[`slurm-backend-setup.md`](slurm-backend-setup.md) and run it once.

## 10. Verify the deploy

### 10a. All three services healthy

```bash
# [admin]
make verify-health
```

Calls `GET /health` on the CP (8080) and CO (8081) and the gRPC health
endpoint on the DP (50051). All three must return `OK` / `SERVING`. A
failure here points at a single service — `journalctl -u qiita-<service>`
gives the boot error.

> **Note: the CP `/health` aggregates downstream.** As of the
> `feat/honest-health-status` PR, the CP's `/health` also probes the
> CO and DP internally and reports a per-service breakdown in a
> `services: {cp, co, dp}` field. The top-level `status` flips to
> `degraded` if any downstream is non-`ok`, which means `make
> verify-health`'s first CP check now exits non-zero on a CO or DP
> outage even though the CP itself is up. If you see CP failing but
> CO/DP passing individually, `curl -s http://localhost:8080/health |
> jq .services` will name the culprit.

### 10b. Operator auth path works

```bash
# [operator]
qiita-admin --base-url https://<fqdn> whoami
```

Expected fields:

- `kind: human`
- `email: <operator-email>`
- `system_role: system_admin`
- `scopes: [...]` — the full `system_admin` ceiling

A 401 means `~/.qiita/token` is missing or revoked; re-run step 6. A 403
on the role line means `set-system-role` didn't take effect.

### 10c. CP routing + DB connectivity

```bash
# [operator]
curl -sS https://<fqdn>/api/v1/reference/1 \
    -H "Authorization: Bearer $(cat ~/.qiita/token)" \
    -w "\nHTTP %{http_code}\n"
```

Expected: `HTTP 404` with body `{"detail":"Reference not found"}`. Proves
nginx routing + PAT verification + Postgres reachability all work.

- `401` — PAT not loaded; check `~/.qiita/token` perms.
- `502` — CP not reachable through nginx; back to step 5.
- `500` — DB connectivity issue; `journalctl -u qiita-control-plane`.

### 10d. Compute-readiness probe

Before driving step 11's end-to-end smoke, verify the path qiita-job
needs from a compute node. `qiita-admin compute-readiness` runs local
checks (SLURM JWT shape + `sun` + `exp`, `SLURM_NATIVE_PYTHON` on the
host, `QIITA_CP_URL/healthz` reachable with the CO→CP token) and, by
default, submits a minimal SLURM probe-job that runs the same checks
on a compute node — including whether the orchestrator's venv is
visible there and whether the compute node can reach the CP.

Run as `qiita-orch` with `/etc/qiita/compute-orchestrator.env` sourced
— the diagnostic needs `SLURMRESTD_URL` / `SLURMRESTD_JWT_PATH` /
`CO_TO_CP_TOKEN_PATH` / etc., which the file installs `0440
root:qiita-orch` (step 9b); the operator's UID can't read it directly:

```bash
# [admin]
sudo -u qiita-orch bash -lc '
    set -a
    # shellcheck disable=SC1091
    source /etc/qiita/compute-orchestrator.env
    set +a
    qiita-admin compute-readiness
'
```

Expected: zero `✗ fail` rows; the summary line reads
`N pass, 0 fail, M skip`. `skip` rows are non-fatal — they mean a
specific check didn't apply (e.g. `SLURM_NATIVE_PYTHON=python`
skips the host-side native-python check because the path resolution
depends on the compute node's PATH; missing `QIITA_CP_URL` skips the
CP reachability check). Any `✗ fail` row names the misconfig: JWT
mismatch / expired, CP unreachable, native-python missing on the
compute node, shared FS not visible, etc. Re-run step 9b / 9c with the
diagnosis applied before continuing.

Pass `--no-slurm-probe` to skip the SLURM submission (host-only
checks). Useful when the cluster is known-unreachable and you want to
triage host-side state first.

## 11. End-to-end smoke

The smoke uses the `fastq-to-parquet` workflow against a tiny single-
sample FASTQ. A single ticket touches every layer:

- **Control plane** — validates the action, mints the per-sample
  `sequence_range`, persists the work ticket, transitions it to
  `COMPLETED` on success.
- **Orchestrator** — dispatches the native `fastq_to_parquet` step,
  calls back to the CP for the sequence_range mint, writes
  `reads.parquet` into the ticket's workspace.
- **Both filesystems** — input FASTQ on `<scratch>` (the scratch root
  from §8a), Parquet output under the workspace path on the same
  shared FS.

### Recipe

End-to-end execution runs through the end-user `qiita` CLI
(`qiita login` / `qiita study create` / `qiita biosample create` /
`qiita sequencing-run create` / `qiita sequenced-pool create` /
`qiita sequenced-sample create` / `qiita ticket submit` /
`qiita ticket status`). The walkthrough lives at
[`docs/runbooks/user-cli-quickstart.md`](user-cli-quickstart.md).

The walkthrough drives the smoke from a fresh user PAT through to a
`COMPLETED` ticket and a `reads.parquet` artifact, with verification
commands at each layer. Compute service-account provisioning per
[`compute-service-account-provisioning.md`](compute-service-account-provisioning.md)
is the one prerequisite that remains operator-side after step 10 —
`qiita-admin actions sync` is now run automatically by
`deploy/activate.sh` against `/opt/qiita/workflows/` (synced from
`workflows/` in the repo) every redeploy.

## Subsequent deploys

After first deploy, every redeploy is **one command** from admin's
shell:

```bash
# [admin]
sudo QIITA_HOSTNAME=<fqdn> /home/qiita/qiita-miint/deploy/local-deploy.sh
```

`local-deploy.sh` does: drop privileges to `qiita` for `git pull` and
`make build-data-plane`, then back to root for rsync-into-`/opt/qiita/`,
`uv sync`, install systemd / nginx, restart services, reload nginx.

Env vars for `local-deploy.sh`:

| Env var | Default | When to set |
|---|---|---|
| `QIITA_HOSTNAME` | (required) | Always. |
| `QIITA_USER` | `qiita` | If the operator account isn't `qiita`. |
| `QIITA_CLONE` | parent of the script's location | If you want to deploy a clone elsewhere. |
| `SKIP_PULL=1` | unset | Deploying a local feature branch, don't `git pull`. |
| `SKIP_BUILD=1` | unset | Re-running after a non-build failure. |

For CI: when CI is wired up, it builds on a build host and rsyncs to
`/opt/qiita/incoming/`, then invokes `deploy/activate.sh` directly.
`local-deploy.sh` is the manual equivalent of that pipeline.
