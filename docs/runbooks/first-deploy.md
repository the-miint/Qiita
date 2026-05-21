# First-deploy bootstrap

**Purpose.** Operator runbook for bringing up a fresh deployment: from
host provisioning through migrations against a clean database, the
first OIDC login, operator promotion to `system_admin`, data-plane
bootstrap, and orchestrator startup. End state: all three services
authenticated and ready to serve traffic. Each step is independent —
re-running it should be safe (idempotent) unless noted.

For the conceptual reference (principal model, scopes, endpoints), see
[`docs/auth.md`](../auth.md). [`orchestrator-token-rotation.md`](orchestrator-token-rotation.md)
documents the future rotation procedure for the orchestrator's own
service-account PAT — not currently used in v1 (the orchestrator
authenticates to the CP via the shared bearer in step 7; a PAT will be
needed once `SlurmBackend` lands and the orchestrator gains CO→CP
callbacks).

> **Recommended workflow.** Open Claude (claude.ai/code or the CLI),
> point it at this runbook, and ask it to walk you through one command
> at a time. Paste each command's output back; Claude will catch the
> failure modes the runbook doesn't anticipate (sudoer PATH quirks,
> cross-distro path conventions, conda shadowing, AuthRocket UI drift,
> etc.). The non-obvious gotchas captured below were surfaced this way.

## Account model

This runbook uses two account labels on every command — both are *roles*,
not OS users, deliberately distinct from the `qiita-*` service-account
naming (`qiita-api`, `qiita-orch`, `qiita-data`, `qiita-job`):

| Label | What it means |
|---|---|
| `[operator]` | A shared, named operational account on the deploy host — typically a user literally named `qiita` (don't confuse with `qiita-api` etc.). **No sudo.** Owns the clone, holds shell env vars across steps, runs `make migrate`, owns `~/.qiita/token`. |
| `[admin]` | Your own personal account with sudo. Runs everything that touches `/etc/qiita/`, `/opt/qiita/`, `/etc/systemd/`, `/etc/nginx/`, and `systemctl`. |

The `qiita` operator account must **not** be a member of any of
`qiita-services`, `qiita-pipeline`, `qiita-data`, `qiita-fs`, or
`qiita-job` — otherwise the operator gains read access to service
secrets and lake data, which defeats the privilege separation the
service users provide. Verify with `id qiita` in step 0.

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
| `qiita-job` | SLURM workers — runs containerized workflow jobs |

System groups (composition matters):

| Group | Members | Purpose |
|---|---|---|
| `qiita-services` | `qiita-api`, `qiita-orch` | Read shared service secrets (`/etc/qiita/cp-to-co.token`). |
| `qiita-pipeline` | `qiita-data`, `qiita-job` | Staging handoff: jobs write Parquet under the staging dir; the data plane reads via group membership. Sites sometimes provision this under a different name (e.g. `qiita-fs`) — what matters is the *membership*. |
| `qiita-data` | `qiita-data` (single member) | Locks the durable Parquet dir to the DP only. Required as a separate group so the data dir at mode `0750` does **not** also grant read to `qiita-job` (which would defeat the staging/parquet split). |

Verification:
```bash
# [either] system users exist with nologin shell
getent passwd qiita qiita-api qiita-orch qiita-data qiita-job

# [either] groups exist with expected memberships
getent group qiita-services qiita-pipeline qiita-data

# [either] service users have the right primary / secondary groups
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

The data plane writes Parquet under a durable shared mount; SLURM jobs
write to a staging mount that's ideally on the same filesystem (for the
atomic rename fast path).

| Path role | Owner | Group | Mode | Notes |
|---|---|---|---|---|
| Mount point parent | `root` | `root` | `0755` | Infra-owned. |
| Final Parquet dir | `qiita-data` | `qiita-data` | `0750` | DP-only. Two valid postures (see below). |
| Staging dir | `qiita-data` | `qiita-pipeline` | `2770` | Setgid forces `qiita-pipeline` group on inherited files. |

Two valid postures for the final Parquet dir:

- **Subdir-of-mount** (runbook default): mount stays infra-owned; a
  `parquet/` subdir under it is what carries `qiita-data:qiita-data 0750`.
  Use this when the mount may host other content alongside Parquet.
- **Mount-as-data**: the mount point itself is `qiita-data:qiita-data
  0750`. Use this when the mount is dedicated to qiita's lake. Simpler.

**Placeholder vocabulary used downstream.** `<mount>` and `<scratch>`
refer to the *roots* (the parent mounts). `<data-dir>` and `<staging-dir>`
are the *final paths*: under the subdir-of-mount posture
`<data-dir>` = `<mount>/parquet` and `<staging-dir>` = `<scratch>/staging`;
under mount-as-data `<data-dir>` = `<mount>` directly (staging still
follows the subdir form). §8a writes the dirs using `<mount>` / `<scratch>`,
§8b sets `DUCKLAKE_DATA_PATH=<data-dir>`, step 11 references
`<scratch>/...` and `<data-dir>` — same paths, just whichever name the
context wants.

Verification:
```bash
# [either]
stat -c '%n  owner=%U  group=%G  mode=%a  device=%d' <data-dir> <staging-dir>
```

Watch the **device numbers**: if data and staging are on different
volumes, the DP's rename from staging to final falls back to
copy+delete. Works correctly, just slower. Flag to infra if you want
the fast path; not blocking otherwise.

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

If `qiita` is a shared cross-server operator account whose login dotfiles
auto-activate a conda environment (common on HPC sites), conda will
shadow system binaries (`openssl`, `jq`) and add noise. To keep the
deploy host clean without disturbing qiita's setup elsewhere, append a
host-scoped opt-out to `~/.bash_profile` on the deploy host:
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
FQDN.

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
> for a flow that involves manual URL copy + SSH tunnel + curl. Bump
> it before starting:
>
> ```bash
> # [admin]
> sudo bash -c "echo 'CLI_LOGIN_CODE_TTL_SECONDS=300' >> /etc/qiita/control-plane.env"
> sudo systemctl restart qiita-control-plane
> ```
>
> 5 min is still tiny in absolute terms, defensible permanent config.

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

**Default (subdir-of-mount).** The rest of the runbook — `DUCKLAKE_DATA_PATH`
in step 8b, smoke-test paths in step 11 — assumes this. Use it unless
you have a specific reason to deviate.

```bash
# [admin]
sudo install -d -o qiita-data -g qiita-data -m 0750     <mount>/parquet
sudo install -d -o qiita-data -g qiita-pipeline -m 2770 <scratch>/staging
```

Then `DUCKLAKE_DATA_PATH=<mount>/parquet` in step 8b.

**Alternative (mount-as-data)** for deploys where `<mount>` is dedicated
to qiita's lake and will host nothing else:

```bash
# [admin]
sudo chown qiita-data:qiita-data <mount>
sudo chmod 0750 <mount>
sudo install -d -o qiita-data -g qiita-pipeline -m 2770 <scratch>/staging
```

Then `DUCKLAKE_DATA_PATH=<mount>` in step 8b.

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
    -e "s|^# DUCKLAKE_DATA_PATH=.*|DUCKLAKE_DATA_PATH=<data-dir>|" \
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

### 9a. Write the orchestrator env file

```bash
# [operator]
cd ~/qiita-miint
cp .env.compute-orchestrator.example /tmp/compute-orchestrator.env
chmod 0600 /tmp/compute-orchestrator.env

sed -i.bak \
    -e "s|^COMPUTE_BACKEND=.*|COMPUTE_BACKEND=slurm|" \
    -e "s|^# SHARED_FILESYSTEM_ROOT=.*|SHARED_FILESYSTEM_ROOT=<scratch>|" \
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

The SLURM JWT at `SLURMRESTD_JWT_PATH` is generated by SLURM
(`scontrol token`) and rotated periodically. Install it once now; the
orchestrator reloads on 401 automatically. Coordinate with your HPC
team for the rotation schedule.

### 9b. Start the orchestrator

```bash
# [admin]
sudo systemctl enable --now qiita-compute-orchestrator
```

The orchestrator reads `/etc/qiita/cp-to-co.token` (step 7) at startup
and refuses to boot if missing. With `COMPUTE_BACKEND=slurm` it also
reads `SLURMRESTD_JWT_PATH` — boot fails fast if that file is missing.

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

Expected: `HTTP 404` with body `{"detail":"reference not found"}`. Proves
nginx routing + PAT verification + Postgres reachability all work.

- `401` — PAT not loaded; check `~/.qiita/token` perms.
- `502` — CP not reachable through nginx; back to step 5.
- `500` — DB connectivity issue; `journalctl -u qiita-control-plane`.

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

End-to-end execution requires an end-user CLI (`qiita login` /
`qiita study create` / `qiita biosample create` /
`qiita sequenced-sample create` / `qiita ticket submit` /
`qiita ticket status`) plus a small pool-less API surface on the CP.
These land together in a follow-up PR; the resulting walkthrough lives
at [`docs/runbooks/user-cli-quickstart.md`](user-cli-quickstart.md).

The walkthrough drives the smoke from a fresh user PAT through to a
`COMPLETED` ticket and a `reads.parquet` artifact, with verification
commands at each layer. Prerequisites that remain operator-side after
step 10 — `qiita-admin actions sync --workflows-dir
/opt/qiita/control-plane/workflows` and compute service-account
provisioning per
[`compute-service-account-provisioning.md`](compute-service-account-provisioning.md) —
are linked from the walkthrough.

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
