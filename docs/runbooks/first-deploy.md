# First-deploy bootstrap

**Purpose.** Operator runbook for bringing up a fresh deployment: from a
clean database through migrations, env-var configuration, the first OIDC
login, operator promotion to `system_admin`, data-plane bootstrap, and
orchestrator startup. End state: all three services authenticated and
ready to serve traffic. Each step is independent — re-running it should
be safe (idempotent) unless noted.

For the conceptual reference (principal model, scopes, endpoints), see
[`docs/auth.md`](../auth.md). [`orchestrator-token-rotation.md`](orchestrator-token-rotation.md)
documents the future rotation procedure for the orchestrator's own
service-account PAT — not currently used in v1 (the orchestrator
authenticates to the CP via the shared bearer in step 7; a PAT will be
needed once `SlurmBackend` lands and the orchestrator gains CO→CP
callbacks).

## 0. Prerequisites

This runbook assumes the host has already been provisioned. Specifically:

**System users exist** (created once by your sysadmin / infrastructure team
as non-login system accounts):

| User | Role |
|---|---|
| `qiita-api` | Control plane (FastAPI service) |
| `qiita-orch` | Compute orchestrator |
| `qiita-data` | Data plane (Arrow Flight / Rust) |
| `qiita-job` | SLURM workers — runs containerized workflow jobs |

**System groups exist**:

| Group | Members | Purpose |
|---|---|---|
| `qiita-services` | `qiita-api`, `qiita-orch` | Read shared secrets (e.g. `/etc/qiita/cp-to-co.token`) that both the control plane and the compute orchestrator need access to. |
| `qiita-pipeline` | `qiita-data`, `qiita-job` | Lets the data plane read SLURM job outputs. Jobs write Parquet under `/scratch/ephemeral/staging/` as `qiita-job:qiita-pipeline` mode `440`; the data plane reads them via group membership and renames into `/data/parquet/`. The 440 mode (enforced as a pre-registration gate by the DP) prevents any other `qiita-job` process from overwriting a registered file. |

Deliberately *not* one combined group: putting `qiita-job` in `qiita-services` would give workflow job code (which may execute user-authored containers) read access to `/etc/qiita/cp-to-co.token` and any future shared service secret. Keep secret-sharing and pipeline-handoff separate.

These are independent identities from the Postgres roles (`qiita_miint_rw`,
`qiita_miint_lake_rw`) and from qiita's own principal/PAT model — same name
in different layers does not imply a connection.

**Postgres databases and roles exist** (provided by your DBA /
infrastructure team): see step 2 (`qiita_miint`) and step 8
(`qiita_miint_lake`).

**Code is installed** at `/opt/qiita/` via `deploy/activate.sh` (rsync from
your build host). systemd units are copied to `/etc/systemd/system/`,
`daemon-reload` has run, and nginx config is at `/etc/nginx/conf.d/qiita.conf`.

**Reverse proxy is up** with TLS termination, configured to route REST to
the CP and gRPC to the DP. The shipped template (`deploy/nginx/qiita.conf`)
contains a `__QIITA_HOSTNAME__` placeholder that `deploy/activate.sh`
substitutes at deploy time using the `QIITA_HOSTNAME` env var
(e.g. `QIITA_HOSTNAME=qiita-miint.ucsd.edu`). The rendered conf expects:

- TLS cert / key at `/etc/ssl/certs/qiita.crt` and `/etc/ssl/private/qiita.key`
- DNS for `$QIITA_HOSTNAME` resolving to the deploy host
- nginx reloaded after the substitution (the activate script does this)

**You (the operator) have sudo** for the commands below: writing
`/etc/qiita/*.env`, creating data directories, and running
`systemctl enable --now`. Service processes themselves run as the system
users above (systemd drops to `User=` from each unit), not as you.

## 1. Apply migrations

```bash
make migrate
```

Seeds the system principal at `idx=1` and creates the auth tables. After
this, `qiita.principal` has exactly one row (the `system` principal); no
human or service-account principals exist yet.

## 2. Write the control plane env file

systemd loads `/etc/qiita/control-plane.env` for the CP. This step generates
the shared HMAC secret and writes that env file. Keep the secret in your
shell — the data-plane bootstrap (step 8) needs the same value.

Prerequisites obtained from your DBA / infrastructure team:

- `qiita_miint` Postgres database
- `qiita_miint_rw` role with login password
- Network reachability from the CP host to the Postgres host

```bash
# Generate the shared HMAC secret. Both the CP and DP must see the same
# value — they sign and verify Flight tickets against it.
HMAC_SECRET_KEY=$(openssl rand -base64 32)

sudo install -d -m 0755 /etc/qiita
sudo tee /etc/qiita/control-plane.env > /dev/null <<EOF
# Control plane DSN — qiita_miint_rw owns and connects to qiita_miint.
DATABASE_URL=postgresql://qiita_miint_rw:<password>@<pg-host>/qiita_miint

HMAC_SECRET_KEY=$HMAC_SECRET_KEY

# AuthRocket SaaS issuer — the canonical URL, NOT the loginrocket subdomain.
AUTHROCKET_ISSUER=https://authrocket.com

# Realm's loginrocket subdomain — both the JWKS endpoint and the LoginRocket
# Web hosted login URL live here. Substitute your realm's subdomain; for the
# qiita-dev realm specifically this is merry-lion-7652.e2.loginrocket.com.
AUTHROCKET_LOGINROCKET_URL=https://<realm>.loginrocket.com
AUTHROCKET_JWKS_URL=https://<realm>.loginrocket.com/connect/jwks

# Externally-resolvable URL of the control plane itself; used to construct
# the redirect_uri AuthRocket bounces back to.
QIITA_ENDPOINT_URL=https://qiita.example.org

# Compute orchestrator dispatch. When set, the CP fires an in-process
# asyncio task to call the orchestrator's /step/run on every work-ticket
# submission (no polling worker). Leave unset to run the CP without
# dispatch — every work-ticket creation route returns 503.
COMPUTE_ORCHESTRATOR_URL=http://127.0.0.1:8081

# Optional (defaults shown):
# CP_TO_CO_TOKEN_PATH=/etc/qiita/cp-to-co.token   # see step 7
# AUTHROCKET_AUDIENCE=                  # leave unset — LoginRocket Web JWTs lack `aud`
# AUTHROCKET_JWT_LEEWAY_SECONDS=30
# AUTH_HANDOFF_FRESHNESS_SECONDS=60
# CLI_LOGIN_CODE_TTL_SECONDS=30
# QIITA_TOKEN_DEFAULT_TTL_DAYS=90
EOF

sudo chown root:qiita-api /etc/qiita/control-plane.env
sudo chmod 0440 /etc/qiita/control-plane.env

# Keep $HMAC_SECRET_KEY in your shell for step 8 (data-plane bootstrap).
# If you lose it, regenerate and update both env files — services restart
# is enough to pick up the new value.
```

**If HMAC values disagree.** Every Flight DoGet/DoAction will return
`Unauthenticated: invalid HMAC signature` from the data plane. The control
plane logs the signed ticket with no error — the failure shows up only at
DP. Symptom: CP returns 200 OK on `POST /reference/{idx}/ingest`, but
downstream Flight calls 401. Fix: confirm `HMAC_SECRET_KEY` is byte-identical
in both env files (including base64 padding), then
`systemctl restart qiita-control-plane qiita-data-plane@*`.

See [`authrocket-realm-setup.md`](authrocket-realm-setup.md) for the
realm-side configuration (test users, email-verification policy, the
`iss=https://authrocket.com` footgun).

## 3. One-shot JWT shape verify

Log into the AuthRocket realm via the hosted UI and capture the raw JWT.
The script reads the same env vars the CP does — source them from the env
file you wrote in step 2 before running it:

```bash
set -a && source /etc/qiita/control-plane.env && set +a
uv run python scripts/verify_jwt.py "$JWT"
```

On success it prints the parsed claims. If anything fails — bad signature,
wrong issuer, missing required claim — file an issue before proceeding.

## 4. Start the control plane

```bash
sudo systemctl enable --now qiita-control-plane
```

systemd loads `/etc/qiita/control-plane.env`. `AuthRocketVerifier.from_settings`
runs at FastAPI lifespan — if any required env var is missing the boot fails
fast and the service enters `failed` state. Check `journalctl -u
qiita-control-plane` for the specific missing var.

## 5. Verify the reverse proxy is reachable

Before the first login, confirm that the externally-resolvable URL serves
traffic to the control plane through nginx with a valid TLS cert. The
shipped nginx config (`deploy/nginx/qiita.conf`, deployed via
`deploy/activate.sh`) carries a `__QIITA_HOSTNAME__` placeholder that is
substituted at deploy time; if you are reaching this step the substitution
should already have run.

```bash
curl -sSf -o /dev/null -w "%{http_code}\n" https://qiita.example.org/health
# expected: 200
```

The CP exposes `GET /health` at the root (not under `/api/v1/`); nginx's
catch-all `location /` proxies it to `qiita_control_plane` upstream.

If the request fails:

- **DNS error / connection refused** — the hostname does not resolve to
  this host, or nginx is not running. `systemctl status nginx`. Your infra
  team owns the DNS and proxy layer.
- **`curl: (60) SSL certificate problem`** — cert/key mismatch or expired
  cert at `/etc/ssl/certs/qiita.crt` and `/etc/ssl/private/qiita.key`.
- **`404 Not Found` on `/health` but other paths return 502** — likely the
  `__QIITA_HOSTNAME__` substitution did not run; nginx's `server_name` does
  not match the URL you curled. Check `grep server_name
  /etc/nginx/conf.d/qiita.conf` — it should contain your hostname, not the
  placeholder.
- **`502 Bad Gateway`** — nginx is up but cannot reach the CP. Verify step 4
  (`systemctl status qiita-control-plane`) and that the CP is listening on
  `127.0.0.1:8080` (`ss -ltn | grep 8080`).

This step is a *gate*, not a setup. nginx, TLS provisioning, and DNS are
owned by your infra team (see step 0 prerequisites). If the gate fails,
stop and coordinate with them — running steps 6+ will not make the gate
pass.

## 6. First human login (operator promotion + PAT mint)

`qiita-admin login` drives the LoginRocket Web flow end-to-end: it spawns
a localhost loopback HTTP server, opens the browser to qiita's `/auth/login`,
captures the AuthRocket round-trip, exchanges the resulting one-time code
for a PAT, and saves it to `~/.qiita/token` (mode 0600).

```bash
qiita-admin --base-url https://qiita.example.org login
```

On the *first* login, the resolver creates a `principal` row
(`system_role='user'`), a `user` row keyed on that principal, and a
`user_identities(iss, sub)` link in one transaction. An
`oidc_create_principal` audit event is recorded. The freshly-minted PAT is
written to `~/.qiita/token`.

Then promote yourself to `system_admin` (direct DB; no HTTP auth needed):

```bash
qiita-admin set-system-role --email operator@example.org --role system_admin
```

After the role change takes effect, the existing PAT carries `user`-level
scopes. Re-run `qiita-admin login` to mint a fresh PAT scoped to the
`system_admin` ceiling.

### Headless / no-browser fallback

For environments without a browser (SSH'd remote, CI, container without DISPLAY),
`qiita-admin login` will print the URL it tried to open. Open it on a machine
with a browser, complete the AuthRocket login, and the redirect will land at
`http://127.0.0.1:<port>/?ot_code=<value>`. If that loopback isn't reachable
on the same host, capture the JWT manually via the AuthRocket admin UI and use
the legacy direct PAT mint:

```bash
curl -X POST https://qiita.example.org/api/v1/auth/pat \
    -H "Authorization: Bearer $JWT" \
    -H "Content-Type: application/json" \
    -d '{"label":"my-laptop","ttl_days":90}'
```

`POST /api/v1/auth/pat` continues to accept fresh OIDC JWTs in the
`Authorization` header for this purpose. Save the plaintext `qk_...` to
`~/.qiita/token` (mode 0600).

## 7. Install the CP↔CO shared bearer

The control plane and the compute orchestrator authenticate to each other
on a private path — a constant-time string compare against a shared bearer
token. Both services read the same file. This is *not* a PAT and is not
managed via the principal model; it's a simple shared secret on the
internal network between two services.

```bash
openssl rand -base64 32 | sudo tee /etc/qiita/cp-to-co.token > /dev/null
sudo chown root:qiita-services /etc/qiita/cp-to-co.token
sudo chmod 0440 /etc/qiita/cp-to-co.token
```

Owner `root`, group `qiita-services` (created in step 0; contains both
`qiita-api` and `qiita-orch`), mode `0440`: both service users get read
access via group membership; only root can write.

The CO loads it via `CP_TO_CO_TOKEN_PATH` (default
`/etc/qiita/cp-to-co.token`; see
`qiita-compute-orchestrator/src/qiita_compute_orchestrator/config.py`).
The orchestrator will refuse to start if the file is missing or unreadable.
Dev / CI can opt into env-var fallback by setting `QIITA_ALLOW_TOKEN_ENV=true`
and providing `CP_TO_CO_TOKEN`; production must use the file.

The shared bearer is what the CP attaches to its `POST /api/v1/step/run`
calls into the orchestrator and what the orchestrator's auth dependency
constant-time-compares against. Both services refuse to boot without it
on a path they can read, so install it before either service starts.

## 8. Bootstrap the data plane

Prerequisites obtained from your DBA / infrastructure team:

- `qiita_miint_lake` Postgres database
- `qiita_miint_lake_rw` role with login password
- Network reachability from the data plane host to the Postgres host

The data plane bootstraps its own DuckLake catalog on first start — there's
no `make migrate` equivalent. You only need to give it the catalog DB
credentials, a directory on the shared filesystem to hold Parquet files,
and an env file.

### 8a. Create the Parquet data and staging directories

```bash
# Final Parquet location — owned by the DP, not group-writable. The DP
# atomic-renames into here from staging; nothing else writes here.
sudo install -d -o qiita-data -g qiita-data -m 0750 /data/parquet

# SLURM job staging — group-writable + setgid so files written by qiita-job
# inherit the qiita-pipeline group, and the DP (also in qiita-pipeline) can
# read the 440-mode outputs before renaming them into /data/parquet.
sudo install -d -o qiita-data -g qiita-pipeline -m 2770 /scratch/ephemeral/staging
```

If `/scratch` and `/data` are on different filesystems, the atomic rename
falls back to copy+delete (slower; see
`qiita-data-plane/src/flight_service.rs::move_file`). Provision them on
the same filesystem when possible.

The setgid bit (`2770`) on the staging directory is load-bearing: without
it, files created by `qiita-job` would inherit `qiita-job`'s primary group
and the data plane could not read them. With it, every file inherits
`qiita-pipeline` regardless of the writing process's primary group.

### 8b. Write the data plane env file

Use the *same* `HMAC_SECRET_KEY` value you generated in step 2 — the CP
and DP must agree exactly. Substitute the lake DB credentials your DBA
team provided:

```bash
sudo tee /etc/qiita/data-plane.env > /dev/null <<EOF
HMAC_SECRET_KEY=$HMAC_SECRET_KEY
DUCKLAKE_CATALOG_CONNSTR=dbname=qiita_miint_lake host=<pg-host> user=qiita_miint_lake_rw password=<password>
DUCKLAKE_DATA_PATH=/data/parquet
EOF
sudo chown root:qiita-data /etc/qiita/data-plane.env
sudo chmod 0440 /etc/qiita/data-plane.env
```

`DUCKLAKE_CATALOG_CONNSTR` is libpq format — space-separated `key=value`
pairs, NOT a `postgresql://` URL. (`DATABASE_URL` in the CP env is the URL
form because asyncpg accepts that; the data plane reads the raw libpq form
because DuckDB's postgres extension expects it.)

### 8c. Start the data plane

```bash
sudo systemctl enable --now qiita-data-plane@50051
```

The systemd template at `deploy/systemd/qiita-data-plane@.service` reads
`LISTEN_ADDR=127.0.0.1:%i` from the instance specifier — the instance number
*is* the port. `qiita-data-plane@50051` listens on `127.0.0.1:50051`, which
is the upstream nginx already routes to (`deploy/nginx/qiita.conf`). For
first deploy a single instance is fine; add `qiita-data-plane@50052` (etc.)
and the matching nginx upstream entry when traffic warrants horizontal
scaling.

On first start, the DP attaches DuckLake to `qiita_miint_lake` (which creates
the DuckLake metadata tables) and runs `ensure_reference_tables` to create
the reference data tables. All idempotent — restarts are safe.

### 8d. Verify the data plane is up

```bash
make verify-health
```

Expected: `status: SERVING`. If you see `Unauthenticated: invalid HMAC
signature` later from any Flight call, the most common cause is that
`HMAC_SECRET_KEY` differs between the CP and DP env files (see step 2).

## 9. Start the orchestrator

### 9a. Write the orchestrator env file

Pick the backend the orchestrator dispatches with. `local` runs DuckDB+miint
in-process (used for development hosts and the smoke test); `slurm`
submits jobs to a SLURM cluster via slurmrestd. Production deploys use
`slurm`; switch by changing `COMPUTE_BACKEND` and restarting.

```bash
sudo tee /etc/qiita/compute-orchestrator.env > /dev/null <<EOF
# Backend selection: 'local' for development, 'slurm' for production.
COMPUTE_BACKEND=slurm

# Where the orchestrator stages workspace dirs (params.json + outputs).
# Must be on the shared filesystem so SLURM compute nodes see it too.
SHARED_FILESYSTEM_ROOT=/data

# CP↔CO shared bearer (default path; matches step 7's install).
# CP_TO_CO_TOKEN_PATH=/etc/qiita/cp-to-co.token

# --- SLURM backend config (required when COMPUTE_BACKEND=slurm) ---
SLURMRESTD_URL=http://<slurmctld-host>:6820
SLURMRESTD_JWT_PATH=/etc/qiita/slurmrestd.jwt
SLURMRESTD_USER_NAME=qiita-orch
SLURM_PARTITION=qiita
SLURM_ACCOUNT=qiita-prod

# Optional (defaults shown):
# SLURMRESTD_API_VERSION=v0.0.40
# SLURM_POLL_INTERVAL_SECONDS=10
# SLURM_JOB_TIMEOUT_SECONDS=86400
EOF
sudo chown root:qiita-orch /etc/qiita/compute-orchestrator.env
sudo chmod 0440 /etc/qiita/compute-orchestrator.env
```

The SLURM JWT file at `SLURMRESTD_JWT_PATH` is generated by SLURM
(`scontrol token`) and rotated periodically. Install it once now;
the orchestrator reloads on 401 automatically. Coordinate with your
HPC team for the rotation schedule.

### 9b. Start the orchestrator service

```bash
sudo systemctl enable --now qiita-compute-orchestrator
```

systemd loads the env file you just wrote. The orchestrator reads
`/etc/qiita/cp-to-co.token` (installed in step 7) at startup and will
refuse to boot if the file is missing or unreadable by `qiita-orch`.
With `COMPUTE_BACKEND=slurm`, it also reads `SLURMRESTD_JWT_PATH` —
boot fails fast if that file is missing.

> **v1: no orchestrator PAT.** The orchestrator does not load a PAT in
> v1 (`Settings` has no token field; only the CP↔CO shared bearer is
> read). When the orchestrator gains CO→CP callbacks for async-step
> lifecycle, that PR adds the PAT-mint step here and
> [`orchestrator-token-rotation.md`](orchestrator-token-rotation.md)
> becomes the rotation procedure for it.

## 10. Verify the deploy

Per-service liveness plus auth probes — confirms every layer the previous
steps wired up is actually responding. This is the gate that says "the
deploy worked." Step 11 is the end-to-end pipeline smoke that submits a
real workflow and follows it through to DuckLake; run it after step 10
passes.

### 10a. All three services healthy

```bash
make verify-health
```

Calls `GET /health` on the CP (8080) and CO (8081) and the gRPC health
endpoint on the DP (50051). All three must return `OK` / `SERVING`. A
failure here points at a single service — `journalctl -u qiita-<service>`
gives the boot error.

### 10b. Operator auth path works

Confirms the OIDC + PAT flow you exercised in step 6 still works and the
operator's PAT carries the expected scopes after the role promotion:

```bash
qiita-admin --base-url https://qiita.example.org whoami
```

Expected fields:

- `kind: human`
- `email: <operator email>`
- `system_role: system_admin`
- `scopes: [...]` — the full `system_admin` ceiling (see
  [`docs/auth.md`](../auth.md))

A 401 here means `~/.qiita/token` is missing or revoked; re-run step 6.
A 403 on the role line means `set-system-role` did not take effect;
re-check that you ran step 6's promotion command after the first login.

### 10c. CP routing + DB connectivity

Reads a non-existent reference. Proves nginx routing, PAT verification,
and Postgres reachability without depending on any data being present:

```bash
curl -sS https://qiita.example.org/api/v1/reference/1 \
    -H "Authorization: Bearer $(cat ~/.qiita/token)" \
    -w "\nHTTP %{http_code}\n"
```

Expected: `HTTP 404` with body `{"detail":"reference not found"}` (or the
equivalent — exact wording is set by the route handler). Other outcomes:

- `401` — PAT not loaded; check `~/.qiita/token` permissions.
- `502` — CP not reachable through nginx; back to step 5.
- `500` — likely a DB connectivity issue;
  `journalctl -u qiita-control-plane | tail -50`.

### What is *not* verified by step 10

- The CP↔DP HMAC handshake (no Flight call is made by a 404 lookup).
- Orchestrator dispatch end-to-end.
- Parquet registration into DuckLake — depends on the orchestrator path.

These are exercised by step 11.

## 11. End-to-end smoke

The smoke uses the `reference-add` workflow with a 3–5 sequence FASTA.
A single ticket touches every layer:

- **Control plane** — validates the action, mints `feature_idx`, writes
  `reference_membership`.
- **Orchestrator** — dispatches the four-step pipeline to the configured
  backend (`SlurmBackend` runs containerized jobs as `qiita-job` on the
  cluster; `LocalBackend` runs in-process for development).
- **Data plane** — registers Parquet via `ducklake_add_data_files`
  (writes to `qiita_miint_lake`, lands files in `/data/parquet/<table>/`).
- **Both filesystems** — intermediates on `/scratch/ephemeral/staging/`,
  final output on `/data/parquet/`.

### Recipe

1. **Versioned smoke tag.** Each deploy uses a unique reference name so
   the smoke is idempotently re-runnable and leaves an audit trail:

   ```bash
   SMOKE_TAG="smoke-$(git -C /opt/qiita/control-plane rev-parse --short HEAD)"
   ```

2. **Stage a tiny FASTA** at
   `/scratch/ephemeral/references/incoming/$SMOKE_TAG/1.0.0/seqs.fasta`
   (3–5 short sequences are enough; owner `qiita-job`, group readable
   so the hash job can read it).

3. **Submit `reference-add`** against `(name=$SMOKE_TAG, version=1.0.0)`
   using the operator's `system_admin` PAT. The promoted operator role
   covers `feature:mint`, `reference:write`, and
   `reference:register_files` — the full scope set the workflow declares.

4. **Verify the four layers:**

   - Work ticket reaches `COMPLETED` — query it via the `qiita-admin`
     work-ticket subcommand (or directly against `qiita.work_ticket`
     if the CLI surface isn't installed on this host).
   - `qiita_miint` has a new `reference` row plus features and
     membership rows for the smoke tag.
   - `qiita_miint_lake` has new `ducklake_data_file` rows from the last
     few minutes.
   - `/data/parquet/<table>/` contains recent Parquet files, mode 440.

   A failure on any single check pinpoints which layer is broken.

### Post-smoke

The smoke leaves one tagged reference plus a handful of features and
catalog files per deploy — by design. They are cheap (kilobytes) and
the audit trail proves which deploys passed smoke. No cleanup needed:
versioned smoke names keep accumulation harmless until full reference
deletion (membership rows, exclusive features, catalog files, Parquet)
is wired.
