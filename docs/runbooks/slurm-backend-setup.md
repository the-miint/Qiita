# SLURM backend setup

> How the compute orchestrator dispatches workflow jobs to a SLURM
> cluster through slurmrestd. Two audiences: the **HPC admin** owns the
> cluster-side prerequisites, the **qiita operator** owns the
> orchestrator config on the deploy host.
> [`first-deploy.md`](first-deploy.md) §9 points here.
>
> Validated against SLURM **25.05.7**, slurmrestd API **`v0.0.40`**. Last checked: 2026-05-22.

## Topology

The compute orchestrator runs on the deploy host (`qiita-miint`) and
submits jobs to **slurmrestd** over HTTP, authenticating with a SLURM
JWT. slurmrestd forwards to **slurmctld**, which schedules the job onto
a compute node.

slurmrestd and slurmctld are **often on different hosts** — in this
deployment slurmrestd is `barnacle-api`, slurmctld is `barnacle-login`.
`SLURMRESTD_URL` must point at the **slurmrestd** host.

```
qiita-miint (orchestrator) --HTTP+JWT--> barnacle-api (slurmrestd)
                                               |
                                               v
                                  barnacle-login (slurmctld) --> compute nodes
```

## Two identities — do not conflate

| Identity | What it is | Where it must exist |
|---|---|---|
| `qiita-orch` | the orchestrator **process** user | the deploy host only |
| `qiita-job` | the SLURM **job-execution** user | the **cluster** — slurmctld + every compute node |

The orchestrator process runs as `qiita-orch`. It reads a JWT minted
for **`qiita-job`** and submits with `X-SLURM-USER-NAME: qiita-job`, so
the job runs as `qiita-job` on a compute node. `qiita-orch` is *not* a
cluster user and never needs to be one.

→ `SLURMRESTD_USER_NAME` is **`qiita-job`**, not `qiita-orch`.

(See [`first-deploy.md`](first-deploy.md) §0.1: `qiita-job` is the
"SLURM workers" service user; `qiita-orch` runs the orchestrator
service.)

## Cluster-side prerequisites [HPC admin]

These are done on the SLURM cluster, by whoever administers it.

### JWT auth must be enabled

slurmctld and slurmrestd must be configured for JWT auth — `slurm.conf`
carries `AuthAltTypes=auth/jwt` with a `jwt_key`, and slurmrestd runs
with `-a rest_auth/jwt` and can read that key. Standard SchedMD setup;
see the [SchedMD JWT docs](https://slurm.schedmd.com/jwt.html).

### libjwt version

SLURM's `auth/jwt` plugin requires **libjwt ≥ 1.10.0 and < 2.0**
([SchedMD related software](https://slurm.schedmd.com/related_software.html)).

EL10's EPEL ships **libjwt 2.x**, which SLURM 25.05 does not support.
Symptom: slurmctld *mints* tokens fine but cannot *verify* them — its
log shows `auth_p_verify: initial jwt_decode failure: Invalid
argument`, and every slurmrestd REST call returns **HTTP 511 / error
1007** (`SLURM_PROTOCOL_AUTHENTICATION_ERROR`).

Fix: install a libjwt **1.x** release (this deploy uses `libjwt-1.18.3`).

> **soname trap:** libjwt's shared-object soname is `libjwt.so.2` for
> *both* the 1.x and 2.x release lines — the soname does **not** tell
> you the version. The package version (`rpm -qf`) is authoritative.

Verify:
```bash
scontrol show config | grep -i PluginDir
ldd <PluginDir>/auth_jwt.so | grep libjwt   # which libjwt it links
rpm -qf /usr/lib64/libjwt.so.2              # the actual release
```

### `qiita-job`: a cluster Unix user

`qiita-job` must be a Unix user — `nologin`, **consistent uid** — on
slurmctld **and every compute node** (normally via LDAP/sssd). Without
it, slurmctld cannot resolve the job owner and the compute node cannot
`setuid` to launch the job.

```bash
getent passwd qiita-job   # must return the same uid clusterwide
```

### SLURM account + association

`qiita-job` needs a SLURM account association (this deploy: account
`qiita`). With `AccountingStorageEnforce=associations`, a submit
without a valid association is rejected.

```bash
sacctmgr -nP show assoc user=qiita-job format=account,partition,qos
```

### QOS must match the partition

This one bites. A partition's `AllowQos` is a strict allowlist. The
orchestrator submits with **no `--qos`**, so the job inherits the
association's **`DefaultQOS`** — which therefore must be a QOS the
target partition's `AllowQos` admits.

Symptom of a mismatch: the submit returns **error 2015 — "Requested
partition configuration not available now."**

```bash
scontrol show partition qiita | grep -iE 'AllowQos|AllowAccounts'
sacctmgr -P show assoc user=qiita-job format=account,qos,defaultqos
```

Fix (admin) — grant a matching QOS and set it as the default:
```bash
sudo sacctmgr modify user qiita-job where account=qiita \
    set qos+=qiita_norm,qiita_prio defaultqos=qiita_norm
```

## The SLURM JWT

The orchestrator authenticates to slurmrestd with a JWT whose `sun`
claim is `qiita-job`. It loads the file **at boot** (the SLURM client
is constructed in the FastAPI lifespan) and re-reads it once on a `401`
from slurmrestd — but SLURM JWTs **expire**, so something must keep the
file fresh.

**Key point — a self-mint is unprivileged.** `scontrol token
username=<other>` needs root/SlurmUser, but `scontrol token` with *no*
`username=` mints a token for whoever runs it. So a process running as
`qiita-job` mints its own `sun=qiita-job` token with no privilege — the
refresh runs as the `qiita-job` service user: no sudo, no human, no
cluster-admin.

### Refresh mechanism — a `qiita-job` systemd timer

On the deploy host, a `systemd` timer running as `qiita-job` re-mints
the token on a schedule. The JWT file is owned `qiita-job:qiita-orch`
mode `0640` — `qiita-job` (the minter) writes it, `qiita-orch` (the
orchestrator) reads it via group.

Prerequisite: the deploy host's `qiita-job` must have the **same uid as
the cluster's** `qiita-job` (so its MUNGE credential resolves), with
slurm CLI + MUNGE reach to slurmctld.

`/usr/local/bin/qiita-refresh-slurm-jwt` (root-owned, mode `0755`):
```bash
#!/usr/bin/bash
# Self-mint a fresh sun=qiita-job SLURM JWT. Runs as qiita-job.
set -euo pipefail
token=$(/usr/bin/scontrol token lifespan=3600)   # path from: command -v scontrol
token=${token#SLURM_JWT=}
[ -n "$token" ] || { echo "scontrol returned no token" >&2; exit 1; }
printf '%s\n' "$token" > /etc/qiita/slurmrestd.jwt
```

`/etc/systemd/system/qiita-slurm-jwt-refresh.service`:
```ini
[Unit]
Description=Refresh the SLURM JWT (sun=qiita-job) for the compute orchestrator
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=qiita-job
Group=qiita-job
ExecStart=/usr/local/bin/qiita-refresh-slurm-jwt
```

`/etc/systemd/system/qiita-slurm-jwt-refresh.timer`:
```ini
[Unit]
Description=Periodic SLURM JWT refresh for the compute orchestrator

[Timer]
OnBootSec=2min
OnCalendar=*:0/15
Persistent=true

[Install]
WantedBy=timers.target
```

**One-time setup (`[admin]`):** `/etc/qiita/` is `root:root 0755`, so
`qiita-job` cannot create files there. Pre-create the JWT file with the
right ownership so the timer's truncate-then-write succeeds:
```bash
sudo install -m 0640 -o qiita-job -g qiita-orch /dev/null /etc/qiita/slurmrestd.jwt
```

Then enable the timer, trigger the first mint, and verify the file is
non-empty **before** starting the orchestrator (which reads the file at
boot — see the "Boot vs. run" gotcha below):
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now qiita-slurm-jwt-refresh.timer
sudo systemctl start qiita-slurm-jwt-refresh.service
sudo ls -la /etc/qiita/slurmrestd.jwt   # non-zero size, qiita-job:qiita-orch 0640
```

A 1-hour token refreshed every 15 min — `OnCalendar=*:0/15` fires on
wall-clock, so a failed `scontrol` run doesn't pause the schedule (the
trap `OnUnitActiveSec` would set) — means the file always holds a token
with ≥45 min of life (4× margin against transient mint failures). The
script's truncate-then-write has a sub-millisecond window where a
reader could catch a partial token; the orchestrator's `401` re-read
absorbs that.

(This refresh is distinct from
[`orchestrator-token-rotation.md`](orchestrator-token-rotation.md),
which covers the CO→CP PAT.)

## Orchestrator config [operator, deploy host]

`COMPUTE_BACKEND=slurm` adds these to
`/etc/qiita/compute-orchestrator.env` (written in
[`first-deploy.md`](first-deploy.md) §9b — §9a creates the workspace dir):

| Var | This deploy | Notes |
|---|---|---|
| `SLURMRESTD_URL` | `http://barnacle-api.sdsc.edu:6820` | the **slurmrestd** host |
| `SLURMRESTD_JWT_PATH` | `/etc/qiita/slurmrestd.jwt` | the `sun=qiita-job` token file |
| `SLURMRESTD_USER_NAME` | `qiita-job` | **not** `qiita-orch` |
| `SLURM_PARTITION` | `qiita` | |
| `SLURM_ACCOUNT` | `qiita` | |
| `SLURMRESTD_API_VERSION` | `v0.0.40` | optional; default |

`PATH_SCRATCH` — the shared scratch base root. The orchestrator and the
CP both derive the per-ticket workspace (`params.json` + outputs) as
`PATH_SCRATCH/ticket`, which **must be on a filesystem mounted on the
compute nodes**. The job reads its inputs from that path on the node; if
the node cannot see it, the step fails. Pick a location whose **parent**
dirs allow `qiita-orch` (owner), `qiita-api` (CP runner, writes via
group), and `qiita-job` (writes outputs via group) all to traverse —
don't nest it under a tightly-restricted tree like the data plane's
`PATH_PERSISTENT/ducklake` (`qiita-data:qiita-data 0750` blocks everyone
else, even if the leaf dir's own perms look correct).

The `PATH_SCRATCH/ticket` dir itself is **`qiita-orch:qiita-pipeline` mode `2770`** (creation
recipe in [`first-deploy.md`](first-deploy.md) §9a, group composition
in §0.1): `qiita-orch` writes per-step subdirs as owner; `qiita-api`
and `qiita-job` write via the `qiita-pipeline` group; the setgid bit
makes new files inherit `qiita-pipeline` so the next step (which may
run as any of the three) can read them. `qiita-orch` is deliberately
*not* a member of `qiita-pipeline`; being the *owner* is what gives it
write access here, which keeps the network-facing service's group
membership bounded.

The orchestrator's `PATH_SCRATCH` and the CP's `PATH_SCRATCH`
**must be byte-identical** — both derive `PATH_SCRATCH/ticket`, and the CP
runner mints `<PATH_SCRATCH>/ticket/<work_ticket_idx>/<step>/attempt-N/`
and POSTs that exact path to the CO as `body.workspace`. A mismatch means
the CO tries to chdir to a path that doesn't exist (or, worse, exists at a
different inode if the same filesystem is mounted twice).

`QIITA_CP_URL` defaults to `http://localhost:8080`, which is correct on
a single-host deploy where the CP and the orchestrator share the box.
If the orchestrator runs on a different host than the CP, set
`QIITA_CP_URL=https://<cp-fqdn>` so outbound CO→CP calls (e.g.
`POST /sequence-range`) reach the control plane.

## Verifying the path

Bottom-up, each layer proven before the next. Run as a real cluster
user, or as `qiita-job` once it is provisioned. The recipe below names
this deploy's slurmrestd host (`barnacle-api.sdsc.edu:6820`) and SLURM
account / partition (`qiita`) — substitute your site's values:

```bash
# 1. mint a token  [cluster host with slurm CLI; SLURM_JWT must be UNSET]
sudo scontrol token username=qiita-job lifespan=600

# 2. read test — the auth chain  [paste the token]
export SLURM_JWT='<token>'
curl -s -H "X-SLURM-USER-NAME: qiita-job" -H "X-SLURM-USER-TOKEN: $SLURM_JWT" \
    http://barnacle-api.sdsc.edu:6820/slurm/v0.0.40/nodes -w '\nHTTP %{http_code}\n'
#   -> HTTP 200 with node JSON

# 3. submit test — the write path
curl -s -X POST -H "X-SLURM-USER-NAME: qiita-job" -H "X-SLURM-USER-TOKEN: $SLURM_JWT" \
    -H 'Content-Type: application/json' \
    http://barnacle-api.sdsc.edu:6820/slurm/v0.0.40/job/submit \
    -d '{"job":{"name":"qiita-smoke","partition":"qiita","account":"qiita",
         "current_working_directory":"/tmp","environment":["PATH=/bin:/usr/bin"]},
         "script":"#!/bin/bash\nhostname; id; sleep 5"}'
#   -> {"job_id": N, ... "error_code": 0}

# 4. confirm it ran  [SLURM_JWT UNSET again]
sudo sacct -j <N> --format=JobID,User,Account,Partition,QOS,State,ExitCode,NodeList
#   -> State=COMPLETED  ExitCode=0:0
```

> Note on the smoke job's `current_working_directory: /tmp` — that's
> the compute node's local `/tmp`, not the deploy host's. Fine for this
> no-I/O smoke; real workflow steps need a path under
> `PATH_SCRATCH/ticket` (see "Orchestrator config" above).

## Gotchas

- **`SLURM_JWT` env var.** When `SLURM_JWT` is set, `scontrol` /
  `sinfo` / `sacctmgr` switch from MUNGE to JWT auth. Keep it **unset**
  for those (they use MUNGE); set it **only** for the `curl` calls. A
  stale exported `SLURM_JWT` makes `scontrol` fail with "Protocol
  authentication error."

- **`MinJobAge`.** slurmctld drops finished jobs from live memory after
  `MinJobAge` (default 300s). `GET /slurm/.../job/{id}` then returns
  **error 2017 "Invalid job id."** Query `sacct` (the accounting DB)
  for history. The orchestrator's poll loop (10s) stays well inside the
  window, so this only bites manual polling.

- **Boot vs. run.** The orchestrator validates the CP↔CO + CO→CP tokens
  and the five `SLURM*` env vars at boot (fail-fast), and on the SLURM
  backend it also constructs the `SlurmrestdClient`, which **reads the
  JWT file** (`main.py` lifespan → `_build_backend` →
  `SlurmrestdClient.__init__` → `_load_jwt`). So a **missing or empty**
  `SLURMRESTD_JWT_PATH` fails the boot. An **expired** token (file
  present, stale content) lets boot succeed; the first dispatched step
  gets a `401` from slurmrestd, and the client's 401-retry re-reads the
  file once before raising.

- **`meta.client.user: root`.** slurmrestd responses show
  `meta.client.user: root` under `SLURM_JWT=daemon` mode — that is
  slurmrestd's own daemon identity, not your token's subject. Expected;
  not a sign of a problem.

- **slurmrestd ≠ slurmctld host.** `SLURMRESTD_URL` points at the
  slurmrestd host (`barnacle-api`), which is not the slurmctld host
  (`barnacle-login`).
