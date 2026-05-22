# SLURM backend setup

> How the compute orchestrator dispatches workflow jobs to a SLURM
> cluster through slurmrestd. Two audiences: the **HPC admin** owns the
> cluster-side prerequisites, the **qiita operator** owns the
> orchestrator config on the deploy host.
> [`first-deploy.md`](first-deploy.md) §9 points here.
>
> Validated against SLURM **25.05.7**, slurmrestd API **`v0.0.40`**.

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
claim is `qiita-job`. It reads the token from `SLURMRESTD_JWT_PATH` per
dispatched step and re-reads once on a `401` — but SLURM JWTs **expire**,
so something must keep the file fresh.

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
OnUnitActiveSec=15min
Persistent=true

[Install]
WantedBy=timers.target
```

Enable with `systemctl enable --now qiita-slurm-jwt-refresh.timer`.

A 1-hour token refreshed every 15 min means the file always holds a
token with ≥45 min of life — it survives a couple of missed runs. The
script's truncate-then-write has a sub-millisecond window where a reader
could catch a partial token; the orchestrator's `401` re-read absorbs
that.

(This refresh is distinct from
[`orchestrator-token-rotation.md`](orchestrator-token-rotation.md),
which covers the CO→CP PAT.)

## Orchestrator config [operator, deploy host]

`COMPUTE_BACKEND=slurm` adds these to
`/etc/qiita/compute-orchestrator.env` (written in
[`first-deploy.md`](first-deploy.md) §9a):

| Var | This deploy | Notes |
|---|---|---|
| `SLURMRESTD_URL` | `http://barnacle-api.sdsc.edu:6820` | the **slurmrestd** host |
| `SLURMRESTD_JWT_PATH` | `/etc/qiita/slurmrestd.jwt` | the `sun=qiita-job` token file |
| `SLURMRESTD_USER_NAME` | `qiita-job` | **not** `qiita-orch` |
| `SLURM_PARTITION` | `qiita` | |
| `SLURM_ACCOUNT` | `qiita` | |
| `SLURMRESTD_API_VERSION` | `v0.0.40` | optional; default |

`SHARED_FILESYSTEM_ROOT` — where the orchestrator stages each step's
workspace (`params.json` + outputs) — **must be on a filesystem
mounted on the compute nodes**. The job reads its inputs from that path
on the node; if the node cannot see it, the step fails.

## Verifying the path

Bottom-up, each layer proven before the next. Run as a real cluster
user, or as `qiita-job` once it is provisioned:

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
  and the five `SLURM*` env vars **at boot** (fail-fast). It does
  **not** read the JWT *file* at boot — `SLURMRESTD_JWT_PATH` only has
  to name a path. A missing or expired JWT fails the first dispatched
  step, not the boot.

- **`meta.client.user: root`.** slurmrestd responses show
  `meta.client.user: root` under `SLURM_JWT=daemon` mode — that is
  slurmrestd's own daemon identity, not your token's subject. Expected;
  not a sign of a problem.

- **slurmrestd ≠ slurmctld host.** `SLURMRESTD_URL` points at the
  slurmrestd host (`barnacle-api`), which is not the slurmctld host
  (`barnacle-login`).
