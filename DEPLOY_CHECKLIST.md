# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; `## Deployed history` archives past ones.

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Each PR folds its operator steps into the buckets below via `/deploy-note`; at deploy time buckets 1→5 run in order, with buckets 1–3 preceding the bucket-4 restart. Each step carries its source `(#N)` tag.

> ⚠️ **(#73) This deploy renames every filesystem env var.** Old names are gone; the services derive fixed subdirs from three base roots (`PATH_SCRATCH`, `PATH_PERSISTENT`, `PATH_DERIVED`), so the CP/DP/CO won't boot until the new vars are set (bucket 1). The lake (`PATH_PERSISTENT/ducklake`) is currently **empty** — no durable data has been written — so there is **no data to migrate**; bucket 2 only creates the derived dirs and, if the DuckLake catalog refuses the new data_path, recreates the empty catalog (lossless). If the lake is somehow non-empty at deploy time, **stop** and reassess before recreating anything.

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

```bash
# All of bucket 1 is [admin]; same sudo/redirect rules as past deploys. The new
# code reads ONLY the new names; the old WORK_TICKET_WORKSPACE_ROOT /
# SHARED_FILESYSTEM_ROOT / UPLOAD_STAGING_ROOT / DUCKLAKE_DATA_PATH /
# QIITA_IMAGES_DIR lines are now ignored — leave them for now, delete after a
# clean deploy. (#73)

# (#73) First, read the roots already configured so PATH_* lands consistently.
# PATH_SCRATCH must be byte-identical in all three env files (all derive
# /ticket and/or /staging). Pick <scratch> = the scratch root these used; pick
# <persistent> so that <persistent>/ducklake is where the (currently empty)
# lake will live; pick <derived> so <derived>/images holds the SIFs.
sudo grep -hE '^(WORK_TICKET_WORKSPACE_ROOT|SHARED_FILESYSTEM_ROOT|UPLOAD_STAGING_ROOT|DUCKLAKE_DATA_PATH|QIITA_IMAGES_DIR)=' /etc/qiita/control-plane.env /etc/qiita/data-plane.env /etc/qiita/compute-orchestrator.env 2>/dev/null

# control-plane.env — needs PATH_SCRATCH (derives /ticket + /staging)
sudo bash -c 'grep -q "^PATH_SCRATCH=" /etc/qiita/control-plane.env || echo "PATH_SCRATCH=<scratch>" >> /etc/qiita/control-plane.env'   # (#73)

# data-plane.env — PATH_SCRATCH (byte-identical to CP, derives /staging) + PATH_PERSISTENT (derives /ducklake)
sudo bash -c 'grep -q "^PATH_SCRATCH=" /etc/qiita/data-plane.env || grep "^PATH_SCRATCH=" /etc/qiita/control-plane.env >> /etc/qiita/data-plane.env'   # (#73)
sudo bash -c 'grep -q "^PATH_PERSISTENT=" /etc/qiita/data-plane.env || echo "PATH_PERSISTENT=<persistent>" >> /etc/qiita/data-plane.env'   # (#73)

# compute-orchestrator.env — PATH_SCRATCH (byte-identical, derives /ticket for the readiness probe) + PATH_DERIVED (derives /images, required when COMPUTE_BACKEND=slurm)
sudo bash -c 'grep -q "^PATH_SCRATCH=" /etc/qiita/compute-orchestrator.env || grep "^PATH_SCRATCH=" /etc/qiita/control-plane.env >> /etc/qiita/compute-orchestrator.env'   # (#73)
sudo bash -c 'grep -q "^PATH_DERIVED=" /etc/qiita/compute-orchestrator.env || echo "PATH_DERIVED=<derived>" >> /etc/qiita/compute-orchestrator.env'   # (#73) e.g. /scratch/persistent (SIFs live at <derived>/images)
```

### 2. One-time host setup

```bash
# (#73) Create the scratch leaves the services now derive. ticket + staging are
# the renamed orch-workspace + upload-staging dirs; safe to start empty (both
# are scratch — any in-flight upload/ticket should be drained first). Use the
# SAME owner/group/mode the old dirs carried.   [admin]
scratch=$(sudo grep '^PATH_SCRATCH=' /etc/qiita/control-plane.env | tail -1 | cut -d= -f2-)
sudo install -d -o qiita-orch -g qiita-pipeline -m 2770 "$scratch/ticket"
sudo install -d -o qiita-data -g qiita-pipeline -m 2770 "$scratch/staging"

# (#73) Images tier: point <derived>/images at the existing SIF dir. If the old
# QIITA_IMAGES_DIR was already /scratch/persistent/images and PATH_DERIVED is
# /scratch/persistent, this is a no-op. Otherwise move it, then assert the
# first-deploy §0.3 perms (qiita-orch:qiita-orch 0755):   [admin]
#   images="$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)/images"
#   sudo mv <old-images-dir> "$images"
#   sudo chown qiita-orch:qiita-orch "$images" && sudo chmod 0755 "$images"

# (#73) Create the lake data dir the DP now derives (PATH_PERSISTENT/ducklake).
# The lake is EMPTY — nothing has been written — so there is no data to move.   [admin]
persistent=$(sudo grep '^PATH_PERSISTENT=' /etc/qiita/data-plane.env | tail -1 | cut -d= -f2-)
sudo install -d -o qiita-data -g qiita-data -m 0750 "$persistent/ducklake"

# (#73) ⚠️ Empty-lake guard, BEFORE the bucket-4 restart. The DuckLake catalog
# (Postgres named by the DP's DUCKLAKE_CATALOG_CONNSTR) was pinned to the OLD
# DUCKLAKE_DATA_PATH at the DP's first attach. With zero registered data files,
# re-attaching at the new PATH_PERSISTENT/ducklake should just re-pin cleanly;
# if it instead reports a "path mismatch", recreate the EMPTY catalog DB so it
# re-pins the new data_path. This is lossless ONLY because the lake is empty —
# first CONFIRM there are no data files (e.g. the catalog's ducklake data-file
# table is empty / the old data dir holds no parquet). If any data exists, STOP.
#   sudo systemctl stop 'qiita-data-plane@*'
#   # confirm empty, then drop + recreate the lake catalog DB (DBA), e.g.:
#   #   dropdb qiita_miint_lake && createdb -O qiita_miint_lake_rw qiita_miint_lake
#   # the DP recreates the reference tables on its next boot (ensure_reference_tables).
# The DP restarts in bucket 4; confirm a DoGet in bucket 5.
```

### 3. Migrations

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

```bash
# (#73) [admin] After the bucket-4 restart, confirm the lake reads back and the
# derived workspaces are writable end-to-end.
curl -fsS https://qiita-miint.ucsd.edu/health                                  # all three pills green
sudo -u qiita-orch bash -c 'set -a; source /etc/qiita/compute-orchestrator.env; set +a; /home/qiita/.local/bin/qiita-admin compute-readiness'   # PATH_SCRATCH/ticket visible+writable on a compute node
```
- The data plane attaches DuckLake cleanly at `PATH_PERSISTENT/ducklake` (DP boots; `/health` DP pill green). A DoGet/DoPut round-trip works on the freshly-pinned empty lake — (#73)

### Notes (no host action)

- (#73) Filesystem env vars restructured onto base roots: `PATH_SCRATCH` (→`/ticket`, `/staging`), `PATH_PERSISTENT` (→`/ducklake`), `PATH_DERIVED` (→`/images`). The old per-leaf vars are no longer read by any service. After a clean deploy, delete the stale `WORK_TICKET_WORKSPACE_ROOT` / `SHARED_FILESYSTEM_ROOT` / `UPLOAD_STAGING_ROOT` / `DUCKLAKE_DATA_PATH` / `QIITA_IMAGES_DIR` lines from the three env files.

---

## Deployed history

Archived `## Pending deploy` blocks, newest on top, each stamped with deploy date + the commit deployed. Populated by `/deploy-archive` at deploy time.

### Deployed 2026-06-01 — 178f782

Everything merged but not yet deployed. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

```bash
# All of bucket 1 is [admin]. /etc/qiita/*.env is mode 0440 (root:qiita-api /
# root:qiita-data), so reads and writes go through sudo — and the redirect itself
# must run as root, hence `sudo bash -c '... >> file'` (a bare `sudo ... >> file`
# would redirect as your unprivileged shell and fail). One line per var so the
# block copy/pastes cleanly. Each append is guarded by `grep -q ... ||`, so the
# whole block is idempotent — safe to re-run after a partial/failed deploy.

# (#49) UPLOAD_STAGING_ROOT is a NEW dir under the shared scratch FS. Set it ONCE on
# the CP side; the DP value and the dir (bucket 2) are read back from it. First, see
# the roots already configured here so you pick a consistent location:
sudo grep -hE '^(WORK_TICKET_WORKSPACE_ROOT|DUCKLAKE_DATA_PATH|UPLOAD_STAGING_ROOT)=' /etc/qiita/control-plane.env /etc/qiita/data-plane.env 2>/dev/null

# control-plane.env — substitute <scratch> with a path under the scratch FS shown above
sudo bash -c 'grep -q "^CONTACT_EMAIL=" /etc/qiita/control-plane.env || echo "CONTACT_EMAIL=qiita.help@gmail.com" >> /etc/qiita/control-plane.env'                 # (#issue-53)
sudo bash -c 'grep -q "^UPLOAD_STAGING_ROOT=" /etc/qiita/control-plane.env || echo "UPLOAD_STAGING_ROOT=<scratch>/upload-staging" >> /etc/qiita/control-plane.env'  # (#49)

# data-plane.env — derived from the CP value (byte-identical, no retyping)   (#49)
sudo bash -c 'grep -q "^UPLOAD_STAGING_ROOT=" /etc/qiita/data-plane.env || grep "^UPLOAD_STAGING_ROOT=" /etc/qiita/control-plane.env >> /etc/qiita/data-plane.env'

# compute-orchestrator.env
sudo bash -c 'grep -q "^SLURM_NATIVE_PYTHON=" /etc/qiita/compute-orchestrator.env || echo "SLURM_NATIVE_PYTHON=/home/qiita/qiita-miint/qiita-compute-orchestrator/.venv/bin/python" >> /etc/qiita/compute-orchestrator.env'   # (#57)
sudo bash -c 'grep -q "^SLURM_QOS=" /etc/qiita/compute-orchestrator.env || echo "SLURM_QOS=qiita_norm" >> /etc/qiita/compute-orchestrator.env'                      # (#57)
sudo bash -c 'grep -q "^QIITA_CP_URL=" /etc/qiita/compute-orchestrator.env || echo "QIITA_CP_URL=https://qiita-miint.ucsd.edu" >> /etc/qiita/compute-orchestrator.env'   # (#57)
sudo bash -c 'grep -q "^QIITA_IMAGES_DIR=" /etc/qiita/compute-orchestrator.env || echo "QIITA_IMAGES_DIR=/scratch/persistent/images" >> /etc/qiita/compute-orchestrator.env'   # (#62) abs dir, visible from every compute node; validated at CO boot when COMPUTE_BACKEND=slurm
```

#### 2. One-time host setup

```bash
# (#57) qiita-pipeline group membership — verify, fix if needed   [admin]
id qiita-api qiita-orch qiita-data qiita-job | grep qiita-pipeline    # all four should match
sudo usermod -aG qiita-pipeline qiita-api qiita-orch qiita-data qiita-job   # only if any missing

# (#57) compute-node-visible orchestrator venv   [operator]
sudo -u qiita bash -lc 'cd /home/qiita/qiita-miint/qiita-compute-orchestrator && uv sync --reinstall-package qiita-common'

# (#49) create the upload-staging dir at exactly the configured path — read it back
#       via sudo (root-owned env file) so it can't diverge (DP writes as owner, CP
#       reads via qiita-pipeline)   [admin]
staging=$(sudo grep '^UPLOAD_STAGING_ROOT=' /etc/qiita/control-plane.env | tail -1 | cut -d= -f2-)
sudo install -d -o qiita-data -g qiita-pipeline -m 2770 "$staging"

# (#issue-53) confirm AuthRocket realm invitation-acceptance redirect URI is
#   https://qiita-miint.ucsd.edu/api/v1/auth/handoff   (AuthRocket admin dashboard; no host command)

# (#62) bcl-convert RPM placement (Illumina EULA: do NOT commit to git) + SIF build   [operator]
#   download bcl-convert-4.5.4-2.el8.x86_64.rpm from
#   https://support.illumina.com/sequencing/sequencing_software/bcl-convert/downloads.html
sudo install -d -o qiita-orch -g qiita-pipeline -m 0750 /scratch/persistent/images/sources
sudo install -o qiita-orch -g qiita-pipeline -m 0640 bcl-convert-4.5.4-2.el8.x86_64.rpm \
    /scratch/persistent/images/sources/bcl-convert-4.5.4-2.el8.x86_64.rpm
sudo -u qiita-orch bash -lc 'bash /home/qiita/qiita-miint/scripts/build-bcl-convert-sif.sh'   # idempotent

# (#62) grant the new SA scope to compute-worker   [admin]
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
    qiita-admin service-account update --display-name compute-worker --add-scope sequenced_pool:preflight:read'
```

> **Deploy-time deviation (2026-06-01).** This bucket-2 step as written did not work on the host and was performed differently; tracked for a checklist fix (the bucket-5 `compute-readiness` line has its own deviation note below; see [#67](https://github.com/the-miint/Qiita/issues/67) and follow-ups):
> - **(#62) compute-worker scope grant** — `qiita-admin service-account update --add-scope` is not a real command (no such subcommand), and the scope grant must run *after* the bucket-4 deploy (the new ceiling ships in that code). Done instead as a token rotation per [`orchestrator-token-rotation.md`](docs/runbooks/orchestrator-token-rotation.md): minted `compute-rot-2026-06-01` (principal 5) with `["sequence_range:mint","sequenced_pool:preflight:read"]`, swapped `/etc/qiita/co-to-cp.token`, restarted the orchestrator, revoked the old `compute` SA (principal 3). Live SA is named `compute`, not `compute-worker`.

#### 3. Migrations

```bash
# [operator] DATABASE_URL must be in your shell, pointing at the SAME DB as
# control-plane.env (the guard, running as root, checks that one). The operator
# account can't read the 0440 control-plane.env, so use the value from your
# provisioning / first deploy (see first-deploy.md step 1). activate.sh re-checks
# public.schema_migrations at deploy time and ABORTS before any restart if one is
# unapplied — pointing DATABASE_URL elsewhere migrates one DB while the guard
# checks another (which the guard's wrong-DB hint flags).
make -C ~/qiita-miint migrate
```
`dbmate` applies whatever is unapplied (idempotent). The guard — not this checklist — owns the authoritative set of required migrations, so nothing is hand-listed here to drift out of sync.

#### 4. Deploy

```bash
# [admin] SKIP_PULL=1 because redeploy.md step 2 already pulled the clone
sudo SKIP_PULL=1 QIITA_HOSTNAME=qiita-miint.ucsd.edu /home/qiita/qiita-miint/deploy/local-deploy.sh
```

#### 5. Verify

```bash
# [admin]
curl -fsS https://qiita-miint.ucsd.edu/health                                  # CP+CO+DP aggregate + per-service pills (#58/#54)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
    psql "$DATABASE_URL" -c "SELECT action_id, version, enabled FROM qiita.action ORDER BY action_id;"'   # bcl-convert 1.0.0 enabled (#62)
systemctl cat qiita-control-plane qiita-compute-orchestrator | grep UMask      # UMask=0007 dropins (#57)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; qiita-admin compute-readiness'   # (#57)
```
- `/docs` and `/redoc` render (vendored assets, no CDN) — (#64)
- landing page loads with green status pills + working contact mailto — (#issue-53, #58/#54)

> **Deploy-time deviation (2026-06-01).** The `compute-readiness` verify line as written fails: it runs as `qiita-api` sourcing `control-plane.env`, but the `0400 qiita-orch:qiita-orch` `co-to-cp.token` is only readable by `qiita-orch`, and the check needs the CO env. Ran instead as: `sudo -u qiita-orch bash -c 'set -a; source /etc/qiita/compute-orchestrator.env; set +a; /home/qiita/.local/bin/qiita-admin compute-readiness'`. Two of its checks are false negatives — `cp-healthz` (CP serves `/health`, not `/healthz` — [#67](https://github.com/the-miint/Qiita/issues/67)) and `slurm-probe-log` (probe log written to node-local `/tmp`, unreadable from the head node). CP health confirmed independently via `curl /health` (all pills green).

#### Notes (no host action)

- (#62) `POST /sequencing-run` and `POST /sequencing-run/{R}/sequenced-pool` now return **200** on a matching-payload retry (was always 201); **409** with `{conflicting_field, existing_value, supplied_value}` on mismatch. Clients that strictly required 201 should accept 200.
- (#62) bcl-convert FASTQ output is large — a busy NovaSeq X lane can reach multiple TB. Size per-ticket scratch generously; the orchestrator does not pre-allocate, so disk-full mid-run surfaces as a SLURM job failure. Confirm exact per-instrument sizing against a real run before relying on a figure. Supported instruments: NovaSeq 6000, NovaSeq X, iSeq.
- (#63) `reference load` moved from `qiita-admin` to the `qiita` end-user CLI (it's a credentialed API call, not a host operation). Retarget any `qiita-admin reference load` scripts to `qiita reference load`.
- (#64) Interactive API docs now served from this origin: `/docs` (Swagger UI), `/redoc` (ReDoc), `/openapi.json`. No deploy action — assets ride the wheel; restart picks them up.
