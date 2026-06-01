# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; `## Deployed history` archives past ones.

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

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
# (#70) SHARED_FILESYSTEM_ROOT — UNLIKE the rest of this bucket it is NOT fail-fast
# (it has a dev default of $TMPDIR/qiita), so a missing value does NOT keep the unit
# down — it silently defaults. It MUST be set to the shared scratch root (same FS as
# the roots shown by the grep above): build_rype_index writes the rype .ryxdi under
# {SHARED_FILESYSTEM_ROOT}/references/{idx}/rype/ and the orchestrator propagates this
# value into each SLURM job env, so an unset value lands the index in node-local /tmp,
# invisible to the CP. Guarded append won't clobber an existing first-deploy value.
sudo bash -c 'grep -q "^SHARED_FILESYSTEM_ROOT=" /etc/qiita/compute-orchestrator.env || echo "SHARED_FILESYSTEM_ROOT=<scratch>/qiita" >> /etc/qiita/compute-orchestrator.env'   # (#70)
```

### 2. One-time host setup

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

### 3. Migrations

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

### 4. Deploy

```bash
# [admin] SKIP_PULL=1 because redeploy.md step 2 already pulled the clone
sudo SKIP_PULL=1 QIITA_HOSTNAME=qiita-miint.ucsd.edu /home/qiita/qiita-miint/deploy/local-deploy.sh
```

### 5. Verify

```bash
# [admin]
curl -fsS https://qiita-miint.ucsd.edu/health                                  # CP+CO+DP aggregate + per-service pills (#58/#54)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
    psql "$DATABASE_URL" -c "SELECT action_id, version, enabled FROM qiita.action ORDER BY action_id;"'   # bcl-convert 1.0.0 (#62) + host-reference-add 1.0.0 (#70) enabled
systemctl cat qiita-control-plane qiita-compute-orchestrator | grep UMask      # UMask=0007 dropins (#57)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; qiita-admin compute-readiness'   # (#57)
```
- `/docs` and `/redoc` render (vendored assets, no CDN) — (#64)
- landing page loads with green status pills + working contact mailto — (#issue-53, #58/#54)

### Notes (no host action)

- (#62) `POST /sequencing-run` and `POST /sequencing-run/{R}/sequenced-pool` now return **200** on a matching-payload retry (was always 201); **409** with `{conflicting_field, existing_value, supplied_value}` on mismatch. Clients that strictly required 201 should accept 200.
- (#62) bcl-convert FASTQ output is large — a busy NovaSeq X lane can reach multiple TB. Size per-ticket scratch generously; the orchestrator does not pre-allocate, so disk-full mid-run surfaces as a SLURM job failure. Confirm exact per-instrument sizing against a real run before relying on a figure. Supported instruments: NovaSeq 6000, NovaSeq X, iSeq.
- (#63) `reference load` moved from `qiita-admin` to the `qiita` end-user CLI (it's a credentialed API call, not a host operation). Retarget any `qiita-admin reference load` scripts to `qiita reference load`.
- (#64) Interactive API docs now served from this origin: `/docs` (Swagger UI), `/redoc` (ReDoc), `/openapi.json`. No deploy action — assets ride the wheel; restart picks them up.
- (#70) Host references (additive, no client action): `qiita.reference` gains `is_host` and a new `indexing` status (`loading → indexing → active`); new read endpoints `GET /reference` (list, filterable) and `GET /reference/{idx}/index`. The `host-reference-add` workflow is a new `workflows/` entry synced into `qiita.action` by `qiita-admin actions sync` inside `activate.sh` (verify in bucket 5) — no migration. The three `20260601*` reference migrations apply via the standard bucket-3 `make migrate` (the activate.sh guard enforces they're applied before restart). The rype `.ryxdi` index directory is `mkdir`'d at runtime under `SHARED_FILESYSTEM_ROOT` — no manual dir step.

---

## Deployed history

Archived `## Pending deploy` blocks, newest on top, each stamped with deploy date + the commit deployed. Populated by `/deploy-archive` at deploy time; empty until the first deploy under this format.
