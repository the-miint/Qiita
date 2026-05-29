# CHANGELOG

Operator-facing deploy instructions — **not** a "what changed" log (the git log serves that). `## Pending deploy` is the single consolidated checklist for the next deploy; `## Deployed history` archives past ones.

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

```bash
# [admin] control-plane.env
sudo tee -a /etc/qiita/control-plane.env <<'EOF'
CONTACT_EMAIL=qiita-help@ucsd.edu                 # (#issue-53) renders into landing-page mailto links
UPLOAD_STAGING_ROOT=<scratch>/upload-staging      # (#49) MUST be byte-identical to the DP value below
EOF

# [admin] data-plane.env
echo "UPLOAD_STAGING_ROOT=<scratch>/upload-staging" | sudo tee -a /etc/qiita/data-plane.env   # (#49)

# [admin] compute-orchestrator.env
sudo tee -a /etc/qiita/compute-orchestrator.env <<'EOF'
SLURM_NATIVE_PYTHON=/home/qiita/qiita-miint/qiita-compute-orchestrator/.venv/bin/python   # (#57)
SLURM_QOS=qiita_norm                                                                       # (#57)
QIITA_CP_URL=https://qiita-miint.ucsd.edu                                                  # (#57)
QIITA_IMAGES_DIR=/scratch/persistent/images                                                # (#62) abs dir, visible from every compute node; validated at CO boot when COMPUTE_BACKEND=slurm
EOF
```

### 2. One-time host setup

```bash
# (#57) qiita-pipeline group membership — verify, fix if needed   [admin]
id qiita-api qiita-orch qiita-data qiita-job | grep qiita-pipeline    # all four should match
sudo usermod -aG qiita-pipeline qiita-api qiita-orch qiita-data qiita-job   # only if any missing

# (#57) compute-node-visible orchestrator venv   [operator]
sudo -u qiita bash -lc 'cd /home/qiita/qiita-miint/qiita-compute-orchestrator && uv sync --reinstall-package qiita-common'

# (#49) upload-staging dir on shared scratch (DP writes as owner, CP reads via qiita-pipeline)   [admin]
sudo install -d -o qiita-data -g qiita-pipeline -m 2770 <scratch>/upload-staging

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
# [operator] Source the SAME env file the guard reads, so make migrate and the
# guard target one DB. activate.sh re-checks public.schema_migrations at deploy
# time and ABORTS before any restart if one is unapplied.
source /etc/qiita/control-plane.env
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
curl -fsS https://qiita-miint.ucsd.edu/healthz
curl -fsS https://qiita-miint.ucsd.edu/health                                  # per-service pills (#58/#54)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
    psql "$DATABASE_URL" -c "SELECT action_id, version, enabled FROM qiita.action ORDER BY action_id;"'   # bcl-convert 1.0.0 enabled (#62)
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

---

## Deployed history

Archived `## Pending deploy` blocks, newest on top, each stamped with deploy date + the commit deployed. Populated by `/deploy-archive` at deploy time; empty until the first deploy under this format.
