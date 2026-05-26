# CHANGELOG

Operator-facing notes for deploying merged changes. Each entry lives under the PR that introduced it. Entries describe what the operator must do **on the deploy host** before / during `sudo local-deploy.sh` — not a general "what changed" summary; the git log already serves that audience.

The convention: when a PR adds a required env var, a new directory, a migration that needs out-of-band setup, or any other operator-visible action, append an entry to this file in the same PR. Reviewers check that the entry exists. Future operators read this file (`tail CHANGELOG.md`) before deploying to find out what's new since the last deploy.

Newest entry on top. Pre-deploy entries (PRs not yet merged to `main`) are allowed; mark them with the branch name and convert the heading to the PR number at merge.

---

## `feat/post-first-smoke-fixes` — fix(deploy): close gaps surfaced by first user-CLI fastq-to-parquet smoke

Bundled fixes from the first real end-to-end fastq-to-parquet run on
the live deploy. Six items the operator needs to do on an existing
deploy host before `sudo local-deploy.sh`; everything else is handled
by the deploy script itself or runs out of the box on a fresh deploy.

1. Confirm `qiita-pipeline` group membership. Both `qiita-api` and
   `qiita-orch` must be members (commit 2 makes this a documented
   prereq; not yet automated by `local-deploy.sh`). `qiita-data` and
   `qiita-job` should already be members from the first deploy.
   ```bash
   # [admin]
   id qiita-api qiita-orch qiita-data qiita-job | grep qiita-pipeline   # all four should match
   # if any are missing:
   sudo usermod -aG qiita-pipeline qiita-api qiita-orch qiita-data qiita-job
   ```

2. Provision the compute-node-visible orchestrator venv (commit 3
   prereq). The smoke ran with this in place; the new
   `SLURM_NATIVE_PYTHON` env var below points the native-step launcher
   at this interpreter.
   ```bash
   # [operator]
   sudo -u qiita bash -lc 'cd /home/qiita/qiita-miint/qiita-compute-orchestrator && uv sync --reinstall-package qiita-common'
   ```
   `--reinstall-package qiita-common` is required because of cross-
   package staleness (see CLAUDE.md "Cross-package staleness"); plain
   `uv sync` would leave a stale `qiita-common` in site-packages.

3. Add the new env vars to `/etc/qiita/compute-orchestrator.env`:
   ```bash
   # [admin]
   sudo tee -a /etc/qiita/compute-orchestrator.env <<'EOF'
   SLURM_NATIVE_PYTHON=/home/qiita/qiita-miint/qiita-compute-orchestrator/.venv/bin/python
   SLURM_QOS=qiita_norm
   QIITA_CP_URL=https://qiita-miint.ucsd.edu
   EOF
   ```
   `QIITA_CP_URL` was already supported on the CO side; this PR makes
   it effectively required because the value is now propagated into
   every SLURM job's env so the native-step launcher can call back
   into the control plane from the compute node.

4. Run the deploy. `deploy/local-deploy.sh` now also:
   - rsyncs `workflows/` into `/opt/qiita/incoming/` and on to
     `/opt/qiita/workflows/`,
   - runs `qiita-admin actions sync` as `qiita-api` against that path
     (idempotent — no-op if nothing changed),
   - installs the `UMask=0007` systemd dropins for both
     `qiita-control-plane` and `qiita-compute-orchestrator` under
     `/etc/systemd/system/<unit>.service.d/umask.conf`, then
     `daemon-reload` + restart.
   ```bash
   # [admin]
   sudo QIITA_HOSTNAME=qiita-miint.ucsd.edu /home/qiita/qiita-miint/deploy/local-deploy.sh
   ```

5. Verify:
   ```bash
   # [admin]
   # actions sync ran and at least one row is in qiita.action
   sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; psql "$DATABASE_URL" -c "SELECT count(*) FROM qiita.action;"'
   # UMask dropins installed and effective
   systemctl cat qiita-compute-orchestrator | grep UMask
   systemctl cat qiita-control-plane         | grep UMask
   ```

Also new in this PR (no operator action, just so you know what
landed):

- `qiita-admin ticket force-fail --idx N --stage <stage> [--step-name X]
  --reason TEXT` replaces the previous "operator writes UPDATE
  qiita.work_ticket by hand" recovery pattern. Mirrors the
  `work_ticket_failure_step_name_consistent` CHECK constraint
  client-side; refuses to overwrite an already-terminal ticket. See
  `docs/runbooks/fastq-to-parquet-retry-recovery.md` for the supported
  recovery path.
- `SLURM_QOS` env var (empty default = "use the submitting user's
  default QOS") so the orchestrator doesn't silently depend on
  per-user defaults.
- SlurmrestdClient now refuses to start when the JWT's `sun` claim
  doesn't match `SLURMRESTD_USER_NAME` — catches a stale JWT before
  it sends jobs as the wrong identity (which the first smoke
  experienced).
- The SLURM job env now carries `HOME=<workspace>` so DuckDB+miint's
  extension cache lands inside the cleaned-up per-ticket workspace.
- `qiita-admin compute-readiness` exercises the path `qiita-job` needs
  end-to-end and reports per-check status. Local checks (SLURM JWT
  shape + `sun` + `exp`, `SLURM_NATIVE_PYTHON` on host, `QIITA_CP_URL/
  healthz` reachable with the CO→CP token) plus an optional SLURM
  probe-job that runs the same checks from a compute node (orchestrator
  venv visible there, shared FS writable, CP reachable from the
  cluster). Step 10d of `docs/runbooks/first-deploy.md` shows the
  expected pass/fail output. `--no-slurm-probe` runs host-only.
- The SLURM job env now carries only the *outbound* CO→CP token, not
  the inbound CP↔CO shared bearer. `Settings.from_env()` gained a
  `require_cp_to_co_token` flag; `get_settings()`'s no-install
  fallback (the SLURM-launcher path) passes `False`. Narrows the
  `scontrol show job` exposure surface to the one token the launcher
  actually uses.

---

## `feat/issue-53-landing-and-invitation` — feat: public landing page + invitation acceptance fix

Fixes issue #53. Two user-visible problems addressed together:

1. Visiting the bare host (`https://qiita-miint.ucsd.edu/`) returned `{"detail":"Not Found"}`. This PR adds a public landing page at `GET /` (project name, status badge, alpha / invitation-only / CLI-only callout, links into the repo, contact mailto).
2. AuthRocket's post-invitation redirect to `/api/v1/auth/handoff?token=...` returned `401 login session missing` because the handoff route required a cookie that only the regular `/auth/login` flow sets. This PR makes the cookie optional in `/auth/handoff` — when absent, the route treats the request as an invitation acceptance, verifies the JWT alone, mints a PAT, and renders the same browser HTML the cookie-bearing flow does. CLI flow is unchanged.

**New required env var on the CP: `CONTACT_EMAIL`.** Boot fails fast (`RuntimeError: CONTACT_EMAIL must be a local@domain.tld address`) when unset or malformed. The value renders into both `mailto:` links on the landing page (request access + need help).

Before `sudo local-deploy.sh`:

1. Append `CONTACT_EMAIL` to the env file. Pick the address invitation requests and help requests should reach:
   ```bash
   # [admin]
   echo "CONTACT_EMAIL=qiita-help@ucsd.edu" | sudo tee -a /etc/qiita/control-plane.env
   ```

2. Confirm the AuthRocket realm's invitation-acceptance redirect URI is set to `https://qiita-miint.ucsd.edu/api/v1/auth/handoff` in the AuthRocket admin dashboard. (Likely already there if you previously tried to fix this.) After this PR ships, that URL works without any cookie — invitation acceptance lands directly there with the JWT in the query string and mints a PAT.

3. Now run `sudo local-deploy.sh`. No new migrations.

Also new in this PR (no operator action, just so you know what landed):

- `qiita.auth_event.detail.via` now carries `"invitation"` (alongside the existing `"cli_login"` / `"browser_login"`) so anyone reading auth-event audit rows can tell which entry point produced a given PAT.

---

## PR #49 — feat(upload): Arrow Flight DoPut upload domain

**New required env var on both CP and DP: `UPLOAD_STAGING_ROOT`.** Boot fails fast (`RuntimeError: UPLOAD_STAGING_ROOT is required but not set`) without it.

Before `sudo local-deploy.sh`:

1. Create the upload-staging dir on the shared scratch filesystem. Use the same owner/group/mode as the existing `<scratch>/staging` dir (DP writes as owner, CP reads via the `qiita-pipeline` group):
   ```bash
   # [admin] — substitute <scratch> with the actual scratch root from the first deploy
   sudo install -d -o qiita-data -g qiita-pipeline -m 2770 <scratch>/upload-staging
   ```

2. Append `UPLOAD_STAGING_ROOT` to both env files. The value **must be byte-identical** on both sides — CP resolves `*_upload_idx` action_context keys to `{root}/uploads/{idx}/upload.parquet` and the DP writes to the same path on DoPut:
   ```bash
   # [admin]
   echo "UPLOAD_STAGING_ROOT=<scratch>/upload-staging" | sudo tee -a /etc/qiita/control-plane.env
   echo "UPLOAD_STAGING_ROOT=<scratch>/upload-staging" | sudo tee -a /etc/qiita/data-plane.env
   ```

3. Now run `sudo local-deploy.sh`. The new migration `20260521000000_upload.sql` applies as part of the deploy's `make migrate`; it's table-only (no FK to anything outside `qiita.principal`) and runs in seconds.

Also new in this PR (no operator action, just so you know what landed):

- `Settings.workspace_root` renamed to `Settings.work_ticket_workspace_root` to converge on main's field name. `WORK_TICKET_WORKSPACE_ROOT` was already set on the host in the first deploy; no env-file change needed.
- New table `qiita.upload`, new REST surface (`POST /upload`, `POST /upload/{idx}/done`, `GET /upload/{idx}`), new Flight DoPut handler on the DP, new `ticket:doput` scope.
