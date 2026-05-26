# CHANGELOG

Operator-facing notes for deploying merged changes. Each entry lives under the PR that introduced it. Entries describe what the operator must do **on the deploy host** before / during `sudo local-deploy.sh` — not a general "what changed" summary; the git log already serves that audience.

The convention: when a PR adds a required env var, a new directory, a migration that needs out-of-band setup, or any other operator-visible action, append an entry to this file in the same PR. Reviewers check that the entry exists. Future operators read this file (`tail CHANGELOG.md`) before deploying to find out what's new since the last deploy.

Newest entry on top. Pre-deploy entries (PRs not yet merged to `main`) are allowed; mark them with the branch name and convert the heading to the PR number at merge.

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
