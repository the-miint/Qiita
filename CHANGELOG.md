# CHANGELOG

Operator-facing notes for deploying merged changes. Each entry lives under the PR that introduced it. Entries describe what the operator must do **on the deploy host** before / during `sudo local-deploy.sh` — not a general "what changed" summary; the git log already serves that audience.

The convention: when a PR adds a required env var, a new directory, a migration that needs out-of-band setup, or any other operator-visible action, append an entry to this file in the same PR. Reviewers check that the entry exists. Future operators read this file (`tail CHANGELOG.md`) before deploying to find out what's new since the last deploy.

Newest entry on top. Pre-deploy entries (PRs not yet merged to `main`) are allowed; mark them with the branch name and convert the heading to the PR number at merge.

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
