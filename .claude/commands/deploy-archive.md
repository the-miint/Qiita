---
description: After a deploy, archive the Pending-deploy checklist into docs/deploy-archive/ stamped with date + commit
---

You are closing out a deploy: moving the consolidated `## Pending deploy` block out of `DEPLOY_CHECKLIST.md` into its own file under `docs/deploy-archive/`, and leaving an empty Pending section for the next cycle. This is a **maintainer-on-their-own-machine** action run *after* the operator reports a successful deploy — the deploy host has no Claude and the operator doesn't edit the repo. It is a repo edit (commit + push), not an on-host step.

The archive lives in its own directory precisely so `DEPLOY_CHECKLIST.md` stays short enough to read whole — it is the file every PR folds into. **Never append the archived block back into `DEPLOY_CHECKLIST.md`.**

## 1. Gather the stamp

- **Date**: use today's date from the session context (currentDate), `YYYY-MM-DD`.
- **Deployed commit**: the commit the operator reported running on the host (redeploy.md step 7). **Take it from `$ARGUMENTS`.** Do **not** default to the local checkout's `git rev-parse HEAD` — `main` may have advanced past what was deployed, and stamping the wrong SHA corrupts the history record. If `$ARGUMENTS` is empty, ask the user for the operator-reported deployed SHA rather than guessing.
- Confirm with the user that the deploy actually succeeded (bucket-5 checks passed) before archiving — don't archive a deploy that aborted. If the checklist has a bucket 6 (post-verify cleanup), confirm that ran too; an unarchived bucket 6 is the one part of a 'finished' deploy that is easy to forget.

## 2. Move the block

1. **Write a new archive file** `docs/deploy-archive/<YYYY-MM-DD>-<short SHA>.md`, holding the entire current `## Pending deploy` body (every bucket + Notes). Copy the shape of the newest existing file in that directory: an H1 stamp, then each bucket demoted one level (`### 1. Env vars` in the checklist becomes `## 1. Env vars` in the archive).
   ```
   # Deployed YYYY-MM-DD — <short SHA>

   ## 1. Env vars — …
   <the archived buckets, verbatim>
   ```
2. **Add it to the index**, `docs/deploy-archive/README.md`, as the new top entry (newest first).
3. **Empty `## Pending deploy` in place.** Keep every bucket sub-heading and `Notes` exactly as they already stand in the file, and replace only each bucket's *body* with `_None yet._`. Do **not** retype the bucket list from memory — the file is the source of truth for its own shape, and reconstructing it by hand is how bucket 6 (the irreversible-cleanup bucket, the costliest to lose) gets silently dropped. Leave `## Deployed history` as the pointer stub it is; do **not** put the block back there.

Preserve the `(#N)` tags in the archived copy — that's the per-deploy provenance record.

Two invariants the deploy scripts depend on, so don't disturb them when resetting Pending: the literal headings `### 1. Env vars` and `### 3. Migrations` are boundary markers `qiita_buckets_12()` (`deploy/_common.sh`) seds between to decide whether to prompt the operator — anything substantive left between them makes every deploy prompt for steps that don't exist. And `## Deployed history` must remain, as the terminator for the operator's own `sed` range in `redeploy.md` §1. `qiita-compute-orchestrator/tests/test_deploy_scripts.py` pins both against the real file.

## 3. Report

Show the user the new `docs/deploy-archive/<...>.md` file and confirm Pending is empty. Remind them to record the deployed commit on the host / ops channel too (redeploy.md step 8). Do not commit unless asked.
