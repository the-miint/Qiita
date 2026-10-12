---
description: After a deploy, archive the Pending-deploy checklist into docs/deploy-archive/ and the pending changelog entries into docs/changelog-archive/, stamped with date + commit
---

You are closing out a deploy: moving the consolidated `## Pending deploy` block out of `DEPLOY_CHECKLIST.md` into its own file under `docs/deploy-archive/`, leaving an empty Pending section for the next cycle, and moving the changelog entry files the deploy shipped into their own folder under `docs/changelog-archive/`. This is a **maintainer-on-their-own-machine** action run *after* the operator reports a successful deploy — the deploy host has no Claude and the operator doesn't edit the repo. It is a repo edit (commit + push), not an on-host step.

The archive lives in its own directory precisely so `DEPLOY_CHECKLIST.md` stays short enough to read whole — it is the file every PR folds into. **Never append the archived block back into `DEPLOY_CHECKLIST.md`.**

## 1. Gather the stamp

- **Date**: use today's date from the session context (currentDate), `YYYY-MM-DD`.
- **Deployed commit**: the commit the operator reported running on the host (redeploy.md step 7). **Take it from `$ARGUMENTS`.** Do **not** default to the local checkout's `git rev-parse HEAD` — `main` may have advanced past what was deployed, and stamping the wrong SHA corrupts the history record. If `$ARGUMENTS` is empty, ask the user for the operator-reported deployed SHA rather than guessing.
- Confirm with the user that the deploy actually succeeded (bucket-5 checks passed) before archiving — don't archive a deploy that aborted. If the checklist has a bucket 6 (post-verify cleanup), confirm that ran too; an unarchived bucket 6 is the one part of a 'finished' deploy that is easy to forget.

## 2. Move the block

1. **Write a new archive file** `docs/deploy-archive/<YYYY-MM-DD>-<short SHA>.md`, holding the entire current `## Pending deploy` body (every bucket + Notes). Copy the shape of the newest existing file in that directory: an H1 stamp, then each bucket demoted one level (`### 1. Env vars` in the checklist becomes `## 1. Env vars` in the archive). **Rewrite every relative link as you move it:** the checklist sits at the repo root, so `](docs/runbooks/redeploy.md)` is right there and resolves to `docs/deploy-archive/docs/runbooks/…` once archived — it becomes `](../runbooks/redeploy.md)`, and a link to a sibling archive becomes the bare filename. A line pointing at the archive that will hold it — `([archived](…))` — is a self-reference once moved; drop it. `qiita-common/tests/test_doc_link.py` fails the build on both.
   ```
   # Deployed YYYY-MM-DD — <short SHA>

   ## 1. Env vars — …
   <the archived buckets, verbatim>
   ```
2. **Add it to the index**, `docs/deploy-archive/README.md`, as the new top entry (newest first).
3. **Empty `## Pending deploy` in place.** Keep every bucket sub-heading and `Notes` exactly as they already stand in the file, and replace only each bucket's *body* with `_None yet._`. Do **not** retype the bucket list from memory — the file is the source of truth for its own shape, and reconstructing it by hand is how bucket 6 (the irreversible-cleanup bucket, the costliest to lose) gets silently dropped. Leave `## Deployed history` as the pointer stub it is; do **not** put the block back there.

Preserve the `(#N)` tags in the archived copy — that's the per-deploy provenance record.

Two invariants the deploy scripts depend on, so don't disturb them when resetting Pending: the literal headings `### 1. Env vars` and `### 3. Migrations` are boundary markers `qiita_buckets_12()` (`deploy/_common.sh`) seds between to decide whether to prompt the operator — anything substantive left between them makes every deploy prompt for steps that don't exist. And `## Deployed history` must remain, as the terminator for the operator's own `sed` range in `redeploy.md` §1. `qiita-compute-orchestrator/tests/test_deploy_scripts.py` pins both against the real file.

## 3. Move the changelog entries

Each PR's changelog entry is its own file under `docs/changelog-pending/` (`CHANGELOG.md` describes the scheme). Move the ones this deploy shipped into a folder carrying the same stamp as the checklist archive:

```bash
# from the repo root
sha=<deployed commit>; dest=docs/changelog-archive/<YYYY-MM-DD>-<short SHA>
git ls-tree --name-only "$sha" docs/changelog-pending/ | while read -r f; do
  if [ -e "$f" ]; then mkdir -p "$dest" && git mv "$f" "$dest/"
  else echo "not in the working tree, skipped: $f"; fi
done
```

List the files from the **deployed commit**, as above, not from the working tree: an entry merged after that commit has not been deployed and stays in `docs/changelog-pending/`. Move the files as they are — no merging into one file, no edits. A deployed commit with no `docs/changelog-pending/` entries leaves no folder. A skipped name is an entry that was renamed or removed after the deployed commit; report it instead of guessing where it went.

The commit that carries this move adds no entry of its own; if it goes through a PR, that PR takes the `no-changelog` label.

## 4. Report

Show the user the new `docs/deploy-archive/<...>.md` file and the new `docs/changelog-archive/<...>/` folder (or say that the deploy shipped no entries), and confirm Pending is empty. Remind them to record the deployed commit on the host / ops channel too (redeploy.md step 8). Do not commit unless asked.
