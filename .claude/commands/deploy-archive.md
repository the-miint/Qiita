---
description: After a deploy, archive the Pending-deploy checklist into Deployed history stamped with date + commit
---

You are closing out a deploy: moving the consolidated `## Pending deploy` block in `DEPLOY_CHECKLIST.md` into `## Deployed history` and leaving an empty Pending section for the next cycle. This is a **maintainer-on-their-own-machine** action run *after* the operator reports a successful deploy — the deploy host has no Claude and the operator doesn't edit the repo. It is a repo edit (commit + push), not an on-host step.

## 1. Gather the stamp

- **Date**: use today's date from the session context (currentDate), `YYYY-MM-DD`.
- **Deployed commit**: the commit the operator reported running on the host (redeploy.md step 7). **Take it from `$ARGUMENTS`.** Do **not** default to the local checkout's `git rev-parse HEAD` — `main` may have advanced past what was deployed, and stamping the wrong SHA corrupts the history record. If `$ARGUMENTS` is empty, ask the user for the operator-reported deployed SHA rather than guessing.
- Confirm with the user that the deploy actually succeeded (bucket-5 checks passed) before archiving — don't archive a deploy that aborted.

## 2. Move the block

Edit `DEPLOY_CHECKLIST.md`:
1. Take the entire current `## Pending deploy` body (buckets 1–5 + Notes) and move it under `## Deployed history` as a new newest-on-top subsection:
   ```
   ### Deployed YYYY-MM-DD — <short SHA>
   <the archived buckets, verbatim>
   ```
2. Reset `## Pending deploy` to its empty shape: the heading, the one-line "nothing pending" note, and the five empty bucket sub-headings ready for the next PR to fold into. Match the structure the preamble describes.

Preserve the `(#N)` tags in the archived copy — that's the per-deploy provenance record.

## 3. Report

Show the user the new `### Deployed YYYY-MM-DD — <sha>` heading and confirm Pending is empty. Remind them to record the deployed commit on the host / ops channel too (redeploy.md step 8). Do not commit unless asked.
