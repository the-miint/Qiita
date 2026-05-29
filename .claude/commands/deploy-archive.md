---
description: At deploy time, archive the Pending-deploy checklist into Deployed history stamped with date + commit
---

You are closing out a deploy: moving the consolidated `## Pending deploy` block in `CHANGELOG.md` into `## Deployed history` and leaving an empty Pending section for the next cycle. Run this **after** a successful deploy (the operator has finished bucket 5 verification).

## 1. Gather the stamp

- **Date**: use today's date from the session context (currentDate), `YYYY-MM-DD`.
- **Deployed commit**: the commit now running on the host. If `$ARGUMENTS` provides a SHA, use it; otherwise run `git rev-parse HEAD` in the clone and confirm with the user that this is what was deployed (the local clone HEAD is normally the deployed commit right after `local-deploy.sh`).
- Ask the user to confirm the deploy actually succeeded (bucket-5 checks passed) before archiving — don't archive a deploy that aborted.

## 2. Move the block

Edit `CHANGELOG.md`:
1. Take the entire current `## Pending deploy` body (buckets 1–5 + Notes) and move it under `## Deployed history` as a new newest-on-top subsection:
   ```
   ### Deployed YYYY-MM-DD — <short SHA>
   <the archived buckets, verbatim>
   ```
2. Reset `## Pending deploy` to its empty shape: the heading, the one-line "nothing pending" note, and the five empty bucket sub-headings ready for the next PR to fold into. Match the structure the preamble describes.

Preserve the `(#N)` tags in the archived copy — that's the per-deploy provenance record.

## 3. Report

Show the user the new `### Deployed YYYY-MM-DD — <sha>` heading and confirm Pending is empty. Remind them to record the deployed commit on the host / ops channel too (redeploy.md step 8). Do not commit unless asked.
