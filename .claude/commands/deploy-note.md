---
description: Fold the current branch's operator-impacting changes into the Pending-deploy checklist in DEPLOY_CHECKLIST.md
---

You are folding this branch's operator-facing deploy steps into the **single consolidated `## Pending deploy` checklist** in `DEPLOY_CHECKLIST.md`. Read `DEPLOY_CHECKLIST.md`'s preamble and the "Operator-facing changes" + "Deployments" sections of `CLAUDE.md` first — they define the model. Do **not** create a standalone per-PR entry; this repo replaced that format with the living checklist.

## 1. Detect what this branch changes that the operator must act on

Diff the branch against `main` and look for each category below. `$ARGUMENTS` may name a PR number to tag with; if absent, use the branch name (e.g. `#feat/foo`) as the tag and tell the user to retag once the PR number exists.

```
git fetch origin --quiet
git diff --name-status origin/main...HEAD
```

Categories (only these matter — pure code/test/doc changes need no entry):
- **Required env vars** — new/renamed keys read by a `from_env()` / settings loader (grep `config.py`, `settings`, `os.environ`, the `.env.*.example` files for additions). Note which service(s): CP / DP / CO. A var with a safe default is *not* required — skip it.
- **Migrations** — new files under `qiita-control-plane/db/migrations/`. List filenames; note any that need out-of-band setup (CREATE EXTENSION, manual backfill) beyond a plain `make migrate`.
- **New `workflows/` entries** — picked up by `qiita-admin actions sync` at deploy; note the action_id + version to verify.
- **One-time host setup** — new shared directory (owner/group/mode), service-account scope grant, vendored binary/SIF, group membership, TLS/cert change.
- **Soft API-contract changes** — status-code or payload-shape changes downstream clients should know about (no host action; goes in the Notes bucket).

If none apply, say so and stop — make no edit.

## 2. Fold into the right bucket — merge, don't append

Edit `## Pending deploy` in `DEPLOY_CHECKLIST.md`. Place each item in its bucket: **1. Env vars**, **2. One-time host setup**, **3. Migrations**, **5. Verify** (add a check for what you introduced), **Notes** (no-action items). Rules:
- **Bucket 5: the generic checks are `make verify-deploy` — do NOT re-paste them.** Health, the `qiita.action` list, and `compute-readiness` are all run (each with the correct service account/env baked in) by `sudo make verify-deploy QIITA_HOSTNAME=<fqdn>`. A verify fold adds only the *deploy-specific* assert your PR introduces (a new `action_id`+version to look for, a new probe row to grep, a new endpoint to curl) — never the generic `compute-readiness` invocation, whose hand-copied wrong run-as is the bug `make verify-deploy` retires (issue #72).
- **Merge into the existing lines, don't duplicate them.** If bucket 1 already appends to `compute-orchestrator.env` (one idempotent `sudo bash -c 'grep -q … || echo "KEY=value" >> …'` per var), add your var as another such line next to them — do not start a second CO group. Same for the migration list and verify checks.
- **Tag every line you add with `(#N)`** (the PR/branch ref) so the archive step and the operator can trace provenance.
- **Keep it concise** — a copy-pasteable command and a half-line of why, matching the surrounding style. No prose narration of the change; the git log covers that.
- Respect ordering: anything `from_env()` requires goes in bucket 1 (must precede the restart); migrations in bucket 3; verification in bucket 5.
- Don't maintain any parallel PR roll-call list — the per-line `(#N)` tags are the only provenance record.

## 3. Report

Tell the user exactly which buckets you touched and which lines you added, so they can eyeball it in the PR diff. Remind them the reviewer checks that this fold landed (CLAUDE.md rule). Do not commit unless asked.
