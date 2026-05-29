---
description: Fold the current branch's operator-impacting changes into the Pending-deploy checklist in CHANGELOG.md
---

You are folding this branch's operator-facing deploy steps into the **single consolidated `## Pending deploy` checklist** in `CHANGELOG.md`. Read `CHANGELOG.md`'s preamble and the "Operator-facing changes" + "Deployments" sections of `CLAUDE.md` first — they define the model. Do **not** create a standalone per-PR entry; this repo replaced that format with the living checklist.

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

Edit `## Pending deploy` in `CHANGELOG.md`. Place each item in its bucket: **1. Env vars**, **2. One-time host setup**, **3. Migrations**, **5. Verify** (add a check for what you introduced), **Notes** (no-action items). Rules:
- **Merge into the existing block, don't duplicate it.** If bucket 1 already has a `compute-orchestrator.env` `tee` block, add your var as another line inside it — do not create a second CO block. Same for the migration list and verify checks.
- **Tag every line you add with `(#N)`** (the PR/branch ref) so the archive step and the operator can trace provenance.
- **Keep it concise** — a copy-pasteable command and a half-line of why, matching the surrounding style. No prose narration of the change; the git log covers that.
- Respect ordering: anything `from_env()` requires goes in bucket 1 (must precede the restart); migrations in bucket 3; verification in bucket 5.
- Don't maintain any parallel PR roll-call list — the per-line `(#N)` tags are the only provenance record.

## 3. Report

Tell the user exactly which buckets you touched and which lines you added, so they can eyeball it in the PR diff. Remind them the reviewer checks that this fold landed (CLAUDE.md rule). Do not commit unless asked.
