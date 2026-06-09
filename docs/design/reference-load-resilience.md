# Reference-load resilience, redrive correctness & compute-env consistency

**Status:** accepted (implementation in progress on `fix/compute-resilience-and-redrive`)
**Date:** 2026-06-09
**Scope:** `qiita-compute-orchestrator` (SLURM backend, native jobs), `qiita-control-plane`
(runner, work-ticket routes, CLI), `qiita-common` (miint install), deploy scripts + runbooks.

## Why this exists

Loading a host reference (`T2T-CHM13 2.0`) via `qiita reference load --host --local` against the live
qiita-miint deploy required peeling through **ten distinct defects**, debugged live in sequence — each
masked the next. The journey went: install/auth friction → a ticket wedged in `processing` for ~40 minutes
→ a `force-fail` that didn't stop the live loop → a `422` that was actually a transient backend error → a
laptop manifest path → a broken headless login → a host-side manifest check → a partition probe → a stale
`qiita_common` in the compute env → a `/run` redrive that re-failed on stale step state → finally a stale
`duckdb-miint` extension whose `read_fastx` lacked `max_batch_bytes`.

The unifying problem: **failures that should have been fast, loud, and self-evident were instead silent,
eternal, or misattributed.** This document records each finding, the invariant it touches, and the chosen
direction, so the fixes are deliberate rather than reactive.

## Cross-cutting themes

1. **The deploy refreshes the deploy host, never the SLURM compute environment.** `activate.sh` runs
   `uv sync --reinstall-package qiita-common` for the *service* venvs (`/opt/qiita/{control-plane,
   compute-orchestrator}`) but never the native compute env (`SLURM_NATIVE_PYTHON` →
   `/home/qiita/.../qiita-compute-orchestrator/.venv`) nor the `duckdb-miint` extension on compute nodes.
   Two separate production failures (F7, F10) are the same disease.
2. **Transient/operator state changes don't reach the live in-process work.** `force-fail` (F2) and the
   reference reset / redrive (F8, F9) mutate the DB, but the running `asyncio` runner task and its
   `work_ticket_step` rows don't reflect those mutations.
3. **Backend errors are swallowed or misclassified.** `submit_job` ignores the SLURM submission return code
   (F4); the unbounded retry hides the reason (F3).

## Findings

### F1 — Stale slurmrestd JWT, no recovery *(investigate → resilience)*
- **Symptom:** orchestrator up since 6/8 14:33; on 6/9 the slurmrestd submit failed `slurmrestd_unreachable`
  every 10s for ~40min. The on-disk JWT + a manual `curl` with it worked throughout; only restarting the
  orchestrator fixed it.
- **Root cause (open):** `SlurmrestdClient` caches the JWT at construction and reloads on a 401
  (`slurm/client.py:417`). That *should* self-heal, so either the failures weren't clean 401s or the reload
  path has a gap. Ambiguous because the CP only logs the coarse `slurmrestd_unreachable` kind
  (= transport / 5xx / 401).
- **Invariant touched:** none documented; this is a resilience gap.
- **Direction:** add exact-status logging to disambiguate next time; add a **proactive** JWT freshness check
  (decode `exp` via the existing `decode_jwt_payload`, reload before expiry) so recovery doesn't depend on
  catching a 401.

### F2 — `force-fail` doesn't stop the live retry loop *(bug)*
- **Symptom:** after `qiita-admin ticket force-fail`, `slurmrestd_unreachable` retries kept logging.
- **Root cause:** `force-fail` is a direct-DB transition (`cli/admin.py:170`); the running runner task's
  `while True` retry loop never re-reads ticket state.
- **Invariant touched:** the single-CP-process dispatch model (a ticket's live owner is its `asyncio.Task`).
- **Direction:** the retry loop re-checks the ticket's DB state each iteration and bails if terminal
  (folded into F3's "escapable").

### F3 — Unbounded + silent in-place retry *(resilience/UX)*
- **Symptom:** ticket sat in `processing` with `failure_*: null` for 40min; CLI watch + `ticket status`
  showed nothing about *why*.
- **Root cause:** `runner.py:1348` `while True` with a flat `sleep(poll_interval)` for
  `SLURMRESTD_UNREACHABLE`/`ORCHESTRATOR_UNREACHABLE`; no backoff, no ceiling, no surfaced reason; doesn't
  consume `retry_count`.
- **Invariant touched:** *"the runner's poll loop never fails on an unreachable orchestrator … so a deploy
  that stops both services is safe"* (`dispatch.py` reconcile docstring, CLAUDE.md). Deliberately preserved.
- **Direction (per user decision — "visible + escapable"):** add capped exponential backoff; surface the
  latest transient reason + "stuck since" on `ticket status`; make the loop bail when the ticket is terminal
  (delivers F2). **No hard give-up ceiling** — preserve the deploy-safety invariant.

### F4 — `submit_job` ignores the SLURM submission return code *(bug)*
- **Symptom:** a no-partition probe returned HTTP 200 + `result.error_code 2015` ("partition not available")
  + a `job_id`.
- **Root cause:** `submit_job` (`slurm/client.py:293`) returns `body.get("job_id")` without inspecting
  `result.error_code` / top-level `errors`. A rejected submit is mis-read as success; the runner then polls a
  job that was never queued.
- **Invariant touched:** "fail fast, fail loud" (CLAUDE.md ethos).
- **Direction:** inspect `result.error_code` (and non-warning `errors`) after extracting `job_id`; raise
  `SlurmrestdError` classified `CONTRACT_VIOLATION` (a rejected submit is permanent). Keep slurmrestd
  *warnings* (e.g. the `nodes` int/string type warning) non-fatal.

### F5 — CLI manifest existence-check asymmetry *(bug/inconsistency)*
- **Symptom:** `--fasta-manifest /home/mcdonadt/...` from the laptop → `not found or not a file`, before any
  network call.
- **Root cause:** `cli/reference_load.py:539` does a host-side `exists()/is_file()` on the manifest, but
  companions (`--taxonomy`, `--tree`, …) are explicitly *not* checked (they may live on a shared FS the CLI
  host can't see). Under `--local` the manifest is read on the compute node, so the asymmetry is surprising.
- **Invariant touched:** the documented `--local` contract (paths are compute-node-visible, not CLI-visible).
- **Direction:** make the manifest consistent with companions — drop the hard existence failure under
  `--local` (or downgrade to a warning).

### F6 — Headless `login` unusable *(UX/docs)*
- **Symptom:** on a headless host, `qiita login` opens an AuthRocket URL that redirects to `127.0.0.1:<port>`
  (the *laptop's* localhost); editing it to the CP host hangs.
- **Root cause:** the loopback OAuth flow (`cli/_common.py do_login`) assumes browser + CLI co-located; no
  `--no-browser`/`--port`/`--ot-code` affordance. The real answer (a PAT is a portable bearer string;
  set `$QIITA_TOKEN`) is undocumented.
- **Invariant touched:** none.
- **Direction:** document "carry the PAT" as the first-class headless path in the user-CLI quickstart;
  optionally add a small `--no-browser`/`--ot-code` paste affordance.

### F7 — Deploy never refreshes the native compute Python env *(bug/deploy)*
- **Symptom:** native SLURM job failed `ModuleNotFoundError: qiita_common.chunking` even after re-syncing the
  service venv.
- **Root cause:** the job runs `srun $SLURM_NATIVE_PYTHON -m qiita_compute_orchestrator.jobs` on a compute
  node; `SLURM_NATIVE_PYTHON` (`/home/qiita/.../qiita-compute-orchestrator/.venv`) is a *separate* checkout
  that `activate.sh:125` never touches. Its `qiita_common` was stale (the documented cross-package-staleness
  footgun, in the compute env).
- **Invariant touched:** `local-deploy.sh` "refreshes everything merged since last deploy" — it doesn't, for
  the compute env.
- **Direction:** the deploy refreshes the native venv (`uv sync --reinstall-package qiita-common`) like the
  service venvs; if it's an out-of-reach shared-FS checkout, document + script the refresh in the runbooks.
  (See F10 for the paired extension refresh + the deploy/boot guard.)

### F8 — `/run` doesn't reset the `scope_target` resource *(bug)*
- **Symptom:** redriving a FAILED reference workflow re-failed.
- **Root cause:** `/run` (`routes/work_ticket.py:888`) resets the *ticket* FAILED→PENDING but leaves the
  reference `failed`; the workflow's first status PATCH (`failed→hashing`) is illegal
  (`models.py:462`: `failed → {pending}` only).
- **Invariant touched:** the documented "`/run` is the only retry mechanism."
- **Direction:** on FAILED→PENDING, `/run` also resets the `scope_target` resource to its initial status via
  the existing `transition_reference_status`.

### F9 — `/run` doesn't reset `work_ticket_step` rows *(bug)*
- **Symptom:** redrive logged "job unreadable → deciding from manifest → manifest.json missing", then
  `RuntimeError: could not transition work_ticket_step (14,0,0) … actual state 'failed', allowed
  ['submitting','submitted','running']`.
- **Root cause:** `/run` leaves the prior attempt's terminal `work_ticket_step` row; resume re-adjudicates
  the dead attempt and `step_progress.record_failed` rejects a `failed`→`failed` write.
- **Invariant touched:** the resume / write-ahead step-progress contract.
- **Direction:** on redrive, supersede the prior attempt's step rows so resume starts a *clean* attempt
  (prefer advancing the attempt counter over deleting — preserves postmortem history).

### F10 — `duckdb-miint` unpinned + never refreshed on the compute side *(bug/deploy)*
- **Symptom:** native job failed `Binder Error: Invalid named parameter "max_batch_bytes" for function
  read_fastx`.
- **Root cause:** `qiita_common/duckdb_miint.py` installs miint *unpinned* (`INSTALL miint FROM community`,
  or `FORCE INSTALL` from the mirror only when `MIINT_EXTENSION_REPO` is set) — no version, and the compute
  env had a stale cached extension. Code added `read_fastx(max_batch_bytes:=…)` 2026-06-04; the deployed
  extension predates it.
- **Invariant touched:** miint must be **the same version across all of Qiita** (CP CLI, CO jobs, data-plane)
  — *no patchwork* (user mandate).
- **Direction (per user decision — "install from the mirror, same everywhere"):** set
  `MIINT_EXTENSION_REPO=<mirror>` in **every** environment so all consumers `FORCE INSTALL` the same mirror
  build (`FORCE` overwrites a stale cached extension dir); refresh it on the compute env at deploy; add a
  **smoke test** that loads miint and asserts `read_fastx` accepts `max_batch_bytes` (+ the functions the
  jobs use); extend the `compute-readiness` SLURM probe to verify it on a compute node so a stale env fails
  at deploy, not at first job.

## Implementation phasing

Test-driven, surgical, on one branch, with a code review + written summary at each phase stop:

0. Branch + this design note.
1. **Compute-env consistency (F10, F7)** — the green-run unblocker + the "miint same everywhere" mandate.
2. **Submit honors the SLURM return code (F4).**
3. **`/run` redrive correctness (F8, F9).**
4. **Retry resilience + visibility + escapability (F2, F3).**
5. **JWT recovery (F1)** — investigation-led.
6. **CLI manifest check + headless login + docs (F5, F6).**

## Decisions of record

- **Retry:** visible + escapable, **no hard give-up** — preserves the deploy-safety invariant (F3/F2).
- **miint:** keep installing from the mirror, but **one consistent version everywhere**, force-installed and
  guarded by a smoke test (F10).
- **Scope:** all ten findings, phased (above).

## Open item

Phase 1's green run depends on the mirror serving a `read_fastx` build that has `max_batch_bytes`. If it
doesn't yet, Phase 1 still lands the consistency + guard, and the remaining blocker (the mirror build) is
surfaced explicitly rather than hidden.
