# Changelog

The "what changed" log for this repo, one bullet per change. The git history is
the authoritative record; the per-line `(#N)` tag traces each entry to its PR.
Operator deploy steps live separately in
[`DEPLOY_CHECKLIST.md`](DEPLOY_CHECKLIST.md) — keep the two from drifting into
each other (a change can warrant an entry here, a step there, or both).

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The
project does not cut versioned releases yet, so everything lands under
**Unreleased**. Every PR adds an entry here (CI `changelog-check`; opt out with
the `no-changelog` label).

## [Unreleased]

### Added

- New nullable `bioproject_accession` column on the study table (unique
  when present), reserved for future NCBI/ENA BioProject tracking. Schema
  only — not yet exposed through the API, repository, or CLI (#87)

### Changed

- Renamed the study EBI accession to ENA across the stack: the study table
  column and its UNIQUE constraint (`ebi_study_accession` →
  `ena_study_accession`), the REST request/response field of the same name,
  and the `qiita study create`/`patch` CLI flag (`--ebi-study-accession` →
  `--ena-study-accession`). Clients sending the old field name must update (#87)
- Sequence chunking now uses miint's native `sequence_split` (single linear
  pass) instead of the pure-SQL `list_transform`/`substring` macro, which was
  **O(L²)** on large single records (host reference genomes) due to DuckDB
  #23229 — inside a lambda a captured column loses the statistics that select
  `substring`'s O(1) ASCII fast path, so it rescans from byte 0 on every chunk.
  Affects `stage_local_fasta` and the CLI `reference load` FASTA path; ~480×
  faster on a 256 MB record. Requires the miint build with `sequence_split`
  (duckdb-miint #121) on the mirror — gated in `DEPLOY_CHECKLIST.md` (#86)
- Completed the DuckDB 1.5.3 bump (#85) by updating the three spots it missed:
  the `setup-libduckdb` action default (`1.5.2` → `1.5.3`, so the Rust data-plane
  CI links the libduckdb matching its `1.10503.1` crate), the `deploy.yml`
  extension cache key, and the `v1.5.2` mentions in `docs/architecture.md` and
  the data-plane README. Added a CI guard (`test_duckdb_version_sync`) asserting
  the action default and deploy cache key match the data-plane `duckdb` crate, so
  a future DuckDB bump can't half-land (#86)
- Bumped the pinned DuckDB across all components to **1.5.3** to match the team
  miint mirror's current build: `duckdb>=1.5.3` in control-plane,
  compute-orchestrator, and the integration tests (locks re-resolved), and the
  data-plane Rust crate `1.10502.0` → `1.10503.1`. The compute env was on DuckDB
  1.5.2, so the native `stage_local_fasta` job installed the **stale** `v1.5.2`
  miint build from the mirror instead of the current `v1.5.3` one — DuckDB
  resolves the miint extension for its *own* version, so running 1.5.3 is the
  only way onto the current build (#85)
- Bumped the pinned `run-preflight` dependency to a newer upstream SHA in both
  the control-plane and compute-orchestrator, kept in lockstep by the SHA parity
  test (#82)
- `matrix_tube_id` must now be exactly 10 digits (previously 8–10), tightened on
  both the Pydantic field pattern and the `qiita.biosample` column CHECK (#81)
- Biosample/sequenced-sample create and biosample patch now take a checklist
  **name** (e.g. `ERC000015`) instead of a `metadata_checklist_idx`, resolving
  it to the idx server-side and returning a clean 422 for an unknown name —
  mirroring how terminology term_ids resolve. CLI flag is now
  `--metadata-checklist-name` on `biosample create`, `biosample patch`, and
  `sequenced-sample create` (#81)
- `BiosampleResponse` and `SequencedSampleResponse` now carry the checklist as
  a `metadata_checklist` ref (`{idx, name}`, where name is the ENA accession)
  instead of a bare `metadata_checklist_idx`, mirroring the missing-reason /
  terminology-term read-back refs (#81)

### Added

- ENVO terminology seed for the environmental-context biosample fields
  (`broad_scale_environmental_context`, `local_environmental_context`,
  `environmental_medium`), plus a reusable
  `rebind_biosample_global_field_data_type` migration helper that guards a
  field's data_type flip against existing metadata rows (#81)
- Study submission tracking: `qiita.study` gains `last_submission_at` /
  `submission_error`, exposed for read in `StudyResponse`. The three tables
  now share one `clear_submission_error_on_new_attempt` trigger function.
  These columns are subsystem-owned and are not on the (owner-accessible)
  study PATCH surface; on biosample and sequenced_sample, whose PATCH routes
  are wet_lab_admin-gated, they remain PATCHable. Not exposed through the
  CLI. (#81)
- `qiita study patch`, `qiita biosample patch`, and `qiita sequenced-sample
  patch` — update a study's or sample's editable fields (including ENA
  accession write-back) over the REST API, under If-Match optimistic
  concurrency (#81)
- `qiita study get`, `qiita biosample get`, and `qiita biosample list-idxs` —
  read a study or biosample by idx, and list a study's biosample idxs, over the
  REST API (#81)
- `qiita biosample create --ena-sample-accession` and `qiita sequenced-sample
  create --ena-experiment-accession` / `--ena-run-accession` — set an entity's
  ENA accession(s) at create time when ingesting already-submitted data
  (allowed, not required), matching `study create --ebi-study-accession` (#81)
- `qiita study create --extra-metadata` — attach a free-form JSON object
  (stored as JSONB) when minting a study, matching the existing
  `--extra-metadata` flag on `sequencing-run create` / `sequenced-pool create`
  (#81)
- Work-ticket in-place-retry visibility: `transient_reason` / `transient_since`
  on the work-ticket status (`GET /work-ticket/{idx}` and the list view) and two
  matching `qiita.work_ticket` columns. While the runner retries an unreachable
  orchestrator/slurmrestd in place, it records *why* and *since when* so a
  ticket stuck in `processing` is explainable instead of looking silently
  wedged; cleared once it makes progress or fails (#80)
- `GET /work-ticket` — list work tickets, each with a snapshot of its current
  step's compute placement (`compute_target`, `slurm_job_id`, `step_state`,
  `current_step_index/name`) from a single join against the new
  `qiita.work_ticket_step` progress table. Caller-relative by default;
  `?all=true` (wet_lab_admin+) widens to every originator; filters `state` /
  `active` / `limit` (#77)
- `qiita ticket list [--state … --active --all --limit N]` — CLI over the new
  list endpoint (#77)
- `POST /step/find-by-name` (CP→CO) — look up live SLURM jobs by their
  deterministic name so the runner can adopt a job it submitted but never
  recorded the id for, instead of launching a duplicate on resume (#77)
- `qiita.work_ticket_step` table — per-`(work_ticket_idx, step_index, attempt)`
  write-ahead progress (compute_target, slurm_job_id, job_name, state, failure
  surface) that is the spine of restart recovery (#77)
- Local-host FASTA ingest: `qiita reference load --local --fasta-manifest <path>`
  builds a reference from many host-resident FASTA files **by path** (no DoPut
  upload), backed by the `stage_local_fasta` native job and two new workflows,
  `local-reference-add` and `local-host-reference-add`; companions
  (taxonomy/tree/jplace/genome_map) ride as raw absolute paths
  (#78)
- Host references for host-read filtering: `is_host` column on `qiita.reference`,
  the `reference_index` table tracking built indexes, an `indexing` reference
  status (`loading → indexing → active`), and the `host-reference-add` workflow
  that builds a rype `.ryxdi` negative-filter index (`build_rype_index` native
  job + `register-index` library action) (#70)
- `GET /reference` (list; filter by `kind` / `is_host` / `status`) and
  `GET /reference/{reference_idx}/index` (list a reference's built indexes) (#70)
- `qiita reference load --host` — create a host reference (or bind an existing
  one) and run `host-reference-add`; requires `--taxonomy` (#70)
- Arrow Flight DoPut upload domain + chunked reference-load pipeline (#49)
- Support for known-missing and terminology-term metadata values (#56)
- `/health` aggregator probing CP + CO + DP with cached aggregation, a
  three-pill per-service status strip on the landing page, and gRPC reflection
  on the data plane (closes #54) (#58)
- bcl-convert workflow: container image, workflow YAML, and build script;
  `QIITA_IMAGES_DIR` with container bind/path resolution; the `bcl_convert_prep`
  native job; per-sample sequenced-sample minting via `qiita submit-bcl-convert`;
  the `SEQUENCED_POOL` scope target (#62)
- Self-hosted OpenAPI docs at `/docs` (Swagger UI) and `/redoc` (ReDoc), linked
  from the landing page and served from vendored assets (no CDN) (#64)
- `changelog-check` CI gate requiring every PR to record its change here (opt
  out with the `no-changelog` label) (#65)
- `matrix_tube_id` column on biosample with digit-only format and uniqueness
  constraints, exposed via the biosample REST routes and the
  `qiita biosample create --matrix-tube-id` CLI flag (#68)
- `POST /study/lookup-by-accession` for bulk `ebi_study_accession` →
  `study_idx` resolution; body-shaped so a long accession list rides
  past nginx's default URL-line cap (#74)
- `PATCH /study/{idx}` for editing the post-create study columns
  (PI, title, alias, description, abstract, funding,
  `ebi_study_accession`, notes, `extra_metadata`) under required
  `If-Match` optimistic-concurrency control (#74)
- `UNIQUE` constraint on `study.ebi_study_accession` (NULLs distinct,
  so "unique when present") (#74)

### Changed

- The user-CLI quickstart now documents the headless / remote-host auth path:
  `qiita login` needs a co-located browser + loopback receiver, so on an SSH
  session / HPC login node / CI runner you carry the PAT instead — log in once
  on a browser machine, then `export QIITA_TOKEN=…` (+ `QIITA_CONTROL_PLANE_URL`)
  on the headless host. `$QIITA_TOKEN` already took precedence over the token
  file; this just makes the supported path discoverable (#80)
- miint now installs from the team mirror by default in every component (CP CLI,
  CO service, native SLURM jobs): `miint_install_sql()` always `FORCE INSTALL`s
  from `MIINT_MIRROR_URL` (override with `MIINT_EXTENSION_REPO`) instead of
  falling back to the DuckDB community channel — so one host can't drift to a
  different `read_fastx` build, and `FORCE` overwrites a stale cached extension
  (#80)
- Decoupled compute-step execution: the orchestrator's single blocking
  `POST /step/run` is replaced by the stateless `submit` / `status` / `result`
  trio, and the **control plane** now owns the poll loop. A long SLURM job no
  longer holds the CP→CO connection open, and the orchestrator keeps no
  in-flight state between calls (the `StepHandle` it returns carries everything
  status/result need; the CP persists it) (#77)
- Restart recovery re-attaches instead of failing: on CP startup,
  `reconcile_inflight_tickets` resumes every non-terminal ticket through
  `run_workflow(resume=True)` — fast-forwarding completed entries, re-attaching
  a live SLURM job by its persisted id (or adopting an orphan by deterministic
  name), and deciding a purged job from its on-disk output manifest — rather
  than the old blanket-fail of all in-flight work on every deploy (#77)
- CO-unreachable during submit/poll/result (transport error or HTTP 5xx) is now
  a transient `ORCHESTRATOR_UNREACHABLE` the runner retries in place, so
  stopping the orchestrator mid-deploy never fails a running ticket (#77)
- `qiita reference load` now parses FASTA with miint's `read_fastx` and a shared
  DuckDB `chunk_list` macro (in new `qiita_common.chunking` /
  `qiita_common.duckdb_miint` modules) instead of a hand-rolled Python FASTA
  chunker; the control-plane CLI loads the miint DuckDB extension client-side.
  No sequence bytes pass through Python and memory stays bounded for
  genome-scale records (#78)
- The SLURM backend now propagates `PATH_SCRATCH` into the compute-node job
  environment, so native steps that derive a persistent path from it (e.g.
  `build_rype_index` writing the rype `.ryxdi`) resolve the real scratch root
  instead of the `$TMPDIR/qiita` default (#70)
- Centralized all REST path string literals into `qiita-common`'s
  `api_paths.py` (closes #12) (#60)
- Bumped the study / prep_sample identity sequence start to 25000 (#61)
- Moved the `reference load` command from `qiita-admin` to the `qiita` end-user
  CLI (it is a credentialed API call, not a host operation) (#63)
- Renamed the operator deploy checklist `CHANGELOG.md` → `DEPLOY_CHECKLIST.md`;
  `CHANGELOG.md` is now this per-change log (#65)
- Scoped the `push` CI trigger to `main` so PR branches get a single
  `pull_request` run instead of duplicate push + PR runs (#65)
- Restructured the filesystem env vars onto three base roots with derived
  leaves: `WORK_TICKET_WORKSPACE_ROOT` + `SHARED_FILESYSTEM_ROOT` →
  `PATH_SCRATCH/ticket`, `UPLOAD_STAGING_ROOT` → `PATH_SCRATCH/staging`,
  `DUCKLAKE_DATA_PATH` → `PATH_PERSISTENT/ducklake`, and `QIITA_IMAGES_DIR`
  → `PATH_DERIVED/images`. Operators now set `PATH_SCRATCH` /
  `PATH_PERSISTENT` / `PATH_DERIVED`; the services derive the fixed
  subdirs. Hard cutover — the old names are gone and boot fails fast until
  the new ones are set (#73)
- Switched `bcl_convert_prep` from `run_preflight.legacy.api.save_legacy_csv`
  to the public `run_preflight.save_bclconvert_v1_csv`, and `qiita
  submit-bcl-convert` from a hard-coded JOIN against the kl-run-preflight
  SQLite schema to upstream's `get_illumina_sample_info` plus the new
  `POST /study/lookup-by-accession`; bumped the `run-preflight` SHA pin
  in lock-step across CP + CO and guarded the parity with a new
  `tests/integration/test_run_preflight_pin_parity.py` (#74)
- Tightened `min_length=1` on `biosample_accession` / `ena_sample_accession`
  (`BiosampleImportRequest`, `BiosamplePatchRequest`) and on
  `ebi_study_accession` (`StudyCreate`) so empty strings no longer reach
  the DB (#74)
- SIF builds go through a single generic `scripts/build-sif.sh <workflow>`
  driven by a declarative `workflows/<workflow>/sif-build.env`; replaces the
  per-workflow `scripts/build-bcl-convert-sif.sh`. The builder stages into a
  temp root owned by the invoking user (the checkout is read-only), so a
  service account can build without write access to the qiita-owned checkout.
  A `test_sif_build_spec.py` guard forbids per-workflow build scripts, requires
  each spec to be complete, and asserts `SIF_FILENAME` matches the workflow
  YAML's `container:`; `make test-workflows` builds a `_sif-build-smoke`
  sentinel through `build-sif.sh` so the temp-root staging is covered against
  real apptainer in CI (#75)

### Fixed

- `qiita-admin compute-readiness` no longer aborts its SLURM probe at parse
  time. A newline-escape inside an f-string comment in `build_probe_script`
  expanded to a real newline, splitting the comment and leaving an unmatched
  backtick, so the generated probe script failed `bash` parsing (exit 2) before
  any check ran — meaning the `native-import` / `miint-read-fastx` compute-env
  guards #80 added never actually executed. Fixed the comment, added a `bash -n`
  regression test over the generated script (the existing substring tests
  couldn't catch it), and defaulted the probe log onto the shared filesystem
  (`PATH_SCRATCH/ticket`) instead of node-local `/tmp` so the head node can read
  the compute-node probe results back (#84)
- `qiita reference load --local` no longer hard-fails when the
  `--fasta-manifest` path isn't visible from the host running the CLI (e.g. a
  login node without the compute node's shared-FS view). The manifest is read
  by `stage_local_fasta` on the compute node, not by the CLI, so a missing path
  is now a warning (still flags a real typo) and the submit proceeds —
  consistent with the companion paths, which were never existence-checked. The
  absoluteness check is unchanged (a relative path still errors) (#80)
- SLURM JWT recovery no longer depends on a clean 401. `SlurmrestdClient` now
  proactively reloads the JWT from its file when the cached token is within 60s
  of its `exp`, *before* sending the request — so a long-lived orchestrator
  can't run on a boot-cached token past expiry until a restart when slurmrestd
  rejects an expired token with a 5xx / dropped connection instead of a 401
  (the reload-on-401 path only fires on a 401). The 401-reload path is kept as a
  fallback, and both the 401 reload and the submit-error classification now log
  the exact status so the next stuck-on-submit incident is diagnosable without a
  repro (#80)
- The runner's in-place infra-unreachable retry is now escapable and bounded.
  An operator `qiita-admin ticket force-fail` (a direct-DB FAILED transition) is
  now noticed: every infra-retry/poll iteration re-checks the ticket's DB state
  and bails if it has gone terminal, instead of spinning forever against a
  ticket it no longer owns — without clobbering the operator's failure surface.
  The retry sleep is now capped exponential backoff (base = poll interval,
  doubling to a 60s cap) rather than a flat hammer, and the never-fail-on-outage
  invariant is preserved — there is still no hard give-up (#80)
- Redriving a FAILED reference workflow via `POST /work-ticket/{idx}/run` now
  actually works. Two redrive defects fixed in the same atomic reset: the
  `reference` scope_target was left pinned at `failed`, so the redriven
  workflow's first status PATCH (`failed → hashing`) was illegal and the redrive
  died immediately — `/run` now resets the reference `failed → pending` (the
  FSM's only legal exit from `failed`); the prior run's terminal `failed`
  `work_ticket_step` rows survived, so the runner's fresh attempt-0 collided with
  the dead row (the step-progress writers reject any transition out of `failed`)
  — `/run` now drops every non-`completed` step row (keeping `completed` ones so
  fast-forward still works). A reference that failed at a later step still fails
  cleanly on redrive (the FSM can't rewind past `pending`); that multi-step case
  is not yet supported (#80)
- A rejected SLURM submit no longer looks like a success: slurmrestd answers
  HTTP 200 even when slurmctld refuses the job (unavailable partition, QOS
  limit, …), and `SlurmrestdClient.submit_job` trusted the echoed `job_id`
  blindly. It now inspects `result.error_code` and the top-level `errors[]`
  array first and raises (classified as a permanent `CONTRACT_VIOLATION`, since
  re-submitting the same payload won't help); benign `warnings[]` (e.g. the
  `nodes` type warning) stay non-fatal (#80)
- Stale compute-environment failures now surface at deploy, not at the first
  job: `compute-readiness` probes that the compute node's miint build binds
  `read_fastx(max_batch_bytes:=…)` (the call `stage_local_fasta`/`reference load`
  issue), and the redeploy runbook now documents refreshing the separate
  `SLURM_NATIVE_PYTHON` checkout so native jobs don't import stale `qiita-common`
  (#80)
- Long compute steps no longer self-fail: under the old held-connection model a
  step exceeding the 600s CP→CO client timeout tripped an httpx error that
  skipped the retry loop and marked the ticket FAILED while the SLURM job kept
  running. The CP-driven poll loop has no such ceiling (#77)
- No duplicate concurrent SLURM jobs: a write-ahead progress row + deterministic
  job name `qiita-wt{idx}-{step}-a{attempt}` let resume adopt a job whose id was
  never persisted (via `find-by-name`) instead of re-submitting; retriable
  failures no longer resubmit without checking the prior job (#77)
- Corrected stale identifier field names in `docs/architecture.md` to match the
  current schema: `sample_idx` → `biosample_idx` (the physical sample is
  `biosample`; there is no `sample` table), noted design issue to resolve the
  non-existent `prep` entity and dangling `prep_idx` surviving only as a
  vestigial `work_ticket` scope tuple), documented that `study`/`biosample` are
  many-to-many with `prep_sample`, and dropped `study_idx`/`biosample_idx` from
  the result-Parquet identifier columns (recovered via control-plane joins),
  resolving the prior `(prep_idx, processing_idx)` vs `(prep_sample_idx,
  processing_idx)` inconsistency (#76)
- Assert `HumanUser` before reading `.system_role` in the sequenced-sample /
  biosample routes (closes #45) (#59)
- Closed deploy gaps surfaced by the first user-CLI fastq-to-parquet smoke
  test (#57)
- Added a lightweight CP `/healthz` liveness route so `qiita-admin
  compute-readiness` (and its SLURM probe) stops reporting a false 404 against
  a healthy deploy — the checker hit `/healthz`, which the CP never served
  (closes #67) (#69)

### Removed

- The legacy synchronous step path: `POST /step/run`, `ComputeBackend.run_step`
  (+ the SLURM/Local overrides and the CO `_poll_until_terminal` poll loop),
  `ComputeBackendClient.run_step`, and the `StepRunRequest` / `StepRunResponse`
  wire models. The decoupled submit/status/result trio fully replaces it; CP
  and CO must deploy together since the route contract changed (#77)

[Unreleased]: https://github.com/the-miint/Qiita/commits/main
