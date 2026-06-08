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

- Long compute steps no longer self-fail: under the old held-connection model a
  step exceeding the 600s CP→CO client timeout tripped an httpx error that
  skipped the retry loop and marked the ticket FAILED while the SLURM job kept
  running. The CP-driven poll loop has no such ceiling (#77)
- No duplicate concurrent SLURM jobs: a write-ahead progress row + deterministic
  job name `qiita-wt{idx}-{step}-a{attempt}` let resume adopt a job whose id was
  never persisted (via `find-by-name`) instead of re-submitting; retriable
  failures no longer resubmit without checking the prior job (#77)
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
