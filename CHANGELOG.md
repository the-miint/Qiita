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

- `qiita study patch`, `qiita biosample patch`, and `qiita sequenced-sample
  patch` — update a study's or sample's editable fields (including ENA
  accession write-back) over the REST API, under If-Match optimistic
  concurrency (#TBD)
- `qiita study get`, `qiita biosample get`, and `qiita biosample list-idxs` —
  read a study or biosample by idx, and list a study's biosample idxs, over the
  REST API (#TBD)
- `qiita biosample create --ena-sample-accession` and `qiita sequenced-sample
  create --ena-experiment-accession` / `--ena-run-accession` — set an entity's
  ENA accession(s) at create time when ingesting already-submitted data
  (allowed, not required), matching `study create --ebi-study-accession` (#TBD)
- `qiita study create --extra-metadata` — attach a free-form JSON object
  (stored as JSONB) when minting a study, matching the existing
  `--extra-metadata` flag on `sequencing-run create` / `sequenced-pool create`
  (#75)
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

[Unreleased]: https://github.com/the-miint/Qiita/commits/main
