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

- Remove a full preparation (sequenced_pool) from the system. New
  `DELETE /sequencing-run/{run}/sequenced-pool/{pool}` hard-deletes a
  sequenced_pool and everything under it — the pool row, every
  `sequenced_sample`/`prep_sample` it holds, their `prep_sample_metadata`,
  `prep_sample_field_exception`, and `prep_sample_to_study` links, and any
  pool-/sample-scoped `work_ticket` rows (`work_ticket_step` and `sequence_range`
  cascade) — in one FK-ordered transaction. The parent `sequencing_run` and the
  underlying `biosample` rows are intentionally retained (a biosample is a
  physical sample, not pool-owned). Because each prep_sample is exclusive to one
  pool, this removes those samples from every study they link to. system_admin
  only, gated by a new `sequenced_pool:delete` scope. Gating mirrors
  `DELETE /reference`: in-flight work tickets (pending/queued/processing) block
  unconditionally; completed/failed tickets, prep_samples published into a study,
  and ENA-submitted samples block unless `?force=true`. Exposed as the
  `qiita delete-sequenced-pool` CLI command. The data-plane DuckLake purge is a
  no-op until processing-result tables exist; on-disk demux FASTQ cleanup is a
  follow-up (#125)
- Per-host-reference index selection and tunable build params. `qiita reference
  load --host` gains `--no-rype-index` / `--no-minimap2-index` (build only one
  of the two host-filter indexes; default still builds both, at least one
  required), plus `--rype-w N` (rype minimizer window, default now **20**, was
  25) and `--minimap2-preset` (one of
  `sr`/`map-ont`/`map-pb`/`map-hifi`/`asm5`/`asm10`/`asm20`, default `sr`). These
  ride in `action_context`
  (`build_rype`/`build_minimap2`/`rype_w`/`minimap2_preset`), validated by the
  `(local-)host-reference-add` `context_schema` (a `not` backstop rejects
  building neither). Backed by two reusable workflow-engine additions:
  `WorkflowEntry.when` (skip an entry when its named action_context flag is
  falsy — default-on) gates each build step and its `register-index`, and
  `WorkflowStep.params` (action_context key → native `Inputs` field) carries
  scalar build params to a native step without a wire-contract change. The
  fastq-to-parquet host-filter consumer now accepts a single-index host
  reference too: `_resolve_host_filter_indexes` binds whichever of rype/minimap2
  exist (requiring at least one) and the `host_filter` step skips the stage
  whose index is absent. Index selection is initial-build-time only (the status
  FSM is terminal at `active`) (#124)
- Delete a reference database from the system. New
  `DELETE /reference/{idx}` fully purges a reference — Postgres rows
  (`reference`, `reference_membership`, `reference_index`, plus orphaned
  `feature`/`feature_genome`/`genome` no surviving reference claims), DuckLake
  data (taxonomy/phylogeny/placements by `reference_idx`; sequences/chunks only
  for orphan features, computed in the data plane via a new `delete_reference`
  DoAction), and on-disk indexes (`rype`/`minimap2` under `PATH_DERIVED`, removed
  by a new orchestrator `DELETE /reference-artifact/{idx}` endpoint). system_admin
  only, gated by a new `reference:delete` scope. Work tickets that reference it
  block the delete: in-flight (pending/queued/processing) unconditionally,
  completed/failed unless `?force=true`. Shared features (claimed by another
  reference) are never deleted. Lets operators remove test references accumulated
  by repeated re-loads (#29)
- Work-ticket step logs are now retrievable without sudo. New
  `GET /work-ticket/{idx}/step/{step_index}/logs` returns a bounded
  stdout/stderr tail (defaulting to the latest attempt; `attempt` and
  `tail_lines` query params override), and `qiita ticket logs <idx>
  --step-index N [--attempt N] [--tail-lines N]` surfaces it from the CLI. The
  control plane reads the logs straight off shared scratch
  (`PATH_SCRATCH/ticket/...`) via its existing `qiita-pipeline` group access,
  so an operator can diagnose an OOM / bad input / contract violation without a
  host shell. Auth mirrors `GET /work-ticket/{idx}` (originator or
  wet_lab_admin+; non-owners get an enumeration-safe 404) (#104)
- Per-run memory override for workflow steps. `qiita reference load` and `qiita
  ticket submit` gain `--mem-gb N`, carried as an optional `resource_override`
  on `POST /work-ticket`: at dispatch the runner raises each SLURM step's memory
  floor to `max(step baseline, N)` (raise-only — never lowers a step the YAML
  sized higher), still clamped to the action's mem ceiling. Lets an operator
  load a genome-scale host reference (e.g. a human genome that OOMs the
  conservative 8 GB default) without editing the workflow YAML. Gated to
  wet_lab_admin / system_admin (a regular user who can otherwise submit the
  workflow still gets 403); an override above the ceiling is a clean 422.
  Persisted on `qiita.work_ticket` so a control-plane restart re-attaches
  in-flight work with the same override (#102)
- `qiita submit-host-filter-pool` — a bundled operator gesture that fans out
  one host-filtered `fastq-to-parquet/1.1.0` work-ticket per sample in a
  completed bcl-convert pool. It pre-checks the `--host-reference-idx` is ACTIVE
  and carries both a rype and a minimap2 index, lists the pool's samples via a
  new pool-scoped route, resolves each sample's R1/R2 FASTQ under `--convert-dir`
  by the `sequenced_pool_item_id` prefix (recursive, single-lane), and submits
  every ticket with `host_filter_enabled` — resolving all samples before any POST
  so a misconfiguration aborts with zero side effects. Backed by a new
  `GET /sequencing-run/{run}/sequenced-pool/{pool}/sequenced-sample/list`
  returning `(sequenced_sample_idx, prep_sample_idx, sequenced_pool_item_id)`
  per active sample (#99)
- `compute-readiness` now probes that the deploy-staged miint build registers the
  short-read host-filter functions `save_minimap2_index` (the
  `build_minimap2_index` step) and `align_minimap2` (the `host_filter` step), via
  a `miint-host-filter-fns` check against `duckdb_functions()`. These were the
  newest miint additions with no probe, so a v1.5.3 mirror build missing either
  was only caught at the first `host-reference-add` run; it now surfaces at
  deploy alongside `miint-read-fastx` / `miint-sequence-split` (#101)
- The public landing page footer now shows the deployed commit's short git
  SHA next to the package version (e.g. `v2026.3.0 (a28c96e)`), linked to its
  GitHub commit. The SHA is captured at deploy (`deploy/local-deploy.sh` from
  the git clone, or `GITHUB_SHA` on the CI path) and passed to the control
  plane via an optional `BUILD_SHA` env var written into a deploy-owned
  `build.env`; a from-source / first-deploy boot leaves it unset and the
  footer renders the version alone (#94)
- Short-read host filtering. A new `host_filter` native job depletes host reads
  from `reads.parquet` in two stages — rype `rype_classify` against a host's
  POSITIVE `.ryxdi` (host = any match, not rype's `negative_index`), then
  minimap2 `align_minimap2` (`preset 'sr'`) on the survivors — dropping any read
  flagged by either tool. Paired-end is handled natively: a read pair's R1/R2
  ride one `sequence_idx` as `(sequence1, sequence2)` straight into the tools
  (`rype_classify` reads both mates; `align_minimap2` aligns the pair in PE
  mode), so either mate matching drops the whole pair without flattening. It's a
  gated, optional step in a new `fastq-to-parquet/1.1.0` workflow
  (`host_filter_enabled` + `host_reference_idx` context; pass-through when
  disabled; `1.0.0` is kept and the submit route picks the version) (#89)
- `build_minimap2_index` native job + a `minimap2` value for
  `reference_index.index_type` (migration
  `20260612000000_reference_index_minimap2_type`), so a host reference now
  carries BOTH a rype `.ryxdi` and a minimap2 `.mmi`. The `host-reference-add` /
  `local-host-reference-add` workflows gain the minimap2 build + a second
  `register-index`. Like `build_rype_index`, it consumes the feature-keyed
  `reference_sequence_chunks` (reassembling whole contigs via `string_agg`), so
  the minimap2 index is built from the same data-plane bytes as everything else
  — no raw-FASTA side channel (#89)
- `make verify-deploy` (`deploy/verify.sh`) — one command runs the generic
  post-deploy checks (health aggregate, `qiita.action` list, and
  compute-readiness) each with the correct service account/env baked in, so the
  compute-readiness run-as line is no longer hand-copied into every deploy
  (#72)
- `make preflight` (`deploy/preflight.sh`) — read-only config/secret consistency
  check (PATH_SCRATCH byte-identity across env files, HMAC CP==DP, token-file
  perms, connection-string shape) that prints non-secret SHA-256 fingerprints;
  catches the silent runtime-failure class before a restart (#72)
- `make redeploy` (`deploy/redeploy.sh`) — guided incremental redeploy that
  codifies redeploy.md's skeleton (pull → preflight → migration gate →
  local-deploy.sh → stage → verify); migrations stay out-of-band (verify and
  refuse, never auto-apply) (#72)
- New nullable `bioproject_accession` column on the study table (unique
  when present), for NCBI/ENA BioProject tracking (#87)
- Exposed study `bioproject_accession` through create, get, and patch: the
  REST request/response field and the `qiita study create`/`patch`
  `--bioproject-accession` flag (#91)
- The study and biosample lookup-by-accession endpoints accept an
  `accession_field` selector so a caller can resolve by either accession
  column (study: `ena_study_accession` or `bioproject_accession`; biosample:
  `biosample_accession` or `ena_sample_accession`) (#91)

### Changed

- `stage_local_fasta` now ingests the whole manifest in a single
  `read_fastx(VARCHAR[])` scan and streams read → `sequence_split` → Parquet
  without ever materialising sequences in a temp table. The previous per-file
  `INSERT … SELECT` staged every genome's bytes into a `reads` table and spilled
  hard when loading hundreds of human genomes; sanity checks (empty-body,
  duplicate read_id) now run over a small `(read_id, length, filepath)` table
  instead. The duplicate-read_id error names the offending files;
  read_id stays globally unique (#128)
- Raised compute resources for genome-scale reference loads in the
  `local-reference-add` and `local-host-reference-add` workflows:
  `stage_local_fasta` and `hash_sequences` to cpu=8/mem_gb=32,
  `build_rype_index` to cpu=8 and `build_minimap2_index` to mem_gb=32, and step
  walltimes to PT24H under a PT48H `action_ceiling`. The matching DuckDB
  `_DUCKDB_THREADS` bumps (`hash_sequences`, `build_rype_index` → 8) keep the
  caps in lockstep so the extra cores are actually used (`build_minimap2_index`
  stays at 4 — minimap2 index build is single-threaded). The orchestrator's
  SLURM poll-loop timeout default rises 24h → 48h to allow the longer walltimes
  (override via `SLURM_JOB_TIMEOUT_SECONDS`) (#128)
- `make redeploy` no longer prompts to do work it has already proven is needed.
  The SLURM native-venv refresh now runs automatically (no confirm) when the
  native checkout is the same clone redeploy just pulled — the prompt remains
  only for a separate checkout, where redeploy is about to mutate a tree it
  didn't pull. miint staging is now gated like the native-venv refresh: a new
  `stage-miint --check` probe (`qiita_compute_orchestrator.miint_staging`) skips
  staging when the staged build still matches the mirror and stages
  automatically otherwise (not staged, DuckDB-version/platform change, or a
  mirror build bump detected via an HTTP `HEAD` on the extension URL + a
  fingerprint marker written at stage time). `FORCE_STAGE_MIINT=1` stages
  unconditionally; `SKIP_STAGE_MIINT=1` still skips entirely. Removes the two
  recurring deploy prompts that fired every run regardless of need (#127)
- The orchestrator's **derived-storage** path layout now has a single owner. The
  `{PATH_DERIVED}/references/{idx}/...` convention for the persistent host-filter
  indexes was reconstructed by hand in three places (`build_rype_index`,
  `build_minimap2_index`, and the `DELETE /reference-artifact/{idx}` purge
  endpoint); it now lives in one module, `qiita_compute_orchestrator/derived_store.py`
  (`reference_derived_dir` / `rype_index_path` / `minimap2_index_path`), which all
  three call. No behavior change — the paths are byte-identical — this names
  derived storage as an explicit orchestrator concern (distinct from the data
  plane's persistent DuckLake data and the ephemeral per-attempt workspace) and
  gives the in-tree-vs-out-of-tree boundary one home. `docs/architecture.md` gains
  the matching note: a derived/persistent artifact is never a step output (it
  can't resolve under `$QIITA_OUTPUT_PATH`), so its location travels in an in-tree
  meta JSON. Also corrects a docs/test drift — the minimap2 `reference_index.params`
  shape is `{preset, source_chunks, num_subjects}` (what `build_minimap2_index`
  actually writes), not the stale `{preset, source_files}` (#119)
- `deploy/redeploy.sh` (`make redeploy`) now **only stops to ask when there is
  real work or a real decision** — it no longer pauses on no-ops. The buckets
  1 & 2 acknowledgement (env vars + one-time host setup) is skipped when both
  are empty in `DEPLOY_CHECKLIST.md` (nothing to apply out-of-band → nothing to
  confirm), via a new unit-tested `qiita_buckets_12` helper in
  `deploy/_common.sh`. The SLURM native-venv refresh is skipped entirely — no
  prompt, no `uv sync` — when it can prove the venv is already current (the
  native checkout is the clone this run just pulled, that pull changed neither
  `qiita-common` nor `qiita-compute-orchestrator`, and the existing venv still
  imports); any doubt (a separate checkout, an actual code change, an unreadable
  checklist, or a failing import probe) falls back to prompting and refreshing
  exactly as before, so the optimisation never skips work a change requires.
  `FORCE_NATIVE_REFRESH=1` overrides the skip for the one case it can't see — a
  re-run after a deploy that died mid-`uv sync`. Both new decisions delegate to
  pure, unit-tested helpers in `deploy/_common.sh` (`qiita_buckets_12` and
  `qiita_paths_touch_native`), matching the existing
  `qiita_native_checkout_from_python` pattern. The migration gate, `RUN_MIGRATE`
  confirm, and miint-stage prompt are unchanged (#113)
- `deploy/redeploy.sh` (`make redeploy`) now **runs** the SLURM native-venv
  refresh in step 5 instead of only printing a reminder. It derives the
  `qiita-compute-orchestrator` checkout from `SLURM_NATIVE_PYTHON`, runs `uv sync
  --reinstall-package qiita-common` there as the checkout owner (`qiita`, never
  root — a root-owned `.venv` is the #80 footgun), and fails loud if the synced
  venv can't import `qiita_common` / `qiita_compute_orchestrator.jobs`. It skips
  cleanly when `SLURM_NATIVE_PYTHON` is unset (local backend) and aborts rather
  than `uv sync` a wrong path; `SKIP_NATIVE_REFRESH=1` opts out. This closes the
  recurring footgun where a deploy that changed `qiita-common` /
  `qiita-compute-orchestrator` left native jobs importing stale code unless the
  operator remembered to refresh by hand. The derivation lives in a pure
  `qiita_native_checkout_from_python` helper in `deploy/_common.sh` (unit-tested)
  (#106)
- `deploy/redeploy.sh` (`make redeploy`) is now an all-in-one **root-run**
  orchestrator: run it as `sudo make redeploy` from the admin account and it
  `sudo -u`'s into the operator (`qiita`) for pull/migrate and into the service
  accounts (`qiita-api`/`qiita-orch`) for the verify checks. This fixes the
  prior "run as the operator, elevate via sudo" model, which could not work on
  the documented default where the operator account has no sudo. It also reads
  `DATABASE_URL` from `control-plane.env` itself (handing it to the operator's
  `make migrate`), so the operator's shell no longer needs it and the #72 ACL
  is no longer required for a normal redeploy. Migrations stay out-of-band
  (`RUN_MIGRATE=1` opts in after a typed confirm). `deploy/verify.sh` also gains
  `QIITA_API_USER` / `QIITA_ORCH_USER` overrides (defaults unchanged) for
  consistency with the new `QIITA_USER` knob. The deploy scripts'
  copy-pasted helpers (root-gate, env-file reader, operator/clone resolution,
  pass/fail/skip reporters, `/etc/qiita/*.env` path + service-account constants)
  are consolidated into `deploy/_common.sh` — single-source, no behavior change.
  Docs updated in `redeploy.md` / `first-deploy.md` / `CLAUDE.md` (#101)
- The `collection_date` global biosample field is now a `text` field instead of
  a formal `date`, so it can hold partial dates such as a bare year (`2025`)
  (migration `20260616000000_collection_date_text`) (#98)
- Pruned the seeded `prep_sample_global_field` registry to the two fields
  actually in use: removed the seven fields (`alias`,
  `library_name`, `library_strategy`, `library_source`, `library_selection`,
  `library_layout`, `library_construction_protocol`) , all of which but alias should come from sequenced_pool, and made the retained
  `title` and `design_description` optional (migration
  `20260616000002_prune_prep_sample_global_fields`) (#98)
- The `qiita.sequenced_pool.idx` identity sequence now starts at 25000,
  reserving `[1, 25000)` for legacy-Qiita import rows (matching the existing
  `study` / `prep_sample` reservation) (migration
  `20260616000001_sequenced_pool_idx_bump`) (#98)
- Reference index artifacts now live under a new orchestrator `PATH_DERIVED`
  root (`{PATH_DERIVED}/references/{idx}/{rype,minimap2}/…`), relocated from
  `PATH_SCRATCH`. `build_rype_index` / `build_minimap2_index` read
  `Settings.path_derived` and the SLURM backend propagates `PATH_DERIVED` into
  the job env (no host references exist in prod, so no migration of existing
  artifacts) (#89)
- The runner's `register-index` action reads its YAML-declared input
  (`entry.inputs[0]`) instead of a hardcoded `rype_index_meta`, so one workflow
  can register multiple index types (rype + minimap2) from their own metas (#89)
- `ActionDefinition` now rejects duplicate `step:` entry names within an action
  at load time — SLURM job naming and in-flight job adoption (`_find_job_by_name`)
  key on the entry name, so two same-named steps would collide silently.
  `action:` entries run in-process (keyed on step index) and may still repeat,
  e.g. the two `register-index` actions in the host-reference workflows (#89)
- The compute service-account name is documented as **site-chosen** (`compute`
  on the live deploy) across the provisioning/rotation runbooks, `docs/auth.md`,
  `first-deploy.md`, `CLAUDE.md`, and the orchestrator `config.py` comments —
  the docs no longer imply a fixed `compute-worker` name that drifts from the
  live SA (#72)
- Operators now get a narrow POSIX ACL read on the three `/etc/qiita/*.env`
  files (granted to the existing operator account, e.g. `u:qiita:r`; not the
  bearer tokens, not lake data), so `make migrate` can source `DATABASE_URL` and
  config consistency is verifiable without sudo or hand-copied secrets. Operators
  still join no service group, preserving DuckLake/scratch isolation. Documented
  in `first-deploy.md` §0.1 + the deploy checklist (#72)
- The study lookup-by-accession default is now `bioproject_accession`
  (was `ena_study_accession`): callers omitting `accession_field` resolve
  against the BioProject column. This aligns the `qiita submit-bcl-convert`
  preflight, whose project accessions are BioProject identifiers, with the
  column it actually matches (#91)
- miint is no longer installed lazily on every compute run. The deploy stages
  the extension **once** into a shared `MIINT_EXTENSION_DIRECTORY`
  (`scripts/stage-miint-extension.sh` →
  `python -m qiita_compute_orchestrator.cli.stage_miint`), and the CO service,
  all five native jobs, and the compute-readiness probe only `LOAD` it
  (`miint.open_miint_conn`) — no per-job download, no compute-node mirror
  dependency, no writable-`$HOME` requirement (the latter was the deploy
  footgun: a slurmrestd job has no login `$HOME`, so `FORCE INSTALL` couldn't
  write `~/.duckdb`). New orchestrator env var `MIINT_EXTENSION_DIRECTORY` (see
  `DEPLOY_CHECKLIST.md`). The client-side `qiita reference load` CLI keeps an
  install but plain + cached (was `FORCE INSTALL`, which re-downloaded every
  invocation). `miint_install_sql()` is now plain `INSTALL` with opt-in
  `force=` for deploy staging; new `miint_load_sql()` / `miint_job_env()`
  single-source the load + remote-job-env contract (#90)
- The compute-readiness probe now reports *why* a miint check failed (the
  captured DuckDB/Python error) instead of a swallowed bare `=fail`, and LOADs
  the staged build exactly like the native jobs — a broken miint deploy is now
  diagnosable from the probe output alone (#90)
- The data-plane `miint_extension_smoke` test installs from the team mirror
  (honoring `MIINT_EXTENSION_REPO` / `MIINT_EXTENSION_DIRECTORY`) instead of the
  hardcoded `community` channel, so it verifies the same build the rest of the
  system runs (#90)
- The bcl-convert flow now derives the instrument run ID and model from the
  run folder's top-level `RunInfo.xml` (`Run@Id` plus the `Instrument` serial number
  resolved against the vendored prefix table) instead of parsing the folder
  basename, which operators rename. Both the `qiita submit-bcl-convert` CLI
  and the orchestrator's `bcl_convert_prep` step fail fast on a missing or
  malformed `RunInfo.xml` (#88)
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

### Fixed

- The bcl-convert step no longer fails with `chmod: changing permissions of
  '.../bcl_convert/attempt-0/output': Operation not permitted` after bcl-convert
  and `manifest_writer.py` both succeed. The entrypoint's final mode-fixing
  `find … -exec chmod` walked `$QIITA_OUTPUT_PATH` itself — created on the host
  by the orchestrator (owned by the orchestrator user, not the in-container
  user) — so chmod returned EPERM and `set -e` failed the otherwise-successful
  job. Both finds now carry `-mindepth 1`, re-moding only what the container
  created inside `output/`. Separately, `scripts/build-sif.sh` gains a `FORCE=1`
  opt-out of its version-only idempotency check, so an image-baked change to
  `entrypoint.sh`/`manifest_writer.py`/`Apptainer.def` that does not bump the
  vendored binary version can still force a rebuild (without it the fix would
  never reach the host). Surfaced running bcl_convert end-to-end (SLURM job
  156785) (#130)
- The bcl-convert step no longer fails with `TypeError: 'type' object is not
  subscriptable` after bcl-convert itself succeeds. Its container is built
  `From: oraclelinux:8`, whose default `python3` is 3.6 — predating PEP 585
  builtin generics (`list[str]`), which the workflow-agnostic
  `workflows/_shared/manifest_writer.py` uses and evaluates at import. The
  container now installs the appstream `python3.11` and the entrypoint invokes
  it explicitly; the `%test` block additionally `exec_module`s
  `manifest_writer.py` under the shipped interpreter so a too-old Python fails
  the SIF build instead of a live SLURM job. Surfaced running the first
  container step (bcl-convert) end-to-end (SLURM job 153623) (#126)
- Container workflow steps no longer exit 64 (`QIITA_INPUT_PATH not set`) once
  they reach the entrypoint. `apptainer exec --containall` contains the
  environment as well as the filesystem, so the `QIITA_*` vars set in the SLURM
  job env never crossed into the container; the entrypoint reads
  `$QIITA_INPUT_PATH/params.json` and writes to `$QIITA_OUTPUT_PATH`, so it
  bailed immediately. The orchestrator now forwards the container-contract vars
  (`QIITA_INPUT_PATH`, `QIITA_OUTPUT_PATH`, `QIITA_WORK_TICKET_IDX`) via
  apptainer `--env`; native-only env (CO→CP token, miint dirs) is deliberately
  not exposed to containers. Surfaced running the first container step
  (bcl-convert) end-to-end, after #116 fixed the upstream container-creation
  failure (#122)
- Host-reference index builds (`build_rype_index`, `build_minimap2_index`) no
  longer fail at the post-success manifest step on a real SLURM run. Both jobs
  write a *persistent* index under `PATH_DERIVED` (outside the per-attempt
  workspace, so it outlives the ticket) and also declared that out-of-tree path
  as a step output (`rype_index_path` / `minimap2_index_path`). The native-step
  launcher and the verifier both require every declared output to resolve under
  `$QIITA_OUTPUT_PATH`, so the launcher's `relative_to` blew up *after* the job
  succeeded — a `CONTRACT_VIOLATION` (`"... is not in the subpath of ..."`). The
  binding was vestigial: nothing consumes it — `register-index` reads the index
  location from the in-tree meta JSON's `fs_path`. Both jobs now return only the
  meta output, and the workflow YAMLs declare only `*_index_meta`. The local
  backend never writes/verifies a manifest, so this only surfaced under SLURM;
  the launcher now also rejects an out-of-tree output with an actionable message
  (naming the output and the rule) instead of leaking the opaque `relative_to`
  error, covered by a new launcher unit test (#118)
- Container workflow steps no longer fail at apptainer container creation under
  the locked-down SLURM job account. The orchestrator now passes
  `--home <workspace>` to `apptainer exec --containall` for container steps:
  `--containall` derives the container's home mount target from the job user's
  passwd entry, and `qiita-job` is a service account whose passwd home is
  `/dev/null`, which collided with the device of the same name in the container
  layout (`failed to add /dev/null as session directory`). Pinning the home mount
  to the per-ticket workspace (matching the `HOME` env already set for native
  steps) resolves it; native steps are unaffected (they run no container) (#116)
- `make redeploy`'s SLURM native-venv refresh no longer fails with `uv: command
  not found`. The refresh ran `sudo -u qiita bash -lc '... uv sync ...'` with a
  bare `uv`, trusting the login PATH — but `uv` lives in `/usr/local/bin`, which
  is absent from sudo's `secure_path` and need not be on qiita's login PATH. It
  now invokes uv by absolute path (`$UV=/usr/local/bin/uv`), matching the
  long-standing pattern in `activate.sh`; the manual-fallback hint and the
  `SKIP_NATIVE_REFRESH` echo use the absolute path too (#114)
- Retrying a `failed` reference load no longer dies instantly. A fresh
  `POST /work-ticket` bound to an existing reference (the `qiita reference load
  --reference-idx N` retry) now resets a `failed` reference back to `pending`
  before dispatch — mirroring the `/run` redrive path — so the run's first
  status PATCH is the legal `pending → hashing` instead of the illegal
  `failed → hashing` that killed the ticket at the first step. Only a `failed`
  reference is touched (any other state is a no-op, an unrewindable in-progress
  state is logged at WARNING); the shared reset helper is reused by both the
  submit and redrive paths (#112)
- `build_rype_index` no longer OOMs DuckDB on a genome-scale host reference.
  The step split the SLURM cgroup DuckDB(4 GB, capped) / rype(elastic) on the
  assumption DuckDB "never needs more" than the 4 GB off-SLURM fallback — but
  feeding the full chunk scan to rype's read needs far more, so a human host
  reference (T2T-CHM13) OOMed DuckDB at ~3.7 GB while reading `rype_chunk_input`,
  before rype's `max_memory` was ever exercised (and `--mem-gb` could not raise
  it — it only grew rype's share). DuckDB's under-SLURM cap is now
  `_DUCKDB_MEMORY_CAP_GB` (16 GB) instead of the 4 GB fallback; rype stays the
  elastic consumer (its share still grows with the allocation). The 16 GB cap is
  a heuristic and should be tuned against a real genome-scale MaxRSS (#111)
- The `--mem-gb` per-run override (#102) now actually reaches the DuckDB-backed
  reference-load steps, instead of being silently clamped by a hardcoded
  per-job DuckDB `memory_limit`. Each native job pinned its DuckDB cap to a
  literal (`stage_local_fasta` 7 GB, `hash_sequences` 24 GB, `load` 31 GB,
  `build_rype_index` rype 24 GB, `build_minimap2_index` 8 GB) sized to the YAML
  baseline, so raising the SLURM allocation grew the cgroup but DuckDB still
  OOM'd at the literal — a genome-scale human host reference died in
  `stage_local_fasta` at ~6.5 GiB even under `--mem-gb 48`. The jobs now size
  DuckDB (and rype's `max_memory` / minimap2's reserve) from the real cgroup via
  `SLURM_MEM_PER_NODE`, falling back to the literal off SLURM (local backend /
  tests unchanged). This also resolves the latent `hash_sequences` case where
  its 24 GB literal exceeded its own 8 GB YAML allocation. Scoped to the
  reference-add workflow: `fastq_to_parquet` (the read-ingest path) is a
  deliberate follow-up, and `host_filter` is intentionally left as-is — its
  genome-scale memory is the rype/minimap2 indexes held out of DuckDB's heap,
  which already grow into the cgroup a `--mem-gb` raise provides, so converting
  its DuckDB cap would starve them (#107)
- OOM-killed workflow steps are no longer mis-reported as a bare
  `NonZeroExitCode`. A cgroup step-level `oom_kill` surfaces to slurmrestd only
  as a coarse job-level `FAILED`/`exit_code=1`, so the orchestrator's
  `OUT_OF_MEMORY → OOM_KILLED` classifier never fired and the launcher's
  structured stderr line was never written (the process was killed first). The
  SLURM backend now scans the job's stderr on an otherwise-unclassified
  terminal failure: an OOM signature upgrades the classification to the
  (retriable) `OOM_KILLED`, and a short stderr tail is folded into
  `failure_reason` for every state-based failure — so `qiita ticket status`
  reports a memory-related reason directly instead of requiring a root shell to
  read the SLURM log. A specific infra kind (NODE_FAIL/TIMEOUT/PREEMPTED) is
  never downgraded (#104)
- `docs/runbooks/redeploy.md` §7 no longer tells the operator to run
  `compute-readiness` as `qiita-api` sourcing `control-plane.env` (which fails:
  it needs the `qiita-orch` account + `compute-orchestrator.env` + the 0400
  `co-to-cp.token`) — the recurring deploy defect from #72. It now points at
  `make verify-deploy` and documents the correct `qiita-orch` form (#72)
- `qiita-admin compute-readiness` / `python -m …compute_readiness` now fail
  loudly with the correct `sudo -u qiita-orch …` invocation when misinvoked,
  instead of silently exiting 0: a non-slurm backend on a real orchestrator host
  (env file present) is a `fail` row, and a present-but-unreadable
  `co-to-cp.token` raises an actionable error naming the file + ownership (#72)
- `qiita submit-bcl-convert` now opens the preflight blob via
  run-preflight's `open_db_file` instead of a hand-rolled read-only
  `sqlite3.connect`, opening it the way the library expects (#92)
- `docs/runbooks/first-deploy.md` now documents the `PATH_DERIVED/references/`
  host-reference index directory in the filesystem-bootstrap table (owner
  `qiita-orch`, group `qiita-pipeline`, mode `2770`, setgid). The host-reference
  index build and its `host_filter` consumer both run as `qiita-job`, which
  `mkdir`s `{idx}/{rype,minimap2}/` at runtime; without the group-writable
  setgid leaf the first `host-reference-add` build fails Permission Denied on
  the `root:root 0755` base root. Previously only `…/images` was listed (#100)

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
  that builds a rype `.ryxdi` host-filter index (`build_rype_index` native
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
  environment, so native steps that derive a path from the shared scratch base
  (the per-ticket workspace root) resolve the real value instead of the
  `$TMPDIR/qiita` default (#70). (Persistent index artifacts later moved off
  `PATH_SCRATCH` to `PATH_DERIVED` — see the #89 Changed entry above.)
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
