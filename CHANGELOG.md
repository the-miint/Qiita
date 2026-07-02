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

- **Email notification on work-ticket terminal transitions.** When a work
  ticket reaches a terminal state (`completed` / `no_data` / `permanent`-failed),
  the control plane emails the originator. A new in-process asyncio sweeper
  coalesces a user's finished tickets into one digest via a trailing-debounce
  with a max-wait cap, gated on `qiita.user.receive_processing_emails`. Sends
  go through a pluggable transport (`aiosmtplib` SMTP relay when `SMTP_HOST` is
  set, else a no-op) and every send writes a `qiita.email_receipt` audit row.
  The digest footer carries the configured `CONTACT_EMAIL` (also set as the
  message `Reply-To`) as a contact line. Retriable failures are withheld until
  their true outcome, and a `/run` redrive re-arms notification. New `SMTP_*` /
  `NOTIFY_*` settings (all defaulted); migrations add
  `work_ticket.notified_at` / `notify_attempts`, the
  `qiita.email_receipt` table, and a partial owed-set index. (#238)
- **Optional `plan()` phase for native jobs — input-driven resource sizing.** A
  native job module may now export an optional `plan(inputs) -> JobPlan` (a
  third symbol alongside `Inputs` + `execute`; absent → today's behavior). The
  control-plane runner fetches the hint ONCE per native step before its retry
  loop via a new backend-agnostic `POST /step/plan` route
  (`ComputeBackendClient.plan_step`), and composes it in
  `_resolve_baseline_for_step` as a raise-NEVER **down-size**: a hint lowers a
  step below its YAML baseline, is applied before the raise-only escalation
  floors (so an OOM/TIMEOUT retry always restores at least the baseline), and is
  fully advisory (any failure — unreachable CO, broken module, malformed
  response — degrades to the baseline). First consumer: the `qc` step sizes its
  **walltime** (not memory — qc streams, so peak RAM is ~flat in read count)
  from the input read count, tightening SLURM backfill for small inputs. New
  `JobPlan`/`JobResourcePlan` contract types, `run_native_job_plan` dispatcher,
  `job_resource_plan` helpers, `StepPlanRequest`/`StepPlanResponse` wire models,
  and `PATH_/URL_STEP_PLAN` constants; `docs/writing-a-job.md` documents the full
  native-job contract. No new env var, scope, migration, or operator action. (#237)
- **CLI discovery commands for prep-protocol and host-reference idxes.** Two new
  read-only subcommands so operators stop hand-querying Postgres for the idxes
  `submit-bcl-convert` / `submit-host-filter-pool` need. `qiita prep-protocol
  list` (`--all` to include retired) is backed by a new anonymous-OK `GET
  /prep-protocol` route (same posture as `GET /reference`; retired protocols
  excluded by default). `qiita reference list` (`--host` / `--active` /
  `--index-type {rype,minimap2}`) reuses the existing `GET /reference` plus a
  per-row `GET /reference/{idx}/index`, enriching each reference with its built
  `index_types` and filtering by `--index-type` so the result is exactly the set
  `submit-host-filter-pool`'s `_assert_host_reference_ready` gate accepts. New
  `PrepProtocolResponse` model + `PATH_/URL_PREP_PROTOCOL` constants; anonymous-OK
  so no new scope, migration, or operator action. (#232)
- **Walltime escalation on TIMEOUT retry**, mirroring the existing OOM→memory
  growth. When a step's SLURM job exceeds its walltime (`TIMEOUT`, a retriable
  kind), the runner now grows that step's walltime floor ×2 on each retry,
  clamped to `action_ceiling.walltime`, instead of re-running every attempt at
  the same limit (which timed out identically). Process-local like the memory
  floor: a CP restart re-attaches to the in-flight job and re-escalates from the
  YAML baseline. (#216)
- Pool completion now reports **end-to-end processing**, not just host-masking.
  `GET /sequencing-run/{R}/sequenced-pool/{P}/completion` (`qiita pool-completion`)
  gains `demux_state` (the pool-scoped bcl-convert stage: completed / in_flight /
  no_data / failed / not_submitted) and a computed `fully_processed` (demux
  completed AND every sample's read-mask `complete`) — the single "this pool is
  done and clean" signal. Also corrects the route/repo/`api_paths`/CLI docstrings,
  which described the rollup as "fastq-to-parquet / prep-generation" though it has
  measured **read-mask** (host-masking) since the read-storage/masking split. No
  new route/migration; the `PoolCompletionStatus` response gains two fields. (#218)
- Admin per-pool **masked-read export**: pull a sequenced_pool's masked sequence
  data to local disk, per sample, as parquet or fastq. New `qiita-admin
  masked-read-export --sequenced-pool-idx P --mask-idx M [--format parquet|fastq]
  --output-dir DIR --data-plane-url U` CLI, backed by two routes — `GET
  /api/v1/admin/sequenced-pool/{idx}/masked-read-export?mask_idx=` (roster
  manifest) and `POST /api/v1/admin/masked-read-export/ticket` (per-sample DoGet
  ticket). The CLI streams each sample's `read_masked` rows straight from the data
  plane into a local DuckDB+miint `COPY` (bounded memory, no server-side scratch),
  writing `<biosample_accession>.<run>.<pool>.<prep>` files atomically at mode
  0600: `.parquet`, or `.fastq` (single-end) / `.R1.fastq`+`.R2.fastq` (paired,
  via miint's `{ORIENTATION}`; pairing is detected by peeking the first streamed
  batch, so the stream is never materialized). Dual-gated by `system_admin` + a new
  `admin:masked_read_export` scope. Privacy invariant unchanged: the `read_masked`
  view (`WHERE reason='pass'`) is the only Flight-reachable read surface, so
  host/QC reads are never exported. The data-plane DoGet now streams its result
  set instead of buffering it whole. (#192)
- `qiita run-preflight update-lane` — wet_lab_admin+ correction of a stored run
  preflight's lane assignment. New `POST /api/v1/sequencing-run/{R}/sequenced-pool/{P}/preflight/update-lane`
  route loads the pool's run-preflight SQLite blob, applies `run_preflight.update_lane`
  (bulk `from_lane` → `to_lane` reassignment on the illumina/tellseq sample table,
  one `change_log` audit row per reassigned sample), and writes the blob back — all
  server-side, so the SA-only "humans can't read the preflight" boundary is
  preserved. Gated on the run not having been processed: an in-flight or completed
  work ticket on the pool or its samples → 409 (a failed or unsubmitted run stays
  editable, since a stale lane may be why it failed); update_lane's
  uniformity/collision `ValueError` → 422. Reuses the existing pinned `run-preflight`
  dependency (no version bump). (#190)
- New `GET /api/v1/admin/study/{study_idx}/owner-biosample-id` route + `qiita-admin
  owner-biosample-id` CLI: a system_admin-only re-identification export mapping a
  study's `biosample_idx` + `biosample_accession` back to the owner-submitted
  original sample name (the PII-pinned `biosample_metadata` value flagged
  `is_owner_biosample_id`, masked on every other read path). With
  `?sequenced_pool_idx=` it restricts to that pool's `sequenced_sample`s in the study
  and adds `prep_sample_idx` + ENA experiment/run accessions. Dual-gated by
  `system_admin` + a new `admin:biosample_owner_id_read` scope; the CLI writes a TSV
  to `--output` (mode 0600, never stdout, so the names stay off the terminal). (#188)
- An `export_read` data-plane DoAction that re-materializes one prep_sample's
  reads from the DuckLake `read` table into a per-ticket `reads.parquet` on shared
  scratch (DuckDB `COPY` run on the blocking pool, written to a `.partial` sibling
  then published atomically; the destination is validated lexically and via a
  symlink-resolving containment check under the scratch root; row count is read
  back from the written file). The control plane signs the HMAC action token but
  the data plane writes the file, so the bulk (human-containing) read bytes never
  transit the control plane. Raw `read` remains absent from the Flight DoGet
  `ALLOWED_TABLES` — it is reachable only via this admin-gated write path. (#187)
- A `runner._do_action_export_read` control-plane client for the above. (#187)
- A `delete_mask` primitive for removing a registered read mask. New
  `mask_definition:delete` scope (system_admin via the role ceiling),
  `DELETE /api/v1/mask-definition/{mask_idx}` route (lake-first: a new
  `delete_mask` data-plane DoAction logically `DELETE`s the mask's rows from the
  DuckLake `read_mask` table, then the `mask_definition` Postgres row is removed),
  and a `delete_mask_data` CP client. Idempotent (0 rows deleted is success); no
  raw parquet unlink (mirrors `delete_reference`); 404 on an absent mask. Surfaced
  as `qiita-admin mask delete <mask_idx>`. (#181)
- `qiita-admin mask purge-failed --action {read-mask,fastq-to-parquet,all}` — bulk
  recovery tooling that selects FAILED read-mask / fastq-to-parquet tickets stranded
  by the move-then-read ordering bug, deletes each ticket's orphaned mask, and
  resubmits it clean on the reordered workflow (so the re-run mints a fresh
  `mask_idx` rather than appending a duplicate to the append-only `read_mask`
  table). Dry-run by default; `--execute` to act, `--with-tickets` to also delete
  the FAILED ticket rows, `--limit` / `--rate` / `--wait` to bound and throttle the
  batch. A shared-mask guard refuses to delete a mask referenced by any non-FAILED
  ticket, and a pre-flight refuses to run if the `work_ticket.mask_idx` backfill is
  incomplete. (#181)
- `qiita-admin work-ticket backfill-mask-idx [--apply]` — one-time idempotent
  backfill that populates the new `work_ticket.mask_idx` column for existing
  read-mask / fastq-to-parquet tickets by recomputing the mask params hash and
  looking it up in `mask_definition` (no new mask minted). For adapter-bearing
  tickets it re-materializes the canonical adapter set via DoGet to reproduce the
  hash, so it needs `DATABASE_URL`, `QIITA_DEFAULT_ADAPTER_REFERENCE_IDX`,
  `HMAC_SECRET_KEY`, and a reachable `DATA_PLANE_URL`; the dry-run reports
  `populated` so the operator can confirm `populated > 0` before `--apply`. (#181)
- New nullable `work_ticket.mask_idx` column (FK → `mask_definition`, ON DELETE SET
  NULL, partial index) recording the mask a read-mask / fastq-to-parquet ticket
  produced, for durable traceability and a cheap shared-mask guard. The runner
  writes it at mint time; existing rows are populated by the
  `backfill-mask-idx` command above (migration
  `20260624110000_work_ticket_mask_idx.sql`; additive, backfill-free at migrate
  time — existing rows read NULL). (#181)
- A first-class terminal `no_data` outcome for empty FASTQ wells, distinct from
  failure, so a real plate full of blank / no-template-control / failed-yield
  wells can still reach a "done" signal. New `WorkTicketState.NO_DATA` enum value
  (additive `ALTER TYPE ... ADD VALUE 'no_data'` migration + the Python twin;
  `WorkTicketState` already in `ENUM_PAIRS`). `fastq_to_parquet` on empty input
  now raises a new typed terminal `StepNoData` signal (in
  `qiita-common/backend_failure.py`, parallel to `BackendFailure` — its own wire
  body + `X-Qiita-Step-No-Data` header round-tripping the `/step/*` boundary, NOT
  a `FailureKind`) instead of minting a sequence range or writing `read.parquet`;
  it mints no identifiers and writes no output. The dispatcher re-raises
  `StepNoData` unchanged (above the generic `ValueError → BAD_INPUT` arm), both
  backends round-trip it, and the runner transitions the ticket `PROCESSING →
  NO_DATA` with NULL failure columns — no `failure_status` PATCH, no
  `success_status` advance, transient markers cleared. `NO_DATA` is terminal for
  resubmission (DELETE-gated) and the `/run` redrive (409). (#176)
- A `prep_sample` retire surface so an operator can disposition a sample (drop an
  empty / failed-yield well out of a pool's active set) without raw SQL. New
  reversible `PATCH /api/v1/prep-sample/{idx}/retired` (gated on
  `Scope.PREP_SAMPLE_WRITE` + the wet_lab_admin role the prep_sample read route
  uses; `retired=false` un-retires a misclassified well) plus `qiita prep-sample
  retire` / `qiita prep-sample un-retire` CLI subcommands. The `prep_sample.retired`
  column + CHECK already existed and the completion rollup already excludes retired
  rows. (#176)
- Producer cutover for the full-read+mask feature (PR 3). The orchestrator now
  PRODUCES the reads and masks the DuckLake tables consume, replacing the
  destructive host/QC read-dropping. `ReadMaskReason` (a `qiita-common`
  `StrEnum`: `pass` / `qc_too_short` / `qc_too_long` / `qc_low_quality` /
  `qc_too_many_n` / `host_rype` / `host_minimap2`; backs a DuckLake VARCHAR, not
  a Postgres ENUM, so no `ENUM_PAIRS` entry). `fastq_to_parquet` writes the full
  reads with a `prep_sample_idx` column (sorted `(prep_sample_idx,
  sequence_idx)`) and exposes a staging dir so a `register-files` step loads them
  into the DuckLake `read` table. `qc` stops dropping reads and emits a partial
  mask (`sequence_idx`, reason, per-end trims) via `filter_read` fail-reason →
  `ReadMaskReason`. `host_filter` runs rype/minimap2 on the trimmed QC-pass
  subset, merges host hits into the QC mask under a privacy precedence (host
  wins over `pass`; QC-failed reads keep their `qc_*` reason), and registers the
  final `read_mask` (tagged with the CP-minted `mask_idx`) into the DuckLake
  `read_mask` table. The runner mints `mask_idx` before the step loop (host
  references from the sample's `sequenced_sample` row + the resolved QC config,
  deduped on a config hash) and threads it into `host_filter`. `persist-read-metrics`
  is re-sourced from the mask, counting `COUNT(*) + COUNT(right_trim2)` per
  reason bucket so paired-end `_r1r2` totals don't silently halve. New workflow
  `fastq-to-parquet/1.3.0` reflects the new step shape; the dead
  `qc_reads.parquet` / `filtered_reads.parquet` outputs are removed, and the new
  COPY/read_parquet/CREATE VIEW path literals route through
  `validate_parquet_path`. (#173)
- Data-plane read tables + masked-read view (PR 2 of the full-read+mask
  feature). The data plane now creates the DuckLake `read` and `read_mask`
  tables and the `read_masked` view at startup (`ensure_read_tables`, called
  alongside `ensure_reference_tables`; idempotent via `CREATE TABLE/VIEW IF NOT
  EXISTS`, the view is catalog-stored so it persists across DP restarts).
  `read_masked` joins `read` to `read_mask`, applies the recorded per-mate trims
  (`substr` on the sequence, list-slice on the `UTINYINT[]` qual), and excludes
  every non-`pass` row (`WHERE m.reason = 'pass'`), so host/human and QC-failed
  reads are unreachable by construction. `read_masked` added to the Flight
  `ALLOWED_TABLES`, and `mask_idx`/`prep_sample_idx` to `ALLOWED_FILTER_COLUMNS`;
  raw `read`/`read_mask` are deliberately NOT Flight-reachable. No producer of
  read data yet (PR 3). (#171)
- Read-mask identity + masked-read DoGet route (PR 1 of the full-read+mask
  feature). New `qiita.mask_definition` table + `qiita.mint_mask_definition`
  function mint a `mask_idx` identifying a read-filtering config, deduplicated on
  a canonical-JSON SHA-256 of the config so the same config resolves to the same
  `mask_idx` fleet-wide (idempotent upsert; no advisory lock). New
  `POST /api/v1/mask-definition` (mint) and `POST /api/v1/read-masked/ticket/doget`
  (signs an HMAC DoGet ticket scoped to a mandatory `(prep_sample_idx, mask_idx)`
  filter on the data plane's `read_masked` view — an unfiltered ticket is never
  signed). Both service-account-only under a new `read_masked:doget` scope.
  `read_masked` added to the CP-side DoGet table allowlist (the data-plane view
  itself lands in PR 2). New `qiita_common.hashing` canonical-hash helper. (#170)
- `qiita ticket run <idx>` CLI subcommand — wraps the existing
  `POST /work-ticket/{idx}/run` operator override (reset a FAILED ticket and
  re-dispatch; the only retry mechanism, no auto-retry worker). The runner
  fast-forwards already-COMPLETED steps and resumes at the first incomplete one,
  so an expensive finished step (e.g. `stage_local_fasta`) is not recomputed.
  Mirrors `qiita ticket status`; no server change (the route and api_paths
  constants already existed) (#157)
- Pool prep-generation completion rollup. New `GET
  /api/v1/sequencing-run/{run}/sequenced-pool/{pool}/completion` route (and
  `qiita-user pool-completion`) classifies each non-retired `sequenced_sample` by
  the state of its `fastq-to-parquet` work tickets (any version) and tallies
  `samples_completed` / `samples_in_flight` / `samples_failed` /
  `samples_not_submitted` over the pool, with a `complete` flag (every sample
  COMPLETED, pool non-empty). The SPP `GenPrepFileJob` end-state equivalent: it
  tells the operator whether the per-sample fan-out finished. Compute-on-read
  over the work tickets, so it never drifts when a sample is re-processed,
  re-submitted, or deleted. Read-gated like the other pool rollups
  (prep_sample:read + wet_lab_admin). Part of #146 (#158)

- Per-sample host-filter references. `sequenced_sample` gains two nullable FK
  columns (`host_rype_reference_idx`, `host_minimap2_reference_idx` → `reference`,
  with a CHECK that minimap2 only accompanies rype) recording which host the
  sample is depleted against — both NULL means no host filtering. They map 1:1
  onto `fastq-to-parquet/1.2.0`'s `host_rype_reference_idx` /
  `host_minimap2_reference_idx`, so a later pool fan-out is a pass-through; the
  reference being `(name, version)` pins the exact host build per sample, and a
  non-human host is just a different reference (no schema change). The
  sequenced-sample composer request/response and the pool/run sample-list items
  carry them. `qiita-user submit-bcl-convert` gains `--host-rype-reference-idx`
  (+ optional `--host-minimap2-reference-idx`): it reads each sample's project
  `human_filtering` flag from the preflight and records the host reference(s) on
  `human_filtering` samples (blanks/controls follow their project) while leaving
  `human_filtering=0` samples unfiltered, pre-flighting the references ACTIVE +
  indexed up front (#156)

- Merged (multiqc-equivalent) run-level QC report for a pool. `sequenced_sample`
  gains two nullable `jsonb` columns (`raw_qc_report`, `filtered_qc_report`)
  holding the per-sample `qc_report.json` documents; a new `persist-qc-report`
  library primitive — added as the final `action:` step of
  `fastq-to-parquet/1.2.0` — writes them from the `qc_report_raw` /
  `qc_report_filtered` sidecars (the same persist-from-sidecar pattern as
  `persist-read-metrics`). New `GET
  /api/v1/sequencing-run/{run}/sequenced-pool/{pool}/qc-report` route returns the
  pool's merged report: the read-metric rollup, every non-retired sample's
  persisted raw/filtered report, and a run-level `merged` aggregate (per-mate
  histograms summed across samples, means base/read-weighted). Compute-on-read —
  the merge runs at request time, so it never drifts when a sample is
  re-processed or deleted. Read-gated like the pool roster (prep_sample:read +
  wet_lab_admin). implements #145 (#154)
- New `qc_report` native job: a fastqc-equivalent per-sample QC summary computed
  in DuckDB straight from `reads.parquet` (no container, no miint extension). Per
  mate (r1/r2) it reports read/base counts, mean quality, GC and N content,
  length stats, and per-sequence mean-quality / GC-percent / length histograms,
  written as a `qc_report.json` sidecar. Wired into `fastq-to-parquet/1.2.0` as
  two steps mirroring SPP's bclconvert/filtered_sequences split — `qc_report_raw`
  (on the raw fastq output, before qc) and `qc_report_filtered` (on the
  host-filtered output) — sharing one module, disambiguated by input/output
  binding (`reads`→`raw_qc_report`, `filtered_reads`→`filtered_qc_report`). The
  artifacts feed the upcoming merged-report step; reporting only, no filtering
  change (#152)

- New `GET /api/v1/sequencing-run/{run}/sequenced-pool/{pool}` route returning a
  pool's metadata plus a compute-on-read read-metric rollup (#143): per-stage
  read-count SUMS over the pool's non-retired `sequenced_sample` rows
  (`raw_read_count_r1r2` / `biological_read_count_r1r2` /
  `quality_filtered_read_count_r1r2`), a `fraction_passing_quality_filter`
  recomputed from the sums (not a mean of per-sample fractions), and
  `sample_count` / `samples_with_metrics` so a partially-processed pool is
  interpretable. Nothing is stored at the pool level — the rollup is aggregated
  at request time, so it never drifts when a sample is re-processed or deleted.
  Read-gated like the pool roster (prep_sample:read + wet_lab_admin). implements
  #143 (#149)
- Per-`sequenced_sample` read metrics: `sequenced_sample` gains three nullable
  `BIGINT` columns (`raw_read_count_r1r2`, `biological_read_count_r1r2`,
  `quality_filtered_read_count_r1r2`) with a CHECK enforcing
  quality_filtered <= biological <= raw. A new `persist-read-metrics` library
  primitive — added as the final `action:` step of `fastq-to-parquet/1.2.0` —
  reads the three `read_count.json` sidecars (#141) and writes them onto the
  sample's 1:1 `sequenced_sample`; `GET /sequenced-sample/{idx}` surfaces them
  plus a computed-on-read `fraction_passing_quality_filter`
  (quality_filtered / raw). The workflow now declares `prep_sample:write` (in the
  USER ceiling, so its audience is unchanged). implements #142 (#148)
- The `fastq`, `qc`, and `host_filter` native steps now emit a `read_count.json`
  sidecar recording how many reads survive each parquet stage, captured as the
  three SPP boundary counts per `prep_sample`: raw (`fastq` → `raw_read_count`),
  biological (`qc` → `biological_read_count`), and quality-filtered
  (`host_filter` → `quality_filtered_read_count`). The count is
  `count(*) + count(sequence2)` (both mates, the `*_r1r2` convention) via a new
  shared `read_count.write_read_count` helper. `fastq-to-parquet/1.2.0` declares
  the three outputs so the runner forwards them in `bound`; persisting them onto
  `sequenced_sample` is a follow-up (#142). Emission only — no schema change;
  implements #141 (#147)
- New `GET /api/v1/sequencing-run/{sequencing_run_idx}` route returning a run's
  caller-visible metadata (notably `instrument_model`). Read-gated like the pool
  roster route (prep_sample:read + wet_lab_admin). `qiita submit-host-filter-pool`
  reads it to forward QC's polyG-gating `instrument_model` per sample (#129)
- New `fastq-to-parquet/1.2.0` workflow: an additive successor to 1.1.0 that
  inserts an ALWAYS-ON `qc` step between `fastq` and `host_filter`
  (`fastq → qc → host_filter`). Each stage re-emits the `reads` binding it
  consumes (a transform in place), so `host_filter` is identical to 1.1.0 and
  consumes the QC'd reads. `context_schema` gains `instrument_model` (forwarded to
  qc's polyG gate via the step's `params`) and the two-reference host-filter keys
  (`host_rype_reference_idx` + optional `host_minimap2_reference_idx`); the qc
  step lists `adapter_parquet`, which triggers the runner's adapter materialization.
  1.0.0 and 1.1.0 stay available unchanged (#129)
- Verified and documented the duckdb-miint fastp-port QC functions
  (`filter_read`, `trim_adapters` / `trim_adapters_pe`, `trim_polyg`) that the
  upcoming `qc` native job builds on. New
  `qiita-compute-orchestrator/tests/jobs/test_qc_miint_contract.py` pins their
  **positional-arg-only** contract and fastp-default values against the
  team-mirror build (the upstream `docs/qc.md` documents named params the build
  rejects); `docs/duckdb-miint.md` gains a QC section. Groundwork for the
  bcl-convert → `fastq` → `qc` → `host_filter` pipeline (#129)
- New `artifact_sequence_set` reference kind — an indexless set of artifact
  sequences (the canonical adapter set the QC step trims against), ingested
  through the same kind-agnostic reference-add flow (no taxonomy, no index).
  `qiita reference load --kind artifact_sequence_set` and a `reference.kind`
  CHECK widen back it. The control plane gained
  `QIITA_DEFAULT_ADAPTER_REFERENCE_IDX` (the canonical set's reference_idx) and a
  runner resolver (`_resolve_qc_adapters`) that DoGets that set's sequences from
  the data plane and stages them as a one-`sequence`-column Parquet for the QC
  step — materialized only for a workflow whose steps need it (#129)
- New `qc` native job (`qiita_compute_orchestrator.jobs.qc`): a fastp-equivalent
  read-QC transform `reads.parquet` → `qc_reads.parquet` over the duckdb-miint
  fastp-port functions. Per read it runs adapter trim (`trim_adapters` SE /
  `trim_adapters_pe` PE) → optional polyG trim (`trim_polyg`, gated on a 2-color
  `instrument_model`) → length/quality filter (`filter_read`, fastp `-l 100`
  defaults); drop-only and `sequence_idx`-preserving, dropping a read pair when
  EITHER mate falls below min_length after trimming. The canonical adapter set
  is read from the runner-staged `adapter_parquet` via `read_parquet` and inlined
  as a constant `VARCHAR[]`; the two SE/PE seams emit SELECTs that UNION ALL
  straight into one streaming COPY (no intermediate accumulator table). Slots into
  the bcl-convert → `fastq` → `qc` → `host_filter` pipeline (#129)
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
- List a sequencing run's sequenced-samples in one call. New
  `GET /sequencing-run/{idx}/sequenced-sample/list` returns the run's active
  sequenced-samples as `SequencedSampleListResponse` rows, and
  `SequencedSampleListItem` now also carries `biosample_idx`, both ENA
  accessions (`ena_experiment_accession`, `ena_run_accession`), and both
  biosample accessions (`biosample_accession`, `ena_sample_accession`) — enough
  for the ENA experiment fan-out (which needs the BioSample accession as the
  experiment sample_descriptor) without per-sample GETs. Gated on
  `prep_sample:read` + wet_lab_admin; the existing idx-only `…/list-idxs`
  run route is unchanged. The `qiita` CLI gains `sequenced-sample list` (#135)
- List the studies a prep-sample belongs to. New
  `GET /prep-sample/{idx}/study/list` returns the active (non-retired) linked
  studies ascending by idx as `StudyListResponse` rows, each carrying the
  study's `bioproject_accession` and `ena_study_accession` — enough for the ENA
  experiment fan-out (which uses the BioProject accession as the experiment
  study_ref) without a per-study GET; 404 on an unknown prep-sample. Gated on
  `prep_sample:read` + wet_lab_admin. The `qiita` CLI gains
  `prep-sample list-studies` (#135)
- Resolve sequencing runs by instrument_run_id. New
  `POST /sequencing-run/lookup-by-instrument-run-id` bulk-resolves
  instrument_run_id values to sequencing_run idxs (idx-only, mirroring the
  study/biosample accession lookups), gated on `prep_sample:read`. The `qiita`
  CLI gains `sequencing-run get` and `sequencing-run lookup` for the
  resolve-then-read flow against `GET /sequencing-run/{idx}` (#135)
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

- Auth integer env-knobs (`AUTHROCKET_JWT_LEEWAY_SECONDS`, `AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS`, `QIITA_TOKEN_DEFAULT_TTL_DAYS`, `AUTH_HANDOFF_FRESHNESS_SECONDS`, `CLI_LOGIN_CODE_TTL_SECONDS`) are now validated at boot instead of parsed with a bare `int()` — a non-int or non-positive value fails loudly (leeway may be 0). (techdebt-sweep-pr1)
- `GET /reference` and `GET /prep-protocol` accept a bounded `limit` query param (default 1000, max 5000) so the anonymous catalog lists can't return an unbounded payload. (techdebt-sweep-pr1)
- Flight-ticket and login-cookie signing now share `qiita_common.hashing.canonical_json` instead of three hand-rolled `json.dumps(sort_keys=…)` spellings, removing the risk of the HMAC'd wire serialization drifting. (techdebt-sweep-pr1)
- Accepting an AuthRocket invitation redirects to the cookie-anchored `/auth/login` instead of minting a full-ceiling PAT from the un-anchored invitation JWT. (techdebt-sweep-pr1)
- `qiita-admin masked-read-export` is now **re-runnable**: it creates `--output-dir`
  (with parents) if missing instead of erroring, and for parquet it skips a sample
  whose output file already exists when the count matches and overwrites it only
  when it differs. The count comes from a new `count_masked` data-plane DoAction —
  a cheap `count(*)` against the light `read_mask` table (no read sequences
  streamed or materialized) that reuses the sample's existing signed export ticket,
  so there's no new control-plane route. fastq has no cheap on-disk count, so an
  existing fastq target is refused up front rather than re-exported. (#230)
- **CI build speedups.** The `test-integration` job now sets up the Rust
  toolchain + `Swatinem/rust-cache` + cached `libduckdb` (mirroring the `rust`
  job), so the data-plane debug build it drives — previously a cold ~80s
  recompile of all deps and the largest slice of the job — is incremental on
  repeat runs. The separate `lint-rust` / `test-rust` jobs merged into one
  `rust` job that shares a checkout, toolchain, and warm `rust-cache` (no more
  two jobs racing to write the same cache). The macOS host-Postgres provisioning,
  previously inlined and duplicated across two jobs, moved into a reusable
  `.github/actions/setup-host-postgres` composite action with a weekly-refreshable
  Homebrew download cache. No change to what is tested. (#225)
- **Native DuckDB jobs share one spill-dir context manager** (`duckdb_tmp_dir` in
  the orchestrator's `miint.py`), making `<workspace>/.duckdb_tmp` teardown
  structural across all ten jobs instead of a per-job `try/finally`. This closes a
  leak in `build_rype_index`, which created the spill dir but never removed it
  (spilled bytes accumulated in the shared work-ticket workspace — SLURM has hit
  "no space in /tmp"). Same consistency sweep: the two index builders and
  `qc_report` now route their `read_parquet` path literals through
  `validate_parquet_path` (the repo's fail-fast quote/backslash/control-char
  reject) like the sibling jobs, and the builders' `index_type` meta JSON uses the
  shared `HOST_FILTER_INDEX_TYPE_{RYPE,MINIMAP2}` constants instead of bare
  `"rype"`/`"minimap2"` literals. Behavior-preserving (the existing per-job unit
  suites are the guard); no env var, host dir, scope, migration, or workflow
  change. (#229)
- A job's input `params.json` and a native step's output `manifest.json` are now
  pretty-printed (2-space indent, trailing newline; the manifest also sorts keys
  to mirror the container-side `manifest_writer.py`) instead of dumped as a single
  dense line — far easier to read when debugging a job's input/output dir. Both
  files are parsed (`model_validate_json` / `json.loads`), so the whitespace change
  is transparent to every consumer. (#208)
- `qc` step walltime raised in both actions that run it (`read-mask/1.0.0` and
  `fastq-to-parquet/1.3.0`): `baseline_resources.walltime` PT2H → PT4H and
  `action_ceiling.walltime` PT4H → PT8H, giving the first attempt more time and
  the new TIMEOUT escalation (above) room to climb to PT8H. The ceiling is
  action-wide, so `host_filter` (baseline PT4H) can now also escalate to PT8H on
  a TIMEOUT retry. YAMLs edited in place; re-synced via `qiita-admin actions
  sync`. (#216)
- bcl-convert re-submission over an already-**COMPLETED** sequenced_pool is now
  refused by default and requires `--force` (wet_lab_admin+). A re-run
  re-registers the pool's reads into the lake, and DuckLake has no uniqueness, so
  a silent re-submit duplicated read rows. `WorkTicketCreateRequest` gains a
  `force` flag (privileged like `resource_override`); `submit_work_ticket` /
  `_check_disallow_without_delete` gate a COMPLETED-pool resubmit; `qiita
  submit-bcl-convert` gains `--force`. Non-force recovery for a stored result is
  `delete-sequenced-pool` then resubmit; FAILED tickets remain freely resumable
  via `qiita ticket run`. (#206)
- `host_filter` step memory raised 16 → 32 GB in both actions that run it
  (`read-mask/1.0.0` and `fastq-to-parquet/1.3.0`): the step's
  `baseline_resources.mem_gb` and the `action_ceiling.mem_gb` both go 16 → 32, so
  a `host_filter` run lands at 32 GB directly (the genome-scale rype/minimap2 host
  index didn't fit in 16). YAMLs edited in place; re-synced via `qiita-admin
  actions sync`. (#209)
- `qiita submit-host-filter-pool` no longer takes a `--preflight-blob` file. Its
  pool-wide host-filter guard needs each sample's intake `human_filtering` intent,
  which already lives in the pool's **stored** run-preflight blob — so requiring
  the operator to re-supply the file was redundant, and impossible once the stored
  preflight diverged from any local copy (e.g. after `run-preflight update-lane`
  edits it in the database). The intent is now derived server-side: the pool
  sample-list route (`GET
  /api/v1/sequencing-run/{R}/sequenced-pool/{P}/sequenced-sample/list`) gains a
  per-sample `human_filtering` field (additive, nullable), read at request time
  from the stored blob — the single source of truth, so a later `update-lane` is
  reflected automatically. The command reads that field from the roster it already
  fetches; an unparseable/absent stored preflight degrades the field to null
  (listing never 500s, and the parse failure is logged) and the command's
  existing guard turns a null intent into an actionable abort at submit time.
  (#205)
- `qiita-admin masked-read-export` is faster and its fastq output is now
  gzip-compressed. The **parquet** path streams the Flight reader straight to a
  `pyarrow.parquet.ParquetWriter` instead of `DuckDB COPY`, so the bulk read bytes
  are no longer materialized into DuckDB vectors (one fewer full copy) and the
  parquet path no longer loads the miint extension at all (measured ~1.6× faster
  on a synthetic stream, and zero Acero passes). The streamed batches are coalesced
  into row groups sized by `qiita_common.parquet.ROW_GROUP_SIZE_BYTES` (the 64 MB
  byte cap from `PARQUET_OPTS`, now exported as an int), so the file keeps the
  byte-sized row-group layout qiita uses everywhere instead of one tiny row group
  per ~2048-row DataChunk. The **fastq** path writes
  `<stem>.fastq.gz` / `<stem>.R1.fastq.gz` + `<stem>.R2.fastq.gz` (`FORMAT FASTQ,
  COMPRESSION 'gzip'`) instead of uncompressed `.fastq`, and reuses a single
  miint DuckDB connection across all samples rather than opening one (with an
  extension `LOAD`) per sample. (#198)
- `ingest_reads` now parses each sample's FASTQ(s) **once** instead of twice: the
  read count that sizes the `sequence_range` mint is taken from the staged
  intermediate Parquet's `COPY` row-count return rather than a separate
  `read_fastx` counting pass. The intermediate is written before the mint (it is
  mint-independent, keyed by the per-file `sequence_index`), so the count comes
  for free off the parse we already do. On a large paired-end sample (~13.5M
  pairs) this removes a full ~20s serial FASTQ parse per sample. The durable
  `read.parquet` is now sorted by `sequence_idx` alone — `prep_sample_idx` is a
  constant for a single sample, so dropping it from the `ORDER BY` orders nothing
  and yields identical output while shrinking the sort (~66s→~46s, 12GB→9GB peak
  on the same sample). (#201)
- `fastq_to_parquet` likewise sorts its durable `read.parquet` by `sequence_idx`
  alone (dropping the constant `prep_sample_idx` from the `ORDER BY`) — same
  identical-output sort shrink. It was already single-pass (it counts off the
  intermediate Parquet footer), so the parse-once change does not apply there.
  (#201)
- `ingest_reads` now processes up to 4 pool samples **concurrently** (bounded
  `asyncio.gather` + a semaphore; each sample's DuckDB stage/sorted-write runs in
  a worker thread, the mint stays async on the loop) instead of one at a time.
  Per-sample work is independent (own FASTQ, atomic mint, own output file) so
  results are unchanged — this just overlaps the inherently-serial `read_fastx`
  parses across samples. Per-slot DuckDB memory/threads are derived from the SLURM
  cgroup (2 threads/slot to keep the sort parallel and clear wells fast); the
  bcl-convert `ingest_reads` step's `baseline_resources` rise to `cpu: 8` /
  `mem_gb: 56` to match (still well under the action ceiling). (#201)
- `runner._resolve_staged_reads` now falls back to the data plane when a
  read-mask workflow can't find the prep_sample's ephemeral durable staging copy:
  it signs an `export_read` action token and binds the per-ticket `reads.parquet`
  the data plane writes from the persistent DuckLake `read` table (an empty result
  or unreachable data plane still FAILs the ticket cleanly as a SUBMISSION
  BAD_INPUT). This lets `submit-host-filter-pool` reprocess a run whose staging
  copy has been reaped, instead of hard-failing "no stored reads". (#187)
- Tightened the `read-mask/1.0.0` action audience from
  `[user, wet_lab_admin, system_admin]` to `[wet_lab_admin, system_admin]`:
  submitting a read mask (host filter / QC reprocessing) now drives the data plane
  to re-materialize the sample's RAW (human-containing) reads via `export_read`,
  so it is a privileged operation — never a plain `user`. (#187)
- Split read storage from masking so a sample's reads are stored ONCE and can be
  masked repeatedly against different host references. Previously the single
  `fastq-to-parquet` workflow parsed FASTQ, minted a `sequence_idx` range, AND
  masked in one ticket — so re-masking the same sample against a second host
  reference hit the `sequence_range` UNIQUE(prep_sample_idx) constraint (409) and
  failed. Now: the **bcl-convert** workflow gains an `ingest_reads` step that,
  after demux, parses every pool sample's FASTQ(s), mints the range, and writes
  the full reads into the DuckLake `read` table once (plus a durable per-sample
  `read.parquet` under `<scratch>/reads/<prep_sample_idx>/`). A new **`read-mask`**
  workflow (`qc → host_filter → register read_mask → persist-read-metrics`) binds
  those stored reads and records one mask per submission — `qc.py`/`host_filter.py`
  are unchanged. `submit-bcl-convert` now embeds the pool roster
  (`prep_sample_idx ↔ pool_item_id`) in the ticket's `action_context` so the
  pool-scoped ingest step (which has no DB access) can store reads; the runner
  materializes it to a Parquet (`_resolve_sample_map`) and binds the staged reads
  for a mask ticket (`_resolve_staged_reads`). `submit-host-filter-pool` is now
  mask-only: it drops `--convert-dir` and FASTQ resolution and submits one
  `read-mask/1.0.0` ticket per sample, so the SAME pool can be re-submitted later
  against host reference 4 to produce a side-by-side mask over host reference 2's
  reads — neither re-runs ingest. The pool-completion rollup now keys on the
  `read-mask` action (a sample is "processed" once it has a mask). The legacy
  `fastq-to-parquet` workflows remain registered but dormant (no gesture submits
  them); full retirement is a fast-follow.
- `build_rype_index` rebalances the DuckDB/rype memory split now that
  `rype_index_create` windows its chunk feed (miint windowed-feed fix): DuckDB's
  under-SLURM hard cap drops 30 → 8 GB (the windowed feed bounds DuckDB's working
  set to ~256 MiB per window rather than the whole corpus), handing the freed
  ~22 GB to rype's in-process index build. rype's `max_memory` now starts ~50 GB
  at the 64 GB baseline and grows to ~114 GB at the 128 GB OOM-retry ceiling (was
  ~30 → ~92 GB). The off-SLURM fallbacks (DuckDB 4 GB, rype 30 GB floor) are
  unchanged. Relies on the windowed-feed miint build being live on the mirror.
  (#179)
- The sequenced-pool completion rollup gains a `samples_no_data` bucket and its
  `complete` flag now fires when every active sample is in a terminal-accounted
  state — COMPLETED **or** NO_DATA — instead of requiring every sample COMPLETED.
  A plate of real data containing empty wells now reaches `complete=True` rather
  than sitting `false` forever behind permanent empty-well failures. The per-sample
  precedence is `completed > in_flight > no_data > failed > not_submitted` (no_data
  outranks failed, so a well with both a no_data and a stale failed ticket counts
  as no_data); empty wells are no longer folded into `samples_failed`. The
  `GET .../sequenced-pool/{P}/completion` response gains the `samples_no_data`
  field (soft contract addition). Until expected-empty control-well preflight
  marking lands (deferred), EVERY empty well becomes `no_data` — data wells
  included, not only flagged controls. (#176)
- Host-filter references moved off `sequenced_sample` onto the human-filter
  submission (PR 4 of the full-read+mask feature). Host references are a
  filtering-config choice, not a sample property, so two configs can coexist over
  the same reads. `submit-host-filter-pool` now takes `--host-rype-reference-idx`
  / `--host-minimap2-reference-idx` (pool-wide for the submission; omit for a
  QC-only pass-through), pre-flights them once at submission, and threads them
  into the work-ticket `action_context` where the runner reads them to mint the
  `mask_idx` and drive `host_filter`. `submit-bcl-convert` no longer accepts or
  records host references (it only demultiplexes the run); the preflight's
  per-project `human_filtering` flag is still echoed per sample for reference.
  `prep_protocol_idx` stays on the sample. Soft API change: sequenced-sample GET
  responses and the pool/run sample-list rows no longer carry host references.
  `submit-host-filter-pool` also takes `--preflight-blob` (the same SQLite given
  to `submit-bcl-convert`) and guards against a pool-wide host-ref choice that
  disagrees with the samples' intake `human_filtering` intent: a mismatch aborts
  before any ticket is submitted unless `--force` downgrades it to a warning.
  (#175)
- `build_rype_index` resized for large host sets (many human genomes that OOMed
  at 32 GB). The step's `baseline_resources.mem_gb` rises 32 → 64 in both
  `host-reference-add/1.0.0` and `local-host-reference-add/1.0.0`, and
  `local-host-reference-add`'s `action_ceiling.mem_gb` rises 64 → 128 (matching
  `host-reference-add`) so an OOM-killed retry can double the step 64 → 128 GB
  (the escalator clamps to the ceiling). The job now hard-caps DuckDB at 30 GB
  (was 16) regardless of allocation, so the larger cgroup — and the bigger one
  an OOM retry escalates to — flows to rype: rype's `max_memory` starts at 30 GB
  and grows with the allocation (≈92 GB at the 128 GB ceiling). Builds on the
  OOM-retry escalation below (#169)
- Workflow steps now escalate their memory allocation on an OOM-killed retry.
  Previously every retry re-ran at the same `mem_gb`, so an OOM just OOM'd again
  until the retry budget was exhausted. `_run_entry_with_retry` now grows the
  step's memory floor ×2 (clamped to the action's `mem_gb` ceiling) on each
  `OOM_KILLED` retry; other transient kinds still retry unchanged. The escalated
  floor is process-local — a CP restart re-attaches and re-escalates from the
  ticket's static `resource_override`. The `reference-add` and
  `host-reference-add` action ceilings are raised 64 → 128 GB so the OOM-prone
  `reference_load` step can climb 32 → 64 → 128 GB across retries (#167)
- `qiita-user submit-host-filter-pool` now host-filters each pool sample against
  the reference(s) recorded on it at `submit-bcl-convert` time, instead of a
  single uniform reference for the whole pool. **Operator-facing CLI contract
  change:** the global `--host-rype-reference-idx` / `--host-minimap2-reference-idx`
  flags are removed (host filtering is per-sample now). Samples with a recorded
  `host_rype_reference_idx` are depleted against it (plus their optional minimap2
  reference); samples with none recorded (preflight `human_filtering=0`) get a
  QC-only `host_filter_enabled=false` pass-through ticket — the first fan-out path
  for unfiltered samples. The gesture pre-flights each distinct recorded reference
  (ACTIVE + the required index) once up front, so a misconfiguration aborts with
  zero side effects. Part of #146 (#158)
- Stripped this repo's GitHub issue/PR numbers from code comments, docstrings,
  and string literals across all components (comment-only; no behavior change),
  and recorded the convention in `CLAUDE.md`: provenance lives in git / CHANGELOG
  / the PR, not the source. External-tracker refs (e.g. `DuckDB #23229`) and the
  `(#N)` tags in CHANGELOG/DEPLOY_CHECKLIST are kept (#150)
- The `stage_local_fasta` step in `local-host-reference-add/1.0.0` now requests
  `cpu: 4` / `mem_gb: 64` (was `cpu: 8` / `mem_gb: 32`) — fewer cores, more
  memory for staging many host FASTA files into one chunked Parquet. Still within
  the action's `cpu: 16` / `mem_gb: 64` ceiling (#140)
- All Parquet writes now add `ROW_GROUP_SIZE_BYTES '64MB'` — row groups flush at
  ~64 MB encoded size instead of buffering one large group, sharpening row-group
  predicate pushdown (tighter per-group min/max) and lowering peak write memory.
  The canonical `PARQUET_OPTS` / `PARQUET_OPTS_INTERMEDIATE` constants moved to
  `qiita_common.parquet` (single-sourced for both services); the orchestrator
  re-exports them and derives `PARQUET_OPTS_CHUNKED`, and the control-plane
  `mint_features` write now imports `PARQUET_OPTS` instead of hardcoding the
  string. The option requires `preserve_insertion_order=false`, already set on
  every orchestrator pipeline connection via `apply_duckdb_settings`; the
  control-plane write now sets it explicitly. Output stays clustered on each
  COPY's `ORDER BY` key (what pruning reads), so the sorted-result contract is
  unaffected. The Rust data-plane DoPut writer is unchanged — parquet-rs has no
  byte-based row-group knob (#140)
- Bumped the pinned DuckDB across all components from **1.5.3** to **1.5.4** to
  track the team miint mirror's current build. Python floor raised to
  `duckdb>=1.5.4` in control-plane, compute-orchestrator, and integration tests
  (locks regenerated); data-plane Rust crate `1.10503.1` → `1.10504.0` (DuckDB
  1.5.4); the `setup-libduckdb` action default and the `deploy.yml` extension
  cache key moved to `1.5.4` so CI links a matching libduckdb. The miint mirror
  already publishes v1.5.4 builds for `linux_amd64` and `osx_arm64`. The
  `test_duckdb_version_sync` guard keeps the crate, action default, and cache key
  in lockstep (#138)
- The `stage_local_fasta` native job now caps `read_fastx`'s per-batch buffer at
  128MB (was 512MB), lowering peak memory during FASTA staging. One of the job's
  three memory levers, alongside the DuckDB `memory_limit`/`temp_directory` spill
  and the Parquet write buffer (#137)
- Retired the manual "rebuild the SIF" deploy step now that the deploy
  auto-builds. `/deploy-note` and `CLAUDE.md` ("Container image tier") now direct
  a container-artifact change to a Notes entry + an optional verify, never a
  bucket-2 manual build — the auto-build's content hash picks the change up on the
  next deploy. Bucket 2 keeps only genuinely new host setup the build depends on
  (e.g. staging a new licensed source). The out-of-band manual `build-sif.sh` is
  documented as a root-only escape hatch (`apptainer build` mounts the caller's
  home; `qiita-orch`'s is `/dev/null`), and `/deploy-note` now requires any
  `apptainer exec` verify to be home-/cwd-independent (`cd` + `--no-home`). (#134)
- The deploy now builds container SIFs automatically. `activate.sh` runs a new
  `deploy/build-sifs.sh` after the rsync and before any service restart: it
  iterates `workflows/*/sif-build.env`, builds each via the existing generic
  `scripts/build-sif.sh` (as root), then chowns the produced SIF to `qiita-orch`.
  It is idempotent — `build-sif.sh` now also stamps a content hash of the in-repo
  build inputs (`Apptainer.def`/`entrypoint.sh`/`manifest_writer.py`) next to the
  SIF, so an edit to any of those (which `VERIFY_MATCH`, version-only, could not
  see) triggers a rebuild without the old manual `FORCE=1`. Missing prerequisites
  (no `apptainer`, no `PATH_DERIVED`, an unstaged licensed `SOURCES`, or
  `AUTO_BUILD=0` in a spec) clean-skip an image; only a real build/chown failure
  aborts the deploy, before any restart. `local-deploy.sh` now also rsyncs
  `scripts/` into the staging tree so the CI deploy path can build too
- `qiita submit-host-filter-pool` now fans out fastq-to-parquet/**1.2.0** (QC +
  two-reference host filter) instead of 1.1.0. `--host-reference-idx` is replaced
  by `--host-rype-reference-idx` (required) and `--host-minimap2-reference-idx`
  (optional), each pre-flighted for ACTIVE status + its named index; the run's
  `instrument_model` is read once (GET /sequencing-run) and forwarded per sample
  so QC's polyG gate is set correctly (#129)
- Host filtering can now draw its two indexes from two INDEPENDENT references.
  The runner's `_resolve_host_filter_indexes` gained a two-reference layout
  (fastq-to-parquet/1.2.0): `host_rype_reference_idx` (required) supplies the rype
  `.ryxdi` and the optional `host_minimap2_reference_idx` supplies the minimap2
  `.mmi`, each from its own ACTIVE reference that MUST carry the named index (a
  designated reference missing its index is a hard error). The legacy
  single-reference `host_reference_idx` layout (1.1.0, ≥1-of-either, skip on
  missing) is unchanged and back-compatible; the two layouts are mutually
  exclusive (mixing them, or enabling with no reference key, is a clear
  SUBMISSION BAD_INPUT). `host_filter.py` itself is untouched — it still skips the
  stage whose index path is None (#129)
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

### Removed

- Dead SLURM poll/timeout config in the compute-orchestrator (`SlurmBackend` `poll_interval_seconds` / `job_timeout_seconds`, their `SlurmSettings` fields, the `SLURM_POLL_INTERVAL_SECONDS` / `SLURM_JOB_TIMEOUT_SECONDS` env vars, and the `DEFAULT_SLURM_*` constants) — assigned but never read since the CP took over the poll loop. (techdebt-sweep-pr1)
- **`.github/workflows/deploy.yml`** — the unused `v*`-tag auto-deploy workflow.
  It SSH'd to `$DEPLOY_HOST` and ran a real production deploy on any `v*` tag
  push, but production has only ever deployed manually via `deploy/local-deploy.sh`
  / `redeploy.sh` — so it was a latent footgun (a stray release tag could trigger
  an unattended deploy onto a host that hadn't done the bucket 1–3 pre-steps).
  Reconciled the now-contradictory deploy docs + script comments
  (`docs/runbooks/first-deploy.md`, `docs/architecture.md`,
  `deploy/{activate,local-deploy,build-sifs}.sh`) to state plainly that deploys
  are manual and there is no CI/tag-triggered deploy path. (#233)

### Fixed

- **CLI-login plaintext PATs are no longer stored at rest.** `cli_login_code.plaintext_pat` is scrubbed the instant an ot_code is redeemed and a background sweeper deletes consumed/expired rows; previously a consumed row kept a usable bearer token for the token's full life (up to 90 days). (techdebt-sweep-pr1)
- `sign_ticket` rejects an empty Flight-ticket filter (which the data plane treats as `SELECT * FROM <table>`) at the signing boundary, not just per-route. (techdebt-sweep-pr1)
- DB constraint/trigger violations (principal disable/retire, prep_sample publication lock) return stable client messages instead of leaking internal constraint/trigger names. (techdebt-sweep-pr1)
- `verify_api_token` retains its fire-and-forget `record_token_use` task (was GC-droppable before the `last_used_at` write landed); `_parse_job` guards a null `exit_code` (was an untyped `AttributeError`); the SLURM payload rejects `gpu>0` at submit instead of silently dropping it. (techdebt-sweep-pr1)
- Doc drift: architecture.md marks the unbuilt processing-results subsystem as *planned*; CLAUDE.md's data-plane test example names a real test; auth.md corrects the empty-`Bearer` behavior. (techdebt-sweep-pr1)
- Integration-test harness: the `data_plane` fixture's gRPC-startup wait ceiling
  is raised from 10s to 30s (override via `QIITA_DP_START_TIMEOUT_S`), fixing an
  intermittent `test-integration` setup failure where the first module to use the
  fixture paid the coldest start (catalog reset → boot → load DuckDB + miint →
  create DuckLake tables) and occasionally exceeded the old window on a loaded CI
  runner. The poll still returns the instant the port opens, so the higher ceiling
  costs nothing on success; the timeout message now names the pid/port and is
  honest about the actual timeout instead of a hardcoded "10s". (#202)
- A transient control-plane **Postgres** error on the workflow runner's own DB
  calls — most often a per-statement `command_timeout` (a bare
  `asyncio.TimeoutError`) on the poll loop's force-fail check under a lock wait /
  checkpoint / load spike, or a brief connection blip — no longer permanently
  fails an otherwise-healthy work ticket (which orphaned the still-running SLURM
  job and left its output unregistered). The poll loop now gives that cheap read
  a generous per-call timeout and retries it in place a few times, so the common
  brief hiccup is absorbed without abandoning the job; and if a transient DB
  error still escapes any other runner DB call, `run_workflow` records it
  `failure_type='retriable'` (not `permanent`) — once Postgres is reachable again
  to write that row — so a `/run` redrive re-attempts. A real SQL error
  (constraint violation, query bug) is unaffected and stays permanent. (#214)
- CLI HTTP subcommands (`qiita` / `qiita-admin`) now print a friendly, actionable
  message when the control plane is unreachable — a connection refusal, DNS
  failure, TLS error, or timeout — instead of dumping a raw `httpx` traceback.
  The message names the target URL and points at `--base-url` /
  `$QIITA_CONTROL_PLANE_URL`, so a wrong base URL or a down server is obvious.
  `run_http_subcommand` gains an `httpx.RequestError` branch alongside the
  existing `HTTPStatusError` (a non-2xx *response*) handling. (#120)
- A step that OOM-kills (or times out) while **already at its action resource
  ceiling** no longer burns its remaining retry budget re-running at the same
  size. The runner escalates memory on `OOM_KILLED` and walltime on `TIMEOUT`,
  but once the floor is pinned at the ceiling there is no larger allocation to
  try — a re-run would fail identically. The runner now detects that the
  escalation can't grow and fails the ticket immediately with a new permanent
  `RESOURCE_CEILING_EXHAUSTED` failure kind (failure_type `permanent`), whose
  reason names the ceiling and tells the operator to raise it or shrink the
  input — instead of looping through every retry to the same OOM/timeout. (#210)
- A transient HTTP 5xx or network error on the per-sample `POST /sequence-range`
  callback the native `ingest_reads` and `fastq_to_parquet` steps make back to
  the control plane no longer permanently fails the whole pool ingest. Each
  callback now gets a small in-job bounded retry (on a 5xx / 408 / 429, or an
  httpx transport error like a connection reset / read timeout), so a single
  blip on one of a pool's N per-sample callbacks self-heals instead of
  discarding hours of demux and every already-ingested sample. If the retries
  exhaust, the step raises a new retriable `CONTROL_PLANE_UNREACHABLE` failure
  (the CO→CP mirror of `ORCHESTRATOR_UNREACHABLE`) so the runner re-dispatches
  the idempotent step, rather than the old `UNKNOWN_PERMANENT` that consumed no
  retries. 401/403 stay permanent (`CONTRACT_VIOLATION` — a token/scope misconfig
  a retry can't fix) and other 4xx stay `UNKNOWN_PERMANENT`. The retry +
  classification is shared by both steps via `sequence_range_retry` so they
  can't drift. (#212)
- `submit-host-filter-pool` no longer abandons the rest of a pool when one
  sample's `POST /work-ticket` fails. The per-sample fan-out now isolates each
  POST: a transient 5xx, a 409 in-flight, or a network blip is recorded and the
  fan-out continues to the remaining samples, the summary lists every submitted
  and failed sample, and the command exits non-zero if any failed — instead of
  an uncaught raise silently stranding every later sample (those after the
  failure in `sequenced_pool_item_id` order). New `--only-missing` flag submits
  only samples that have no read-mask ticket yet (via a new `has_read_mask_ticket`
  field on the pool- and run-scoped sequenced-sample list responses), so a pool
  whose prior fan-out was interrupted can be filled in without duplicating
  already-submitted work; off by default so a deliberate re-submit against a
  different host reference still fans out pool-wide. (#218)
- Deleting a sequenced_pool now purges the DuckLake data its prep_samples
  produced, not just the Postgres rows. `DELETE
  /sequencing-run/{R}/sequenced-pool/{P}` (`qiita delete-sequenced-pool`) issues a
  new `delete_pool_reads` data-plane DoAction that drops the `read` and
  `read_mask` rows keyed by the pool's prep_sample set (one DuckLake transaction,
  idempotent, retriable — same data-plane → Postgres-last ordering as DELETE
  /reference, so a Flight failure 502s with nothing removed), and the control
  plane reaps the durable `reads/{prep_sample_idx}/read.parquet` staged copies
  on disk. Previously the delete left those reads/files orphaned in DuckLake on
  every pool delete — a storage leak and a surprise for operators who expect a
  `--force` delete to be complete. The response and CLI help now report the
  DuckLake/disk counts (`read_rows_deleted`, `read_mask_rows_deleted`,
  `staged_reads_reaped`). Reclaiming the orphaned Parquet bytes the logical
  DuckLake delete leaves behind remains a future maintenance pass (as with
  reference delete); pre-existing orphans from past deletes are not swept. (#204)
- sequenced_pool find-or-create now keys on the preflight **content**
  (`run_preflight_sha256`, a STORED generated column) instead of its filename, so
  a byte-identical preflight re-uploaded under a different basename resolves to
  the same pool instead of minting a duplicate (the root cause of the run-15
  duplicate pools). Adds the `sequenced_pool_one_per_run_and_hash` partial unique
  index and repoints `insert_sequenced_pool`'s `ON CONFLICT` to it. The existing
  `sequenced_pool_one_per_run_and_filename` index is kept as an independent,
  permanent uniqueness rule — distinct pools in a run must differ in both content
  and filename, so a different-content upload reusing an existing filename is a
  409 by design (never a 500). (#206)
- `qiita-admin masked-read-export` no longer floods stderr with Arrow Acero
  `An input buffer was poorly aligned` warnings (one per column per batch). The
  PyArrow Flight client zero-copies the gRPC message body, whose absolute base
  address carries no element-alignment guarantee, so a column buffer routinely
  lands off its natural alignment even though the data plane writes 64-byte-aligned
  IPC; DuckDB then scans the registered reader through `pyarrow.dataset` → Acero,
  which warns. The fastq path (which still uses DuckDB+miint) now asks the Flight
  reader to realign each buffer to its type's required alignment on receive
  (`IpcReadOptions(ensure_alignment=DataTypeSpecific)`, copying only the small
  offset/validity/fixed-width buffers); the parquet path bypasses DuckDB/Acero
  entirely (see Changed). Benign on x86_64 (output was always correct) — this only
  silences the noise. Upstream: apache/arrow#37195. (#198)
- bcl-convert `ingest_reads` now retries transparently after an OOM mid-write.
  A pool sample whose range was minted by a prior attempt that then crashed
  before publishing its durable `read.parquet` (the classic case: OOM-killed
  writing an oversized sample) used to fail the retry with `prep_sample N has a
  sequence_range but no durable read.parquet … delete the prep_sample` — which
  defeated the runner's OOM memory-escalation, since the escalated attempt died
  on the one-shot mint contract before spending its extra memory. The step now
  reads the existing range back (`GET /sequence-range/{idx}`), validates it still
  covers exactly the FASTQ's read count, and reuses it, so the escalated retry
  completes with no orphaned range and no operator action. The GET route now
  accepts `sequence_range:mint` (via a new `require_any_scope` guard) in addition
  to `prep_sample:read`, letting the scope-minimal compute SA read back its own
  range; a count mismatch fails `BAD_INPUT`, a range deleted mid-retry fails
  `UNKNOWN_PERMANENT`. (#196)
- A dropped-row step re-run (a `run-preflight update-lane` redrive, or a `/run`
  redrive) no longer fails when its prior attempt dir is still on disk. The
  previous fix had the control-plane runner `shutil.rmtree` that dir, but a
  container step's output is owned by the SLURM job user with read-only (0550)
  dirs — the control-plane process can neither unlink nor chmod it, so the wipe
  died with `EACCES` ("could not clear stale attempt dir … Permission denied")
  and failed the ticket at `step_run`. The runner now **advances to a fresh
  attempt dir** instead of deleting the orphaned one (which is left intact for
  postmortem), reusing the existing per-attempt isolation rather than reaching
  into a foreign-owned tree. Resume-adoption still reuses a dir owned by a live
  progress row. (#195)
- `run-preflight update-lane` now actually takes effect on a `ticket run`
  redrive. Correcting a pool's preflight makes any samplesheet a *successful*
  `bcl_convert_prep` already produced stale, but a redrive fast-forwards that
  COMPLETED step (rebuilding its output from the workspace manifest), so the
  corrected lanes were never re-read and `bcl_convert` re-failed. The edit now
  drops the pool's COMPLETED `work_ticket_step` rows in the same transaction as
  the blob write, forcing the redrive to re-run from prep. Paired with a runner
  fix: a step that re-runs after its progress row was dropped (this path, or a
  `/run` redrive clearing failed rows) now clears its stale attempt dir first,
  so the prior run's read-only (0o440) output can't trip the output verifier or
  block the overwrite — guarded so resume-adoption never touches a live dir.
  (#193)
- `read-mask` (1.0.0) and `fastq-to-parquet` (1.3.0) workflows ran
  `persist-read-metrics` *after* `register-files`, but `register-files` MOVES
  `read_mask.parquet` out of the staging dir into permanent DuckLake storage —
  so `persist-read-metrics` re-opened a path that no longer existed and failed
  with `FileNotFoundError: read_mask parquet not found`. Reordered both
  workflows so `persist-read-metrics` reads the staged parquet first, then
  `register-files` moves it. (#181)
- The `qiita` / `qiita-admin` CLIs now emit an actionable error instead of a raw
  import-time traceback when launched against a **stale `qiita_common`** (the
  cross-package staleness trap: a plain `uv sync` skips reinstalling the
  unchanged-version path-dep, leaving stale sources in the venv). The console-script
  entry points now target a new import-clean shim (`qiita_control_plane.cli._bootstrap`)
  that imports the real CLI `main` lazily; a `qiita_common` `ImportError` is
  translated to a one-line message naming the exact fix
  (`uv sync --reinstall-package qiita-common`) and echoing the original error,
  while any unrelated `ImportError` is re-raised untouched (real bugs are never
  masked). Complements the `make redeploy` checkout-venv refresh above — this
  covers the case where the CLI is run without going through the redeploy script.
  The real `cli.user:main` / `cli.admin:main` are unchanged and still used by the
  shim, tests, and the redeploy import probe (#163)
- `make redeploy` now refreshes the operator's **checkout** CLI venv
  (`$QIITA_CLONE/qiita-control-plane/.venv`, where `uv run qiita` / `qiita-admin`
  resolve), closing a two-tree gap: `activate.sh` `uv sync`s only the `/opt/qiita`
  service venvs and the existing native-venv step covers only
  `qiita-compute-orchestrator`, so a pull that changed `qiita-common` without a
  version bump left the checkout CLI ImportError-ing on a stale path-dep until the
  operator ran `uv sync --reinstall-package qiita-common` by hand. A new step 6
  runs that reinstall as the checkout owner (never root), with a cheap skip when
  neither `qiita-common` nor `qiita-control-plane` changed in the pull and the venv
  still imports the CLI entrypoint (`FORCE_CLI_REFRESH=1` overrides). The skip
  delegates to a new pure `qiita_paths_touch_cli` helper in `deploy/_common.sh`
  (unit-tested), mirroring `qiita_paths_touch_native` (#163)
- `POST /work-ticket/{idx}/run` (`qiita ticket run`) can now redrive a FAILED
  multi-transition reference workflow instead of dead-ending at a `permanent`
  `IllegalStatusTransition`. The redrive resets a `failed` reference to `pending`
  (its only legal exit from `failed`) while keeping the COMPLETED step rows, but
  the runner's fast-forward used to *skip* those completed steps' `target_status`
  PATCHes — so the reference stayed at `pending` while the first re-run step tried
  to advance from mid-FSM (e.g. `minting → loading`), which is illegal. The
  fast-forward now RE-WALKS each completed step's status edge, advancing the
  resource forward along the FSM only when it is behind; on a normal
  startup-recovery resume (resource not rewound) the re-apply is a no-op or a
  rejected backward edge, both benign. Fixes redrives of `local-host-reference-add`
  / `host-reference-add` (which walk `pending → hashing → minting → loading →
  indexing → active`) after a `load`-step failure (#165)
- `mint-features` no longer starves the control-plane event loop on genome-scale
  reference loads. The in-process primitive read every `sequence_hash` from the
  manifest with a blocking, ORDER-BY (full-sort) DuckDB `fetchall()` and then
  built an O(N) Python list + dict + string-pair list — all on the single
  uvicorn event loop — so a human-comprehensive host reference pinned the API at
  high CPU and made every request (even a one-row `ticket status`) time out.
  Rewritten to stream the manifest in `_CHUNK_SIZE` batches (matching
  `write-membership`), drop the needless input sort, accumulate into a spillable
  DuckDB temp table de-duplicated at write time, and offload the final Parquet
  COPY to a thread. `_associate_genomes` likewise streams and resolves
  `feature_idx` via a DuckDB JOIN against the written feature_map instead of an
  in-memory mapping. The CP-side analog of the `hash_sequences` genome-scale fix
  below; output Parquet schema and idempotency are unchanged.
- `reference_load` no longer OOMs writing `reference_sequence_chunks` on
  genome-scale reference loads. Each per-part `COPY` did scan + join +
  `ORDER BY (feature_idx, chunk_index)` + write in one statement; the sort is a
  pipeline breaker, so it buffered the batch's wide ~64 KB chunk rows while the
  full ~30 GB glob scan and the 8-thread write buffers were all still live,
  blowing the cap (observed 38.7 GiB against ~39 GiB). Split into two phases per
  part: phase 1 streams the batch's chunks into a temp table (re-keyed
  hash → feature_idx, no sort), phase 2 sorts that isolated temp table (≤ one
  batch, never the 30 GB glob) and writes the part. The sort is kept on purpose
  — it clusters row groups so a `WHERE feature_idx IN (...)` DoGet prunes within
  a part, and feature_idx-ascending batches keep the parts a disjoint-range
  dataset for catalog-level file pruning; without it a point query would scan a
  whole part, since input order is parallel-scrambled upstream
  (`preserve_insertion_order=false`). Sibling to the `hash_sequences`
  genome-scale fix below.
- `hash_sequences` no longer OOMs writing `reference_sequence_chunks` on
  genome-scale reference loads. The per-batch output COPY joined the full
  `hashed` table (which grows 1:1 with the input), so at scale the optimizer
  could reorder that join ahead of the batch filter and materialize the entire
  file's `chunk_data` (observed 38 GiB against a 39 GiB cap). It also re-scanned
  the whole upload once per batch (~420× at 21M rows) and globally sorted by
  `sequence_hash` — a sort no consumer needs (`reference_load` re-keys to
  `feature_idx` with its own scan, the data plane's DoGet filters by
  `feature_idx`, and reassembly sorts `chunk_index` in-memory per feature).
  Replaced with a single streaming scan that relabels read_id → canonical
  `sequence_hash` in one pass: `canonical` (one narrow row per distinct hash) is
  always the lower-cardinality join input, so it's the hash-join build side and
  `chunk_data` streams through the probe to the writer — peak memory ~1 GB/thread
  and constant in file size, and the upload is scanned once instead of per batch.
  Output schema and canonical-dedup semantics are unchanged (one
  `part_00000.parquet` in the directory). (#155)
- `reference_load`'s per-batch chunk re-key (`sequence_hash` → `feature_idx`)
  carried the same latent OOM as the hash_sequences output side: each batch
  joined the full `feature_map` table, which grows 1:1 with the feature count,
  so at reference scale the optimizer could reorder that join ahead of the batch
  filter and materialize the whole glob's `chunk_data`. The join is now bounded
  to the batch's hashes by construction (an `fmb` CTE pre-filtered to the batch),
  so no join order can exceed one batch. The feature_idx-clustered, disjoint-range
  part layout (load-bearing for DuckLake / row-group pruning on DoGet's feature_idx
  lookups) and per-batch sort are unchanged. (#155)
- Native (`module:`) SLURM steps no longer collapse to a single CPU. The
  generated launcher ran a bare `srun`, but SLURM >= 22.05 srun no longer
  inherits `--cpus-per-task` from the batch allocation, so it laid the single
  task out at cpus-per-task=1 and its default `--cpu-bind` pinned that task —
  and every thread it spawned — to one allocated CPU. Native jobs run DuckDB
  with a multi-thread pool, so an N-CPU allocation silently ran on a single
  core (a TB-scale `stage_local_fasta` host-reference load crawled at ~75 MB/s
  on 1 of 4 allocated cores while the job's cgroup cpuset granted all of them).
  The launcher now exports `SRUN_CPUS_PER_TASK` from the allocation and passes
  `srun --cpu-bind=none`, letting the thread pool float across the whole cgroup
  cpuset (which already constrains the job to its allocation). Container
  (`apptainer exec`, no srun) steps were never affected. (#153)
- Data-plane file registration no longer collides with — and attempts to
  overwrite — an already-registered DuckLake data file. `register_files` placed
  each Parquet at `DATA_PATH/<table>/<producer-basename>`, but the reference-load
  job emits fixed basenames (`part_00000.parquet`, `reference_<table>.parquet`)
  on every load, so a second registration into a shared table targeted the first
  load's live, catalog-registered file. Because registered files are mode `0440`,
  the clobber surfaced on the live host as a cryptic `cross-fs copy failed …
  Permission denied` rather than silent lake corruption. The data plane now mints
  a unique, ticket-traceable destination name (`wt{work_ticket_idx}-{basename}`)
  — it owns lake-storage layout, as DuckLake does for its own INSERT-written
  files — and `move_file` refuses to overwrite an existing destination
  (`AlreadyExists`) as a hard safety net. The control plane threads the
  originating `work_ticket_idx` into the signed `register_files` action payload.
  Fixes loading more than one reference into a lake (and reloading). (#136)
- bcl-convert SIF auto-build no longer fails its `dnf install`. The
  `Apptainer.def` staged the licensed RPM to `/tmp/bcl-convert.rpm`, but a
  privileged `apptainer build` (the deploy's auto-build runs as root) bind-mounts
  the host `/tmp` over the image's `/tmp` during `%post`, shadowing the staged RPM
  → `Could not open … bcl-convert.rpm`. The RPM now stages to `/opt` (not
  bind-mounted), and `%post` runs under `set -e` with an explicit "RPM missing in
  image" guard so a future staging regression fails the build loudly instead of
  mid-`dnf`. (#134)
- SIF build no longer aborts when run as a service account from a directory that
  account can't read. `qiita_sif_build_inputs_hash` ran `find`, which restores its
  initial working directory on exit; a manual `sudo -u qiita-orch build-sif.sh`
  launched from an admin's `0700` home left `find` unable to restore that cwd, so
  it exited non-zero and tripped `set -o pipefail`, aborting the build before it
  started (`find: Failed to restore initial working directory`). `build-sif.sh`
  now `cd`s to `/` after its precondition checks (and the hash helper does so in
  a subshell), so the whole build — the hash `find` and the `apptainer exec`
  verify steps — is independent of the caller's cwd. All paths used are absolute,
  so the build keeps producing a `qiita-orch`-owned SIF without needing root or a
  `cd` workaround (#132 follow-up)
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

- `sequenced_sample.host_rype_reference_idx` / `host_minimap2_reference_idx`
  columns (their FKs and the minimap2-requires-rype CHECK drop with them). Host
  references are now a human-filter submission argument, not a sample column
  (PR 4 of the full-read+mask feature). Single drop migration, no
  expand/contract: the deploy wipes all legacy sequenced/pool samples first
  (their reads predate the lake-read model). (#175)
- The legacy synchronous step path: `POST /step/run`, `ComputeBackend.run_step`
  (+ the SLURM/Local overrides and the CO `_poll_until_terminal` poll loop),
  `ComputeBackendClient.run_step`, and the `StepRunRequest` / `StepRunResponse`
  wire models. The decoupled submit/status/result trio fully replaces it; CP
  and CO must deploy together since the route contract changed (#77)

[Unreleased]: https://github.com/the-miint/Qiita/commits/main
