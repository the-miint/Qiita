# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's the changelog, see [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

- `[operator]` (#713) `data-plane.env` gains six optional vars, all documented in
  `.env.data-plane.example`: `DUCKLAKE_POOL_SIZE`, `DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`,
  `DUCKDB_TEMP_DIRECTORY`, `DUCKDB_MAX_TEMP_DIRECTORY_SIZE`, `FLIGHT_MAX_CONCURRENT_DOPUT`.
  Left unset the data plane starts on its own
  values: `memory_limit` derived from the memory it finds free, `threads` from the CPU
  allowance, the spill root `PATH_SCRATCH/duckdb-spill` (which bucket 2 creates), and fixed defaults for the spill cap
  and for the two counts (pool size and concurrent uploads, 8 each). Boot fails, among other
  cases, if the pool size leaves under 512 MiB per connection — measured as the point below which
  the export DoAction cannot run — naming both ways out, and if the spill root cannot be
  created or written. Setting `DUCKDB_MEMORY_LIMIT` explicitly is what stops
  boot depending on how much was free at the moment of a restart. Set the pool size
  deliberately as well: it is both the number of DoGets and DoActions one data-plane process
  serves at once — uploads have their own bound, `FLIGHT_MAX_CONCURRENT_DOPUT`, also 8 by
  default — and a catalog-connection budget: each attached connection holds one
  lake-Postgres backend while it lives, against `max_connections = 100` (3 superuser-reserved)
  shared with the control plane and every other data-plane process. The default is 8 per
  process. `FLIGHT_MAX_CONCURRENT_DOPUT` is the one value nothing derives and nothing fails
  boot over: extrapolated from the measured cost of four, eight concurrent uploads at the
  batch size the CLIs send hold ~18 GiB of decode buffers, which this host has and a smaller
  one does not — lower it there.

### 2. One-time host setup

- `[admin]` (#713) Create the data plane's spill root before the restart — each pooled
  connection spills into a directory of its own under it, and boot fails if it cannot:
  `sudo install -d -o qiita-data -g qiita-data -m 0700 <scratch>/duckdb-spill`. Skip it only
  if `DUCKDB_TEMP_DIRECTORY` names another root the data plane can write. Every stop or
  restart leaves the stopped process's directories there, named `<host>-<pid>-…`; the next
  boot on the same host removes those whose process is gone.

### 3. Migrations

- `[operator]` (#731) `make migrate` applies `20261021000000_sequenced_sample_download_failure.sql` (two nullable columns and a both-or-neither CHECK on `qiita.sequenced_sample`; no backfill).
- `[operator]` (#758) `make migrate` applies `20261022000000_lifecycle_provenance_fields.sql` (four lifecycle CHECKs tightened: a live row may hold none of its provenance fields; `mask_definition`, `mask_sample`, `processing`, `assembly_sample`). It fails atomically if a live row holds one or two of them, so run this query before `make migrate`; it must list no rows, and a listed row needs its stray fields cleared or the row withdrawn (status or state and all three fields set):
  `SELECT 'mask_definition', mask_idx::text FROM qiita.mask_definition WHERE num_nonnulls(deprecated_at, deprecated_by_idx, deprecation_reason) IN (1,2) UNION ALL SELECT 'mask_sample', mask_idx||'/'||prep_sample_idx FROM qiita.mask_sample WHERE num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) IN (1,2) UNION ALL SELECT 'processing', processing_idx::text FROM qiita.processing WHERE num_nonnulls(deprecated_at, deprecated_by_idx, deprecation_reason) IN (1,2) UNION ALL SELECT 'assembly_sample', processing_idx||'/'||prep_sample_idx FROM qiita.assembly_sample WHERE num_nonnulls(invalidated_at, invalidated_by_idx, invalidation_reason) IN (1,2);`
- `[operator]` (#761) `make migrate` applies `20261023000000_mask_sample_empty_ingest_comment.sql` (comment only: the `qiita.mask_sample` table COMMENT names the new empty-sample writer).

### 4. Deploy

- `[admin]` (#731) Before stopping the control plane, confirm no `download-ena-study` ticket is in flight; if the query lists any, wait for them to finish. `download-ena-study` 1.0.0 is edited in place (`qiita-admin actions sync` in `activate.sh` re-syncs it), and a ticket whose ingest step finished on the old code fails on the missing `run_outcomes` output:
  `SELECT work_ticket_idx, state FROM qiita.work_ticket WHERE action_id = 'download-ena-study' AND state IN ('pending','queued','processing');`
  If one was missed and ends FAILED, redrive it (`POST /work-ticket/{idx}/run`): a download redrive drops the completed step rows, so ingest re-runs on the new code and skips runs already stored.

### 5. Verify

- `[operator]` (#713) `make verify-deploy` gains two rows per running data plane, read from
  that instance's journal: `dp-pool/<unit>` prints the pool the instance built (size,
  per-connection `threads` and `memory_limit`, how many concurrent uploads it admits, the
  ceiling they were divided from and which detector answered) and fails if the ceiling came
  from the fallback rather than this host, and `dp-pool/<unit>/reset` fails if any connection
  failed its reset and was rebuilt. Read the printed
  line — on this host the ceiling should come from `/proc/meminfo MemAvailable`, since the
  unit sets no cgroup limit. No separate journal read by hand; `SKIP_DP_POOL=1` drops the
  row.
- `[admin]` (#731) `qiita.action` lists `download-ena-study` 1.0.0 with three steps, ending in `persist-ena-run-outcomes`, and `\d qiita.sequenced_sample` shows `download_failure_reason` and `download_failure_type`.

### 6. After the deploy verifies green

- `[operator]` (#761) Re-run masking for prep_samples whose ingest stored no reads, so each is recorded as masked. List the per-sample tickets:
  `SELECT wt.work_ticket_idx, wt.action_id, wt.state, wt.prep_sample_idx, ss.sequenced_pool_idx FROM qiita.work_ticket wt JOIN qiita.sequenced_sample ss ON ss.prep_sample_idx = wt.prep_sample_idx WHERE (wt.action_id = 'read-mask' AND ((wt.state = 'failed' AND wt.failure_reason LIKE 'no stored reads%') OR (wt.state = 'no_data' AND wt.mask_idx IS NULL))) OR (wt.action_id = 'fastq-to-parquet' AND wt.state = 'no_data' AND wt.mask_idx IS NOT NULL AND NOT EXISTS (SELECT 1 FROM qiita.mask_sample ms WHERE ms.mask_idx = wt.mask_idx AND ms.prep_sample_idx = wt.prep_sample_idx)) ORDER BY wt.work_ticket_idx;`
  Redrive a `failed` row with `qiita ticket run <work_ticket_idx>`. A `no_data` row is terminal, so re-submit it with the same action_context (`qiita ticket submit`). A prep_sample whose newest ingest shows it empty ends `no_data` with its `mask_idx` set (`qiita ticket status`), after which `qiita mask samples --mask-idx <mask_idx> --sequenced-pool-idx <pool>` lists it `completed`. One that no ingest shows empty (it has none, or its newest did not end that way) is not recorded: an expected-empty control ends `no_data` unrecorded, and any other prep_sample fails again with "must be ingested". That covers a prep_sample whose newer ingest failed (re-ingest it) or is still running (wait for it) after an older one ended `no_data`, and an empty golay-demux barcode, which no ingest record covers (#760). Known case: ticket 12402 (prep_sample 31483, pool 25056, mask 11).
  Block-masked pools with an empty prep_sample still unrecorded are listed by `SELECT DISTINCT ss.sequenced_pool_idx FROM qiita.work_ticket wt JOIN qiita.block_member bm ON bm.block_idx = wt.block_idx JOIN qiita.sequenced_sample ss0 ON ss0.prep_sample_idx = bm.prep_sample_idx JOIN qiita.sequenced_sample ss ON ss.sequenced_pool_idx = ss0.sequenced_pool_idx JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx WHERE wt.action_id = 'read-mask-block' AND ps.retired = false AND ss.ena_status IS NULL AND NOT EXISTS (SELECT 1 FROM qiita.sequence_range sr WHERE sr.prep_sample_idx = ss.prep_sample_idx) AND NOT EXISTS (SELECT 1 FROM qiita.mask_sample ms WHERE ms.prep_sample_idx = ss.prep_sample_idx);`. Re-run `submit-block-mask-pool --only-missing` for each exactly as it was first submitted: no host flags for a plan that resolved host filtering itself (the default), or `--force` with the same reference flags for a forced one. Nothing in the database records `--force`, so ask whoever submitted it; any other flags tile its prep_samples again under a different mask. The plan records the empty prep_samples (`samples_completed_empty`) and tiles nothing already gated. Then re-run the masked-read export for the affected pools.
- `[operator]` (#764) Re-drive long-read-assembly ticket 12513 (prep_sample 31407), failed at `binning` because no contig reached metaWRAP's 16,000 bp minimum: `qiita ticket run 12513`. It resumes at `binning`, which now finishes with no bins; confirm with `qiita ticket status 12513`.

### Notes (no host action)

- (#713) Past the pool size, the data plane answers gRPC `RESOURCE_EXHAUSTED` rather than
  serving the request, so a fan-out wider than the pool turns into held or retried work
  rather than slow work. No client retries the call itself. Where a refusal lands today:
  a native job step gets `data_plane_transient` and the runner re-dispatches it within the
  ticket's `max_retries`; a control-plane pre-loop fetch is held for an operator redrive;
  a DoAction from the control plane's action library is re-run by the runner when a
  workflow `action:` entry made it, and answered with a 502 when a REST route did. #539
  and #582 carry the client half — a reader that retries the call itself instead of
  failing the step. The Flight edge's own per-address admission (`deploy/nginx/qiita.conf`)
  is what to weigh the pool size against.
- (#713) Per-request wall time rises with the division. A request used to take every core;
  it now takes `threads`, and measured on this host one masked read fitted
  `25.3 s + 611.8 s / threads` — so at 48 cores and the default pool of 8 it is ~127 s where
  it was ~38 s. Concurrency is what the change buys back.
- (#713) A write under contention holds its connection while it retries: the registration
  path sleeps inside its 120 s commit budget, and every writer takes the same lock, so a
  write storm can occupy the whole pool and reads are refused for that window.
- (#731) `GET /ena-import-batch/{idx}` items can read `completed_partial` (a pool landed with some runs failed permanently), and `ena_runs[].status` can be `download_failed`, with a new `failure_type`; clients matching on the closed set of states or statuses must accept them. `submit-ena-import --watch` exits 1 on a `completed_partial` item. A partially landed ticket is `completed`, so the submitter gets the work-ticket digest unless they opted out of processing emails.
- (#751) `download-ena-study` 1.0.0 gains an optional `run_concurrency`. The optional control-plane `ENA_DOWNLOAD_RUN_CONCURRENCY` (1-8, default 4) stays at 4 in production (see `docs/runbooks/ena-import.md`).
- (#761) `POST …/block-mask-plan` returns two new counts, `samples_completed_empty` and `samples_skipped_invalidated`, and `samples_skipped_no_reads` now counts only prep_samples with no reads that no ingest shows empty (they have none, or their newest did not end that way). It also answers 409 when a prep_sample gains a range or has a newer ingest submitted while the plan runs; planning again tiles new reads, and counts a prep_sample whose newer ingest has not finished in `samples_skipped_no_reads`. A prep_sample that ingest found empty is now recorded as masked, so the pool masking rollup counts it `completed` rather than `no_data`, and the read-metrics rollup counts it in `samples_zero_reads`. Minting a sequence_range for it later removes that record; a newer ingest that fails or has not finished does not, so the export serves it empty until reads are stored. The block plan now resolves host filtering for such a prep_sample too, so one with unresolvable host metadata refuses the plan, as it already did for `submit-host-filter-pool`.
- (#764) The `long-read-assembly-binning-1.0.0.sif` auto-rebuilds on deploy to pick up `binning.sh`'s minimum-contig-length check; long-read-assembly 1.0.0 and 1.0.1 both run that image, so both get it. Its `%test` now also runs `binning-min-contig-check.sh`, which runs metaWRAP twice on a small fixture; a build failure naming that script means the re-solved binners changed the behaviour the check rests on, and its stderr says which. The deploy then stops before any restart (`build-sifs.sh`), so services stay on the old build; the fix is a change to `binning.def` or `binning.sh`, not a host step.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
