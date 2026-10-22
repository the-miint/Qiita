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

_None yet._

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

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
