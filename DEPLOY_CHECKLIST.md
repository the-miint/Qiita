# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's the changelog, see [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

- `[admin]` (#791) Only if `/etc/qiita/data-plane.env` sets `DUCKDB_TEMP_DIRECTORY` without `DUCKDB_MAX_TEMP_DIRECTORY_SIZE`: the per-connection spill cap is now derived at boot as that filesystem's free space ÷ local instances ÷ `DUCKLAKE_POOL_SIZE`, so together the connections may fill it, and the data plane refuses to boot if that comes out under `MIN_DERIVED_SPILL_MIB` (`qiita-data-plane/src/config.rs`) — hence before the restart. To keep today's cap: `sudo bash -c 'grep -q "^DUCKDB_MAX_TEMP_DIRECTORY_SIZE=" /etc/qiita/data-plane.env || printf "\nDUCKDB_MAX_TEMP_DIRECTORY_SIZE=4GiB\n" >> /etc/qiita/data-plane.env'`.
- `[admin]` (#791) Only if `DUCKLAKE_CATALOG_CONNSTR` in `/etc/qiita/data-plane.env` sets `application_name`: remove it, or the data plane refuses to boot (`sudo make preflight` fails `connstr/ducklake` on it).

### 2. One-time host setup

- `[operator]` **Confirm the mirror publishes the DuckDB 2.0.0 miint build** before the
  restart — every component now runs 2.0.0, and the stage step fetches from here.
  (#801)
  ```bash
  curl -fsSI https://ftp.microbio.me/pub/miint/v2.0.0/linux_amd64/miint.duckdb_extension.gz | head -1   # expect 200
  ```
- `[admin]` **Install the DuckDB 2.0.0 CLI for `make lake-shell` / `scripts/lake-gc.sh`.**
  Both refuse any `duckdb` that is not the version the data plane links. The release now
  ships a tarball, not a zip. Replace any per-account copy in `~/.local/bin` the same way.
  (#801)
  ```bash
  ( cd "$(mktemp -d)" \
    && curl -sSfL -O https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-linux-amd64.tar.gz \
    && tar -xzf duckdb-cli-linux-amd64.tar.gz && sudo install -m 0755 duckdb /usr/local/bin/duckdb )
  sudo -u qiita-data /usr/local/bin/duckdb --version   # expect v2.0.0 (Cyanoptera) d54cbbe3df
  ```

- `[admin]` **Let in-flight `long-read-assembly` tickets finish, and submit no new ones,
  before the restart.** The sync enables 1.0.2 and disables 1.0.1, and a ticket whose
  action is disabled fails when the restarted runner picks it up (`not found or
  disabled`). Expect `0`; resubmit anything that fails anyway at 1.0.2. (#813)
  ```bash
  sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -Atc "SELECT count(*) FROM qiita.work_ticket WHERE action_id = '\''long-read-assembly'\'' AND state IN ('\''pending'\'', '\''queued'\'', '\''processing'\'');"'
  ```
- `[admin]` (#791) On the catalog's Postgres server, as superuser: `GRANT pg_read_all_stats TO <role>;`, where `<role>` is the `user=` in `DUCKLAKE_CATALOG_CONNSTR` (`qiita_miint_lake_rw` per `first-deploy.md`). `verify-deploy`'s `catalog-connections` row fails without it ([`data-plane-scaling.md` § Catalog connections](docs/runbooks/data-plane-scaling.md#catalog-connections)).

### 3. Migrations

- `[operator]` (#779) `make migrate` applies `20261024000000_metadata_visibility.sql` (four new SQL functions, `qiita.metadata_field_tier`, `qiita.metadata_required_tier` and `qiita.{biosample,prep_sample}_metadata_visibility`, and corrected COMMENTs on `qiita.{biosample,prep_sample}_field_exception`; no table, row or grant changes).
- `[operator]` (#779) `make migrate` applies `20261024000001_metadata_write_keeps_parent_updated_at.sql` (the biosample and prep_sample `set_updated_at` triggers skip the metadata-touch UPDATE, and the two touch functions skip a no-op UPDATE; column COMMENTs on `updated_at` / `last_metadata_change_at`; no table or row changes).
- `[operator]` (#829) `make migrate` applies `20261104000000_alignment_lifecycle.sql` (lifecycle columns and CHECKs on `qiita.alignment_definition`, `invalidated` on `qiita.alignment_sample`, `abandoned` on `qiita.block`, `qiita.mint_alignment_definition` refusing a deprecated config, `qiita.qc_verdict` restated; no row changes).
- `[operator]` (#831) `make migrate` applies `20261105000000_exported_retire_wording.sql` (`qiita.retire_detached_exported_identifier` and `qiita.retire_detached_exported_processing` re-created with a reworded retire reason, and their COMMENTs; no table or row changes).

### 4. Deploy

- `[admin]` (#767) `activate.sh` now waits, before any restart, for the data plane to go idle (no DoGet stream, DoPut or registration in flight), up to 3900 s, printing `waiting for qiita-data-plane@50051 to go idle …` once a minute; it then restarts whether or not it went idle. `QIITA_DP_IDLE_WAIT_S=<s>` in the deploy's environment changes the bound (0 skips it). The deploy stops before any restart if that value is not whole seconds or the wait cannot query the catalog; `QIITA_DP_IDLE_WAIT_S=0` gets past the second. This deploy still stops the old data-plane binary at once on SIGTERM: the drain ships with the new binary, so from the next deploy the data plane's own restart can take up to the unit's `TimeoutStopSec=3900` too.
- Nothing extra for a single instance (#363): with no `QIITA_DATA_PLANE_*` keys in
  `/etc/qiita/data-plane.env`, the deploy runs the one instance `@50051` as before. To run
  more instances or add data planes on other hosts, see
  [`docs/runbooks/data-plane-scaling.md`](docs/runbooks/data-plane-scaling.md) before
  deploying.

### 5. Verify

- `[admin]` (#767) `systemctl show -p TimeoutStopUSec qiita-data-plane@50051` prints `TimeoutStopUSec=1h 5min`.

- `verify-deploy`'s data-plane rows (#363): `health/data-plane` is now
  `health/data-plane@<port>`, one row per instance; `health/data-plane-peer@<host:port>`
  appears per peer; `health/data-plane-lb` checks nginx's loopback listener
  `127.0.0.1:50050` (skipped when the TLS files are absent); `health/data-plane-upstream`
  fails when verify cannot read the members from the rendered nginx config, and is skipped
  when grpcurl is missing or `SKIP_HEALTH=1`; `data-plane-units/<unit>` fails for a running
  or enabled `qiita-data-plane@` unit whose port is not in `QIITA_DATA_PLANE_PORTS`, and
  the deploy itself stops before installing anything when it finds such a unit, unless
  run with `QIITA_DP_DISABLE_REMOVED=1`, which disables and stops it;
  `commit/data-plane@<port>` and `commit/data-plane-peer@<host:port>` fail for a member
  not running this host's deployed commit (`BUILD_COMMIT` in
  `/opt/qiita/data-plane/build.env`, which the deploy writes);
  `dp-memory-max/qiita-data-plane@<port>` (#791) fails when a unit's `MemoryMax` differs
  from `QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX`, and is skipped when that key is unset;
  `catalog-connections` (#791) lists every connected instance as `<host>:<port> pool=<n>`
  and fails when their pools plus the other catalog sessions exceed the catalog server's
  `max_connections` less its reserved slots, or when the bucket-2 grant is missing. Apart
  from `catalog-connections`, a host running only `qiita-data-plane@50051` is unaffected.

- **The staged miint builds are present, and the rebuilt `long-read-assembly` images carry
  DuckDB 2.0.0** — each image loads miint only from its own DuckDB version's directory.
  Expect `DUCKDB_200_OK`. (#801)
  ```bash
  sudo -u qiita-orch bash -c 'set -a; . /etc/qiita/compute-orchestrator.env; set +a
  for v in 2.0.0 1.5.5 1.5.4; do
    test -s "$MIINT_EXTENSION_DIRECTORY/v$v/linux_amd64/miint.duckdb_extension" \
      || { echo "no staged miint for DuckDB $v"; exit 1; }
  done
  cd /tmp && for s in long-read-assembly-assemble-1.0.2.sif long-read-assembly-checkm-1.0.1.sif; do
    apptainer exec --no-home "${PATH_DERIVED}/images/$s" \
      python3 -c "import duckdb; print(duckdb.__version__)" | grep -Fxq 2.0.0 \
      || { echo "$s is not on DuckDB 2.0.0"; exit 1; }
  done && echo DUCKDB_200_OK'
  ```
- **One myloasm assembly at `long-read-assembly` 1.0.2 lands its provenance.** Submit one
  prep_sample, wait for the ticket to complete, look up the run it minted, then check that
  run's six `myloasm_*` tables. Expect `runs`, `contigs`, `layouts` and `mappings` above 0
  (`runs` is one per myloasm log, so 1 unless myloasm restarted); `links` and `splits`
  can be 0; `reads_not_in_read` and `primaries_without_feature` must be 0. (#813)
  ```bash
  PS='<prep_sample_idx>'; MASK='<mask_idx completed for it>'
  qiita ticket submit --action-id long-read-assembly --action-version 1.0.2 \
      --prep-sample-idx "$PS" --context-json "{\"mask_idx\": $MASK, \"assembler\": \"myloasm\"}"
  # once `qiita ticket status <ticket>` says completed:
  RUN=$(sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a
    psql "$DATABASE_URL" -Atc "SELECT a.processing_idx FROM qiita.assembly_sample a JOIN qiita.processing p USING (processing_idx) WHERE a.prep_sample_idx = '"$PS"' AND a.state = '\''completed'\'' AND p.workflow = '\''long-read-assembly'\'' AND p.version = '\''1.0.2'\'' AND p.params->>'\''assembler'\'' = '\''myloasm'\'';"')
  echo "run=$RUN"   # expect one processing_idx
  bash scripts/lake-shell.sh --no-cp -c "
    SELECT (SELECT count(*) FROM qiita_lake.myloasm_run      WHERE prep_sample_idx = $PS AND processing_idx = $RUN) AS runs,
           (SELECT count(*) FROM qiita_lake.myloasm_contig   WHERE prep_sample_idx = $PS AND processing_idx = $RUN) AS contigs,
           (SELECT count(*) FROM qiita_lake.myloasm_layout   WHERE prep_sample_idx = $PS AND processing_idx = $RUN) AS layouts,
           (SELECT count(*) FROM qiita_lake.myloasm_mapping  WHERE prep_sample_idx = $PS AND processing_idx = $RUN) AS mappings,
           (SELECT count(*) FROM qiita_lake.myloasm_link     WHERE prep_sample_idx = $PS AND processing_idx = $RUN) AS links,
           (SELECT count(*) FROM qiita_lake.myloasm_split    WHERE prep_sample_idx = $PS AND processing_idx = $RUN) AS splits,
           (SELECT count(*) FROM qiita_lake.myloasm_mapping m
              ANTI JOIN (SELECT sequence_idx FROM qiita_lake.read WHERE prep_sample_idx = $PS) r USING (sequence_idx)
              WHERE m.prep_sample_idx = $PS AND m.processing_idx = $RUN) AS reads_not_in_read,
           (SELECT count(*) FROM qiita_lake.myloasm_contig
              WHERE prep_sample_idx = $PS AND processing_idx = $RUN
                AND dereplication_status = 'primary' AND feature_idx IS NULL) AS primaries_without_feature"
  ```
- **The DuckLake catalog is still format 1.0.** Expect `1.0`; the attach itself fails if
  the catalog was ever migrated. (#801)
  ```bash
  bash scripts/lake-shell.sh --no-cp -noheader -list \
    -c "SELECT value FROM qiita_lake.options() WHERE option_name = 'version'"
  ```
- `make verify-deploy`'s action list shows `myloasm-provenance-recover` 1.0.0 enabled.
  (#830)

### 6. After the deploy verifies green

- Only on a host running more than one data-plane instance (#363): point the control plane
  at nginx's loopback listener so its calls spread across them —
  [`data-plane-scaling.md` § The control plane through nginx](docs/runbooks/data-plane-scaling.md#the-control-plane-through-nginx).
  Undo it before any rollback to a commit without that listener.
- `[admin]` **Recover the provenance of the myloasm runs finished before this deploy**
  (long-read-assembly 1.0.0/1.0.1; the dry run counts them). Each run gets one
  `myloasm-provenance-recover` ticket, which reads the `assemble` output left in that run's
  ticket workspace. Run it soon after the deploy, since those files last only as long as
  the scratch workspace does. (#830)
  ```bash
  # DATABASE_URL and PATH_SCRATCH exported, as for any `qiita-admin backfill`;
  # QIITA_TOKEN is a system_admin's PAT (only --execute reads it).
  qiita-admin backfill myloasm-provenance                       # dry run: counts per status, and each run it cannot recover (residue), with the reason
  qiita-admin backfill myloasm-provenance --execute --limit 5
  ```
  Once those five tickets complete, run bucket 5's 1.0.2 provenance query for each one, with
  `PS`/`RUN` taken from the `prep_sample <PS> run <RUN> -> ticket <T>` lines that the
  `--limit 5` run printed. Expect the same results as that check. Measure each one's
  peak memory and elapsed time against the step's unmeasured 16 GB / 2 h baseline,
  for every attempt (an OOM retry runs at a higher memory floor):
  `sacct -j <job> -o MaxRSS,Elapsed`, with each `<job>` from
  `SELECT attempt, state, slurm_job_id FROM qiita.work_ticket_step WHERE work_ticket_idx = <T> AND slurm_job_id IS NOT NULL ORDER BY attempt`.
  Then submit the rest, paced:
  `qiita-admin backfill myloasm-provenance --execute --rate 10`.

  Each pass submits one ticket per prep_sample, so rerun it once the pass's tickets finish.
  You are done when the dry run counts no writable and no in-flight runs and every
  residue line has an explanation. `--retry-failed` resubmits runs whose recovery
  ticket ended without completing (failed, cancelled or no_data). If the dry run stops
  with a permission error, run it as an account that can read the ticket workspaces.
  On #827, which removes the one-off, post the measurements and the
  dry run's status counts and `residue, <kind>: <n>` lines, each kind with its
  explanation. Name no run, ticket or job by its id there: the per-run lines carry
  `_idx` values, and the repository is public.

### Notes (no host action)

- (#767) A killed DoPut leaves `staging/uploads/<idx>/upload.parquet.partial`, which nothing reclaims, as nothing reclaims finished uploads. `qiita-admin masked-read-export` gains `--retry-seconds` (default 3900, the data-plane unit's stop timeout).
- **DuckDB 1.5.5 → 2.0.0 everywhere; the DuckLake catalog stays at format 1.0.** (#801)
  - The redeploy re-stages miint and httpfs into `MIINT_EXTENSION_DIRECTORY/v2.0.0/` on its
    own, because `stage-miint --check` sees the version change. `make verify-deploy`'s
    `compute-readiness` and `cp-miint` checks LOAD the new build.
  - The `checkm` (`-1.0.1`) SIF auto-rebuilds on deploy to pick up 2.0.0, and the new
    `long-read-assembly-assemble-1.0.2.sif` is built on it. `-assemble-1.0.0.sif` is no
    longer built, so it stays on the DuckDB it has (#813).
  - **Work that loads miint between the bucket-4 restart and the redeploy's miint stage
    fails at LOAD**: `v2.0.0/` is staged after everything moves to 2.0.0. Resubmit
    anything that failed there; #676 would stage first and close the window.
  - **Keep `v1.5.4/` and `v1.5.5/`.** The frozen `long-read-assembly-checkm-1.0.0.sif` still
    loads miint from `v1.5.4/`, the frozen `long-read-assembly-assemble-1.0.0.sif` (named by
    1.0.0 and 1.0.1) from `v1.5.5/` (#813), and `v1.5.5/` is also what a rollback runs.
    Staging never removes an old version dir.
  - Each data-plane instance installs the 2.0.0 `ducklake` and `postgres` extensions under
    its `HOME` on first start, so it needs to reach extensions.duckdb.org.
  - That ducklake writes catalog format 1.1 by default, so every attach names
    `DUCKLAKE_VERSION '1.0'` and the catalog is not migrated. Never attach it with
    `AUTOMATIC_MIGRATION` by hand: the migration is one-way. A rollback to the 1.5.5 build
    still reads everything 2.0 wrote to the catalog.
  - `host_filter` can count a read pair as host that it did not before: miint's 2.0
    build maps a pair under the `sr` preset as native minimap2 does
    (duckdb-miint#341).
- **`long-read-assembly` 1.0.2 keeps myloasm's read-level provenance in six new DuckLake
  tables.** (#813)
  - The data plane creates `myloasm_contig`, `myloasm_layout`, `myloasm_mapping`,
    `myloasm_link`, `myloasm_split` and `myloasm_run` on its next start; nothing to run.
  - The assemble image lands under a new filename, `long-read-assembly-assemble-1.0.2.sif`,
    built on deploy. **Keep `long-read-assembly-assemble-1.0.0.sif`**: 1.0.0 and 1.0.1 still
    name it and no spec builds it any more (`sif-build.d/assemble.env`).
  - Runs completed before this deploy have no provenance rows until bucket 6's
    recovery writes them (#830).
- (#791) Optional `QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX` in `/etc/qiita/data-plane.env` sets `MemoryMax=` per local data-plane instance ([`data-plane-scaling.md` § More instances on this host](docs/runbooks/data-plane-scaling.md#more-instances-on-this-host)). Every deploy now writes `/etc/systemd/system/qiita-data-plane@<port>.service.d/instance-limits.conf` (instance count; `MemoryMax=` only when the key is set) and stops if `data-plane.env` sets `QIITA_DATA_PLANE_LOCAL_INSTANCES`, or sets `DUCKDB_MEMORY_LIMIT` to more than the key ÷ `DUCKLAKE_POOL_SIZE`; `preflight` gains a `dp-memory-max` row, skipped when the key is unset. With `DUCKDB_THREADS` unset, each instance's derived threads are also divided by the local instance count (no change at one instance).
- (#779) Soft API-contract change: biosample and prep_sample metadata
  reads omit any value above the caller's tier on the study that wrote it (no placeholder).
  The study-scoped biosample and sequenced-sample reads (`GET /study/{s}/biosample/{b}`,
  `POST /study/{s}/biosample/by-unique-field`, `GET /study/{s}/sequenced-sample/{q}`) now
  admit `viewer` and above instead of only `admin`; `GET /biosample/{b}` drops global values
  the caller's tier does not reach, including for the biosample's owner. An
  owner-biosample-id value requires `member` or above, whatever its field's tier.
  `last_metadata_change_at` now reflects only the metadata in the response, and
  `updated_at` / `effective_updated_at` no longer move on metadata writes; the ETag is the
  later of `last_metadata_change_at` and the record's timestamp, so a client that read
  `updated_at` to detect metadata changes should read `last_metadata_change_at` or the
  ETag instead. The study-scoped reads' ETag no longer works as If-Match for
  `PATCH /biosample/{b}` or `PATCH /sequenced-sample/{q}`; a client uses the ETag from the
  GET on the PATCH's own path (the `qiita` CLI already does).
- (#feat/metadata-tier-writes) Soft API-contract change: the study-scoped biosample and
  sequenced-sample metadata PATCHes and the study-field create and edit routes
  (`/study/{s}/biosample-field`, `/study/{s}/prep-sample-field`) now admit `member` and
  above instead of only `admin`. A write naming a field, or touching a value, above the
  caller's tier is a 403 that writes nothing. A collision with a value the caller cannot
  read is that same 403 instead of a 409, and no longer names the other study. Setting a
  field's `tier_override` still requires `admin`; imports are unchanged.
- (#824) Soft API-contract change: `DELETE /mask-definition/{mask_idx}`,
  `qiita-admin mask delete` / `mask purge-failed` and the `mask_definition:delete` scope are
  removed. A PAT mint requesting that scope now gets `422 unknown scopes`; an existing token
  carrying it keeps working without it. Deprecate a mask or invalidate its runs instead.
- (#829) Soft API-contract change: `DELETE /alignment-definition/{alignment_idx}` and the
  `alignment_definition:delete` scope are removed. A PAT mint requesting that scope now gets
  `422 unknown scopes`; an existing token carrying it keeps working without it. The new
  `alignment_definition:lifecycle` scope reaches `system_admin` through
  `ROLE_IMPLIED_SCOPES`, with no host action. Deprecate an alignment, invalidate its runs,
  or abandon a failed plan instead.
- (#829) Soft API-contract change: deprecating a mask or an alignment
  (`PATCH /mask-definition/{mask_idx}/status`, `PATCH /alignment-definition/{alignment_idx}/status`)
  now returns 409, listing the tickets, while any work ticket under it is unfinished.
  Cancel or wait for them first. The pool alignment listing
  (`GET /sequencing-run/{run}/sequenced-pool/{pool}/alignment`) adds `status`,
  `deprecation_reason` and `superseded_by`; its cohort route returns 409 for a
  deprecated alignment.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
