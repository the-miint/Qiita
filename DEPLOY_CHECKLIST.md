# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; `## Deployed history` archives past ones.

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

- **Email notification (control-plane).** To turn on work-ticket terminal-digest emails, set `SMTP_*` in `/etc/qiita/control-plane.env` before the restart; leaving `SMTP_HOST` unset keeps the no-op transport (no mail). None are secrets (relay is no-auth, IP-allowlisted, validated end-to-end). Optional `NOTIFY_*` knobs keep their defaults if unset. (#238)
  ```bash
  sudo bash -c 'cat >> /etc/qiita/control-plane.env <<EOF
  SMTP_HOST=its-smtp-master.ucsd.edu
  SMTP_PORT=25
  SMTP_FROM=donotreply@ucsd.edu
  SMTP_STARTTLS=opportunistic
  EOF'
  ```
- **`PATH_PERSISTENT` now required for the data plane.** It was optional (fell back to `$TMPDIR/qiita`); it is now a required, absolute `from_env()` var, so a data-plane instance that lacks it will **refuse to start**. Confirm it is set in `/etc/qiita/data-plane.env` before the restart — it must already point at the durable lake root the running instances use (deriving `PATH_PERSISTENT/ducklake`); if it is somehow unset today the lake has been landing under `/tmp` and needs operator attention before this deploy. No value change is intended — this only makes the existing requirement fail loudly. (#246)
  ```bash
  grep -q '^PATH_PERSISTENT=' /etc/qiita/data-plane.env || echo 'MISSING: set PATH_PERSISTENT in /etc/qiita/data-plane.env'
  ```

### 2. One-time host setup

- **CheckM reference database for `long-read-assembly`.** The workflow's `checkm`
  step needs CheckM's ~1.4 GB reference data, which is deliberately NOT baked into
  the SIF. Stage it once under `PATH_DERIVED` and make the container see it at run
  time — the `checkm.sh` entrypoint reads `CHECKM_DATA_PATH` (default
  `/opt/checkm_data`), so the DB dir must be bind-mounted there (or
  `QIITA_CHECKM_DB` set to its in-container path). (#255)
  ```bash
  # verify the tarball against CheckM's published checksum before extracting.
  sudo -u qiita-orch bash -c 'mkdir -p "$PATH_DERIVED/checkm_data" && cd "$PATH_DERIVED/checkm_data" \
    && curl -LO https://data.ace.uq.edu.au/public/CheckM_databases/checkm_data_2015_01_16.tar.gz \
    && tar xzf checkm_data_2015_01_16.tar.gz && rm checkm_data_2015_01_16.tar.gz'
  ```
  REQUIRED before the first `long-read-assembly` run: the `checkm` step now **fails
  loud** (non-zero exit) when the DB is absent or misconfigured — it no longer
  degrades to an empty `checkm_dir` — so a MAG-producing sample's ticket cannot
  COMPLETE until the DB is staged AND the container can see it (bind-mount it to
  `CHECKM_DATA_PATH`, or set `QIITA_CHECKM_DB` to its in-container path). Wiring the
  bind into the container step is part of this setup (the orchestrator binds only
  declared step inputs today). assemble/binning/bin_refine are unaffected.

### 3. Migrations

- **Email-notification schema.** `make migrate` applies both migrations: `..._email_notification.sql` (adds `work_ticket.notified_at` / `notify_attempts`, creates `qiita.email_receipt`, backfills existing terminal tickets as already-notified) and `..._work_ticket_notified_idx.sql` (a `CREATE INDEX CONCURRENTLY` — runs outside a txn; if interrupted, drop the leftover `INVALID` index and re-run). No out-of-band steps. (#238)
- **CLI-login plaintext-PAT scrub.** `make migrate` applies `..._cli_login_code_scrub_plaintext.sql`: drops the `NOT NULL` on `cli_login_code.plaintext_pat` and nulls it on already-consumed/expired rows (reclaims plaintext tokens left at rest). No out-of-band steps. (#241)
- Bulk-block read-mask schema (four additive migrations, plain `make migrate`, no
  backfill): (#243)
  - `20260701000002_block.sql` — `block` + `block_member` core (the compute unit +
    the block↔sample cover-map).
  - `20260701000003_mask_sample.sql` — the per-`(mask_idx, prep_sample)` completion
    gate the masked-read export + future alignment read.
  - `20260701000004_scope_target_kind_add_block.sql` — `ALTER TYPE
    qiita.scope_target_kind ADD VALUE 'block'` (own `transaction:false` file; dbmate
    applies it standalone).
  - `20260701000005_work_ticket_block.sql` — `work_ticket.block_idx` scope arm +
    extended scope-target CHECK + `work_ticket_one_in_flight_per_block` unique index.
- **Assembly-processing schema for `long-read-assembly`.** `make migrate` applies
  `20260707000000_assembly.sql`: creates `qiita.processing` (+ the idempotent
  `qiita.mint_processing` mint function) and `qiita.assembly_membership`. Plain
  `make migrate`, no backfill or out-of-band setup. (The four DuckLake assembly
  tables are auto-created at data-plane startup — see the note below.) (#255)

### 4. Deploy

_None yet._

### 5. Verify

- Confirm the `read-mask-block/1.0.0` workflow synced into `qiita.action` (the block
  runner path — synced by `qiita-admin actions sync` inside `activate.sh`, covered by
  `make verify-deploy`'s `qiita.action` list; this asserts the specific new action):
  (#243)

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, version, target_kind FROM qiita.action WHERE action_id='read-mask-block'"
  # expect: read-mask-block|1.0.0|block
  ```

- Confirm the `long-read-assembly/1.0.0` workflow synced into `qiita.action`
  (synced by `qiita-admin actions sync` inside `activate.sh`): (#255)

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, version, target_kind FROM qiita.action WHERE action_id='long-read-assembly'"
  # expect: long-read-assembly|1.0.0|prep_sample
  ```
- Confirm the `bam-to-parquet/1.0.0` workflow synced into `qiita.action` (new
  BAM read-loader — synced by `qiita-admin actions sync` inside `activate.sh`,
  covered by `make verify-deploy`'s `qiita.action` list; this asserts the specific
  new action): (#254)

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, version, target_kind FROM qiita.action WHERE action_id='bam-to-parquet'"
  # expect: bam-to-parquet|1.0.0|prep_sample
  ```

### Notes (no host action)

- **Auth env-var parsing is now strict (control-plane).** The five auth int knobs (`AUTHROCKET_JWT_LEEWAY_SECONDS`, `AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS`, `QIITA_TOKEN_DEFAULT_TTL_DAYS`, `AUTH_HANDOFF_FRESHNESS_SECONDS`, `CLI_LOGIN_CODE_TTL_SECONDS`) now fail boot on a non-int or non-positive value (leeway may legitimately be 0). If any is set to 0/negative in a live env file, fix it before the restart. New optional `CLI_LOGIN_CODE_SWEEP_INTERVAL_SECONDS` (default 60) tunes the plaintext-PAT sweeper. (#241)
- **Invitation handoff no longer mints a PAT directly.** Accepting an AuthRocket invitation now redirects the user to `/auth/login` (the cookie-anchored flow) instead of displaying a PAT; users complete one normal login after accepting. No host action. (#241)
- **Removed inert SLURM env vars (compute-orchestrator).** `SLURM_POLL_INTERVAL_SECONDS` / `SLURM_JOB_TIMEOUT_SECONDS` are no longer read — they never enforced anything (the CP owns the poll loop; SLURM `--time` enforces walltime). Safe to leave or drop from `/etc/qiita/compute-orchestrator.env`. (#241)
- **Human PATs are now scope-bounded to the live role at resolve time.** After this deploys, any existing human token whose scopes exceed the principal's *current* role ceiling (role was downgraded, or `ROLE_IMPLIED_SCOPES` shrunk since mint) silently loses those scopes on scope-only-gated routes — no revocation. If an operator fields "my token stopped working on route X," this is the provenance: re-mint after confirming the user's role grants X. No host action. (#242)
- Bulk-block read masking (`read-mask-block/1.0.0` + `submit-block-mask-pool`) is
  **inert until an operator runs it** — the four new tables (`block`,
  `block_member`, `mask_sample`), the `block` scope kind, the
  `POST …/sequenced-pool/{P}/block-mask-plan` route, and the workflow add nothing to
  the existing per-sample `read-mask` / `submit-host-filter-pool` path, which stays
  valid (a single-sample block is byte-identical). No new env var, host directory,
  scope, group, or SIF (the workflow reuses `read-mask`'s native `qc` + `host_filter`
  modules — no container). Masked-read export now 409s for a block-masked sample whose
  `mask_sample` gate is not `completed` (a partially-masked sample); per-sample
  read-masked samples are unaffected (no gate row ⇒ allowed). (#243)
- **`long-read-assembly/1.0.0` is inert until an operator runs it.** The four new
  DuckLake tables (`assembled_sequence`, `assembled_sequence_chunks`,
  `assembly_membership`, `bin_quality`) are created automatically at data-plane
  startup by `ensure_assembly_tables` — **no data-plane action** (the Postgres
  `qiita.processing` / `qiita.assembly_membership` side is the bucket-3 migration).
  The workflow's FOUR per-tool images (`long-read-assembly-assemble-1.0.0.sif`,
  `long-read-assembly-binning-1.0.0.sif`, `long-read-assembly-dastool-1.0.0.sif`,
  `long-read-assembly-checkm-1.0.0.sif`) are built automatically by `build-sifs.sh` during
  the deploy — it now iterates `workflows/*/sif-build.d/*.env` in addition to the
  legacy `sif-build.env`, so no new manual build step. The first build resolves
  several bioconda envs (metawrap / DAS_Tool / CheckM), so it is **slow** and
  needs apptainer + network on the build host; the two-gate idempotency is now
  per-image, so a later change to one tool rebuilds only its SIF. Beyond the
  bucket-2 CheckM DB, no new env var, scope, or group. (#255)

---

## Deployed history

Archived `## Pending deploy` blocks, newest on top, each stamped with deploy date + the commit deployed. Populated by `/deploy-archive` at deploy time.

### Deployed 2026-06-29 — 89440a5

Nothing was pending at archive time — the PRs deployed since the previous archive (#225, #229, #230) carried no operator-impacting steps. Recorded for provenance only.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None._

#### 2. One-time host setup

_None._

#### 3. Migrations

_None._

#### 4. Deploy

_None._

#### 5. Verify

_None._

#### Notes (no host action)

_None._

### Deployed 2026-06-29 — d93cccb

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

- **PRE-CHECK before the bucket-3 `20260628000000` migration.** That migration
  builds a UNIQUE index on `(sequencing_run_idx, sha256(run_preflight_blob))`,
  which **fails** if any run already holds two pools with byte-identical
  preflights. Find them and resolve each with `qiita delete-sequenced-pool
  --force` (keep one pool per content) BEFORE running `make migrate`: (#206)

  ```bash
  psql "$DATABASE_URL" -tAc "
    SELECT sequencing_run_idx, encode(sha256(run_preflight_blob),'hex') AS content_hash,
           count(*) AS n, array_agg(idx ORDER BY idx) AS pool_idxs
      FROM qiita.sequenced_pool
     WHERE run_preflight_blob IS NOT NULL
     GROUP BY 1, 2 HAVING count(*) > 1"
  # expect: zero rows. Any row → delete the redundant pool(s) first.
  ```

  (The known run-15 duplicate — pools 25013/25014 — is being remediated
  separately; this pre-check is the backstop that catches it or any other.)

#### 3. Migrations

- `20260628000000_sequenced_pool_content_hash.sql` — adds the
  `run_preflight_sha256` STORED generated column and the
  `sequenced_pool_one_per_run_and_hash` partial unique index (content-keyed pool
  find-or-create). Plain `make migrate` — **but only after the bucket-2
  pre-check passes** (the unique index build aborts on existing duplicate-content
  pools). Additive; no backfill (the generated column computes in-DB for every
  row). (#206)

#### 4. Deploy

_None yet._

#### 5. Verify

- Confirm the raised `qc` walltime ceiling synced into `qiita.action` for both
  actions that run the step (so the new PT4H `qc` baseline and its PT8H escalation
  target sit within the ceiling): (#216)

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, walltime_ceiling FROM qiita.action WHERE (action_id, version) IN (('read-mask','1.0.0'),('fastq-to-parquet','1.3.0')) ORDER BY action_id"
  # expect: fastq-to-parquet|08:00:00  and  read-mask|08:00:00
  ```

- Confirm the content-hash index landed (pool find-or-create is now content-keyed): (#206)

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT count(*) FROM pg_indexes WHERE schemaname='qiita' AND indexname='sequenced_pool_one_per_run_and_hash'"
  # expect: 1
  ```

#### Notes (no host action)

- `qc` step walltime raised + new TIMEOUT walltime escalation. In both
  `read-mask/1.0.0` and `fastq-to-parquet/1.3.0` the `qc` step's
  `baseline_resources.walltime` rises PT2H → PT4H and the `action_ceiling.walltime`
  rises PT4H → PT8H. The runner now grows a step's walltime ×2 on each `TIMEOUT`
  retry (clamped to the ceiling), mirroring the existing OOM→memory growth — so a
  qc job that hit the 2 h wall on every attempt now starts at 4 h and escalates to
  8 h. The action-wide ceiling also lets `host_filter` (baseline PT4H) escalate to
  PT8H on a TIMEOUT retry. Both YAMLs are **edited in place** — re-synced into
  `qiita.action` by `qiita-admin actions sync` inside `activate.sh` (covered by
  bucket 5's `qiita.action` check), **not** a migration. No new env var, host dir,
  scope, or SIF. Ensure the SLURM partition/QOS permits an 8 h single-step job.
  (#216)
- Soft contract change (#206): bcl-convert re-submit over an already-COMPLETED
  sequenced_pool now 409s unless `--force` (wet_lab_admin+) — a re-run
  re-registers the pool's reads (duplicate lake rows). `qiita submit-bcl-convert`
  gains `--force`; the non-force recovery is `delete-sequenced-pool` then
  resubmit (FAILED tickets stay resumable via `qiita ticket run`). Pool
  find-or-create is now keyed on preflight content, not filename; the existing
  `sequenced_pool_one_per_run_and_filename` index is kept as a permanent
  independent uniqueness rule, so a different-content upload reusing an existing
  filename in a run is refused with a 409 (distinct pools must differ in both
  content and filename — the operator renames). One-time: during the
  migrate→restart window a same-content/renamed-file POST can hit a transient,
  fail-safe 500 on the pre-restart CP (no duplicate created); the restarted CP
  serves it as a 200 reuse. No new env var, host dir, scope, or SIF.

### Deployed 2026-06-28 — 7fa0bb9

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

- Confirm the raised `host_filter` mem ceiling synced into `qiita.action` for
  both actions that run the step (so the new 32 GB `host_filter` baseline sits
  within the ceiling): (#209)

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, mem_ceiling_gb FROM qiita.action WHERE (action_id, version) IN (('read-mask','1.0.0'),('fastq-to-parquet','1.3.0')) ORDER BY action_id"
  # expect: fastq-to-parquet|32  and  read-mask|32
  ```

#### Notes (no host action)

- `host_filter` step memory bump for host references that OOMed at 16 GB. In both
  `read-mask/1.0.0` and `fastq-to-parquet/1.3.0` the step's
  `baseline_resources.mem_gb` rises 16 → 32 and the `action_ceiling.mem_gb` rises
  16 → 32 to match. The job's internal DuckDB cap stays at 8 GB (intentional — the
  genome-scale memory is the loaded rype `.ryxdi` + minimap2 `.mmi` indexes held
  out of DuckDB's heap, which grow into the cgroup remainder the raise provides).
  Both YAMLs are **edited in place** — re-synced into `qiita.action` by
  `qiita-admin actions sync` inside `activate.sh` (covered by bucket 5's
  `qiita.action` check), **not** a migration. No new env var, host dir, scope, or
  SIF. Ensure the SLURM partition/QOS permits a 32 GB / 4-CPU single-step job.
  (#209)

### Deployed 2026-06-27 — 8d93add

Nothing was pending at archive time — the only PR deployed since the previous archive (#205) carried no operator-impacting steps. Recorded for provenance only.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None._

#### 2. One-time host setup

_None._

#### 3. Migrations

_None._

#### 4. Deploy

_None._

#### 5. Verify

_None._

#### Notes (no host action)

_None._

### Deployed 2026-06-26 — 210e243

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

- `bcl-convert/1.0.0`'s `ingest_reads` step now requests `cpu: 8 / mem_gb: 56`
  (was 2 / 8) — it parses up to 4 pool samples concurrently. The change syncs
  automatically via `qiita-admin actions sync` during the deploy (no manual
  step); just confirm the SLURM partition can schedule an 8-CPU / 56 GB job for
  that step. The sibling `bcl_convert` step already requests cpu 16 / mem 480 on
  the same partition, so this is well within reach. (#201)

#### Notes (no host action)

_None yet._

### Deployed 2026-06-25 — 980078e

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

_None yet._

#### Notes (no host action)

- `read-mask/1.0.0` audience tightened to `[wet_lab_admin, system_admin]` (drops
  plain `user`): submitting a read mask (host filter / QC reprocessing) now drives
  the data plane to re-materialize the sample's RAW (human-containing) reads, so it
  is admin-only. **Soft contract change** — a non-admin `user` who could submit a
  read mask before now gets 403. The YAML is **edited in place**, re-synced into
  `qiita.action` by `qiita-admin actions sync` inside `activate.sh` (covered by the
  generic `make verify-deploy` action list), **not** a migration. The new
  `export_read` data-plane DoAction backing the runner's reprocessing fallback
  writes a per-ticket `reads.parquet` under `<scratch>/ticket/<idx>/` — already
  group-writable by the DP (`qiita-data`) via its existing `qiita-pipeline`
  membership, and read back by `qiita-job` (same group) at mode `0440`, so **no new
  env var, host dir, scope, or group change**. (#187)
- (#188) New system_admin-only scope `admin:biosample_owner_id_read`, gating the
  owner-id re-identification export (`GET /admin/study/{study_idx}/owner-biosample-id`
  + `qiita-admin owner-biosample-id`). Pure-Python scope (no migration); added to the
  system_admin ceiling in `auth/scopes.py`. **A system_admin's existing PAT does not
  carry the new scope until re-minted** — after this deploy, a system_admin who needs
  the export re-runs `qiita-admin login` (or mints a fresh PAT) to obtain a token that
  includes it; the loopback login grants the full role ceiling, so no scope need be
  named. No new env var, host dir, migration, or service-account grant.
- (#192) New system_admin-only scope `admin:masked_read_export`, gating the per-pool
  masked-read export (`GET /admin/sequenced-pool/{idx}/masked-read-export` + `POST
  /admin/masked-read-export/ticket` + `qiita-admin masked-read-export`). Pure-Python
  scope (no migration); added to the system_admin ceiling in `auth/scopes.py`. Same
  PAT story as `admin:biosample_owner_id_read` above — **an existing system_admin PAT
  does not carry it until re-minted** (`qiita-admin login` or a fresh PAT). The export
  only ever reads the data plane's `read_masked` view (`WHERE reason='pass'`), so no
  privacy surface is widened. The CLI runs on the operator's **client** host and needs
  `miint`+`duckdb` available there (same as `qiita reference load`) — no server-side
  env var, host dir, migration, or service-account grant. The data-plane DoGet now
  streams its result set instead of buffering it whole; picked up by the normal
  binary restart, no operator action.

### Deployed 2026-06-25 — 8f4d4cd

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

- `20260624110000_work_ticket_mask_idx.sql` — adds the nullable
  `qiita.work_ticket.mask_idx` column (FK → `mask_definition`, ON DELETE SET NULL,
  partial index). Plain `make migrate`; additive and backfill-free at migrate time
  (existing rows read NULL — they are populated by the bucket-5 backfill). (#181)

#### 4. Deploy

_None yet._

#### 5. Verify

- One-time mask recovery — run AFTER the bucket-3 migrate + bucket-4 restart (the
  reordered `read-mask`/`fastq-to-parquet` workflows from the Notes entry must be
  live, else resubmits re-fail). This populates `work_ticket.mask_idx`, then purges
  and resubmits the FAILED read-mask tickets stranded by the move-then-read bug —
  which doubles as the e2e validation: (#181)

  a. Backfill `mask_idx` on existing tickets. Dry-run first and confirm
  `populated > 0`, then apply. Needs `DATABASE_URL`,
  `QIITA_DEFAULT_ADAPTER_REFERENCE_IDX`, `HMAC_SECRET_KEY`, and a reachable
  `DATA_PLANE_URL` (it re-materializes the canonical adapter set via DoGet to
  reproduce the mask params hash):

  ```bash
  qiita-admin work-ticket backfill-mask-idx           # dry-run; expect populated > 0
  qiita-admin work-ticket backfill-mask-idx --apply
  ```

  b. Purge the orphaned masks and resubmit on the fixed workflow. Dry-run to
  review the (ticket, mask_idx, prep_sample) rows, then execute:

  ```bash
  qiita-admin mask purge-failed --action read-mask                          # dry-run; review
  qiita-admin mask purge-failed --action read-mask --execute --with-tickets # optionally --rate/--limit
  ```

  c. Confirm the resubmitted tickets reach COMPLETED and the per-sample
  `sequenced_sample` read-metric counts populate (pool-completion rollups move).

#### Notes (no host action)

- New `mask_definition:delete` scope (gating `DELETE /mask-definition/{mask_idx}` +
  the `qiita-admin mask delete` / `mask purge-failed` recovery tooling). Granted
  automatically to `system_admin` via the role ceiling, so **no grant step** — but
  the scope is **not** in admin PATs minted before this deploy (tokens carry a
  fixed scope snapshot). An admin running the bucket-5 mask recovery must re-mint a
  fresh PAT (or use a fresh OIDC bearer) after the deploy to carry it. The delete
  drives the data plane (new `delete_mask` DoAction over the existing HMAC Flight
  path) and removes the `mask_definition` Postgres row; no env var, host dir, or
  service-account grant. (#181)
- `read-mask/1.0.0` and `fastq-to-parquet/1.3.0` reorder their final two actions
  so `persist-read-metrics` runs **before** `register-files` (register-files
  MOVES `read_mask.parquet` out of the staging dir into DuckLake, so the metrics
  read must precede it — it was failing with `read_mask parquet not found`). Both
  YAMLs are **edited in place** — re-synced into `qiita.action` by `qiita-admin
  actions sync` inside `activate.sh` (already covered by the generic
  `make verify-deploy` action list), **not** a migration. No new env var, host
  dir, scope, or SIF. (#181)

### Deployed 2026-06-24 — be01438

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

- Confirm the read-storage / masking split synced: the new `read-mask` action is
  registered and `bcl-convert` now carries the `ingest_reads` + `register-files`
  steps. `qiita-admin actions sync` runs inside `activate.sh`, so this is a
  read-back, not a host action:

  ```bash
  qiita-admin actions list | grep -E 'read-mask|bcl-convert'
  ```
  (#180)

#### Notes (no host action)

- Read storage is split out of host-filtering. `submit-bcl-convert` now stores
  the pool's reads (a new `ingest_reads` step writes the DuckLake `read` table
  plus a durable per-sample copy under `<scratch>/reads/<prep_sample_idx>/`,
  auto-created by the step — no host setup). `submit-host-filter-pool` is now
  mask-only (no `--convert-dir`): it submits `read-mask/1.0.0` tickets over the
  stored reads, and can be re-run against a different host reference to add a
  side-by-side mask. Both workflows ship via `qiita-admin actions sync`; no env
  var, migration, or host action. The legacy `fastq-to-parquet` workflows remain
  registered but dormant. (#180)

- `make redeploy` now auto-refreshes the operator's **checkout** CLI venv
  (`$QIITA_CLONE/qiita-control-plane/.venv`, where `uv run qiita` / `qiita-admin`
  resolve) as a new step 6, as the checkout owner. The manual
  `cd .../qiita-control-plane && uv sync --reinstall-package qiita-common`
  workaround after a pull that bumped `qiita-common` without a version change is
  no longer needed when deploying via `make redeploy`. And if the CLI is ever run
  with a stale `qiita_common` anyway (outside the redeploy path), it now prints
  that exact `uv sync --reinstall-package qiita-common` fix instead of a raw
  import traceback. Behaviour ships with the script / CLI; no host action. (#163)

### Deployed 2026-06-24 — fee935f

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

- Wipe all legacy sequenced / sequenced-pool sample data BEFORE the deploy. These
  samples predate the lake-read model — their reads were never registered into
  DuckLake — and the `sequenced_sample.host_rype_reference_idx` /
  `host_minimap2_reference_idx` columns they carry are being dropped (bucket 3).
  Delete the pools through the CLI (system_admin; per pool):

  ```bash
  qiita delete-sequenced-pool --sequencing-run-idx <R> --sequenced-pool-idx <P> --force
  ```

  Run this before the migration; the drop migration is a single relocate with no
  data-preservation step, so there must be no legacy sample data to strand. (#175)

#### 3. Migrations

- `20260624000000_drop_sequenced_sample_host_references.sql` — drops the two
  host-reference columns (and their FKs + the minimap2-requires-rype CHECK) from
  `qiita.sequenced_sample`. Plain `make migrate` (after the bucket-2 wipe). (#175)
- `20260624100000_work_ticket_state_no_data.sql` — additive
  `ALTER TYPE qiita.work_ticket_state ADD VALUE 'no_data'` (the new terminal
  empty-well outcome). `transaction:false` directive (Postgres forbids using a
  freshly-added enum value in the same transaction); plain `make migrate`, no
  out-of-band setup or backfill. (#176)

#### 4. Deploy

_None yet._

#### 5. Verify

- New workflow `fastq-to-parquet/1.3.0` is synced into `qiita.action` by
  `qiita-admin actions sync` inside `activate.sh` (no migration). Confirm it
  registered after the deploy: (#173)

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, version FROM qiita.action WHERE action_id = 'fastq-to-parquet' AND version = '1.3.0'"
  ```

#### Notes (no host action)

- Soft API change (PR 4 of the full-read+mask feature): host references moved off
  `sequenced_sample` onto the human-filter submission. Sequenced-sample GET
  responses and the pool/run sample-list rows no longer carry
  `host_rype_reference_idx` / `host_minimap2_reference_idx`; `seqsample-create` /
  `submit-bcl-convert` no longer accept them. The operator now passes
  `--host-rype-reference-idx` (and optional `--host-minimap2-reference-idx`) to
  `qiita submit-host-filter-pool` — pool-wide for that submission, omitted for a
  QC-only pass-through. `prep_protocol_idx` is unchanged. `submit-host-filter-pool`
  now also **requires** `--preflight-blob` (the same kl-run-preflight SQLite given
  to `submit-bcl-convert`): it cross-checks each sample's intake `human_filtering`
  intent against the host-ref choice and aborts on a mismatch unless `--force` is
  passed. (#175)
- The full-read+mask producer cutover (PR 3) ships `fastq-to-parquet/1.3.0`,
  which writes the full reads into the DuckLake `read` table and a downstream
  `read_mask` (PRs 1–2 already deployed the `mask_definition` table + the
  data-plane `read`/`read_mask`/`read_masked` surfaces). 1.0.0–1.2.0 stay
  available; nothing forces a re-run of in-flight tickets. No new env var or
  host directory. (#173)
- Soft API change: empty FASTQ wells are now a terminal `no_data` outcome
  (distinct from failure). The `GET .../sequenced-pool/{P}/completion` response
  gains a `samples_no_data` count and its `complete` flag now fires when every
  active sample is COMPLETED **or** NO_DATA (so a plate with empty wells reaches
  "done"); empty wells are no longer in `samples_failed`. A work_ticket can now be
  terminal in state `no_data` (409 on `/run` like `completed`, but freely
  resubmittable — no result is minted, so no DELETE is required). New reversible
  `PATCH /api/v1/prep-sample/{idx}/retired`
  (prep_sample:write + wet_lab_admin) and `qiita prep-sample retire` /
  `un-retire`. Until expected-empty control-well preflight marking lands
  (deferred), EVERY empty well becomes `no_data` — data wells included, not only
  flagged controls. No new env var, host dir, scope, or workflow. (#176)

### Deployed 2026-06-23 — 3ac105c

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

- (#170) `20260623000000_mask_definition.sql`
  — adds the `qiita.mask_definition` table + `qiita.mint_mask_definition`
  function. Plain `make migrate`; additive, no extension or backfill.

#### 4. Deploy

_None yet._

#### 5. Verify

- (#169) Confirm the raised `local-host-reference-add` mem ceiling synced into
  `qiita.action` (so its `build_rype_index` OOM-retry escalation can climb to
  128 GB; `host-reference-add` was already 128):

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, mem_ceiling_gb FROM qiita.action WHERE action_id IN ('host-reference-add','local-host-reference-add') AND version='1.0.0' ORDER BY action_id"
  # expect: host-reference-add|128  and  local-host-reference-add|128
  ```

#### Notes (no host action)

- (#170) New `read_masked:doget` scope on the
  service-account ceiling, gating the new `POST /mask-definition` and
  `POST /read-masked/ticket/doget` routes. No host action this deploy: no
  production service account consumes these routes yet (the masked-read consumer
  path lands in a later PR), so no token needs re-minting now. When a worker is
  wired to pull masked reads, mint/rotate its token to include the scope.
- (#169) `build_rype_index` resource bump for large host sets (many human
  genomes that OOMed at 32 GB). In both `host-reference-add/1.0.0` and
  `local-host-reference-add/1.0.0` the step's `baseline_resources.mem_gb` rises
  32 → 64, and `local-host-reference-add`'s `action_ceiling.mem_gb` rises 64 →
  128 (matching `host-reference-add`) so an OOM-killed retry can double the step
  64 → 128 GB. The job now hard-caps DuckDB at 8 GB regardless of allocation
  (safe because `rype_index_create`'s windowed feed bounds DuckDB's working set
  to window size, not corpus size — relies on the windowed-feed miint build being
  live on the mirror) and hands the growing remainder to rype's `max_memory`
  (starts ~50 GB, ≈114 GB at the 128 GB ceiling). Both YAMLs are **edited in place** — re-synced into
  `qiita.action` by `qiita-admin actions sync` inside `activate.sh` (already in
  bucket 5's `qiita.action` check), **not** a migration. No new env var, host
  dir, scope, or SIF. Ensure the SLURM partition/QOS permits 128 GB single-step
  jobs.

### Deployed 2026-06-23 — f56a470

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

- (#167) Confirm the raised `reference-add` / `host-reference-add` mem ceilings
  synced into `qiita.action` (so a `resource_override.mem_gb` up to 128 is
  accepted and the OOM-retry escalation can climb to 128 GB):
  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, mem_ceiling_gb FROM qiita.action WHERE action_id IN ('reference-add','host-reference-add') AND version='1.0.0' ORDER BY action_id"
  # expect: host-reference-add|128  and  reference-add|128
  ```

#### Notes (no host action)

- (#167) `reference-add/1.0.0` and `host-reference-add/1.0.0` raise their
  `action_ceiling.mem_gb` 64 → 128 (the `reference_load` step OOMs above 40 GB
  at GG2 scale). Edited in place — re-synced into `qiita.action` by `qiita-admin
  actions sync` inside `activate.sh`, **not** a migration. Pairs with the runner
  change that escalates a step's memory ×2 (clamped to this ceiling) on each
  OOM-killed retry. No new env var, host dir, scope, or SIF.

### Deployed 2026-06-23 — 40674d7

Nothing was pending at archive time — the PRs deployed since the previous archive (#159, #160, #165) carried no operator-impacting steps. Recorded for provenance only.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None._

#### 2. One-time host setup

_None._

#### 3. Migrations

_None._

#### 4. Deploy

_None._

#### 5. Verify

_None._

#### Notes (no host action)

_None._

### Deployed 2026-06-22 — f07359e

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

- (#148) `20260622000000_sequenced_sample_read_metrics.sql` — adds three nullable
  `BIGINT` read-count columns + CHECK constraints to `qiita.sequenced_sample`.
  Plain `make migrate`; additive and backfill-free (existing rows read NULL).
- (#154) `20260622010000_sequenced_sample_qc_report.sql` — adds two nullable
  `jsonb` QC-report columns (`raw_qc_report`, `filtered_qc_report`) to
  `qiita.sequenced_sample`. Plain `make migrate`; additive and backfill-free
  (existing rows read NULL).
- (#156) `20260622020000_sequenced_sample_host_references.sql` — adds two nullable
  FK columns (`host_rype_reference_idx`, `host_minimap2_reference_idx` →
  `qiita.reference`, ON DELETE RESTRICT) + a CHECK (minimap2 requires rype) to
  `qiita.sequenced_sample`. Plain `make migrate`; additive and backfill-free
  (existing rows read NULL/NULL).

#### 4. Deploy

_None yet._

#### 5. Verify

_None yet._

#### Notes (no host action)

- (#147) `fastq-to-parquet/1.2.0` now declares three additional step outputs
  (`raw_read_count` / `biological_read_count` / `quality_filtered_read_count` — the
  per-stage `read_count.json` sidecars). The `workflows/fastq-to-parquet/1.2.0.yaml`
  entry is **edited in place** — re-synced into `qiita.action` by `qiita-admin actions
  sync` inside `activate.sh` (already covered by bucket 5's `qiita.action` list check),
  **not** a migration. Emission only (no consumer yet); no client breakage, no new env
  var, host dir, scope, or migration.
- (#148) `fastq-to-parquet/1.2.0` gains a final `persist-read-metrics` action and
  now declares `scopes: [prep_sample:write]`. Re-synced into `qiita.action` by
  `qiita-admin actions sync` (same in-place edit as #147); the new column targets
  ship in the bucket-3 migration above, applied before the restart. **Submitter
  contract tightening:** submitting 1.2.0 now requires `prep_sample:write` (already
  in the USER ceiling, so all three audience roles keep access — only a token
  scoped *below* its role ceiling is affected). No new host dir, env var, or
  service-account grant.
- (#152) `fastq-to-parquet/1.2.0` gains two `qc_report` steps (`qc_report_raw`,
  `qc_report_filtered`) backed by the new native `qc_report` job module. Re-synced
  into `qiita.action` by `qiita-admin actions sync` (same in-place edit as #147);
  the module ships with the orchestrator code (a native step, not a container — no
  SIF). Reporting only; no client breakage, no new env var, host dir, scope, or
  migration.
- (#154) `fastq-to-parquet/1.2.0` gains a final `persist-qc-report` action that
  writes the per-sample QC reports into the bucket-3 `jsonb` columns. Re-synced
  into `qiita.action` by `qiita-admin actions sync` (same in-place edit as #147);
  no new env var, host dir, scope (it reuses the `prep_sample:write` #148 already
  added), or SIF. New read-only `GET .../sequenced-pool/{pool}/qc-report` endpoint
  (the merged pool report) — additive, read-gated like the existing pool roster;
  no host action.
- (#156) Per-sample host-filter references (new bucket-3 columns). The
  sequenced-sample composer now accepts optional `host_rype_reference_idx` /
  `host_minimap2_reference_idx`, and `qiita-user submit-bcl-convert` gains
  `--host-rype-reference-idx` (+ optional `--host-minimap2-reference-idx`).
  **Operator-facing CLI change:** a bcl-convert run whose preflight has any
  `human_filtering` sample now requires `--host-rype-reference-idx` (the host
  reference is recorded per sample for the later fan-out); the reference must be
  ACTIVE + carry the right index (built via `host-reference-add`) or the gesture
  aborts before any side effect. No new env var, host dir, scope, or migration
  beyond the additive bucket-3 columns.
- (#158) `qiita-user submit-host-filter-pool` now host-filters each pool sample
  against the reference(s) recorded on it (by #156's `submit-bcl-convert`), not a
  pool-wide reference. **Operator-facing CLI change:** the
  `--host-rype-reference-idx` / `--host-minimap2-reference-idx` flags are
  **removed** — an invocation still passing them now errors; host filtering is
  per-sample (preflight `human_filtering=0` samples get a QC-only pass-through
  ticket). New read-only `GET .../sequenced-pool/{pool}/completion` endpoint (and
  `qiita-user pool-completion`) reporting per-sample fastq-to-parquet completion —
  additive, read-gated like the existing pool rollups. No new env var, host dir,
  scope, migration, or workflow/SIF change.

### Deployed 2026-06-22 — af1fa22

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

_None yet._

#### Notes (no host action)

- (#140) `stage_local_fasta` resource retune in `local-host-reference-add/1.0.0`
  (`cpu: 8`/`mem_gb: 32` → `cpu: 4`/`mem_gb: 64`; still within the `cpu: 16`/`mem_gb: 64`
  ceiling). The `workflows/local-host-reference-add/1.0.0.yaml` entry is **edited in
  place** — re-synced into `qiita.action` by `qiita-admin actions sync` inside
  `activate.sh` (already covered by bucket 5's `qiita.action` list check), **not** a
  migration. No client breakage, no new env var, host dir, scope, or migration.
- (#140) Parquet result files are now written with `ROW_GROUP_SIZE_BYTES '64MB'`
  (smaller row groups: finer pushdown, lower write memory). Code-only write-side
  tuning; the data plane reads these files via the same pinned DuckDB 1.5.4, the
  format is unchanged, and output stays clustered on the `ORDER BY` key, so DuckLake
  registration + pruning are unaffected. No host action, env var, scope, or migration.

### Deployed 2026-06-20 — 5b21afe

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

- (#138) **DuckDB bumped 1.5.3 → 1.5.4** — the miint extension mirror is keyed by
  DuckDB version, so the new code needs a `v1.5.4` miint build on the mirror.
  `miint_staging` auto-detects the version change locally and re-fetches at the
  next orchestrator boot (no manual host step, no env var), so the only operator
  action is confirming the mirror publishes the build before the bucket-4 restart
  (already verified present for `linux_amd64`):

  ```bash
  curl -fsSI "https://ftp.microbio.me/pub/miint/v1.5.4/linux_amd64/miint.duckdb_extension.gz" \
    | head -1   # expect: HTTP/.. 200
  ```

  Post-deploy, the bucket-5 `make verify-deploy` `compute-readiness` check
  exercises the v1.5.4 miint download end-to-end.

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

_None yet._

#### Notes (no host action)

_None yet._

### Deployed 2026-06-20 — 6c43d2b

Nothing was pending at archive time — the only PR deployed since the previous archive (#137) carried no operator-impacting steps. Recorded for provenance only.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None._

#### 2. One-time host setup

_None._

#### 3. Migrations

_None._

#### 4. Deploy

_None._

#### 5. Verify

_None._

#### Notes (no host action)

_None._

### Deployed 2026-06-19 — 1ad104b

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

- (#134) Confirm the auto-rebuilt bcl-convert SIF installed bcl-convert from the
  new `/opt` staging path. Run home-independently (`cd /tmp` + `--no-home`, since
  deploys run from an NFS home and `qiita-orch`'s home is `/dev/null`):

  ```bash
  cd /tmp
  derived=$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)
  sudo -u qiita-orch apptainer exec --no-home "$derived/images/bcl-convert-4.5.4.sif" \
    bcl-convert --version
  ```

  Expect `bcl-convert Version 4.5.4`.

#### Notes (no host action)

- (#134) The bcl-convert SIF **auto-rebuilds on the next deploy** — `Apptainer.def`
  changed (the licensed RPM now stages to `/opt`, not the bind-mounted `/tmp`, so
  the root auto-build's `dnf install` stops failing with "Could not open … rpm").
  `build-sif.sh`'s content hash detects the def change and `build-sifs.sh` rebuilds
  it during the deploy; no manual `build-sif.sh` step. Needs the licensed RPM still
  staged under `${PATH_DERIVED}/images/sources/` (already there) — if absent, the
  auto-build skips with a warning. No new env var, host dir, scope, or migration.

### Deployed 2026-06-19 — 50b85df

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

- (#130) Rebuild the bcl-convert SIF to pick up the `entrypoint.sh` chmod fix
  (the step's final mode-fixing `find … chmod` no longer touches the
  orchestrator-owned `$QIITA_OUTPUT_PATH` root, which was failing live
  bcl_convert jobs with `chmod: … Operation not permitted` under `set -e`).
  `entrypoint.sh` is baked into the SIF, but `build-sif.sh`'s idempotency check
  only probes the bcl-convert *version* (unchanged here) — so a plain rebuild
  prints "nothing to do". Use the new `FORCE=1` to rebuild unconditionally. The
  python3.11 SIF (#126) is already live on the host, so this is its own rebuild
  (not piggybacking on a #126 step). Run after the `git pull`, before the
  bucket-4 deploy:

  ```bash
  derived=$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)
  sudo -u qiita-orch bash -lc "FORCE=1 PATH_DERIVED='$derived' \
    bash /home/qiita/qiita-miint/scripts/build-sif.sh bcl-convert"
  ```

- (#129) **Enable QC: load the adapter set + set `QIITA_DEFAULT_ADAPTER_REFERENCE_IDX`.**
  fastq-to-parquet/1.2.0's always-on QC trims against a canonical adapter set
  stored as an `artifact_sequence_set` reference. **Run AFTER the bucket-3
  migration + bucket-4 deploy** — the new `artifact_sequence_set` kind and the
  `qiita reference load --kind` flag must be live first. Load the set (it prints
  the new reference_idx), pin that idx, restart CP:

  ```bash
  # [operator, with a reference:write PAT] load the canonical adapter set;
  # note the printed reference_idx
  qiita reference load --kind artifact_sequence_set \
    --name qc-adapters --version 1.0 --fasta /path/to/adapters.fasta
  # [admin] pin the printed idx (idempotent) and restart CP so it picks it up
  sudo bash -c 'grep -q "^QIITA_DEFAULT_ADAPTER_REFERENCE_IDX=" /etc/qiita/control-plane.env \
    || echo "QIITA_DEFAULT_ADAPTER_REFERENCE_IDX=13" >> /etc/qiita/control-plane.env'
  sudo systemctl restart qiita-control-plane
  ```

  Optional at boot (CP starts without it; 1.1.0 is unaffected) — a 1.2.0 /
  `submit-host-filter-pool` submission fails until both the adapter set and this
  var are in place.

  On the qiita-miint deploy this is `reference_idx` **13**, the **TruSeq**
  adapter set. TruSeq adapters are appropriate for TruSeq-style libraries only —
  this is **not** a general-purpose adapter set. A deploy running other
  library-prep protocols must load and pin its own `artifact_sequence_set`
  instead; the pinned idx is applied pool-wide to every `submit-host-filter-pool`
  submission, so it must match the libraries being filtered.

#### 3. Migrations

- (#129) `20260618000000_reference_kind_artifact_sequence_set.sql` — widens the
  `reference.kind` CHECK to allow `artifact_sequence_set`. Plain `make migrate`,
  no out-of-band setup.

#### 4. Deploy

_None yet._

#### 5. Verify

- (#130) Confirm the rebuilt bcl-convert SIF carries the fixed entrypoint:

  ```bash
  derived=$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)
  sudo -u qiita-orch apptainer exec "$derived/images/bcl-convert-4.5.4.sif" \
    grep -q 'mindepth 1' /opt/qiita/entrypoint.sh && echo OK
  ```

  Expect `OK` (the `-mindepth 1` guard is present). The real proof is the next
  bcl_convert ticket completing past the output-chmod step.

- (#129) Confirm `fastq-to-parquet 1.2.0` is in the `make verify-deploy` action
  list (the always-on-QC + two-reference host-filter workflow `submit-host-filter-pool`
  now targets). A 1.2.0 QC submission also needs the bucket-2 adapter set +
  `QIITA_DEFAULT_ADAPTER_REFERENCE_IDX`.

#### Notes (no host action)

- (#132) The deploy now builds container SIFs automatically.
  `activate.sh` runs `deploy/build-sifs.sh` (after the rsync, before the
  restarts): it iterates `workflows/*/sif-build.env`, builds each via the generic
  `scripts/build-sif.sh` as root, and chowns the SIF to `qiita-orch`. **No manual
  bucket-2 SIF rebuild is needed going forward** — an edited `Apptainer.def` /
  `entrypoint.sh` / `manifest_writer.py` is now detected by a build-inputs content
  hash and rebuilt during the deploy (the old `FORCE=1` manual step is no longer
  required for those; the bucket-2 #130 step above is therefore redundant if it
  ships in the same deploy, but harmless — it just rebuilds slightly earlier).
  **Expect a one-time rebuild on the first deploy carrying this change:** the live
  SIFs have no `.buildhash` stamp yet, so each is rebuilt once (then stamped, and
  skipped thereafter). That rebuild needs the licensed `SOURCES` still staged
  under `$PATH_DERIVED/images/sources/` — bcl-convert's RPM already is on the live
  host, so no action; if an image's source is missing the deploy **skips** that
  image (with a warning) rather than failing. A spec can opt out with
  `AUTO_BUILD=0`. No new env var, host dir, scope, or migration.
- (#129) New `GET /sequencing-run/{idx}` route (run metadata incl.
  `instrument_model`; prep_sample:read + wet_lab_admin) — `submit-host-filter-pool`
  reads it to forward QC's polyG `instrument_model` per sample. Code-only, no host
  action, no new scope. The new `workflows/fastq-to-parquet/1.2.0.yaml` (plus a
  comment-only `1.1.0` edit) re-syncs into `qiita.action` via `qiita-admin actions
  sync` inside `activate.sh` (verified in bucket 5), **not** a migration.
- (#128) Genome-scale reference-load resource tuning (no client breakage). The two `workflows/` entries `local-reference-add` and `local-host-reference-add` (both still 1.0.0) are **edited in place** — re-synced into `qiita.action` by `qiita-admin actions sync` inside `activate.sh` (already covered by bucket 5's `qiita.action` list check), **not** a migration. Raised baseline_resources + walltimes so loading hundreds of human genomes no longer hits the old 1h step cap (`stage_local_fasta`/`hash_sequences` → cpu=8/mem_gb=32, `build_rype_index` → cpu=8, `build_minimap2_index` → mem_gb=32; step walltimes → PT24H under a PT48H `action_ceiling`). To permit those longer walltimes the orchestrator's SLURM poll-loop timeout **default** rose 24h → 48h (`config.py` `DEFAULT_SLURM_JOB_TIMEOUT_SECONDS`); it applies on the normal bucket-4 CO restart — no new env var. **Caveat:** if `/etc/qiita/compute-orchestrator.env` pins `SLURM_JOB_TIMEOUT_SECONDS` explicitly, raise it to ≥ the longest step walltime (currently PT24H / 86400s) or genome-scale loads will be reaped mid-run. No new host dir, scope, or migration.

### Deployed 2026-06-19 — 8e55b99

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None._

#### 2. One-time host setup

- (#126) Rebuild the bcl-convert SIF. The image's Python was bumped 3.6→3.11
  (OL8's default `python3`=3.6 crashed `manifest_writer.py` on PEP 585
  `list[str]` annotations). `build-sif.sh` is idempotent on the bcl-convert
  *version* (`VERIFY_MATCH`), which this change does **not** touch — so it would
  print "nothing to do" and leave the broken SIF in place. Delete the existing
  SIF first to force the rebuild; the rebuild's `%test` now `exec_module`s
  `manifest_writer.py` under python3.11, so a bad interpreter fails the build
  here instead of a live job. Run after the `git pull`, before the bucket-4
  deploy:

  ```bash
  derived=$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)
  sudo -u qiita-orch bash -lc "rm -f '$derived/images/bcl-convert-4.5.4.sif' && \
    PATH_DERIVED='$derived' bash /home/qiita/qiita-miint/scripts/build-sif.sh bcl-convert"
  ```

#### 3. Migrations

_None._

#### 4. Deploy

_None._

#### 5. Verify

- (#126) Confirm the rebuilt bcl-convert SIF ships the new interpreter:

  ```bash
  derived=$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)
  sudo -u qiita-orch apptainer exec "$derived/images/bcl-convert-4.5.4.sif" python3.11 --version
  ```

  Expect `Python 3.11.x`. (A clean SIF rebuild already proved `manifest_writer.py`
  imports under it via the `%test` block.)

#### Notes (no host action)

- (#124) Per-host-reference index selection + tunable build params (additive, no client breakage). The two `workflows/` entries `host-reference-add` and `local-host-reference-add` (both still 1.0.0) are **edited in place** — re-synced into `qiita.action` by `qiita-admin actions sync` inside `activate.sh` (already in bucket 5's `qiita.action` list check), **not** a migration. New optional `action_context` keys (`build_rype`/`build_minimap2` to pick which host-filter indexes to build, `rype_w`/`minimap2_preset` to tune them) surfaced by `qiita reference load --host --no-rype-index|--no-minimap2-index|--rype-w|--minimap2-preset`; omitted, behaviour is unchanged (both indexes built; minimap2 `preset` default `sr`). No new env var, host dir, scope, or migration. Two behaviour notes (both code-internal, no host action): the rype build window `w` default changed 25 → 20; and the fastq-to-parquet host-filter consumer now accepts a single-index host reference (binds whichever of rype/minimap2 exist, requires ≥1). The minimap2 build step also gained `target_status: indexing` so a minimap2-only build still transitions the reference out of `loading`.
- New `sequenced_pool:delete` scope + `DELETE /sequencing-run/{run}/sequenced-pool/{pool}`
  (and the `qiita delete-sequenced-pool` CLI) for removing a full preparation.
  Auto-granted to system_admin via `ROLE_IMPLIED_SCOPES`, so no grant step — but
  the scope is **not** in admin PATs minted before this deploy (tokens carry a
  fixed scope snapshot). An admin who wants to use the delete must mint a fresh
  PAT after the deploy. No migration, env var, or host change. (#125)

### Deployed 2026-06-18 — 70eb519

Nothing was pending at archive time — the only PR deployed since the previous archive (#122) carried no operator-impacting steps. Recorded for provenance only.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None._

#### 2. One-time host setup

_None._

#### 3. Migrations

_None._

#### 4. Deploy

_None._

#### 5. Verify

_None._

#### Notes (no host action)

_None._

### Deployed 2026-06-18 — 20adace

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

- `host-reference-add` / `local-host-reference-add` changed: `build_rype_index`
  and `build_minimap2_index` now declare only `*_index_meta` as a step output
  (the persistent `.ryxdi`/`.mmi` under `PATH_DERIVED` is no longer an output —
  it was an impossible one that failed the launcher manifest). `activate.sh`
  re-syncs these via `qiita-admin actions sync` automatically — no manual step.
  Verify by resubmitting a host-reference load (e.g. the failed
  `--reference-idx 2`) and confirming it clears `build_rype_index` /
  `build_minimap2_index` instead of failing with a post-success
  `CONTRACT_VIOLATION`. (#118)

#### Notes (no host action)

_None yet._

### Deployed 2026-06-18 — ee0842a

Nothing was pending at archive time — the PRs deployed since the previous archive (#114, #116) carried no operator-impacting steps. Recorded for provenance only.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None._

#### 2. One-time host setup

_None._

#### 3. Migrations

_None._

#### 4. Deploy

_None._

#### 5. Verify

_None._

#### Notes (no host action)

_None._

### Deployed 2026-06-18 — 1bd7c81

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

_None yet._

#### Notes (no host action)

- `make redeploy` prompts less from this checkout onward — it skips the buckets
  1 & 2 ack when both are empty here, and skips the native-venv refresh when it's
  provably already current (`FORCE_NATIVE_REFRESH=1` overrides, e.g. recovering a
  deploy that died mid-`uv sync`). Behaviour ships with the script; no host
  action. See CHANGELOG / `redeploy.md` for the full rules. (#113)

### Deployed 2026-06-17 — 11405b6

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

_None yet._

#### Notes (no host action)

- (#29) New `reference:delete` scope + `DELETE /reference/{idx}` (full reference
  purge). The scope is granted automatically to `system_admin` via the role
  ceiling (computed live at auth time), so existing admin tokens gain it on the
  next CP restart — **no token re-mint, no DB or scope migration**. Note the
  cross-service reach so operators aren't surprised: the delete drives the data
  plane (new `delete_reference` DoAction over the existing HMAC Flight path) and
  the orchestrator (new `DELETE /reference-artifact/{idx}` on the existing CP↔CO
  bearer) to remove DuckLake rows and on-disk `rype`/`minimap2` indexes under
  `PATH_DERIVED`. No env var, host dir, or service-account grant.

### Deployed 2026-06-17 — 8d340f0

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

- (#102) `make migrate` applies `20260617000000_work_ticket_resource_override` —
  an additive nullable `resource_override JSONB` column on `qiita.work_ticket`.
  No out-of-band setup; existing rows read as NULL.

#### 4. Deploy

_None yet._

#### 5. Verify

_None yet._

#### Notes (no host action)

- (#102) `qiita reference load` and `qiita ticket submit` gain `--mem-gb`, a
  per-run memory floor for a workflow's SLURM steps (wet_lab_admin /
  system_admin only, bounded by the action's mem ceiling). Use it to load a
  genome-scale host reference that OOMs the conservative default. No host
  action; surfaced so operators know the lever exists.

### Deployed 2026-06-16 — c8981aa

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

- (#89) [admin] Create the host-reference index dir before the first
  `host-reference-add` run. The index build **and** its consumer (`host_filter`)
  run as `qiita-job`, which `mkdir`s `{idx}/{rype,minimap2}/` under
  `{PATH_DERIVED}/references/` at runtime; the base root is `root:root 0755`, so a
  missing leaf fails that first build with Permission Denied at `mkdir` (stranding
  the reference in `indexing`). Pre-create the leaf group-writable by
  `qiita-pipeline` (NOT owned `qiita-orch:qiita-orch` like `…/images`, whose SIFs
  `qiita-orch` builds) — setgid carries `qiita-pipeline` onto the subdirs
  `qiita-job` creates, mirroring `PATH_SCRATCH/ticket`. No prod host references
  exist yet, so nothing to migrate. (Dir documented in `first-deploy.md`'s
  dirs-perms table by #100.)
  ```bash
  derived=$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)
  sudo install -d -o qiita-orch -g qiita-pipeline -m 2770 "$derived/references"
  ```

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

_None yet._

#### Notes (no host action)

_None yet._

### Deployed 2026-06-16 — 26838ca

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

- (#89) [operator] Two prerequisites for the host-filter / minimap2-index work,
  both BEFORE the bucket-4 deploy:
  1. The **v1.5.3** miint mirror build must also carry the host-filter functions
     — `save_minimap2_index` (used by the new `build_minimap2_index` step) and
     `align_minimap2` (used by `host_filter`). `rype_classify` is already
     present; the duckdb-miint #126 BIGINT-`id_column` change is nice-to-have,
     NOT required (`host_filter` appends the rype id into a BIGINT accumulator
     column, which coerces a VARCHAR-returning build on insert, so a pre-#126
     build also works). Components `FORCE INSTALL miint` for
     v1.5.3, so they pull this build; a missing `save_minimap2_index` fails
     `build_minimap2_index` at the first host-reference build. Unlike
     `sequence_split` there is **no** compute-readiness probe for these yet (a
     follow-up could add one) — the first `host-reference-add` run is the
     functional gate.
  2. The index builders now write under **`PATH_DERIVED`**
     (`{PATH_DERIVED}/references/{idx}/{rype,minimap2}/…`), relocated from
     `PATH_SCRATCH`. `PATH_DERIVED` is already mandatory on the SLURM deploy (it
     also roots the SIF images dir), so this adds no new env var — just ensure
     the orchestrator service account can create/write
     `{PATH_DERIVED}/references/` (the jobs `mkdir` it at runtime). No prod host
     references exist yet, so nothing to migrate.
- (#72) [admin] Grant the **operator account** read on the three service env
  files (NOT the bearer tokens, NOT lake data) so it can source `DATABASE_URL`
  for `make migrate` and verify `PATH_SCRATCH`/`HMAC` consistency without sudo.
  Idempotent; re-run only if an env file is reinstalled (a fresh `install` drops
  the ACL). Confirm the operator principal with `id qiita` first (the model uses
  one shared `qiita` account; a multi-login site substitutes `g:<operators-group>:r`):
  ```bash
  sudo setfacl -m u:qiita:x /etc/qiita
  sudo setfacl -m u:qiita:r /etc/qiita/control-plane.env \
                            /etc/qiita/data-plane.env \
                            /etc/qiita/compute-orchestrator.env
  # verify
  getfacl -c /etc/qiita/control-plane.env | grep -q '^user:qiita:r' && \
    sudo -u qiita test -r /etc/qiita/control-plane.env && echo "operator read OK"
  ```

> **Deploy-time deviation (2026-06-16) — (#72) ACL grant NOT applied; OUTSTANDING.**
> This host's account model differs from the runbook's single-shared-`qiita`
> assumption: `qiita` is a non-sudo **service** account (owns the checkout, ran
> `git pull` + `make migrate`, already has `DATABASE_URL` in its environment, and
> **cannot** read `control-plane.env`), while the real operators are separate
> sudo users in a different group (`knightlab` etc.). Because no shared operators
> group was decided, the `setfacl` target was left open and the grant was skipped
> — it is idempotent and non-blocking (this deploy ran `make migrate` fine via
> the `DATABASE_URL` already in `qiita`'s env). **Action still owed:** apply
> `g:<operators-group>:r` (NOT `u:qiita:r` — see the multi-login carve-out above)
> once the operators group is chosen. Being addressed in the deploy-ergonomics
> follow-up that re-fits the runbook/tooling to this account model.

#### 3. Migrations

```sql
-- [admin] Pre-check before `make migrate`. The collection_date migration
-- rebinds the collection_date global field from 'date' to 'text' (so it can
-- hold partial dates like a bare year); its guard aborts the migration (and
-- halts `dbmate` mid-deploy) if any biosample_metadata row already references
-- the field, since such a row would be left misaligned under the new
-- data_type. Resolve any rows surfaced here BEFORE running `make migrate`. An
-- empty result means none exist; proceed. (#98)
SELECT bgf.internal_name, COUNT(*) AS metadata_rows
  FROM qiita.biosample_metadata m
  JOIN qiita.biosample_study_field bsf ON bsf.idx = m.biosample_study_field_idx
  JOIN qiita.biosample_global_field bgf ON bgf.idx = bsf.biosample_global_field_idx
 WHERE bgf.internal_name = 'collection_date'
 GROUP BY bgf.internal_name;
```

```sql
-- [admin] Pre-check before `make migrate`. The prune migration removes seven
-- unused seeded prep_sample_global_field rows. Every inbound reference is
-- ON DELETE RESTRICT, so the DELETE aborts the migration (and halts `dbmate`
-- mid-deploy) if any study field, metadata value, field exception, or protocol
-- association already links one of them. This counts all four reference kinds;
-- every count must be 0. Resolve any non-zero row BEFORE running `make migrate`.
-- An empty result means none of the seven exist anymore; proceed. (#98)
SELECT g.internal_name,
       (SELECT count(*) FROM qiita.prep_sample_study_field s
         WHERE s.prep_sample_global_field_idx = g.idx)     AS study_fields,
       (SELECT count(*) FROM qiita.prep_sample_metadata m
         WHERE m.global_field_idx = g.idx)                 AS metadata_rows,
       (SELECT count(*) FROM qiita.prep_sample_field_exception e
         WHERE e.global_field_idx = g.idx)                 AS field_exceptions,
       (SELECT count(*) FROM qiita.prep_protocol_field p
         WHERE p.prep_sample_global_field_idx = g.idx)     AS protocol_fields
  FROM qiita.prep_sample_global_field g
 WHERE g.internal_name IN ('alias', 'library_name', 'library_strategy',
                           'library_source', 'library_selection',
                           'library_layout', 'library_construction_protocol')
 ORDER BY g.internal_name;
```

```bash
# [operator] DATABASE_URL must be in your shell, pointing at the SAME DB as
# control-plane.env. activate.sh re-checks public.schema_migrations at deploy
# time and ABORTS before any restart if one is unapplied.
make -C ~/qiita-miint migrate
```
`dbmate` applies whatever is unapplied (idempotent); the guard — not this checklist — owns the authoritative set, so nothing is hand-listed here. (#89) adds `20260612000000_reference_index_minimap2_type` (drops + re-adds the `reference_index.index_type` CHECK to allow `'minimap2'` alongside `'rype'`; no extension, backfill, or pre-check). (#98) adds `20260616000000_collection_date_text` (rebinds the `collection_date` global field to `text`; gated on the collection_date pre-check above). (#98) adds `20260616000001_sequenced_pool_idx_bump` (RESTARTs `qiita.sequenced_pool.idx` at 25000; no extension, backfill, or pre-check). (#98) adds `20260616000002_prune_prep_sample_global_fields` (deletes seven unused seeded prep-sample global fields and makes `title` / `design_description` optional; gated on the prep-field pre-check above).

#### 4. Deploy

_None yet._

#### 5. Verify

```bash
# (#89) [admin] spot-check that the brand-new fastq-to-parquet/1.1.0 row reached
# qiita.action. activate.sh runs `qiita-admin actions sync` and ABORTS on
# failure, so the in-place upserts of the *changed* host-reference-add /
# local-host-reference-add rows are gated there — this query just confirms the
# one new row landed.
psql "$DATABASE_URL" -tAc \
  "SELECT count(*) FROM qiita.action WHERE action_id='fastq-to-parquet' AND version='1.1.0'"   # 1 (#89)
```

#### Notes (no host action)

- (#89) Short-read host filtering is **opt-in** and changes no existing behavior: `fastq-to-parquet/1.0.0` is unchanged, and `1.1.0` only runs the host filter when a ticket sets `host_filter_enabled: true` + a `host_reference_idx` (a built host reference). Clients submitting `1.0.0` (or `1.1.0` without the flag) are unaffected. The host-reference-add workflows now build a minimap2 `.mmi` in addition to the rype `.ryxdi` (bucket-2 mirror prerequisite); no API/client change.
- New deploy tooling is available from the checkout this deploy onward: run
  `sudo make verify-deploy QIITA_HOSTNAME=<fqdn>` for the generic post-deploy
  checks (health, `qiita.action` list, compute-readiness — each with the correct
  run-as baked in), `sudo make preflight` for the read-only config/secret
  consistency check (PATH_SCRATCH/HMAC/token-perm + non-secret fingerprints), and
  `make redeploy QIITA_HOSTNAME=<fqdn>` (as the operator) to run the whole
  skeleton. The hand-copied `compute-readiness` verify line is retired — bucket-5
  verifies add only deploy-*specific* asserts on top of `make verify-deploy`. See
  [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) §7. (#72)

### Deployed 2026-06-15 — 03699e8

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

- (#90) [operator] Point the orchestrator at the shared dir the deploy stages
  miint into (`<derived>` = the `PATH_DERIVED` root). Cluster jobs and the
  compute-readiness probe LOAD from here, and the orchestrator propagates it into
  every job's environment, so it must precede the bucket-4 restart. (Not a hard
  boot fail-fast like the others — an unset/unstaged dir surfaces at the bucket-5
  probe, not at boot.)
  ```bash
  sudo bash -c 'grep -q "^MIINT_EXTENSION_DIRECTORY=" /etc/qiita/compute-orchestrator.env || echo "MIINT_EXTENSION_DIRECTORY=<derived>/duckdb-ext" >> /etc/qiita/compute-orchestrator.env'
  ```

#### 2. One-time host setup

- (#86, #90) [operator] Ensure the **v1.5.3** miint build on the mirror
  (`https://ftp.microbio.me/pub/miint/v1.5.3/`) includes the `sequence_split`
  scalar (duckdb-miint #121) BEFORE the bucket-4 stage step. Components run DuckDB
  1.5.3 (#85); the bucket-4 stage step (#90) pulls the `v1.5.3` build from the
  mirror into the shared extension dir and the cluster LOADs it, so the chunking
  SQL (`UNNEST(sequence_split(...))`) only resolves if that build has the
  function. Adding it is backward-compatible (it only adds a scalar), so
  publishing early does not affect already-deployed code. If the v1.5.3 build
  lacks `sequence_split` when staged, `stage_local_fasta` and the CLI
  `reference load` FASTA path fail with "Scalar Function with name
  sequence_split does not exist" — the bucket-5 probe catches this first.
- (#90) [operator] Create the shared dir the orchestrator's
  `MIINT_EXTENSION_DIRECTORY` points at — `qiita-orch` owns it and writes the
  staged extension; every compute node reads it. The stage step itself runs in
  bucket 4 (it needs the newly-deployed code).
  ```bash
  derived=$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)
  sudo install -d -o qiita-orch -g qiita-orch -m 0755 "$derived/duckdb-ext"
  ```

#### 3. Migrations

```bash
# [operator] DATABASE_URL must be in your shell, pointing at the SAME DB as
# control-plane.env. activate.sh re-checks public.schema_migrations at deploy
# time and ABORTS before any restart if one is unapplied.
make -C ~/qiita-miint migrate
```
`dbmate` applies whatever is unapplied (idempotent); the guard — not this checklist — owns the authoritative set, so nothing is hand-listed here. (#87) adds `20260611000000_study_ena_accession_and_bioproject` (renames the study `ebi_study_accession` column + its UNIQUE constraint to `ena_study_accession`, and adds a nullable, unique-when-present `bioproject_accession` column; no extension, backfill, or pre-check).

#### 4. Deploy

After `local-deploy.sh` (the standard deploy — see the runbook), which ships the
new stage-miint code, stage the miint extension into the shared dir. The cluster
is LOAD-only now, so jobs won't find miint until this runs; re-run it on any
miint or DuckDB version bump.

```bash
derived=$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)
py=$(sudo grep '^SLURM_NATIVE_PYTHON=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)
sudo -u qiita-orch env PATH_DERIVED="$derived" SLURM_NATIVE_PYTHON="$py" \
    bash /home/qiita/qiita-miint/scripts/stage-miint-extension.sh   # (#90)
```

#### 5. Verify

```bash
# (#86, #90) [admin] the deployed compute node LOADs the staged v1.5.3 miint
# build, which must expose sequence_split (the native chunker stage_local_fasta
# / reference_load depend on). It is newer than read_fastx, so a staged build
# missing it passes the read_fastx probe but FAILS here — confirming the
# bucket-4 stage produced a current build. The probe now prints the underlying
# error on a failure, so a red row is self-diagnosing.
#
# RUN AS qiita-orch WITH THE CO ENV — not qiita-api with control-plane.env.
# `qiita-admin compute-readiness` subprocesses into the orchestrator venv and
# runs Settings.from_env(), so it needs compute-orchestrator.env and reads the
# 0400 qiita-orch:qiita-orch co-to-cp.token; qiita-admin is also not on the
# non-login PATH, hence the absolute path. The qiita-api/control-plane.env form
# fails on all three counts and has had to be hand-corrected every deploy
# (see #67 and the 2026-06-10 archive deviation) — do not reintroduce it.
sudo -u qiita-orch bash -c 'set -a; source /etc/qiita/compute-orchestrator.env; set +a; \
    /home/qiita/.local/bin/qiita-admin compute-readiness' | grep -E 'probe/(miint-read-fastx|miint-sequence-split)'   # both =ok
```

#### Notes (no host action)

- (#91) `POST /study/lookup-by-accession` now resolves by `bioproject_accession`
  by default (was `ena_study_accession`); a caller omitting the new optional
  `accession_field` body field will match a different column than before. The
  `qiita submit-bcl-convert` preflight relies on this (its project accessions
  are BioProjects). `POST /biosample/lookup-by-accession` also gained the
  optional `accession_field` selector (`biosample_accession` default or
  `ena_sample_accession`), default behavior unchanged. No env var or migration.
- (#87) The study REST field and `qiita study create`/`patch` CLI flag
  `ebi_study_accession` / `--ebi-study-accession` were renamed to
  `ena_study_accession` / `--ena-study-accession`. Any client sending the old
  field name (or scripts using the old flag) must update; the column rename
  itself is handled by the bucket-3 migration.
- (#86) Sequence chunking switched from the pure-SQL `list_transform`/`substring` macro to miint's native `sequence_split` (duckdb-miint #121), fixing an O(L²) blow-up on large single FASTA records (DuckDB #23229). No client/API change — same chunked-Parquet shape `(read_id, chunk_index, chunk_data)`. The only operator action is the bucket-2 mirror check (the deployed code needs a v1.5.3 miint build that has `sequence_split`); no env var, host dir, or migration.

### Deployed 2026-06-10 — c230e87

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

```sql
-- [admin] Pre-check before `make migrate`. The matrix_tube_id migration
-- tightens the format CHECK to exactly 10 digits; its guard aborts the
-- migration (and halts `dbmate` mid-deploy) if any row holds a shorter
-- 8–9 digit value. There is no safe automated fix — correct each surfaced
-- row to its real 10-digit id BEFORE running `make migrate`. An empty
-- result means none exist; proceed straight to `make migrate`. (#81)
SELECT idx, matrix_tube_id
  FROM qiita.biosample
 WHERE matrix_tube_id IS NOT NULL
   AND matrix_tube_id !~ '^[0-9]{10}$';
```

```sql
-- [admin] Pre-check before `make migrate`. The ENVO-seed migration rebinds
-- broad_scale_environmental_context, local_environmental_context and
-- environmental_medium to the 'terminology' data_type; its guard aborts the
-- migration (and halts `dbmate` mid-deploy) if any biosample_metadata row
-- already references those fields, since such rows would be left misaligned
-- under the new data_type. Resolve any rows surfaced here BEFORE running
-- `make migrate`. An empty result means none exist; proceed. (#81)
SELECT bgf.internal_name, COUNT(*) AS metadata_rows
  FROM qiita.biosample_metadata m
  JOIN qiita.biosample_study_field bsf ON bsf.idx = m.biosample_study_field_idx
  JOIN qiita.biosample_global_field bgf ON bgf.idx = bsf.biosample_global_field_idx
 WHERE bgf.internal_name IN ('broad_scale_environmental_context',
                             'local_environmental_context',
                             'environmental_medium')
 GROUP BY bgf.internal_name;
```

```bash
# [operator] DATABASE_URL must be in your shell, pointing at the SAME DB as
# control-plane.env. activate.sh re-checks public.schema_migrations at deploy
# time and ABORTS before any restart if one is unapplied.
make -C ~/qiita-miint migrate
```
`dbmate` applies whatever is unapplied (idempotent); the guard — not this checklist — owns the authoritative set, so nothing is hand-listed here. (#80) adds `20260609000000_work_ticket_transient_retry` (plain `ALTER TABLE qiita.work_ticket ADD COLUMN transient_reason/transient_since`, both nullable; no extension or backfill). (#81) adds `20260604000000_study_submission_tracking`, `20260608000000_biosample_field_rebind_fn`, `20260608000001_seed_envo_terminology` (gated on the ENVO pre-check above), and `20260609000001_biosample_matrix_tube_id_exact_length` (gated on the matrix-tube pre-check above).

#### 4. Deploy

```bash
# [admin] SKIP_PULL=1 because redeploy.md step 2 already pulled the clone.
sudo SKIP_PULL=1 QIITA_HOSTNAME=qiita-miint.ucsd.edu /home/qiita/qiita-miint/deploy/local-deploy.sh

# (#80) [operator] local-deploy.sh refreshes only the /opt/qiita service venvs.
# Native SLURM jobs run from SLURM_NATIVE_PYTHON's SEPARATE shared-FS checkout —
# refresh it too, or native jobs import stale qiita-common (and can keep a stale
# cached miint whose read_fastx lacks max_batch_bytes). The next job then
# FORCE-installs miint from the mirror. See redeploy.md §6. Run as the `qiita`
# user that OWNS the checkout — running as the deploying admin hits a
# Permission-denied removing qiita-owned .venv files. uv is not on qiita's login
# PATH, so invoke it by full path (/usr/local/bin/uv on qiita-miint).
sudo -u qiita bash -lc 'cd /home/qiita/qiita-miint/qiita-compute-orchestrator && /usr/local/bin/uv sync --reinstall-package qiita-common'
```

#### 5. Verify

```bash
# (#80) [admin] Run as qiita-orch with the CO env (the CP-side form fails on
# co-to-cp.token perms). KNOWN-BROKEN PROBE — compute-readiness currently exits 2
# with `slurm-probe-completed: state=FAILED` (sacct: ExitCode 2:0, MaxRSS ~20MB,
# ~1s) on EVERY host: the generated probe bash has a syntax error — a `\n` in an
# f-string comment in build_probe_script expands to a real newline, exposing an
# unmatched backtick, so bash aborts at parse time before any check runs. This is
# a probe CODE bug, NOT a compute-env failure (the miint/native-import checks #80
# added never execute). Tracked as a follow-up: escape the comment, add a `bash -n`
# regression test on the generated script, and relocate the probe log off
# node-local /tmp so results are head-node-readable here. The head-node check
# `native-python-on-host=ok` does pass. Until the probe is fixed, confirm the
# compute env with a real reference-load / fastq-to-parquet job.
sudo -u qiita-orch bash -c 'set -a; source /etc/qiita/compute-orchestrator.env; set +a; \
    /home/qiita/.local/bin/qiita-admin compute-readiness'   # native-python-on-host=ok; rest FAILs pending probe fix (#80)
```

#### Notes (no host action)

- (#80) Additive work-ticket status fields `transient_reason` / `transient_since` (`GET /work-ticket/{idx}` and the list view) surface why the runner is retrying an unreachable orchestrator in place. Backed by the plain `20260609000000_work_ticket_transient_retry` migration (bucket 3); additive, so existing clients are unaffected. No host action beyond `make migrate`.
- (#81) Checklist binding is now by **name**: biosample/sequenced-sample create and biosample patch take a checklist name (e.g. `ERC000015`) instead of a `metadata_checklist_idx` (unknown name → 422), and `BiosampleResponse`/`SequencedSampleResponse` now return the checklist as a `metadata_checklist` ref (`{idx, name}`) instead of a bare `metadata_checklist_idx`. Clients sending the old idx field or reading the old response key must update. CLI flag is now `--metadata-checklist-name`.
- (#81) `matrix_tube_id` is now validated as exactly 10 digits (was 8–10); a previously-accepted 8- or 9-digit value now returns 422.

### Deployed 2026-06-08 — 2666587

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._ — (#77) the compute decoupling adds no new env var; the CP poll cadence is a code constant.

#### 2. One-time host setup

_None yet._

#### 3. Migrations

```bash
# [operator] DATABASE_URL must be in your shell, pointing at the SAME DB as
# control-plane.env. activate.sh re-checks public.schema_migrations at deploy
# time and ABORTS before any restart if one is unapplied.
make -C ~/qiita-miint migrate
```
`dbmate` applies whatever is unapplied (idempotent); the guard — not this checklist — owns the authoritative set, so nothing is hand-listed here. (#77) adds `20260603000000_work_ticket_step` (plain `CREATE TABLE qiita.work_ticket_step` + trigger; no extension or backfill).

#### 4. Deploy

```bash
# [admin] SKIP_PULL=1 because redeploy.md step 2 already pulled the clone.
# local-deploy.sh rsyncs all four components, so CP and CO ship together in one
# run — REQUIRED for (#77): the CP↔CO step contract changed (POST /step/run is
# gone, replaced by /step/submit|status|result + /step/find-by-name), so a CP
# and CO on opposite sides of this change can't talk. A single local-deploy.sh
# satisfies this; do not deploy one service alone.
# local-deploy.sh → activate.sh also runs `qiita-admin actions sync`, which picks
# up the two new workflows/ entries. (#78)
sudo SKIP_PULL=1 QIITA_HOSTNAME=qiita-miint.ucsd.edu /home/qiita/qiita-miint/deploy/local-deploy.sh
```

#### 5. Verify

```bash
# (#77) [admin] work_ticket_step table exists after `make migrate`, and the
# decoupled step routes answer (find-by-name is CP→CO-token-gated, so an
# unauthenticated probe should get 401, not 404 — proves the route is mounted).
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
    psql "$DATABASE_URL" -c "SELECT to_regclass('"'"'qiita.work_ticket_step'"'"') IS NOT NULL AS ok;"'   # ok = t (#77)
curl -fsS -o /dev/null -w '%{http_code}\n' https://qiita-miint.ucsd.edu/api/v1/work-ticket   # 401 (auth required), not 404 (#77)
# [admin] local-reference-add + local-host-reference-add 1.0.0 synced into
# qiita.action by `qiita-admin actions sync` inside activate.sh.
# (#78)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
    psql "$DATABASE_URL" -c "SELECT action_id, version, enabled FROM qiita.action ORDER BY action_id;"'   # local-reference-add + local-host-reference-add 1.0.0 enabled (#78)
```

#### Notes (no host action)

- (#77) Compute-step execution is decoupled and the CP now drives the poll loop. **CP and CO must deploy together** (bucket 4 does this) — the synchronous `POST /step/run` is removed in favour of the stateless `submit` / `status` / `result` trio plus `POST /step/find-by-name` (all CP↔CO-token-gated, internal). No external client action. Restart recovery now re-attaches in-flight tickets instead of failing them, so a CP/CO restart mid-deploy no longer nukes running work. Additive public surface: `GET /work-ticket` (list with compute status) and the `qiita ticket list` CLI.
- (#78) Local-host FASTA ingest (additive, no client breakage). Two new `workflows/` entries — `local-reference-add` and `local-host-reference-add` (both 1.0.0) — synced into `qiita.action` by `qiita-admin actions sync` inside `activate.sh` (verify in bucket 5), **not** migrations. They back a new CLI gesture `qiita reference load --local --fasta-manifest <abs path>` that ingests many host-resident FASTA files **by path** (no DoPut upload). The manifest and every FASTA + companion it lists must be **absolute** and visible on the shared FS from the compute node (the workflow `context_schema` enforces `pattern:"^/"`; bind mounts expose host paths, they do not copy). No new env var, host dir, or migration.
- (#78) The `qiita reference load` CLI now parses FASTA with miint's `read_fastx`, so it installs + loads the **miint DuckDB extension client-side** on first use (one-time network egress; cached after under `~/.duckdb` or `MIINT_EXTENSION_DIRECTORY`). It installs from the team mirror by default (FORCE INSTALL, implies allow-unsigned; `MIINT_EXTENSION_REPO` overrides for a local/dev build), so every Qiita component runs the same build — no community-vs-mirror patchwork (#80). No action on the deployed services — this only affects the host a user runs the CLI from.

### Deployed 2026-06-02 — 9ee069d

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

#### 2. One-time host setup

_None yet._

#### 3. Migrations

```sql
-- [admin] Pre-check before `make migrate`. The migration adds
-- UNIQUE (ebi_study_accession) on qiita.study (NULLs distinct); if any
-- two non-NULL rows share a value, `ADD CONSTRAINT UNIQUE` aborts and
-- `dbmate` halts mid-deploy. Resolve any duplicates surfaced here
-- (clear the dup on the row that should keep being unique, or pick
-- one of the rows to retain the value) BEFORE running `make migrate`.
-- An empty result means no duplicates; proceed straight to `make migrate`. (#74)
SELECT ebi_study_accession, COUNT(*) AS dup_count, array_agg(idx ORDER BY idx) AS study_idxs
  FROM qiita.study
 WHERE ebi_study_accession IS NOT NULL
 GROUP BY ebi_study_accession
HAVING COUNT(*) > 1;
```

```bash
# [operator] DATABASE_URL must be in your shell, pointing at the SAME DB as
# control-plane.env. activate.sh re-checks public.schema_migrations at deploy
# time and ABORTS before any restart if one is unapplied.
make -C ~/qiita-miint migrate
```
`dbmate` applies whatever is unapplied (idempotent); the guard — not this checklist — owns the authoritative set, so nothing is hand-listed here. (#74) adds `20260601000003_study_ebi_accession_unique` (gated on the pre-check above).

#### 4. Deploy

_None yet._

#### 5. Verify

_None yet._

#### Notes (no host action)

_None yet._

### Deployed 2026-06-02 — e78d601

Everything merged but not yet deployed. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

No new env var — host references (#70) reuse the existing `PATH_SCRATCH` (set in the 2026-06-01 deploy).

#### 2. One-time host setup

No host setup — the rype `.ryxdi` dir is `mkdir`'d at runtime under `PATH_SCRATCH/references/`; no manual dir step (#70).

#### 3. Migrations

```bash
# [operator] DATABASE_URL must be in your shell, pointing at the SAME DB as
# control-plane.env. activate.sh re-checks public.schema_migrations at deploy
# time and ABORTS before any restart if one is unapplied.
make -C ~/qiita-miint migrate
```
`dbmate` applies whatever is unapplied (idempotent); the guard — not this checklist — owns the authoritative set, so nothing is hand-listed here. (#70) adds the three `20260601*` reference migrations (`is_host`, the `indexing` status CHECK, `reference_index`).

#### 4. Deploy

```bash
# [admin] SKIP_PULL=1 because redeploy.md step 2 already pulled the clone
sudo SKIP_PULL=1 QIITA_HOSTNAME=qiita-miint.ucsd.edu /home/qiita/qiita-miint/deploy/local-deploy.sh
```

#### 5. Verify

```bash
# [admin] host-reference-add 1.0.0 synced into qiita.action by `qiita-admin
# actions sync` inside activate.sh (#70)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
    psql "$DATABASE_URL" -c "SELECT action_id, version, enabled FROM qiita.action ORDER BY action_id;"'   # host-reference-add 1.0.0 enabled (#70)
```

#### Notes (no host action)

- (#70) Host references (additive, no client action): `qiita.reference` gains `is_host` and a new `indexing` status (`loading → indexing → active`); new read endpoints `GET /reference` (list, filterable) and `GET /reference/{idx}/index`. The `host-reference-add` workflow is a new `workflows/` entry synced into `qiita.action` by `qiita-admin actions sync` inside `activate.sh` (verify in bucket 5) — not a migration. The rype `.ryxdi` index is written under `PATH_SCRATCH/references/{idx}/rype/` and the orchestrator propagates `PATH_SCRATCH` into SLURM jobs so it lands on the shared FS, not node-local `/tmp`. Note this is the **scratch** tier — if the deploy's scratch-cleanup policy purges `PATH_SCRATCH`, a built index would need a rebuild (re-run `host-reference-add`); a dedicated persistent tier for built indices is a possible follow-up.
- (#75) bcl-convert SIF build is now generic. The command changed: `bash scripts/build-bcl-convert-sif.sh` → `PATH_DERIVED=<derived> bash scripts/build-sif.sh bcl-convert` (per-workflow spec now lives in `workflows/bcl-convert/sif-build.env`). The builder stages into a temp root **owned by the invoking user** and only reads the checkout, so it no longer needs the `qiita`-owned `workflows/bcl-convert/` dir writable by `qiita-orch` — if a `chmod`/`setfacl` workaround was applied there to get the build to run, it can be removed. The produced SIF is byte-for-byte the same and `local-deploy.sh` does not rebuild SIFs, so a routine deploy needs no action; this only matters next time the SIF is (re)built.

### Deployed 2026-06-01 — aa546c8

Filesystem env vars restructured onto three base roots. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

> ⚠️ **(#73) This deploy renames every filesystem env var.** Old names are gone; the services derive fixed subdirs from three base roots (`PATH_SCRATCH`, `PATH_PERSISTENT`, `PATH_DERIVED`), so the CP/DP/CO won't boot until the new vars are set (bucket 1). The lake (`PATH_PERSISTENT/ducklake`) is currently **empty** — no durable data has been written — so there is **no data to migrate**; bucket 2 only creates the derived dirs and, if the DuckLake catalog refuses the new data_path, recreates the empty catalog (lossless). If the lake is somehow non-empty at deploy time, **stop** and reassess before recreating anything.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

```bash
# All of bucket 1 is [admin]; same sudo/redirect rules as past deploys. The new
# code reads ONLY the new names; the old WORK_TICKET_WORKSPACE_ROOT /
# SHARED_FILESYSTEM_ROOT / UPLOAD_STAGING_ROOT / DUCKLAKE_DATA_PATH /
# QIITA_IMAGES_DIR lines are now ignored — leave them for now, delete after a
# clean deploy. (#73)

# (#73) First, read the roots already configured so PATH_* lands consistently.
# PATH_SCRATCH must be byte-identical in all three env files (all derive
# /ticket and/or /staging). Pick <scratch> = the scratch root these used; pick
# <persistent> so that <persistent>/ducklake is where the (currently empty)
# lake will live; pick <derived> so <derived>/images holds the SIFs.
sudo grep -hE '^(WORK_TICKET_WORKSPACE_ROOT|SHARED_FILESYSTEM_ROOT|UPLOAD_STAGING_ROOT|DUCKLAKE_DATA_PATH|QIITA_IMAGES_DIR)=' /etc/qiita/control-plane.env /etc/qiita/data-plane.env /etc/qiita/compute-orchestrator.env 2>/dev/null

# control-plane.env — needs PATH_SCRATCH (derives /ticket + /staging)
sudo bash -c 'grep -q "^PATH_SCRATCH=" /etc/qiita/control-plane.env || echo "PATH_SCRATCH=<scratch>" >> /etc/qiita/control-plane.env'   # (#73)

# data-plane.env — PATH_SCRATCH (byte-identical to CP, derives /staging) + PATH_PERSISTENT (derives /ducklake)
sudo bash -c 'grep -q "^PATH_SCRATCH=" /etc/qiita/data-plane.env || grep "^PATH_SCRATCH=" /etc/qiita/control-plane.env >> /etc/qiita/data-plane.env'   # (#73)
sudo bash -c 'grep -q "^PATH_PERSISTENT=" /etc/qiita/data-plane.env || echo "PATH_PERSISTENT=<persistent>" >> /etc/qiita/data-plane.env'   # (#73)

# compute-orchestrator.env — PATH_SCRATCH (byte-identical, derives /ticket for the readiness probe) + PATH_DERIVED (derives /images, required when COMPUTE_BACKEND=slurm)
sudo bash -c 'grep -q "^PATH_SCRATCH=" /etc/qiita/compute-orchestrator.env || grep "^PATH_SCRATCH=" /etc/qiita/control-plane.env >> /etc/qiita/compute-orchestrator.env'   # (#73)
sudo bash -c 'grep -q "^PATH_DERIVED=" /etc/qiita/compute-orchestrator.env || echo "PATH_DERIVED=<derived>" >> /etc/qiita/compute-orchestrator.env'   # (#73) e.g. /scratch/persistent (SIFs live at <derived>/images)
```

#### 2. One-time host setup

```bash
# (#73) Create the scratch leaves the services now derive. ticket + staging are
# the renamed orch-workspace + upload-staging dirs; safe to start empty (both
# are scratch — any in-flight upload/ticket should be drained first). Use the
# SAME owner/group/mode the old dirs carried.   [admin]
scratch=$(sudo grep '^PATH_SCRATCH=' /etc/qiita/control-plane.env | tail -1 | cut -d= -f2-)
sudo install -d -o qiita-orch -g qiita-pipeline -m 2770 "$scratch/ticket"
sudo install -d -o qiita-data -g qiita-pipeline -m 2770 "$scratch/staging"

# (#73) Images tier: point <derived>/images at the existing SIF dir. If the old
# QIITA_IMAGES_DIR was already /scratch/persistent/images and PATH_DERIVED is
# /scratch/persistent, this is a no-op. Otherwise move it, then assert the
# first-deploy §0.3 perms (qiita-orch:qiita-orch 0755):   [admin]
#   images="$(sudo grep '^PATH_DERIVED=' /etc/qiita/compute-orchestrator.env | tail -1 | cut -d= -f2-)/images"
#   sudo mv <old-images-dir> "$images"
#   sudo chown qiita-orch:qiita-orch "$images" && sudo chmod 0755 "$images"

# (#73) Create the lake data dir the DP now derives (PATH_PERSISTENT/ducklake).
# The lake is EMPTY — nothing has been written — so there is no data to move.   [admin]
persistent=$(sudo grep '^PATH_PERSISTENT=' /etc/qiita/data-plane.env | tail -1 | cut -d= -f2-)
sudo install -d -o qiita-data -g qiita-data -m 0750 "$persistent/ducklake"

# (#73) ⚠️ Empty-lake guard, BEFORE the bucket-4 restart. The DuckLake catalog
# (Postgres named by the DP's DUCKLAKE_CATALOG_CONNSTR) was pinned to the OLD
# DUCKLAKE_DATA_PATH at the DP's first attach. With zero registered data files,
# re-attaching at the new PATH_PERSISTENT/ducklake should just re-pin cleanly;
# if it instead reports a "path mismatch", recreate the EMPTY catalog DB so it
# re-pins the new data_path. This is lossless ONLY because the lake is empty —
# first CONFIRM there are no data files (e.g. the catalog's ducklake data-file
# table is empty / the old data dir holds no parquet). If any data exists, STOP.
#   sudo systemctl stop 'qiita-data-plane@*'
#   # confirm empty, then drop + recreate the lake catalog DB (DBA), e.g.:
#   #   dropdb qiita_miint_lake && createdb -O qiita_miint_lake_rw qiita_miint_lake
#   # the DP recreates the reference tables on its next boot (ensure_reference_tables).
# The DP restarts in bucket 4; confirm a DoGet in bucket 5.
```

#### 3. Migrations

_None yet._

#### 4. Deploy

_None yet._

#### 5. Verify

```bash
# (#73) [admin] After the bucket-4 restart, confirm the lake reads back and the
# derived workspaces are writable end-to-end.
curl -fsS https://qiita-miint.ucsd.edu/health                                  # all three pills green
sudo -u qiita-orch bash -c 'set -a; source /etc/qiita/compute-orchestrator.env; set +a; /home/qiita/.local/bin/qiita-admin compute-readiness'   # PATH_SCRATCH/ticket visible+writable on a compute node
```
- The data plane attaches DuckLake cleanly at `PATH_PERSISTENT/ducklake` (DP boots; `/health` DP pill green). A DoGet/DoPut round-trip works on the freshly-pinned empty lake — (#73)

#### Notes (no host action)

- (#73) Filesystem env vars restructured onto base roots: `PATH_SCRATCH` (→`/ticket`, `/staging`), `PATH_PERSISTENT` (→`/ducklake`), `PATH_DERIVED` (→`/images`). The old per-leaf vars are no longer read by any service. After a clean deploy, delete the stale `WORK_TICKET_WORKSPACE_ROOT` / `SHARED_FILESYSTEM_ROOT` / `UPLOAD_STAGING_ROOT` / `DUCKLAKE_DATA_PATH` / `QIITA_IMAGES_DIR` lines from the three env files.

### Deployed 2026-06-01 — 178f782

Everything merged but not yet deployed. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

#### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

```bash
# All of bucket 1 is [admin]. /etc/qiita/*.env is mode 0440 (root:qiita-api /
# root:qiita-data), so reads and writes go through sudo — and the redirect itself
# must run as root, hence `sudo bash -c '... >> file'` (a bare `sudo ... >> file`
# would redirect as your unprivileged shell and fail). One line per var so the
# block copy/pastes cleanly. Each append is guarded by `grep -q ... ||`, so the
# whole block is idempotent — safe to re-run after a partial/failed deploy.

# (#49) UPLOAD_STAGING_ROOT is a NEW dir under the shared scratch FS. Set it ONCE on
# the CP side; the DP value and the dir (bucket 2) are read back from it. First, see
# the roots already configured here so you pick a consistent location:
sudo grep -hE '^(WORK_TICKET_WORKSPACE_ROOT|DUCKLAKE_DATA_PATH|UPLOAD_STAGING_ROOT)=' /etc/qiita/control-plane.env /etc/qiita/data-plane.env 2>/dev/null

# control-plane.env — substitute <scratch> with a path under the scratch FS shown above
sudo bash -c 'grep -q "^CONTACT_EMAIL=" /etc/qiita/control-plane.env || echo "CONTACT_EMAIL=qiita.help@gmail.com" >> /etc/qiita/control-plane.env'                 # (#issue-53)
sudo bash -c 'grep -q "^UPLOAD_STAGING_ROOT=" /etc/qiita/control-plane.env || echo "UPLOAD_STAGING_ROOT=<scratch>/upload-staging" >> /etc/qiita/control-plane.env'  # (#49)

# data-plane.env — derived from the CP value (byte-identical, no retyping)   (#49)
sudo bash -c 'grep -q "^UPLOAD_STAGING_ROOT=" /etc/qiita/data-plane.env || grep "^UPLOAD_STAGING_ROOT=" /etc/qiita/control-plane.env >> /etc/qiita/data-plane.env'

# compute-orchestrator.env
sudo bash -c 'grep -q "^SLURM_NATIVE_PYTHON=" /etc/qiita/compute-orchestrator.env || echo "SLURM_NATIVE_PYTHON=/home/qiita/qiita-miint/qiita-compute-orchestrator/.venv/bin/python" >> /etc/qiita/compute-orchestrator.env'   # (#57)
sudo bash -c 'grep -q "^SLURM_QOS=" /etc/qiita/compute-orchestrator.env || echo "SLURM_QOS=qiita_norm" >> /etc/qiita/compute-orchestrator.env'                      # (#57)
sudo bash -c 'grep -q "^QIITA_CP_URL=" /etc/qiita/compute-orchestrator.env || echo "QIITA_CP_URL=https://qiita-miint.ucsd.edu" >> /etc/qiita/compute-orchestrator.env'   # (#57)
sudo bash -c 'grep -q "^QIITA_IMAGES_DIR=" /etc/qiita/compute-orchestrator.env || echo "QIITA_IMAGES_DIR=/scratch/persistent/images" >> /etc/qiita/compute-orchestrator.env'   # (#62) abs dir, visible from every compute node; validated at CO boot when COMPUTE_BACKEND=slurm
```

#### 2. One-time host setup

```bash
# (#57) qiita-pipeline group membership — verify, fix if needed   [admin]
id qiita-api qiita-orch qiita-data qiita-job | grep qiita-pipeline    # all four should match
sudo usermod -aG qiita-pipeline qiita-api qiita-orch qiita-data qiita-job   # only if any missing

# (#57) compute-node-visible orchestrator venv   [operator]
sudo -u qiita bash -lc 'cd /home/qiita/qiita-miint/qiita-compute-orchestrator && uv sync --reinstall-package qiita-common'

# (#49) create the upload-staging dir at exactly the configured path — read it back
#       via sudo (root-owned env file) so it can't diverge (DP writes as owner, CP
#       reads via qiita-pipeline)   [admin]
staging=$(sudo grep '^UPLOAD_STAGING_ROOT=' /etc/qiita/control-plane.env | tail -1 | cut -d= -f2-)
sudo install -d -o qiita-data -g qiita-pipeline -m 2770 "$staging"

# (#issue-53) confirm AuthRocket realm invitation-acceptance redirect URI is
#   https://qiita-miint.ucsd.edu/api/v1/auth/handoff   (AuthRocket admin dashboard; no host command)

# (#62) bcl-convert RPM placement (Illumina EULA: do NOT commit to git) + SIF build   [operator]
#   download bcl-convert-4.5.4-2.el8.x86_64.rpm from
#   https://support.illumina.com/sequencing/sequencing_software/bcl-convert/downloads.html
sudo install -d -o qiita-orch -g qiita-pipeline -m 0750 /scratch/persistent/images/sources
sudo install -o qiita-orch -g qiita-pipeline -m 0640 bcl-convert-4.5.4-2.el8.x86_64.rpm \
    /scratch/persistent/images/sources/bcl-convert-4.5.4-2.el8.x86_64.rpm
sudo -u qiita-orch bash -lc 'bash /home/qiita/qiita-miint/scripts/build-bcl-convert-sif.sh'   # idempotent

# (#62) grant the new SA scope to compute-worker   [admin]
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
    qiita-admin service-account update --display-name compute-worker --add-scope sequenced_pool:preflight:read'
```

> **Deploy-time deviation (2026-06-01).** This bucket-2 step as written did not work on the host and was performed differently; tracked for a checklist fix (the bucket-5 `compute-readiness` line has its own deviation note below; see [#67](https://github.com/the-miint/Qiita/issues/67) and follow-ups):
> - **(#62) compute-worker scope grant** — `qiita-admin service-account update --add-scope` is not a real command (no such subcommand), and the scope grant must run *after* the bucket-4 deploy (the new ceiling ships in that code). Done instead as a token rotation per [`orchestrator-token-rotation.md`](docs/runbooks/orchestrator-token-rotation.md): minted `compute-rot-2026-06-01` (principal 5) with `["sequence_range:mint","sequenced_pool:preflight:read"]`, swapped `/etc/qiita/co-to-cp.token`, restarted the orchestrator, revoked the old `compute` SA (principal 3). Live SA is named `compute`, not `compute-worker`.

#### 3. Migrations

```bash
# [operator] DATABASE_URL must be in your shell, pointing at the SAME DB as
# control-plane.env (the guard, running as root, checks that one). The operator
# account can't read the 0440 control-plane.env, so use the value from your
# provisioning / first deploy (see first-deploy.md step 1). activate.sh re-checks
# public.schema_migrations at deploy time and ABORTS before any restart if one is
# unapplied — pointing DATABASE_URL elsewhere migrates one DB while the guard
# checks another (which the guard's wrong-DB hint flags).
make -C ~/qiita-miint migrate
```
`dbmate` applies whatever is unapplied (idempotent). The guard — not this checklist — owns the authoritative set of required migrations, so nothing is hand-listed here to drift out of sync.

#### 4. Deploy

```bash
# [admin] SKIP_PULL=1 because redeploy.md step 2 already pulled the clone
sudo SKIP_PULL=1 QIITA_HOSTNAME=qiita-miint.ucsd.edu /home/qiita/qiita-miint/deploy/local-deploy.sh
```

#### 5. Verify

```bash
# [admin]
curl -fsS https://qiita-miint.ucsd.edu/health                                  # CP+CO+DP aggregate + per-service pills (#58/#54)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
    psql "$DATABASE_URL" -c "SELECT action_id, version, enabled FROM qiita.action ORDER BY action_id;"'   # bcl-convert 1.0.0 enabled (#62)
systemctl cat qiita-control-plane qiita-compute-orchestrator | grep UMask      # UMask=0007 dropins (#57)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; qiita-admin compute-readiness'   # (#57)
```
- `/docs` and `/redoc` render (vendored assets, no CDN) — (#64)
- landing page loads with green status pills + working contact mailto — (#issue-53, #58/#54)

> **Deploy-time deviation (2026-06-01).** The `compute-readiness` verify line as written fails: it runs as `qiita-api` sourcing `control-plane.env`, but the `0400 qiita-orch:qiita-orch` `co-to-cp.token` is only readable by `qiita-orch`, and the check needs the CO env. Ran instead as: `sudo -u qiita-orch bash -c 'set -a; source /etc/qiita/compute-orchestrator.env; set +a; /home/qiita/.local/bin/qiita-admin compute-readiness'`. Two of its checks are false negatives — `cp-healthz` (CP serves `/health`, not `/healthz` — [#67](https://github.com/the-miint/Qiita/issues/67)) and `slurm-probe-log` (probe log written to node-local `/tmp`, unreadable from the head node). CP health confirmed independently via `curl /health` (all pills green).

#### Notes (no host action)

- (#62) `POST /sequencing-run` and `POST /sequencing-run/{R}/sequenced-pool` now return **200** on a matching-payload retry (was always 201); **409** with `{conflicting_field, existing_value, supplied_value}` on mismatch. Clients that strictly required 201 should accept 200.
- (#62) bcl-convert FASTQ output is large — a busy NovaSeq X lane can reach multiple TB. Size per-ticket scratch generously; the orchestrator does not pre-allocate, so disk-full mid-run surfaces as a SLURM job failure. Confirm exact per-instrument sizing against a real run before relying on a figure. Supported instruments: NovaSeq 6000, NovaSeq X, iSeq.
- (#63) `reference load` moved from `qiita-admin` to the `qiita` end-user CLI (it's a credentialed API call, not a host operation). Retarget any `qiita-admin reference load` scripts to `qiita reference load`.
- (#64) Interactive API docs now served from this origin: `/docs` (Swagger UI), `/redoc` (ReDoc), `/openapi.json`. No deploy action — assets ride the wheel; restart picks them up.
