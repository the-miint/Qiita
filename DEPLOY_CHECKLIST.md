# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; `## Deployed history` archives past ones.

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→5 in order; buckets 1–3 must precede the bucket-4 restart. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

### 2. One-time host setup

_None yet._

### 3. Migrations

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### Notes (no host action)

- `make redeploy` prompts less from this checkout onward — it skips the buckets
  1 & 2 ack when both are empty here, and skips the native-venv refresh when it's
  provably already current (`FORCE_NATIVE_REFRESH=1` overrides, e.g. recovering a
  deploy that died mid-`uv sync`). Behaviour ships with the script; no host
  action. See CHANGELOG / `redeploy.md` for the full rules. (#113)

---

## Deployed history

Archived `## Pending deploy` blocks, newest on top, each stamped with deploy date + the commit deployed. Populated by `/deploy-archive` at deploy time.

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
