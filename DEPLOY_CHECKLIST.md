# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's the changelog, see [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

- `[operator]` Confirm `DATA_PLANE_URL` in `/etc/qiita/control-plane.env` starts with `grpc://`
  (`grep '^DATA_PLANE_URL=' /etc/qiita/control-plane.env`). The control plane now refuses to boot
  with any other scheme, so a bad value keeps the unit down. (#899)

### 2. One-time host setup

- `[operator]` **Before the restart, run the host-taxon backfill.** From the restart a biosample
  with no `host_taxon_id` passes through host filtering undepleted, so skipping `--execute` lets
  residue biosamples (taxa the curated table does not cover) pass through undepleted. `--execute`
  writes them `not provided`, which stays refused until patched, and leaves hostless taxa absent; it
  is safe on the old build. This is the backfill's only run: after the restart an absent value is
  deliberate, so never run it again. (#864)
  1. Dry-run and read the report. Expect no `WARNING:` lines and `no taxon_id at all (not
     backfillable)` to read 0, because a blank that the backfill cannot recognize as a control, or a
     biosample with no `taxon_id`, is no longer refused; if not, stop and raise the report.
  2. Only then run it with `--execute`.
  3. Dry-run again. Expect `Pass --execute to write 0 row(s)`; if not, stop and raise the report.
  ```bash
  # [operator] in the pulled checkout; DATABASE_URL set as in docs/runbooks/redeploy.md (migrate step)
  cd ~/qiita-miint/qiita-control-plane
  /usr/local/bin/uv run --reinstall-package qiita-common qiita-admin backfill host-taxon-id --show-unresolved 50            # 1 and 3: dry run
  /usr/local/bin/uv run --reinstall-package qiita-common qiita-admin backfill host-taxon-id --show-unresolved 0 --execute  # 2: writes
  ```
- `[admin]` **gpl-boundary must report 0.5.0 or later**, or compute-readiness's
  `miint-gpl-boundary` probe fails the deploy. If it is older, replace the binary at that path by
  hand (the deploy does not upgrade it: #859). (#888)
  ```bash
  sudo -u qiita-orch "$(sudo grep -oP '^MIINT_GPL_BOUNDARY_PATH=\K.*' /etc/qiita/compute-orchestrator.env)" --version | grep gpl_boundary
  ```
- `[operator]` **Before the restart, no long-read-assembly ticket may be in flight.**
  long-read-assembly 1.0.2 gains steps in place (a `gene_call` step after `bin_refine`,
  two actions before `assembly_load`), so a ticket past `bin_refine` when the sync lands
  is refused on resume and has to be submitted again. Expect 0; otherwise wait for them
  to finish. (#888)
  ```bash
  psql "$DATABASE_URL" -tAc "SELECT count(*) FROM qiita.work_ticket WHERE action_id = 'long-read-assembly' AND state IN ('pending','queued','processing')"
  ```
- `[operator]` **Do not run `qiita-admin scratch reclaim --execute` until this deploy is in.**
  The bucket-6 gene-call backfill reads the finished long-read-assembly tickets'
  workspaces, and the build deployed now does not know to keep them. From this deploy,
  reclaim skips every ticket that backfill can still recover or is recovering. (#888)
- `[operator]` **Before `make migrate`, read what the bucket-3 migration will publish.** It
  makes public every study that an ENA import created, and does not check 2 or 3 below: read
  them, and before the deploy raise with each study's owner any that should not be public.
  1. The studies that still hold a biosample or sequenced_sample with no ENA accession.
     The migration refuses while any do, so expect no rows; otherwise stop the deploy and
     raise each study with its owner. No route removes a biosample from a study; a
     sequenced_sample leaves one only when a system_admin deletes its whole sequenced_pool.
  2. The studies whose alias, description, abstract or funding a person added (an import
     writes none of them). The owner edits these with `PATCH /api/v1/study/{study_idx}`.
  3. On the studies that an import created, the local fields with no `tier_override` that no
     import wrote: ones not created by an import's submitter while that import of the study
     ran. The owner keeps one off the public tier by setting its `tier_override` with
     `PATCH /api/v1/study/{study_idx}/<kind>/{idx}`, from the row.
  4. How many studies become public. (#929)
  ```bash
  psql "$DATABASE_URL" -tAc "WITH imported AS (SELECT study_idx FROM qiita.ena_import_batch_item WHERE study_created) SELECT bts.study_idx, 'biosample', count(*) FROM qiita.biosample_to_study bts JOIN qiita.biosample b ON b.idx = bts.biosample_idx WHERE NOT bts.retired AND b.ena_sample_accession IS NULL AND bts.study_idx IN (SELECT study_idx FROM imported) GROUP BY 1 UNION ALL SELECT pts.study_idx, 'sequenced_sample', count(*) FROM qiita.prep_sample_to_study pts JOIN qiita.sequenced_sample ss ON ss.prep_sample_idx = pts.prep_sample_idx WHERE NOT pts.retired AND ss.ena_run_accession IS NULL AND pts.study_idx IN (SELECT study_idx FROM imported) GROUP BY 1"
  psql "$DATABASE_URL" -tAc "SELECT idx, title FROM qiita.study WHERE idx IN (SELECT study_idx FROM qiita.ena_import_batch_item WHERE study_created) AND num_nonnulls(alias, description, abstract, funding) > 0"
  psql "$DATABASE_URL" -tAc "WITH imported AS (SELECT study_idx FROM qiita.ena_import_batch_item WHERE study_created), import_run AS (SELECT i.study_idx, b.submitted_by_principal_idx AS by_idx, i.created_at AS from_at, i.updated_at AS to_at FROM qiita.ena_import_batch_item i JOIN qiita.ena_import_batch b ON b.idx = i.batch_idx), field AS (SELECT study_idx, 'biosample-field' AS kind, idx, display_name, created_by_idx, created_at FROM qiita.biosample_study_field WHERE biosample_global_field_idx IS NULL AND tier_override IS NULL UNION ALL SELECT study_idx, 'prep-sample-field', idx, display_name, created_by_idx, created_at FROM qiita.prep_sample_study_field WHERE prep_sample_global_field_idx IS NULL AND tier_override IS NULL) SELECT f.study_idx, f.kind, f.idx, f.display_name FROM field f WHERE f.study_idx IN (SELECT study_idx FROM imported) AND NOT EXISTS (SELECT 1 FROM import_run r WHERE r.study_idx = f.study_idx AND r.by_idx = f.created_by_idx AND f.created_at BETWEEN r.from_at AND r.to_at) ORDER BY 1, 2, 4"
  psql "$DATABASE_URL" -tAc "SELECT count(DISTINCT study_idx) FROM qiita.ena_import_batch_item WHERE study_created"
  ```

### 3. Migrations

- `20261112000000_host_taxon_id_not_required` makes `host_taxon_id` optional, and from the
  restart an absent value means no host. `dbmate` applies it; run the bucket-2 backfill
  first. (#864)
- `20261113000000_output_equivalence` adds `qiita.output_equivalence` and `qiita.output_equivalence_event`, empty, and `20261113000001_exported_processing_mask` adds a nullable `mask_idx` to `qiita.exported_processing` and `qiita.exported_identifier`, widening their one-processing CHECKs to it; no row changes. `dbmate` applies both. `20261116000000_work_ticket_processing_idx` adds a nullable `processing_idx` to `qiita.work_ticket`, and `20261116000001_backfill_work_ticket_processing_idx` sets it on the started `long-read-assembly` tickets to the processing each one minted; idempotent, and a no-op on a fresh DB. A ticket that the old build starts between `make migrate` and the restart is not covered; the bucket-6 re-run closes that. `20261116000002_exported_identifier_prep_sample_delete` makes `qiita.exported_identifier.prep_sample_idx` nullable and ON DELETE SET NULL, adds two nullable description columns and a CHECK that every live row names a prep_sample (every existing row does: the column was NOT NULL), and adds a BEFORE DELETE trigger on `qiita.prep_sample`; no row changes. `20261116000003_work_ticket_reap_pending` adds two nullable columns to `qiita.work_ticket` (`reap_pending_since`, `reap_note`) and a CHECK that they are set together; no row changes. `20261116000004_definition_settings_lock` adds `qiita.definition_settings_edit` (empty), the function `qiita.edit_definition_settings`, and BEFORE UPDATE triggers on `qiita.mask_definition`, `qiita.alignment_definition` and `qiita.processing` that refuse a change to a definition's settings outside that function; no row changes. (#874)
- `[operator]` (#872) `make migrate` applies `20261117000000_field_exception_excludes_unique_in_study.sql` (triggers that keep a field exception off `unique_in_study` fields, and a NOT NULL `updated_by_idx` on both `*_field_exception` tables, filled from `created_by_idx` on any row already there). It refuses to apply, naming the rule, if an existing exception row already keys on a `unique_in_study` field. No route wrote exception rows before this deploy, so only a row written by hand can trip it. If it does refuse, stop and follow its HINT.
- `20261117120000_genome_derivation` (new table) and `20261119000000_exported_feature_genome_idx`
  (an index; the migration's comment gives its measured build time): plain `dbmate`, no setup. The bucket-6 WoL
  repair needs both. (#926)
- `[operator]` (#929) `make migrate` applies `20261120000000_ena_imported_studies_public.sql`, which sets `default_tier = 'public'` on every study that an ENA import created. It refuses to apply, naming the studies, while one holds data that did not come from ENA; the bucket-2 check finds them first. If it does refuse, stop and follow its HINT.

### 4. Deploy

_None yet._

### 5. Verify

- `[operator]` (#874) As a system_admin with a PAT carrying `mask_definition:lifecycle`, `qiita-admin output-equivalence list` exits 0 and prints `0 output equivalence(s)` on stderr (the output-equivalence routes are mounted and the new table is readable).

### 6. After the deploy verifies green

- `[operator]` **Re-run the idempotent `work_ticket.processing_idx` backfill**, for a `long-read-assembly` ticket that the old build started between `make migrate` (bucket 3) and the restart: its `processing_idx` is NULL, so the definition-group resolver does not count it as a run of its processing. dbmate does not re-apply a migration, so run its `migrate:up` block by hand. It only fills a NULL column, so it does not burn the rollback path. Run it right after the restart: a ticket that the new build ends before its mint stays NULL, as it should, but a re-run made after another ticket of the same prep_sample has minted the same params would fill it in. (#874)
  ```bash
  # [operator] in the pulled checkout; DATABASE_URL set as in docs/runbooks/redeploy.md (migrate step)
  cd ~/qiita-miint/qiita-control-plane
  sed -n '/^-- migrate:up/,/^-- migrate:down/p' db/migrations/20261116000001_backfill_work_ticket_processing_idx.sql | psql "$DATABASE_URL" -v ON_ERROR_STOP=1
  ```
- `[operator]` **Re-run the ENA migration's idempotent `migrate:up` block** right after the
  restart, for a study that an import on the old build created between `make migrate` and
  the restart: it was created `member`. It checks and refuses as in bucket 3, and only
  sets `public` where an import created the study. Then expect 0. (#929)
  ```bash
  # [operator] in the pulled checkout; DATABASE_URL set as in docs/runbooks/redeploy.md (migrate step)
  cd ~/qiita-miint/qiita-control-plane
  sed -n '/^-- migrate:up/,/^-- migrate:down/p' db/migrations/20261120000000_ena_imported_studies_public.sql | psql "$DATABASE_URL" -v ON_ERROR_STOP=1
  psql "$DATABASE_URL" -tAc "SELECT count(*) FROM qiita.study WHERE idx IN (SELECT study_idx FROM qiita.ena_import_batch_item WHERE study_created) AND default_tier <> 'public'"
  ```
- `[operator]` **Find the failed tickets that still hide a completed gate, check each, and cancel
  the ones whose completed run should be used.** The definition-group resolver reads a
  prep_sample as `incomplete` when its newest ticket under a definition, or a covering block
  ticket, is `failed`, unless that ticket is older than the gate's last change; the query below
  lists the failed tickets it still counts over a `completed` (or assembly `no_data`) gate.
  The live read-mask tickets left `failed` by the withdraw / re-run / restore redo predate
  their gates' restore, so the resolver already ignores them and they are not listed. For each listed
  ticket, read its failure (`qiita ticket status`) and decide whether the completed run is
  the one to use; if so, cancel it, as a system_admin with a PAT carrying `work_ticket:cancel`, with
  `qiita-admin ticket cancel <work_ticket_idx>`. Cancelling a prep_sample's newest per-sample ticket can expose an older failed one, so run the query again after cancelling, until it lists nothing you mean to cancel. Read-only query; it does not burn the rollback path. (#874)
  ```bash
  # [operator] DATABASE_URL set as in docs/runbooks/redeploy.md (migrate step)
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
  BEGIN READ ONLY;
  WITH gate AS (
      SELECT 'mask' AS kind, mask_idx AS definition_idx, prep_sample_idx, state, updated_at
        FROM qiita.mask_sample WHERE state = 'completed'
      UNION ALL
      SELECT 'alignment', alignment_idx, prep_sample_idx, state, updated_at
        FROM qiita.alignment_sample WHERE state = 'completed'
      UNION ALL
      SELECT 'processing', processing_idx, prep_sample_idx, state, updated_at
        FROM qiita.assembly_sample WHERE state IN ('completed', 'no_data')
  ), sample_ticket AS (
      -- each prep_sample's newest non-cancelled per-sample ticket under a definition
      SELECT DISTINCT ON (kind, definition_idx, prep_sample_idx) *
        FROM (
          SELECT 'mask' AS kind, mask_idx AS definition_idx, prep_sample_idx,
                 work_ticket_idx, state, created_at
            FROM qiita.work_ticket
           WHERE action_id IN ('read-mask', 'fastq-to-parquet') AND mask_idx IS NOT NULL
          UNION ALL
          SELECT 'alignment', alignment_idx, prep_sample_idx, work_ticket_idx, state, created_at
            FROM qiita.work_ticket
           WHERE action_id = 'align-denovo' AND alignment_idx IS NOT NULL
          UNION ALL
          SELECT 'processing', processing_idx, prep_sample_idx, work_ticket_idx, state, created_at
            FROM qiita.work_ticket
           WHERE action_id = 'long-read-assembly' AND processing_idx IS NOT NULL
        ) t
       WHERE prep_sample_idx IS NOT NULL AND state <> 'cancelled'
       ORDER BY kind, definition_idx, prep_sample_idx, work_ticket_idx DESC
  ), block_ticket AS (
      -- every non-cancelled ticket of a non-abandoned block covering the prep_sample
      SELECT CASE wt.action_id WHEN 'read-mask-block' THEN 'mask' ELSE 'alignment' END AS kind,
             CASE wt.action_id WHEN 'read-mask-block' THEN wt.mask_idx ELSE wt.alignment_idx END
               AS definition_idx,
             bm.prep_sample_idx, wt.work_ticket_idx, wt.state, wt.created_at
        FROM qiita.work_ticket wt
        JOIN qiita.block b ON b.work_ticket_idx = wt.work_ticket_idx
        JOIN qiita.block_member bm ON bm.block_idx = b.block_idx
       WHERE wt.action_id IN ('read-mask-block', 'align')
         AND wt.state <> 'cancelled' AND b.state <> 'abandoned'
  )
  SELECT g.kind, g.definition_idx, g.prep_sample_idx, g.state AS gate_state,
         g.updated_at AS gate_changed_at, t.work_ticket_idx, t.created_at AS ticket_created_at
    FROM gate g
    JOIN (SELECT * FROM sample_ticket UNION ALL SELECT * FROM block_ticket) t
      USING (kind, definition_idx, prep_sample_idx)
   WHERE t.state = 'failed' AND t.created_at >= g.updated_at
   ORDER BY g.kind, g.definition_idx, g.prep_sample_idx, t.work_ticket_idx;
  ROLLBACK;
  SQL
  ```
- `[admin]` **Add gene calls to the long-read-assembly runs finished before this deploy**
  (1.0.0, 1.0.1 and 1.0.2; the dry run counts them). Each run gets one
  `assembly-gene-call-recover` ticket, which reads the `assemble`, `bin_refine`, `checkm`,
  `assembly_hash` and `mint-features` outputs left in that run's ticket workspace and
  registers its gene calls and its `assembly_membership` and `bin_quality` again, with
  `genome_idx`. Run it soon
  after the deploy: those files last only as long as the scratch workspace does. (#888)
  ```bash
  # DATABASE_URL and PATH_SCRATCH exported, as for any `qiita-admin backfill`;
  # QIITA_TOKEN is a system_admin's PAT (only --execute reads it).
  qiita-admin backfill assembly-gene-call                       # dry run: counts per status, and each run it cannot recover (residue), with the reason
  qiita-admin backfill assembly-gene-call --execute --limit 5
  ```
  Once those five tickets complete, check each run in `make lake-shell`, with `PS`/`RUN`
  from the lines the `--limit 5` run printed. Expect no NULL `genome_idx` and a non-zero
  gene count:
  `SELECT (SELECT count(*) FROM qiita_lake.gene_call WHERE prep_sample_idx = PS AND processing_idx = RUN), (SELECT count(*) FILTER (WHERE genome_idx IS NULL) FROM qiita_lake.assembly_membership WHERE prep_sample_idx = PS AND processing_idx = RUN), (SELECT count(*) FILTER (WHERE genome_idx IS NULL) FROM qiita_lake.bin_quality WHERE prep_sample_idx = PS AND processing_idx = RUN);`
  Measure each step's peak memory and elapsed time against its unmeasured baseline
  (gene_call 16 GB / 2 h; assembly_load 32 GB / 2 h), for every attempt:
  `sacct -j <job> -o MaxRSS,Elapsed`, with each `<job>` from
  `SELECT step_name, attempt, state, slurm_job_id FROM qiita.work_ticket_step WHERE work_ticket_idx = <T> AND slurm_job_id IS NOT NULL ORDER BY step_index, attempt`.
  Then submit the rest, paced:
  `qiita-admin backfill assembly-gene-call --execute --rate 10`.

  Each pass submits one ticket per prep_sample, so rerun it once the pass's tickets finish.
  You are done when the dry run counts no writable and no in-flight runs and every
  residue line has an explanation. `--retry-failed` resubmits runs whose recovery
  ticket ended without completing. If the dry run stops with a permission error, run it
  as an account that can read the ticket workspaces.
- `[operator]` **Fold reference 18's (`web-of-life` `3-rc4`) `GCA_G<n>` genomes into `G<n>`.**
  Until this runs, every feature table built against reference 18 lists almost every genome
  twice, as `G<n>` and `GCA_G<n>`, each with half its reads. The repair also records each rc4 genome's
  NCBI parent assembly. It is one transaction that refuses anything unexpected. It locks the
  genome tables while it runs; its genome delete alone measured 3.3 s at this size, and the whole
  run is unmeasured. A reference load or label mint waits behind it, and the repair gives up after
  30 s rather than hang if one holds them first. (#926)
  1. Fetch the release's metadata into a fresh directory. The repair refuses a file whose genomes
     are not exactly reference 18's.
  2. Dry-run. Expect `196062 GCA_G genome(s) to fold`: already linked to G<n> 195943, to link
     117, to create 2; stale WoL links 117; derivations to add 196062, present 0. Labels to retire was 46621 on 2026-11-17, and is
     higher only if reference-18 tables were built since. Any other count, or `refused:`, means
     stop and raise the report.
  3. Execute, writing the manifest somewhere kept. It lists every row deleted and label retired,
     and the run refuses to overwrite one. A `refused:` that names a lock timeout or a deadlock
     wrote nothing: run it again.
  4. Dry-run again. Expect `0 GCA_G genome(s) to fold` and 196062 derivations present.
  5. Ask the builder of the reference-18 feature tables (principal 2, alignment 3; the reporter
     of #893) to rebuild them.
  ```bash
  # [operator] in the pulled checkout; DATABASE_URL as above
  W=$(mktemp -d) && curl -fsS -o "$W/metadata.txt" https://ftp.microbio.me/wol3/wol3_rc4_ftp/metadata/metadata.txt
  cd ~/qiita-miint/qiita-control-plane
  /usr/local/bin/uv run --reinstall-package qiita-common qiita-admin backfill wol-genome-repair --reference-idx 18 --metadata "$W/metadata.txt"   # 2 and 4: dry run
  /usr/local/bin/uv run --reinstall-package qiita-common qiita-admin backfill wol-genome-repair --reference-idx 18 --metadata "$W/metadata.txt" \
    --execute --manifest ~/wol-genome-repair-ref18-$(date +%F).csv                                                                             # 3: writes
  ```

### Notes (no host action)

- (#906) `fastq-to-parquet` 1.3.0 is edited in place to end with a `persist-qc-report` step that stores the raw QC report it computed and dropped. The deploy's usual `qiita-admin actions sync` updates its steps and leaves it enabled; in-flight tickets resume and run the new last step. The prep_samples 1.3.0 already processed keep NULL `raw_qc_report` and `filtered_qc_report` until recovery (#904). No new image.
- (#896, #934) `POST /study/lookup-by-accession`, `POST /biosample/lookup-by-accession` and `POST /biosample/lookup-by-matrix-tube-id` are deprecated but still served; a downstream client should move to `POST /study/lookup` and `POST /biosample/lookup`, which return each value's `export_entity_id` unless the body sets `return_key` to `idx`. A client generated from the OpenAPI spec sees those routes' and `POST /sequencing-run/lookup-by-instrument-run-id`'s response type renamed to `NaturalKeyLookupResponse`, whose `resolved` values are typed integer-or-string on every route that returns it; those routes' JSON is unchanged. A `qiita` CLI installed from main since #896 must be reinstalled before it uses `--study-export-id` against this deploy: it still expects integer `resolved` values.
- (#863, #907) `download-ena-study` 1.0.0 gains an optional `ena_run_accessions` and a `final_attempt` param (edited in place) through the normal `qiita-admin actions sync`, and the `held_not_downloaded` run status is no longer reported. A re-import now re-tickets runs whose download failed `retriable`.
- (#874) New: `/api/v1/admin/output-equivalence` routes and `qiita-admin output-equivalence list|show|open|decide|revoke` (verdicts `same`, `similar`, `different`, `not_benchmarked`; each output equivalence gated on its kind's lifecycle scope; procedure in `docs/runbooks/deciding-output-equivalence.md`); `PATCH /api/v1/admin/definition-settings/{kind}/{idx}` and `qiita-admin definition edit-settings` (system_admin, the kind's lifecycle scope), now the only way to change a mask's, alignment's or processing's params, params_hash, workflow or version; a block-mask plan (`POST .../block-mask-plan`) whose minted mask was edited before its plan transaction held it is refused (409, "plan the pool again"), and an align plan (`POST .../align-plan`) whose minted alignment was edited before each of its holds is refused (409); a hand `UPDATE` of them is refused (SQLSTATE 55000); `POST /api/v1/definition-group/resolve` (`prep_sample:read`); `POST /api/v1/exported-processing/mask` and `POST /api/v1/exported-identifier/mask` (system_admin, `admin:masked_read_export`).
- (#874) `GET /admin/sequenced-pool/{idx}/masked-read-export` gains `group` and a per-row `resolution`; it, `POST /admin/masked-read-export/ticket`, `GET /mask-definition/{idx}/syndna-read-count` and the two mask-handle mints (`POST /exported-processing/mask`, `POST /exported-identifier/mask`) return 409 (`reason` `deprecated`) for a deprecated mask unless `include_deprecated` is set, which reads that mask's own runs and marks the roster and the SynDNA response `deprecated` (`qiita-admin masked-read-export` / `qiita mask syndna-read-count --include-deprecated`), and the ticket route returns 409 for a prep_sample that does not resolve to the mask it names. `qiita-admin masked-read-export` skips incomplete and not-processed prep_samples, and ones whose ticket is refused partway (listed on stderr), and writes `masked-read-export.manifest.json` into the output directory. Its exit code changes: 1 when any prep_sample is left out unless `--allow-partial`, and 1 when nothing is exported (an empty roster included). Files are named `<biosample_accession>.<export_id>` instead of `<biosample_accession>.<run>.<pool>.<prep_sample>`; re-exporting into a directory written before this deploy is refused until it is run once with `--adopt-unrecorded`, which rewrites this run's files under the new names, records them and removes the old-named ones it replaced. For a deprecated alignment, `POST /alignment/{idx}/ticket/doget`, `GET .../sequenced-pool/{pool}/alignment/{idx}/cohort`, `POST /exported-identifier` and `POST /exported-processing` keep their 409 but its `detail` becomes an object (`detail`, `reason` `deprecated`, `deprecation_reason`) naming `include_deprecated`, which each now accepts to read that alignment's own rows (`qiita feature-table build --include-deprecated`); with it, the cohort listing also leaves out the prep_samples that read does not resolve processed (any reason but `no_run` / `no_data`) and lists them in a new `left_out`, with each biosample's accession and `QB<n>` handle. An active alignment whose mask, or (de novo) whose assembly run, is deprecated used to read every prep_sample `upstream_deprecated` (a 409 naming the prep_samples); the same four routes now refuse it up front (409, `reason` `deprecated_upstream`, `deprecated_upstreams`) unless `include_deprecated_upstream` is set, which `POST /definition-group/resolve` also accepts, and the cohort DoGet response gains `deprecated_upstreams`, which also names the deprecated upstreams an exact `include_deprecated` read reads; under `include_deprecated_upstream` the pool cohort listing leaves out and returns in `left_out` the prep_samples whose mask run is withdrawn (`qiita feature-table build --include-deprecated-upstream`; `--include-deprecated` also admits a deprecated assembly run of a de novo arm).
- (#874) `GET /mask-definition/{idx}/syndna-read-count` gains `samples[].mask_idx` and `left_out`, returns a partial table when some selected prep_samples are incomplete or not processed, or processed with no stored counts (`left_out` reason `no_syndna_counts`) (previously a 409), and adds 409s for a group member with another SynDNA reference and for a selection with no processed prep_sample. It also gains `samples[].mask_export_processing_id` (it now mints each counted mask's `QP<n>` handle) and `left_out_biosample_accession`; `qiita mask syndna-read-count` writes `<output>.metadata.json` beside the table, and its Parquet gains a `mask_export_processing_id` column.
- (#874) `POST /alignment/{idx}/ticket/doget` (human cohort DoGet) returns 409 (`reason` `resolved_elsewhere`, `incomplete` or `not_processed`) for a prep_sample that does not resolve to the named alignment, including one whose mask run is withdrawn or deprecated. With no output equivalence decided it also refuses (`incomplete`) a prep_sample whose gate is completed but whose newest ticket under the alignment, or a covering block ticket, failed or is waiting or running; and (`incomplete` / `upstream_moved`) one whose mask resolves to another mask of its group.
- (#874) `PATCH /mask-definition/{idx}/status`, `/alignment-definition/{idx}/status` and `/processing/{idx}/status` return 409 (`joining_equivalences`) when deprecating a definition that is a side of a joining (`same` or `similar`) output equivalence. The mask and alignment routes' existing 409 for unfinished work tickets now carries `reason` `live_work` (a code; it was the message) with the message in `detail`; `work_ticket_idx` is unchanged.
- (#874) `qiita feature-table build` manifest: `processing.parts` / `processing.sample_part` (and the same under `table.denovo`) replace `processing.export_processing_id` / `params_hash` / `aligner` and `denovo.requested_alignment_idx` / `processing_idx`, and the manifest gains `schema_version` (2) and `table.left_out`; a script reading the old keys must be updated. Without `--prep-sample-idx` the build now leaves out incomplete and not-processed prep_samples (listed on stderr, recorded in `table.left_out`) instead of refusing, and fails only when none is processed.
- (#874) `POST /definition-group/resolve`, `GET /admin/sequenced-pool/{idx}/masked-read-export`, `POST /admin/masked-read-export/ticket`, `GET /mask-definition/{idx}/syndna-read-count` and `POST /alignment/{idx}/ticket/doget` take an optional `as_of_event` (a `sha256:` decision-set token; 422 when it names no decision set) and return `decisions` (the decision set they resolved under, without output-equivalence or event idxs). The feature-table manifest gains top-level `decisions`, each masked-read export manifest entry and the SynDNA `<output>.metadata.json` gain `decisions`; `qiita feature-table build`, `qiita-admin masked-read-export` and `qiita mask syndna-read-count` gain `--as-of-event`.
- (#874) `POST /work-ticket/cancel` marks the ticket (`reap_pending_since`, `reap_note`) before it scancels the ticket's SLURM jobs; a reap without error clears the mark, a failed reap keeps it with the error, and a cancel interrupted mid-reap leaves it until the next cancel's reap clears it. While marked, `POST /work-ticket/{idx}/run` and `POST /alignment-definition/{idx}/rerun-block` refuse the ticket (409, `reason` `reap_pending`). The ticket GETs gain `reap_pending_since` and `reap_note` (`reap_note` is None unless the caller is a system_admin).
- `.env.compute-orchestrator.example`'s `MIINT_GPL_BOUNDARY_PATH` comment now names the tools behind the boundary (bowtie2, FastTree, Prodigal) and the 0.5.0 minimum; no live env change. (#888)
- New DuckLake tables `gene_call` / `gene_call_contig`, and a `genome_idx` column on `assembly_membership` and `bin_quality`, created by the data plane at start. long-read-assembly 1.0.2 (edited in place) and the new `assembly-gene-call-recover` 1.0.0 arrive through the deploy's usual `qiita-admin actions sync`; no new image. (#888)
- The staged miint must have `prodigal_proteins` (duckdb-miint `2b215901` or later); the miint stage step restages when the mirror's build changes, and compute-readiness's `miint-gpl-boundary` probe fails on a build without it. (#888)
- `DELETE /reference/{reference_idx}` now answers 409, with or without `force`, for a reference that asserts a genome derivation (`qiita.genome_derivation`); after the bucket-6 WoL repair, reference 18 does. (#926)
- The checkm step reads the gene_call step's genes, from a new image, `long-read-assembly-checkm-1.0.2.sif`, which the deploy builds. **Keep `long-read-assembly-checkm-1.0.1.sif`**: 1.0.1 still names it and no spec builds it any more (`sif-build.d/checkm.env`). (#898)
- `POST /work-ticket/{idx}/run` now applies the submit checks to the stored ticket: a caller who is neither the originator nor wet_lab_admin+ gets 404; otherwise, for a ticket in a redrivable state, the refusal a submit of that work would get (403/422). The ticket is checked against its action's current definition, so a ticket stored under a version since edited in place (a tighter `context_schema`, a lower `mem_ceiling_gb`) is refused with 422 and the work must be submitted again. Scripts that re-run other users' tickets need an admin token. (#902)
- (#872) New field-exception routes `GET`/`PUT`/`DELETE /study/{S}/biosample/{B}/field-exception/{F}` and `.../sequenced-sample/{SS}/field-exception/{F}` (CLI `qiita biosample|sequenced-sample get-/set-/clear-field-exception`). Soft contract change: a field `PATCH` declaring `unique_in_study` now answers 409 when field exceptions are set on the field.
- (#929) `GET /study/{idx}` gains a third `view`, `public`, for a caller whom only a study's `public` default admits. It carries only `export_entity_id`, title, alias, description, abstract, funding, the two accessions and `default_tier`: no `study_idx` or other idx, notes, `extra_metadata`, submission state or timestamps. In `GET /study` such a study's row has `record_view: "public"` and neither `study_idx` nor `updated_at` (key such rows on `export_entity_id`), and `q` matches it only on the public view's fields. A study that an ENA import creates is now `public`, so it is listed for every signed-in user.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
