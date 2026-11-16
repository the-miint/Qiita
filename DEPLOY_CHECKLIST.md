# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's the changelog, see [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

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

### 3. Migrations

- `20261112000000_host_taxon_id_not_required` makes `host_taxon_id` optional, and from the
  restart an absent value means no host. `dbmate` applies it; run the bucket-2 backfill
  first. (#864)
- `20261113000000_definition_pair` adds `qiita.definition_pair` and `qiita.definition_pair_event`, empty, and `20261113000001_exported_processing_mask` adds a nullable `mask_idx` to `qiita.exported_processing` and `qiita.exported_identifier`, widening their one-processing CHECKs to it; no row changes. `dbmate` applies both. `20261116000000_work_ticket_processing_idx` adds a nullable `processing_idx` to `qiita.work_ticket`, and `20261116000001_backfill_work_ticket_processing_idx` sets it on the started `long-read-assembly` tickets to the processing each one minted; idempotent, and a no-op on a fresh DB. A ticket that the old build starts between `make migrate` and the restart is not covered; the bucket-6 re-run closes that. `20261116000002_exported_identifier_prep_sample_delete` makes `qiita.exported_identifier.prep_sample_idx` nullable and ON DELETE SET NULL, adds two nullable description columns and a CHECK that every live row names a prep_sample (every existing row does: the column was NOT NULL), and adds a BEFORE DELETE trigger on `qiita.prep_sample`; no row changes. `20261116000003_work_ticket_reap_failure` adds two nullable columns to `qiita.work_ticket` (`reap_failed_at`, `reap_failure`) and a CHECK that they are set together; no row changes. `20261116000004_definition_settings_lock` adds `qiita.definition_settings_edit` (empty), the function `qiita.edit_definition_settings`, and BEFORE UPDATE triggers on `qiita.mask_definition`, `qiita.alignment_definition` and `qiita.processing` that refuse a change to a definition's settings outside that function; no row changes. (#874)

### 4. Deploy

_None yet._

### 5. Verify

- `[operator]` (#874) As a system_admin with a PAT carrying `mask_definition:lifecycle`, `qiita-admin pair list` exits 0 and prints `0 pair(s)` on stderr (the definition-pair routes are mounted and the new table is readable).

### 6. After the deploy verifies green

- `[operator]` **Re-run the idempotent `work_ticket.processing_idx` backfill**, for a `long-read-assembly` ticket that the old build started between `make migrate` (bucket 3) and the restart: its `processing_idx` is NULL, so the definition-group resolver does not count it as a run of its processing. dbmate does not re-apply a migration, so run its `migrate:up` block by hand. It only fills a NULL column, so it does not burn the rollback path. Run it right after the restart: a ticket that the new build ends before its mint stays NULL, as it should, but a re-run made after another ticket of the same prep_sample has minted the same params would fill it in. (#874)
  ```bash
  # [operator] in the pulled checkout; DATABASE_URL set as in docs/runbooks/redeploy.md (migrate step)
  cd ~/qiita-miint/qiita-control-plane
  sed -n '/^-- migrate:up/,/^-- migrate:down/p' db/migrations/20261116000001_backfill_work_ticket_processing_idx.sql | psql "$DATABASE_URL" -v ON_ERROR_STOP=1
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

### Notes (no host action)

- (#863) `download-ena-study` 1.0.0 gains an optional `ena_run_accessions` through the normal `qiita-admin actions sync`, and the `held_not_downloaded` run status is no longer reported.
- (#874) New: `/api/v1/admin/definition-pair` routes and `qiita-admin pair list|show|open|decide|revoke` (each pair gated on its kind's lifecycle scope; procedure in `docs/runbooks/deciding-similar-definitions.md`); `PATCH /api/v1/admin/definition-settings/{kind}/{idx}` and `qiita-admin definition edit-settings` (system_admin, the kind's lifecycle scope), now the only way to change a mask's, alignment's or processing's params, params_hash, workflow or version; a hand `UPDATE` of them is refused (SQLSTATE 55000); `POST /api/v1/definition-group/resolve` (`prep_sample:read`); `POST /api/v1/exported-processing/mask` and `POST /api/v1/exported-identifier/mask` (system_admin, `admin:masked_read_export`).
- (#874) `GET /admin/sequenced-pool/{idx}/masked-read-export` gains `group` and a per-row `resolution`; it, `POST /admin/masked-read-export/ticket`, `GET /mask-definition/{idx}/syndna-read-count` and the two mask-handle mints (`POST /exported-processing/mask`, `POST /exported-identifier/mask`) return 409 (`reason` `deprecated`) for a deprecated mask unless `include_deprecated` is set, which reads that mask's own runs and marks the roster and the SynDNA response `deprecated` (`qiita-admin masked-read-export` / `qiita mask syndna-read-count --include-deprecated`), and the ticket route returns 409 for a prep_sample that does not resolve to the mask it names. `qiita-admin masked-read-export` skips incomplete and not-processed prep_samples, and ones whose ticket is refused partway (listed on stderr), and writes `masked-read-export.manifest.json` into the output directory. Its exit code changes: 1 when any prep_sample is left out unless `--allow-partial`, and 1 when nothing is exported (an empty roster included). Files are named `<biosample_accession>.<export_id>` instead of `<biosample_accession>.<run>.<pool>.<prep_sample>`; re-exporting into a directory written before this deploy is refused until it is run once with `--adopt-unrecorded`, which rewrites this run's files under the new names, records them and removes the old-named ones it replaced. For a deprecated alignment, `POST /alignment/{idx}/ticket/doget`, `GET .../sequenced-pool/{pool}/alignment/{idx}/cohort`, `POST /exported-identifier` and `POST /exported-processing` keep their 409 but its `detail` becomes an object (`detail`, `reason` `deprecated`, `deprecation_reason`) naming `include_deprecated`, which each now accepts to read that alignment's own rows (`qiita feature-table build --include-deprecated`).
- (#874) `GET /mask-definition/{idx}/syndna-read-count` gains `samples[].mask_idx` and `left_out`, returns a partial table when some selected prep_samples are incomplete or not processed, or processed with no stored counts (`left_out` reason `no_syndna_counts`) (previously a 409), and adds 409s for a group member with another SynDNA reference and for a selection with no processed prep_sample. It also gains `samples[].mask_export_processing_id` (it now mints each counted mask's `QP<n>` handle) and `left_out_biosample_accession`; `qiita mask syndna-read-count` writes `<output>.metadata.json` beside the table, and its Parquet gains a `mask_export_processing_id` column.
- (#874) `POST /alignment/{idx}/ticket/doget` (human cohort DoGet) returns 409 (`reason` `resolved_elsewhere`, `incomplete` or `not_processed`) for a prep_sample that does not resolve to the named alignment, including one whose mask run is withdrawn or deprecated. With no pair decided it also refuses (`incomplete`) a prep_sample whose gate is completed but whose newest ticket under the alignment, or a covering block ticket, failed or is waiting or running; and (`incomplete` / `upstream_moved`) one whose mask resolves to another mask of its group.
- (#874) `PATCH /mask-definition/{idx}/status`, `/alignment-definition/{idx}/status` and `/processing/{idx}/status` return 409 (`similar_pairs`) when deprecating a definition that is a side of a `similar` pair. The mask and alignment routes' existing 409 for unfinished work tickets now carries `reason` `live_work` (a code; it was the message) with the message in `detail`; `work_ticket_idx` is unchanged.
- (#874) `qiita feature-table build` manifest: `processing.parts` / `processing.sample_part` (and the same under `table.denovo`) replace `processing.export_processing_id` / `params_hash` / `aligner` and `denovo.requested_alignment_idx` / `processing_idx`, and the manifest gains `schema_version` (2) and `table.left_out`; a script reading the old keys must be updated. Without `--prep-sample-idx` the build now leaves out incomplete and not-processed prep_samples (listed on stderr, recorded in `table.left_out`) instead of refusing, and fails only when none is processed.
- (#874) `POST /definition-group/resolve`, `GET /admin/sequenced-pool/{idx}/masked-read-export`, `POST /admin/masked-read-export/ticket`, `GET /mask-definition/{idx}/syndna-read-count` and `POST /alignment/{idx}/ticket/doget` take an optional `as_of_event` (a `sha256:` decision-set token; 422 when it names no decision set) and return `decisions` (the decision set they resolved under, without pair or event idxs). The feature-table manifest gains top-level `decisions`, each masked-read export manifest entry and the SynDNA `<output>.metadata.json` gain `decisions`; `qiita feature-table build`, `qiita-admin masked-read-export` and `qiita mask syndna-read-count` gain `--as-of-event`.
- (#874) `POST /work-ticket/cancel` records a reap that fails on the ticket (`reap_failed_at`, `reap_failure`); `POST /work-ticket/{idx}/run` and `POST /alignment-definition/{idx}/rerun-block` then refuse it (409, `reason` `reap_failed`) until a later cancel of the same ticket reaps without error, which clears it.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
