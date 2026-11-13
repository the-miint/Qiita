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
  # [operator] in the pulled checkout; DATABASE_URL as above
  cd ~/qiita-miint/qiita-control-plane
  /usr/local/bin/uv run --reinstall-package qiita-common qiita-admin backfill host-taxon-id --show-unresolved 50            # 1 and 3: dry run
  /usr/local/bin/uv run --reinstall-package qiita-common qiita-admin backfill host-taxon-id --show-unresolved 0 --execute  # 2: writes
  ```

### 3. Migrations

- `20261112000000_host_taxon_id_not_required` makes `host_taxon_id` optional, and from the
  restart an absent value means no host. `dbmate` applies it; run the bucket-2 backfill
  first. (#864)
- `20261113000000_definition_pair` adds `qiita.definition_pair` and `qiita.definition_pair_event`, empty, and `20261113000001_exported_processing_mask` adds a nullable `mask_idx` to `qiita.exported_processing` and `qiita.exported_identifier`, widening their one-processing CHECKs to it; no row changes. `dbmate` applies both. (#TBD)

### 4. Deploy

_None yet._

### 5. Verify

- `[operator]` (#TBD) As a system_admin with a PAT carrying `mask_definition:lifecycle`, `qiita-admin pair list` exits 0 and prints `0 pair(s)` on stderr (the definition-pair routes are mounted and the new table is readable).

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (#863) `download-ena-study` 1.0.0 gains an optional `ena_run_accessions` through the normal `qiita-admin actions sync`, and the `held_not_downloaded` run status is no longer reported.
- (#TBD) New: `/api/v1/admin/definition-pair` routes and `qiita-admin pair list|show|open|decide|revoke` (each pair gated on its kind's lifecycle scope; procedure in `docs/runbooks/deciding-similar-definitions.md`); `POST /api/v1/definition-group/resolve` (`prep_sample:read`); `POST /api/v1/exported-processing/mask` and `POST /api/v1/exported-identifier/mask` (system_admin, `admin:masked_read_export`).
- (#TBD) `GET /admin/sequenced-pool/{idx}/masked-read-export` gains `group` and per-sample `state`, `reason`, `resolved_mask_idx`, `work_ticket_idx`; it and `POST /admin/masked-read-export/ticket` return 409 for a deprecated mask. `qiita-admin masked-read-export` skips incomplete and not-processed samples (listed on stderr, exit 0) and writes `masked-read-export.manifest.json` into the output directory.
- (#TBD) `GET /mask-definition/{idx}/syndna-read-count` gains `samples[].mask_idx` and `left_out`, returns a partial table when some selected samples are incomplete or not processed (previously a 409), and adds 409s for a group member with another SynDNA reference and for a selection with no processed sample.
- (#TBD) `POST /alignment/{idx}/ticket/doget` (human cohort DoGet) returns 409 (`reason` `resolved_elsewhere`, `incomplete` or `not_processed`) for a sample that does not resolve to the named alignment, including one whose mask run is withdrawn or deprecated.
- (#TBD) `PATCH /mask-definition/{idx}/status`, `/alignment-definition/{idx}/status` and `/processing/{idx}/status` return 409 (`similar_pairs`) when deprecating a definition that is a side of a `similar` pair.
- (#TBD) `qiita feature-table build` manifest: `processing.parts` / `processing.sample_part` (and the same under `table.denovo`) replace `processing.export_processing_id` / `params_hash` / `aligner` and `denovo.requested_alignment_idx` / `processing_idx`; a script reading the old keys must be updated.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
