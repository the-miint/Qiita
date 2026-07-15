# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

- **`DATA_PLANE_URL` for the compute-orchestrator** — the gRPC origin that native build jobs stream reference chunks from over Arrow Flight. **Not** `from_env()` fail-fast (a missing value falls back to `grpc://localhost:50051`, so the unit still boots), but the sharded-reference index build path — the only path that streams — will hit the wrong origin without it, so set it before the restart. Point it at the same nginx-fronted gRPC origin the control plane's `DATA_PLANE_URL` names. (#268)
  ```bash
  grep -q '^DATA_PLANE_URL=' /etc/qiita/compute-orchestrator.env \
    || sudo bash -c 'echo "DATA_PLANE_URL=grpc://qiita-miint.ucsd.edu:50051" >> /etc/qiita/compute-orchestrator.env'
  ```

### 2. One-time host setup

- **Grant `ticket:doget` to the compute service account.** Sharded-reference build jobs mint a `feature_idx`-scoped DoGet ticket (`POST /reference/{idx}/ticket/doget`, gated on `ticket:doget`) to stream reference sequences from the data plane. The live `compute` account holds only `sequence_range:mint`, and there is **no shipped route to add a scope to an existing principal** (see [`orchestrator-token-rotation.md`](docs/runbooks/orchestrator-token-rotation.md)), so grant it by minting a *new* service account with **both** scopes and pointing the token file at it. `ticket:doget` is on `SERVICE_ACCOUNT_SCOPE_CEILING`; provisioning rationale (and the least-privilege split-principal alternative) is in [`compute-service-account-provisioning.md`](docs/runbooks/compute-service-account-provisioning.md). REQUIRED before the first sharded-reference build/align run; existing raw-read ingestion is unaffected. (#268)
  ```bash
  # 1. Mint a new account with the expanded scope set (capture token + principal_idx):
  curl -X POST "$CONTROL_PLANE_URL/api/v1/admin/service-account" \
      -H "Authorization: Bearer qk_<ADMIN_PAT>" -H "Content-Type: application/json" \
      -d '{"name": "compute-rot-2026-07-13", "scopes": ["sequence_range:mint", "ticket:doget"]}'
  # 2. Install the returned token atomically, then restart the orchestrator:
  ./scripts/install-orchestrator-token.sh /etc/qiita/co-to-cp.token <<<"$NEW_TOKEN"
  sudo systemctl restart qiita-compute-orchestrator
  ```

### 3. Migrations

Standard `make migrate` (bucket order: before the bucket-4 restart). No out-of-band setup — plain `ALTER TABLE`s / additive migrations.

- Reference-sharding + sharded-alignment schema (nine additive migrations, no out-of-band backfill): (#268)
  - `20260706000000_genome_source_and_origin_sample.sql` — genome source + origin-sample columns.
  - `20260707010000_reference_index_shard_id.sql` — `reference_index.shard_id` (per-shard index rows).
  - `20260708000000_reference_membership_shard_id.sql` — `reference_membership.shard_id` (the shard cover-map).
  - `20260709010000_reference_index_bowtie2_type.sql` — `bowtie2` in the `reference_index.index_type` CHECK.
  - `20260710000000_work_ticket_shard_id.sql` — `work_ticket.shard_id` arm + the re-partitioned one-in-flight unique indexes.
  - `20260711000000_reference_index_rype_router_type.sql` — `rype_router` in the `reference_index.index_type` CHECK.
  - `20260712000000_alignment_definition.sql` — `alignment_definition` (params-hash align identity).
  - `20260712010000_alignment_sample.sql` — the per-`(alignment_idx, prep_sample)` completion gate.
  - `20260712020000_work_ticket_alignment_idx.sql` — `work_ticket.alignment_idx` arm (ON DELETE SET NULL, no backfill).
- `20260713010000_sequenced_sample_spikein_read_count.sql` — adds `sequenced_sample.spikein_read_count_r1r2` (a spike-in is added in the lab, so it is disjoint from `biological`). (#270)

- (#293) `20260714000000_host_filter_profile.sql` — creates `qiita.host_filter_profile`
  **empty**. Plain `make migrate` applies it.

- (#293) Seed the human host profile for **illumina** (out-of-band: the row points at
  `reference_idx` values that exist only on this deploy, so it cannot be a migration
  `INSERT`). Re-runnable — the `ON CONFLICT … DO UPDATE` is also the host-DB *rebuild*
  path, repointing the existing profile at a new build rather than inserting a second row.

  ```bash
  sudo bash -c 'psql "$(grep -m1 "^DATABASE_URL=" /etc/qiita/control-plane.env | cut -d= -f2-)"' <<'SQL'
  INSERT INTO qiita.host_filter_profile
      (host_term_idx, platform, rype_reference_idx, minimap2_reference_idx, created_by_idx)
  SELECT tt.idx, 'illumina',
         (SELECT reference_idx FROM qiita.reference
           WHERE is_host AND name = 'HPRCr2-hg38-T2TCHM13v2.0-gencode49' AND version = '1.0'),
         (SELECT reference_idx FROM qiita.reference
           WHERE is_host AND name = 'T2TCHM13v2.0-phiX174' AND version = '1.0'),
         1
    FROM qiita.terminology_term tt
    JOIN qiita.terminology t ON t.idx = tt.terminology_idx AND t.name = 'NCBI Taxonomy'
   WHERE tt.term_id = '9606'
  ON CONFLICT (host_term_idx, platform) DO UPDATE
     SET rype_reference_idx     = EXCLUDED.rype_reference_idx,
         minimap2_reference_idx = EXCLUDED.minimap2_reference_idx;
  SQL
  ```

- (#293) The two stages above are **different** references on purpose — that pair is what
  every recent live mask was actually minted with (`mask_definition.params`): stage 1
  routes against the HPRC pangenome, stage 2 refines against the phiX-bundled T2T build
  (which also clears the Illumina phiX spike-in in the same pass). Both are looked up by
  `(name, version)`, never by hardcoded idx; `created_by_idx = 1` is the seeded system
  principal (`SYSTEM_PRINCIPAL_IDX`), matching the other seed migrations.

- (#293) The seed is **safe to defer** — nothing reads the table on this deploy (see
  Notes), so leaving it empty breaks nothing. Running it now just means the submit-path PR
  that consumes it lands against an already-configured host.
- `20260713020000_reference_annotation.sql` — adds `qiita.reference_annotation` (the reference's claim on features that are ANNOTATED INTERVALS of another feature — a SynDNA insert on its plasmid, a gene on a chromosome), plus the annotation catalog `qiita.annotation_term` and its junction `qiita.annotation_to_term`. Three plain additive `CREATE TABLE`s, standard `make migrate`, no backfill: no reference carries annotations until one is ingested with `--gff`. (#269)

### 4. Deploy

_None yet._

### 5. Verify

- Confirm the two new sharded-reference workflows synced into `qiita.action` (synced by `qiita-admin actions sync` inside `activate.sh`, covered by `make verify-deploy`'s `qiita.action` list; this asserts the specific new actions). The modified `reference-add/1.0.0` + `local-reference-add/1.0.0` re-sync in place — no new action_id to assert. (#268)
  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, version, target_kind FROM qiita.action WHERE action_id IN ('align','build-shard-index') ORDER BY action_id"
  # expect: align|1.0.0|block  and  build-shard-index|1.0.0|reference
  ```
- Confirm the compute service account's live token now carries `ticket:doget` (the bucket-2 grant), so sharded builds can mint the reference-chunk DoGet ticket: (#268)
  ```bash
  psql "$DATABASE_URL" -tAc "SELECT sa.name, t.scopes FROM qiita.service_account sa JOIN qiita.api_token t ON t.principal_idx = sa.principal_idx WHERE t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at > now()) ORDER BY sa.name"
  # expect the compute account's active token scopes to include ticket:doget (and sequence_range:mint)
  ```
- `read-mask` **1.0.0** is present in the `qiita.action` list printed by `make verify-deploy` — the workflow YAML is edited in place (new `syndna` + lima steps) and re-synced by `qiita-admin actions sync` inside `activate.sh`, not migrated. (#270)
- The new `lima` image built and carries the pinned version (the read-mask identity hash pins `lima 2.13.0`, so a drifted binary would silently change where the adapter clip lands):
  ```bash
  cd /tmp && sudo -u qiita-orch apptainer exec --no-home \
      "${PATH_DERIVED}/images/lima-2.13.0.sif" micromamba run -n lima lima --version
  # expect: lima 2.13.0
  ```
  (#270)

- (#293) Only if you ran the bucket-3 seed: confirm **both** stages resolved. A wrong
  *rype* name fails loudly (NOT NULL), but a wrong *minimap2* name resolves to NULL
  **silently** and quietly drops the minimap2 stage — the two mistakes do not fail the
  same way, so the NULL is what to look for.

  ```bash
  sudo bash -c 'psql "$(grep -m1 "^DATABASE_URL=" /etc/qiita/control-plane.env | cut -d= -f2-)" -c \
    "SELECT p.platform, r1.name AS rype_ref, r2.name AS minimap2_ref
       FROM qiita.host_filter_profile p
       JOIN qiita.reference r1 ON r1.reference_idx = p.rype_reference_idx
       LEFT JOIN qiita.reference r2 ON r2.reference_idx = p.minimap2_reference_idx;"'
  ```

  Expect exactly one row: `illumina | HPRCr2-hg38-T2TCHM13v2.0-gencode49 | T2TCHM13v2.0-phiX174`.
  A NULL `minimap2_ref` means the name lookup missed — re-run the seed (it is idempotent).


- (#299) **Backfill `host_taxon_id`. RUN THIS BEFORE THE FIRST `submit-host-filter-pool`.**
  No biosample carries the field, so the resolver reports every sample UNRESOLVED and the
  submit path aborts every pool until this runs.

  A DATA step, not a schema one — `make migrate` does not do it — and it needs the code
  THIS deploy ships (the `qiita-admin backfill` command). That is why it lives here and not
  in bucket 3: bucket 3 runs before the venv is synced, so the command does not exist yet.
  Read-only until you pass `--execute`.

  Run the dry-run first and READ IT. It prints how many samples resolve via the pre-flight
  (controls), how many via their own taxon, and — the part that matters — the UNRESOLVED
  residue, grouped by the taxon that could not be mapped. Those samples stay UNRESOLVED and
  will abort their pool at submit; they are a curation worklist, not a failure.

  ```bash
  sudo -u qiita env DATABASE_URL="$(sudo grep -m1 '^DATABASE_URL=' /etc/qiita/control-plane.env | cut -d= -f2-)" \
      /home/qiita/qiita-miint/qiita-control-plane/.venv/bin/qiita-admin backfill host-taxon-id
  ```

  Then, once the residue looks right:

  ```bash
  sudo -u qiita env DATABASE_URL="…" \
      /home/qiita/qiita-miint/qiita-control-plane/.venv/bin/qiita-admin backfill host-taxon-id --execute
  ```

  Idempotent — re-run it freely as curation lands; already-populated samples are skipped.
  A mid-run failure commits the rows it got to and reports the count; re-running converges.

  Also heed the two WARNINGs it can print: an unreadable pool pre-flight, or a control with
  no biosample accession. Both mean blanks in that pool are NOT recognised as controls and
  will fall to UNRESOLVED — which aborts that pool rather than mis-depleting it, but you
  want to know.


### 6. After the deploy verifies green

Irreversible cleanup the deploy earns only by succeeding — retiring a superseded
secret, deleting a replaced data dir. Never put this in bucket 1: until
verification passes, the OLD build's config is the rollback path.

- **Revoke the old compute service account's tokens.** Once bucket 5 confirms the new `compute-rot-2026-07-13` token works (sharded builds can DoGet), revoke every token on the *prior* compute principal so the narrower-scoped token is no longer accepted. Do NOT do this before bucket 5 is green — the old token is the rollback path if the new one misfires. Use its `principal_idx` (the one whose token file you replaced), per [`orchestrator-token-rotation.md`](docs/runbooks/orchestrator-token-rotation.md). (#268)
  ```bash
  curl -X POST "$CONTROL_PLANE_URL/api/v1/admin/principal/<OLD_COMPUTE_PRINCIPAL_IDX>/revoke-all-tokens" \
      -H "Authorization: Bearer qk_<ADMIN_PAT>"
  ```

### Notes (no host action)

- (#299) **Nothing changes behaviour on this deploy.** The backfill writes sample metadata
  that only the (not-yet-merged) submit-path swap reads; the current submit path still reads
  the intake `human_filtering` flag and is untouched. So the backfill is safe to run now, or
  to defer — but it MUST land before the submit swap is exercised, or that swap aborts every
  pool. Ordering is the entire point of this note.

- (#299) The 25 seawater samples are backfilled to `not applicable`, which resolves
  PASS_THROUGH — i.e. **no host depletion**. They have never been masked, so this changes
  nothing today. But it is the one place the backfill writes a value whose consequence is
  "stop filtering" rather than "keep filtering" or "abort": whether human reads should still
  be removed from a host-LESS sample for contamination/privacy reasons is a separate assay
  question the host model cannot express.

- **Reference sharding + sharded alignment are opt-in and inert until an operator runs them.** Sharded indexing is enabled per reference by a `shard_index` context flag on `reference-add` / `local-reference-add` (absent ⇒ byte-identical to today's whole-reference `loading → active`, no build); the alignment consumer (`align/1.0.0`, `POST …/sequenced-pool/{P}/align-plan`, `DELETE /alignment-definition/{idx}`) needs a sharded, ACTIVE reference plus completed masks to do anything. Existing reference and read-mask flows are unchanged. (#268)
- **The DuckLake `alignment` table is created automatically at data-plane startup** by `ensure_alignment_tables` (idempotent, runs every DP boot) — **no data-plane action**. It is a sink (not in `ALLOWED_TABLES`); there is no Flight read-side yet. (#268)
- **New scope `alignment_definition:delete` is granted automatically to `system_admin`** via `ROLE_IMPLIED_SCOPES` (the disallow-without-delete escape hatch for alignments) — no host action, never granted to service accounts. (#268)
- **The DuckLake `reference_annotation` table is likewise created automatically at data-plane startup** by `ensure_reference_tables` (idempotent, every DP boot) — **no data-plane action, no migration**. It holds annotated INTERVALS of a reference sequence (a SynDNA insert on its plasmid, a gene on a chromosome), each minted its own `feature_idx`. Unlike `alignment` it IS readable (it is in `ALLOWED_TABLES`), and `delete_reference` purges it. (#269)
- **All four `reference-add` workflows gain an optional GFF3 companion (`--gff`) and one new in-process action (`mint-annotation-features`)** — reaches `qiita.action` via the `qiita-admin actions sync` that `activate.sh` already runs, so **no host action**. A reference ingested WITHOUT a `--gff` behaves byte-identically to today apart from one extra zero-row `reference_annotation` staging file; no additional SLURM job is scheduled (the new action runs in-process on the control plane). (#269)
- **The `read-mask` workflow's `syndna` step now emits an extra `alignment` output and applies an aligned-fraction gate under the existing `syndna_enabled` gate** — reaches `qiita.action` via `qiita-admin actions sync` (in `activate.sh`), so **no host action**. No new step; nothing consumes the extra output yet (it is groundwork for a deferred coverage-measurement consumer). Inert unless a pool is masked with `syndna_enabled`; a read-mask run without it is byte-identical to today. To produce real per-insert numbers the SynDNA reference must be re-ingested as **plasmids + a per-insert GFF3** (the current reference is bare inserts, and the 0.90 aligned-fraction gate is only correct against plasmids) — a separate operator action, filed when the reference is rebuilt, not part of this deploy. (#269 part 2)
- **EVERY container SIF rebuilds on this deploy — budget for it in bucket 4.** Not just the new `read-mask` `lima` image: `_lib.sh` moved to `workflows/_shared/`, and the build-inputs hash covers each file's repo-relative PATH as well as its bytes (`deploy/_common.sh`), so the move re-hashes every image that stages it — the four `long-read-assembly` images — and `bcl-convert` too, whose whole-dir hash includes `_shared/`. All are auto-rebuilt by `activate.sh` → `build-sifs.sh` before any service restart; nothing to do by hand, but `bcl-convert` in particular is a slow image. A failed build aborts the deploy before the restart (by design). (#270)
- The new `read-mask` **`lima` image needs nothing staged**: lima comes from bioconda in `%post` and the Twist adapter FASTA is in-repo, so its spec declares no `SOURCES`. (#270)
- **A SynDNA spike-in reference needs a minimap2 (`.mmi`) index**, not a rype one, before the first PacBio absquant read-mask submission — `qiita submit-host-filter-pool --syndna-reference-idx` refuses a reference without one. Load it with `qiita reference load --host --no-rype-index --minimap2-preset map-hifi` (the spike-in inserts are the subject sequences). No action if no absquant pool is being masked yet. (#270)
- The pool `sequenced-sample` roster gains three PacBio fields (`sheet_type`, `twist_adaptor_id`, `syndna_is_twisted`), derived at request time from the pool's stored pre-flight blob. Additive, and `null` for an Illumina pool — no client is required to read them. (#270)

- (#293) `qiita.host_filter_profile` and the host-filter resolver are **inert on this
  deploy** — nothing reads either yet (the submit-path swap is a later PR). No behavior
  change: host references still reach a submission the way they do today, via the
  `--host-rype-reference-idx` / `--host-minimap2-reference-idx` flags. The bucket-3 seed
  is what wires the table up for the PR that consumes it.

- (#294) Soft API change, additive, no host action. The pool `sequenced-sample` roster
  gains a `host_filter` block per sample (the resolved host-filter plan), and a new
  `GET /api/v1/host-filter-profile` lists the available profiles. Both are **read-only**
  — the submit path is unchanged and still reads the intake `human_filtering` flag. No
  client is required to read either.

- (#294) Expect `host_filter.outcome = "unresolved"` on **every** sample until the
  `host_taxon_id` backfill lands — no biosample carries the field yet. That is the
  resolver reporting honestly, not a fault, and it is what makes the roster the
  worklist for the backfill. Nothing depends on it being resolved yet.
- (#293) **No `pacbio_smrt` host profile is seeded, deliberately.** No PacBio pool has
  been masked yet, so there is no live pairing to copy — which human build to deplete HiFi
  reads against is an open **assay** decision, not something the DB can answer. Leaving it
  unseeded is the fail-closed choice: once the submit-path PR lands, a human PacBio sample
  resolves `UNRESOLVED` and aborts with `no host_filter_profile for terminology term N on
  platform 'pacbio_smrt'` — loud and actionable — rather than being silently depleted
  against a build nobody chose. Seed it (same `ON CONFLICT` statement, `'pacbio_smrt'`)
  when that decision is made.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
