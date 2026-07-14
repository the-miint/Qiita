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

- **Reference sharding + sharded alignment are opt-in and inert until an operator runs them.** Sharded indexing is enabled per reference by a `shard_index` context flag on `reference-add` / `local-reference-add` (absent ⇒ byte-identical to today's whole-reference `loading → active`, no build); the alignment consumer (`align/1.0.0`, `POST …/sequenced-pool/{P}/align-plan`, `DELETE /alignment-definition/{idx}`) needs a sharded, ACTIVE reference plus completed masks to do anything. Existing reference and read-mask flows are unchanged. (#268)
- **The DuckLake `alignment` table is created automatically at data-plane startup** by `ensure_alignment_tables` (idempotent, runs every DP boot) — **no data-plane action**. It is a sink (not in `ALLOWED_TABLES`); there is no Flight read-side yet. (#268)
- **New scope `alignment_definition:delete` is granted automatically to `system_admin`** via `ROLE_IMPLIED_SCOPES` (the disallow-without-delete escape hatch for alignments) — no host action, never granted to service accounts. (#268)
- **EVERY container SIF rebuilds on this deploy — budget for it in bucket 4.** Not just the new `read-mask` `lima` image: `_lib.sh` moved to `workflows/_shared/`, and the build-inputs hash covers each file's repo-relative PATH as well as its bytes (`deploy/_common.sh`), so the move re-hashes every image that stages it — the four `long-read-assembly` images — and `bcl-convert` too, whose whole-dir hash includes `_shared/`. All are auto-rebuilt by `activate.sh` → `build-sifs.sh` before any service restart; nothing to do by hand, but `bcl-convert` in particular is a slow image. A failed build aborts the deploy before the restart (by design). (#270)
- The new `read-mask` **`lima` image needs nothing staged**: lima comes from bioconda in `%post` and the Twist adapter FASTA is in-repo, so its spec declares no `SOURCES`. (#270)
- **A SynDNA spike-in reference needs a minimap2 (`.mmi`) index**, not a rype one, before the first PacBio absquant read-mask submission — `qiita submit-host-filter-pool --syndna-reference-idx` refuses a reference without one. Load it with `qiita reference load --host --no-rype-index --minimap2-preset map-hifi` (the spike-in inserts are the subject sequences). No action if no absquant pool is being masked yet. (#270)
- The pool `sequenced-sample` roster gains three PacBio fields (`sheet_type`, `twist_adaptor_id`, `syndna_is_twisted`), derived at request time from the pool's stored pre-flight blob. Additive, and `null` for an Illumina pool — no client is required to read them. (#270)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
