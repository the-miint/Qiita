# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

### 2. One-time host setup

_None yet._

### 3. Migrations

- `make migrate` applies `20260723000000_backfill_mask_sample_per_sample.sql` — a data backfill (no schema change): it writes a `'completed'` `qiita.mask_sample` gate row for every already-completed per-sample mask-model ticket (`read-mask` / `fastq-to-parquet`, `prep_sample`-scoped, non-NULL `mask_idx`), so the newly-tightened readers (masked-read export, long-read-assembly input, align-plan) don't 409 historical masks that predate the first-class completion gate. Idempotent (`ON CONFLICT DO NOTHING`); a no-op on a fresh DB. A ticket completing in the migrate→restart window is NOT covered here (dbmate applies a migration once) — the bucket-6 re-run below closes that edge. (#feat/block-align-mask-selection)

### 4. Deploy

_None yet._

### 5. Verify

- **`read-mask` 1.0.0 and `fastq-to-parquet` 1.3.0 carry the new `finalize-mask-sample` terminal step** — `make verify-deploy` already confirms `qiita.action` is queryable; this additionally confirms both per-sample masking workflows were re-synced, so they now write the `mask_sample` completion gate first-class. Expect `t` for both rows. (#feat/block-align-mask-selection)
  ```bash
  sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -Atc "SELECT action_id, steps::text LIKE '\''%finalize-mask-sample%'\'' FROM qiita.action WHERE (action_id, version) IN (('\''read-mask'\'', '\''1.0.0'\''), ('\''fastq-to-parquet'\'', '\''1.3.0'\''));"'
  ```

### 6. After the deploy verifies green

- **Re-run the idempotent per-sample `mask_sample` backfill** to close the migrate→restart deploy-window: a `read-mask` / `fastq-to-parquet` ticket that completed after `make migrate` (bucket 3) but before the restart ran under old code that did not yet write the completion gate, so its historical mask has no gate row and the tightened readers would 409 it. dbmate will not re-apply the migration, so run its `migrate:up` body by hand. Idempotent (`ON CONFLICT DO NOTHING`) — safe to re-run and it does NOT burn the rollback path (it only adds `completed` rows for already-completed masks). (#feat/block-align-mask-selection)
  ```bash
  sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -c "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state) SELECT wt.mask_idx, wt.prep_sample_idx, '\''completed'\'' FROM qiita.work_ticket wt WHERE wt.action_id IN ('\''read-mask'\'', '\''fastq-to-parquet'\'') AND wt.scope_target_kind = '\''prep_sample'\'' AND wt.state = '\''completed'\'' AND wt.mask_idx IS NOT NULL ON CONFLICT (mask_idx, prep_sample_idx) DO NOTHING;"'
  ```

### Notes (no host action)

- **Breaking API contract — `align-plan` request shape changed (no host action, downstream-client awareness).** `POST /sequencing-run/{idx}/sequenced-pool/{idx}/align-plan` now **requires** `mask_idx` and **drops** `force`, `host_rype_reference_idx`, and `host_minimap2_reference_idx`. A caller sending the old body gets a 422; a caller that omits `mask_idx` cannot submit a plan. Any out-of-repo align-plan client must be updated to name the `mask_idx` its reads were masked under. Rationale: align no longer re-derives the mask config server-side — the reconstruction matched the real per-sample mask only by coincidence and returned `AlignNoMasksFound` for every pool on this deployment; a nonexistent `mask_idx` is now a 404 (`AlignMaskNotFound`). (#feat/block-align-mask-selection)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
