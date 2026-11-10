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

_None yet._

### 3. Migrations

- `[operator]` (#855) `make migrate` applies `20261110000000_block_rearmed_at.sql` (nullable `rearmed_at` on `qiita.block`, which `rerun-block` sets and abandon-plan reads; no row changes).

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

- `[operator]` (#849) Disable the retired `myloasm-provenance-recover` action. The deploy removes its directory and job module, and `actions sync` does not disable an action whose directory is gone, so the row stays enabled. `action_disabled_consistent` needs `disabled_at` and `disabled_by_idx`; use your own principal's idx, from the first query. Load `DATABASE_URL` as [`redeploy.md`](docs/runbooks/redeploy.md) §4 does (`set -a; . /etc/qiita/control-plane.env; set +a`):
  ```bash
  psql "$DATABASE_URL" -c "SELECT idx, display_name FROM qiita.principal WHERE system_role = 'system_admin' AND idx <> 1 AND NOT retired;"   # idx 1 is the seeded system principal
  psql "$DATABASE_URL" -c "UPDATE qiita.action SET enabled = false, disabled_at = now(), disabled_by_idx = <your principal idx>, disabled_reason = 'one-off retired' WHERE action_id = 'myloasm-provenance-recover';"
  psql "$DATABASE_URL" -c "SELECT version, enabled FROM qiita.action WHERE action_id = 'myloasm-provenance-recover';"   # expect 1.0.0 | f
  ```

### Notes (no host action)

- (#849) `qiita-admin backfill myloasm-provenance` is removed, with the `myloasm-provenance-recover` workflow and job: every myloasm run of long-read-assembly 1.0.0/1.0.1 is recovered.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
