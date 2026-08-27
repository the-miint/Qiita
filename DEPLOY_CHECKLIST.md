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

- `20260827000000_assembly_lifecycle.sql` — plain `make migrate`, no out-of-band setup and no
  backfill: `DEFAULT 'active'` covers every existing `qiita.processing` row and no
  `assembly_sample` row changes state. Adds the lifecycle columns to both tables, widens
  `assembly_sample.state` to admit `'invalidated'`, and re-states `qiita.mint_processing`
  (same 4-argument signature, so it replaces in place) to refuse a deprecated run.
  (#505)

### 4. Deploy

_None yet._

### 5. Verify

- **`probe/native-import` got stricter, so a stale compute node now fails `make
  verify-deploy` instead of passing it.** It ran `import qiita_compute_orchestrator.jobs`
  and nothing else; it now imports every dispatchable job module, which is what makes a
  missing `qiita_common` symbol visible, and reports the failing module in its `err=` field
  (it used to print a bare `=fail`). If it fails on the first deploy after this lands, that
  venv was already stale and the check is doing its job — refresh it with
  `sudo -u qiita bash -lc 'cd <native-checkout>/qiita-compute-orchestrator && /usr/local/bin/uv sync --reinstall-package qiita-common'`
  (absolute `uv`, login shell — bare `uv` is not on `sudo`'s PATH). (#507)

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **`pool-completion` answers a different question after this deploy, so its numbers will move
  on pools you have already run.** It bucketed samples by read-mask ticket state; it now reads
  the `qiita.mask_sample` gate, the same one the masked-read pull and the assembly resolver
  read. Three shifts an operator will see, all of them the summary catching up to what the
  consumers already do: a withdrawn run moves out of `samples_completed` into a new
  `samples_invalidated` bucket (and stops holding `fully_processed` true); a block-masked pool
  stops reading as entirely `samples_not_submitted`; and a sample masked by a mask-bearing
  `fastq-to-parquet` ticket is counted (the 1.0.0–1.2.0 versions that minted no mask still are
  not, matching the per-sample gate backfill). A cancelled masking ticket also moves out of
  `samples_not_submitted` into its own `samples_cancelled`. `GET .../work-ticket-summary` is
  unchanged — it still counts tickets, which is what its field names say, and it can now
  legitimately disagree with the completion rollup on a block-masked sample. Soft contract
  change for downstream clients: `samples_invalidated` and `samples_cancelled` are new required
  fields on the `PoolCompletionStatus` body. (#508)
- **A PAT minted before this deploy cannot use the new assembly-lifecycle routes.** The new
  `processing:lifecycle` scope is added to the system_admin ceiling, so callers on the OIDC
  path pick it up automatically; a PAT carries its own stored scope set, so its holder runs
  `qiita login` (or `POST /auth/pat`) once. Same shape as the `mask_definition:lifecycle`
  note in the 2026-08-13 archive, and the 403 says so itself. (#505)
- **Nothing is deprecated or withdrawn by this deploy — it ships the mechanism only.** Which
  assembly runs are void, and which per-sample results to withdraw, is a decision to make
  after it lands. Two behaviour changes take effect immediately, and neither can fire until
  someone marks something: submitting `long-read-assembly` under a deprecated
  `qiita.processing` fails at SUBMISSION with BAD_INPUT instead of minting, and a redrive
  whose `finalize-assembly-sample` lands on a withdrawn `(run, sample)` fails that STEP with
  BAD_INPUT rather than re-completing the pair — the contigs land in DuckLake either way, and
  the withdrawal stands until someone restores it. (#505)
- **New read routes, no host action.** `GET /api/v1/processing`, `GET
  /api/v1/processing/{processing_idx}` and `GET /api/v1/processing/{processing_idx}/prep-sample`
  are the first HTTP surface on the assembly run identity. They sit at `prep_sample:read`
  (every human role holds it, narrowed per study below `wet_lab_admin`) and carry run
  metadata and per-sample gate state only — no sequence data. (#505)
- **The two venv refreshes now run on every deploy.** `redeploy.sh` steps 5 and 6 no longer
  skip `uv sync --reinstall-package qiita-common` when an import probe passes — that probe
  could not see the two stale-venv incidents it was guarding (2026-08-21, 2026-08-27), so
  the skip is gone rather than re-conditioned. Nothing to do differently; the deploy just
  spends the sync every time, and prompts only for a *separate* native checkout it did not
  pull. `FORCE_NATIVE_REFRESH=1` / `FORCE_CLI_REFRESH=1` no longer do anything; the script
  prints that they are redundant and refreshes regardless, so an old runbook line still
  gets what it asked for. Step 6's post-sync import now covers `qiita-admin` as well as
  `qiita`, so a stale symbol only `qiita-admin` imports aborts the deploy instead of
  surfacing the next time you reach for the CLI. (#507)
- **New `SKIP_CLI_REFRESH=1`,** mirroring `SKIP_NATIVE_REFRESH=1`. Step 6 now runs every
  deploy and aborts on failure, so this is the out if its `uv sync` fails for its own
  reasons on a deploy whose services are already up. (#507)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
