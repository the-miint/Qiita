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

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

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

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
