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

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (#fix/align-block-cohort-discriminator-and-cli) **A past `DELETE /alignment-definition` could silently wedge read-mask block fan-out; this deploy un-wedges it, retroactively.** The purge NULLs `work_ticket.alignment_idx`, which used to make an align block ticket look like a read-mask block of the `mask_idx` it still carries — so a `failed` one fail-stopped `read_mask_block_cohort(mask_idx)` and every later block-mask plan under that mask (a fleet-wide config hash) minted its tickets held and released none, returning a 202 that looked fine. Block kind now reads from `action_id`, so any already-detached tickets stop contaminating their cohort the moment the CP restarts. No cleanup, no query to run.
- (#fix/align-block-cohort-discriminator-and-cli) **Starting an alignment no longer needs hand-rolled `curl`:** `qiita pool submit-align-pool --sequencing-run-idx N --sequenced-pool-idx N --reference-idx N --mask-idx N [--only-missing]`. Client-side only. Its summary line reports the per-block read count, which is the cheapest way to catch a pool tiled by a stale planner at submit time rather than one walltime ceiling per block later.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
