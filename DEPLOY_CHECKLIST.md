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

- **`ticket:doget` now also reaches a sample's assembled contigs — no scope grant, no
  re-mint.** A new `POST /assembly/ticket/doget` signs a Flight DoGet ticket for one
  `(prep_sample_idx, processing_idx)` run's contigs on `assembled_sequence` /
  `assembled_sequence_chunks`, gated on the existing service-only `ticket:doget`, which
  the live `compute` account already holds. So every service account carrying that scope
  gains contig read-back at the restart, with nothing to run. Worth knowing rather than
  doing: it is the first *sample-derived* sequence surface that scope opens — every other
  table it reaches is reference data or the derived per-read `alignment` slice — and the
  route authorizes on scope alone, with no per-study or row-level check. If a site
  provisioned a second principal holding only `ticket:doget` for reference streaming (the
  least-privilege split in
  [`compute-service-account-provisioning.md`](docs/runbooks/compute-service-account-provisioning.md)),
  that principal now reaches contigs too. The ticket carries the pair, and the data plane
  resolves which contigs it reaches from the DuckLake `assembly_membership` at read time —
  so a run re-registered inside the mint's 300 s TTL streams the re-registered rows, and a
  run whose contigs are in the lake but whose Postgres membership was cleared answers 404
  at the route. (#476)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
