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

- **`20260831000000_assembly_membership_genome.sql`** — adds `qiita.assembly_membership.genome_idx`
  (nullable, bare FK to `qiita.genome`, plus an index). `make migrate` applies it; no out-of-band
  setup. (#514)

### 4. Deploy

_None yet._

### 5. Verify

- **Run the assembly-genome backfill.** (#514)

  ```bash
  QA=/home/qiita/qiita-miint/qiita-control-plane/.venv/bin/qiita-admin
  DB=$(sudo grep -oP '^DATABASE_URL=\K.*' /etc/qiita/control-plane.env)
  sudo -u qiita env DATABASE_URL="$DB" "$QA" backfill assembly-genome              # dry run
  sudo -u qiita env DATABASE_URL="$DB" "$QA" backfill assembly-genome --execute
  ```

  Assemblies run after this deploy get the mint inline; earlier ones need this. **No existing
  artifact changes and nothing is at risk if it is skipped** — no consumer reads
  `assembly_membership.genome_idx` yet, so this is groundwork for a genome-level roll-up over de
  novo contigs rather than a correction. Re-run the dry run afterwards; an empty plan ("nothing to
  do") is the completeness signal that roll-up will want. Idempotent, so a repeat costs nothing.

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

_None yet._

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
