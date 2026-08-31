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
  artifact changes**, but this is no longer optional groundwork: the combined feature table reads
  `assembly_membership.genome_idx`, and both drivers refuse a run whose memberships are not all
  minted rather than build a table over a short map — the REST map with a 422, a server-side
  submit by failing the work ticket. Either way an assembly run that predates the mint cannot be
  used as a de novo arm until this has run. Re-run the dry run
  afterwards; an empty plan ("nothing to do") is the completeness signal. Idempotent, so a repeat
  costs nothing. (#514, #515)

- **Check the combined feature table's two new routes answer.** (#515)

  ```bash
  # A run known to be assembled and backfilled — pick one from the backfill's dry run.
  curl -sS -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $QIITA_TOKEN" \
    "$BASE/api/v1/assembly/<prep_sample_idx>/<processing_idx>/genome-map"
  ```

  `200` means the map route and the backfill above both landed. `422` means the backfill has not
  covered that run yet — go back to the step above. `404` means that pair assembled nothing, a
  legitimate answer; pick another run. `401` means the PAT is stale, which is why the status code
  is what is printed: a body-only check reads the same whether the route answered or the token
  expired.

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **`estimate-feature-table` 1.0.0 gained an optional `denovo_alignment_idx` context key** and a
  second resolver-staged input. The deploy's workflow sync picks it up; nothing to do by hand. A
  submit that omits the key behaves exactly as before, so existing callers are unaffected. (#515)
- **New scope `assembly:doget`, on every human role ceiling and on no service ceiling.** It is
  role-implied, so no token needs re-minting and no grant is needed — a PAT minted after the
  restart carries it automatically. (#515)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
