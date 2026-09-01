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

- **`20260901000000_assembly_membership_contig_attributes.sql`** — adds four nullable columns
  to `qiita.assembly_membership` (`raw_name`, `circularity`, `depth`, `mult`). `make migrate`
  applies it; no out-of-band setup. **Not backfilled**: the values are read out of the
  assemble step's output, so every existing row keeps NULL and only runs assembled after this
  deploy carry them. (#517)

- **`20260831000000_assembly_membership_genome.sql`** — adds `qiita.assembly_membership.genome_idx`
  (nullable, bare FK to `qiita.genome`, plus an index). `make migrate` applies it; no out-of-band
  setup. (#514)

### 4. Deploy

_None yet._

### 5. Verify

- **Record the rebuilt image's hifiasm_meta version.** (#516, #517)

  ```bash
  apptainer exec "${PATH_DERIVED}/images/long-read-assembly-assemble-1.0.0.sif" \
      micromamba run -n hifiasm_meta hifiasm_meta --version 2>&1
  ```

  This deploy rebuilds the image (`assemble.sh` is in its `HASH_INPUTS`). #517 pins
  hifiasm_meta to `hamtv0.3.5` and asserts its two internal versions in the def's `%test`, so
  the build itself now fails on a moved solve rather than shipping one quietly — this step
  records what that pin resolved to on this host for the archive entry (the versions go to
  stderr, which is why the command redirects). The gate itself is earlier:
  `deploy/build-sifs.sh` runs the def's `%test` during the image rebuild, and a failure there
  aborts `activate.sh` before any service restarts.

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

- **Check the circular genomes (LCGs — large circular genomes, the contigs that bypass binning)
  came back scored.** (#519)

  ```bash
  # A ticket whose assemble step produced a non-empty circular.fa.
  T=<work_ticket_idx>
  ls "${PATH_SCRATCH}/ticket/${T}"/checkm/attempt-*/output/checkm/
  ```

  Four files — `lineage.tsv`, `qa.tsv`, `lcg_lineage.tsv`, `lcg_qa.tsv` — means both CheckM runs
  landed. Only the first pair, on a ticket that HAS circular contigs, means the LCG arm did not
  run: check the step log for `lcg_split`. It exits 64 with the reason on stderr; a missing or
  mislocated `${PATH_DERIVED}/duckdb-ext` is the failure this step gained (see the note below).
  A ticket with an empty `circular.fa` legitimately writes only the first pair, so pick the
  ticket before reading the result.

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **`estimate-feature-table` 1.0.0 gained an optional `denovo_alignment_idx` context key** and a
  second resolver-staged input. The deploy's workflow sync picks it up; nothing to do by hand. A
  submit that omits the key behaves exactly as before, so existing callers are unaffected. (#515)
- **New scope `assembly:doget`, on every human role ceiling and on no service ceiling.** It is
  role-implied, so no token needs re-minting and no grant is needed — a PAT minted after the
  restart carries it automatically. (#515)

- **`assemble` now keeps the assembler's whole output tree, so each attempt holds ~1 GB more
  scratch.** (#516) Measured on 1.24 Gbp of masked reads — 12% of one real ticket's — the
  retained tree is 1.44 GB for myloasm and 697 MB for hifiasm_meta. Assembly output does not
  scale linearly with input and has not been measured at a second point, so that is a floor,
  not a per-ticket figure. It lands in the step's own output directory under the per-attempt
  ticket workspace; nothing new is written outside it and no step reads it. **That scratch is
  not currently reclaimed.** `docs/architecture/storage.md` documents ephemeral per-ticket
  directories as deleted 45 days past a ticket's terminal state, but nothing implements that
  sweep, so ticket workspaces accumulate and these trees will accumulate with them — plan the
  disk on that basis, not on the documented window.

- **The `assemble` SIF rebuilds this deploy, and hifiasm_meta is now pinned.** (#516, #517)
  `assemble.sh` is in the image's `HASH_INPUTS` (`sif-build.d/assemble.env`), so editing it
  invalidates the build hash and forces a full rebuild. #517 pins `hifiasm_meta=hamtv0.3.5`
  and asserts its two internal version strings in `%test`, alongside myloasm's existing 0.6.0
  pin — so this rebuild resolves both assemblers to a fixed version instead of re-solving the
  default one against whatever bioconda serves that day, and a moved solve fails the build.
  Expect a slower-than-usual verify for this one image.

- **The DuckLake `assembly_membership` table gains the four attribute columns on the data
  plane's next start.** (#517) `ensure_assembly_tables` runs `ALTER TABLE … ADD COLUMN IF NOT
  EXISTS` on every DP boot, so the existing lake table widens itself — no operator step.
  Without it `ducklake_add_data_files` would reject every membership Parquet the new
  `assembly_load` writes, since the file would carry columns the table lacks. It refuses the
  other direction too (probed: a Parquet MISSING a column the table has is rejected with
  `Set allow_missing => true`), so a ticket whose `assembly_load` ran on the pre-deploy
  orchestrator and registers after the DP restart fails its register-files step. Both skews
  are narrow — the deploy restarts the DP and the CO together — and a failed register-files
  is a retryable step, not lost data: re-running the ticket's tail after the deploy writes a
  nine-column file. Nothing to do in advance; this is here so the failure is recognisable.

- **The `checkm` step now binds the deploy-staged miint extension, and its SIF rebuilds.** (#519)
  `checkm.sh` splits `circular.fa` into per-contig FASTAs with miint's `read_fastx` /
  `COPY … FORMAT FASTA`, so the step gained `derived_inputs: MIINT_EXTENSION_DIRECTORY:
  duckdb-ext` — resolved against `PATH_DERIVED` exactly as the `assemble` step's already is. The
  bind is **unconditional**: the backend emits it for every `derived_inputs` entry, so a missing
  or mislocated `${PATH_DERIVED}/duckdb-ext` fails apptainer for the whole step, not just the LCG
  arm, including for a sample with no circular contig. Nothing new to stage — it is the same
  directory the assemble step already requires — but this is a second step that now depends on
  it. The image also rebuilds (`checkm.sh`, `lcg_split.py` and `miint_connect.py` are in its
  `HASH_INPUTS`) and gains `python-duckdb`, pinned in lockstep with the orchestrator's resolved
  DuckDB, so expect a slower-than-usual verify for this image too.

- **`bin_quality` gains LCG rows; nothing existing changes.** (#519) The table now carries one row
  per circular genome beside the per-refined-bin rows, tagged by `kind`. Rows written by earlier
  deploys are untouched and are not backfilled — the values come from a CheckM run that did not
  happen, so an assembly stored before this deploy keeps MAG rows only. Re-running the workflow
  for a sample is what produces its LCG rows. The unbinned residue is still never scored.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
