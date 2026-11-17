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

- `[operator]` **Before the restart, run the host-taxon backfill.** From the restart a biosample
  with no `host_taxon_id` passes through host filtering undepleted, so skipping `--execute` lets
  residue biosamples (taxa the curated table does not cover) pass through undepleted. `--execute`
  writes them `not provided`, which stays refused until patched, and leaves hostless taxa absent; it
  is safe on the old build. This is the backfill's only run: after the restart an absent value is
  deliberate, so never run it again. (#864)
  1. Dry-run and read the report. Expect no `WARNING:` lines and `no taxon_id at all (not
     backfillable)` to read 0, because a blank that the backfill cannot recognize as a control, or a
     biosample with no `taxon_id`, is no longer refused; if not, stop and raise the report.
  2. Only then run it with `--execute`.
  3. Dry-run again. Expect `Pass --execute to write 0 row(s)`; if not, stop and raise the report.
  ```bash
  # [operator] in the pulled checkout; DATABASE_URL as above
  cd ~/qiita-miint/qiita-control-plane
  /usr/local/bin/uv run --reinstall-package qiita-common qiita-admin backfill host-taxon-id --show-unresolved 50            # 1 and 3: dry run
  /usr/local/bin/uv run --reinstall-package qiita-common qiita-admin backfill host-taxon-id --show-unresolved 0 --execute  # 2: writes
  ```
- `[admin]` **gpl-boundary must report 0.5.0 or later**, or compute-readiness's
  `miint-gpl-boundary` probe fails the deploy. If it is older, replace the binary at that path by
  hand (the deploy does not upgrade it: #859). (#888)
  ```bash
  sudo -u qiita-orch "$(sudo grep -oP '^MIINT_GPL_BOUNDARY_PATH=\K.*' /etc/qiita/compute-orchestrator.env)" --version | grep gpl_boundary
  ```
- `[operator]` **Before the restart, no long-read-assembly ticket may be in flight.**
  long-read-assembly 1.0.2 gains steps in place (a `gene_call` step after `bin_refine`,
  two actions before `assembly_load`), so a ticket past `bin_refine` when the sync lands
  is refused on resume and has to be submitted again. Expect 0; otherwise wait for them
  to finish. (#888)
  ```bash
  psql "$DATABASE_URL" -tAc "SELECT count(*) FROM qiita.work_ticket WHERE action_id = 'long-read-assembly' AND state IN ('pending','queued','processing')"
  ```
- `[operator]` **Do not run `qiita-admin scratch reclaim --execute` until this deploy is in.**
  The bucket-6 gene-call backfill reads the finished long-read-assembly tickets'
  workspaces, and the build deployed now does not know to keep them. From this deploy,
  reclaim skips every ticket that backfill can still recover or is recovering. (#888)

### 3. Migrations

- `20261112000000_host_taxon_id_not_required` makes `host_taxon_id` optional, and from the
  restart an absent value means no host. `dbmate` applies it; run the bucket-2 backfill
  first. (#864)

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

- `[admin]` **Add gene calls to the long-read-assembly runs finished before this deploy**
  (1.0.0, 1.0.1 and 1.0.2; the dry run counts them). Each run gets one
  `assembly-gene-call-recover` ticket, which reads the `assemble`, `bin_refine`, `checkm`,
  `assembly_hash` and `mint-features` outputs left in that run's ticket workspace and
  registers its gene calls and its `assembly_membership` and `bin_quality` again, with
  `genome_idx`. Run it soon
  after the deploy: those files last only as long as the scratch workspace does. (#888)
  ```bash
  # DATABASE_URL and PATH_SCRATCH exported, as for any `qiita-admin backfill`;
  # QIITA_TOKEN is a system_admin's PAT (only --execute reads it).
  qiita-admin backfill assembly-gene-call                       # dry run: counts per status, and each run it cannot recover (residue), with the reason
  qiita-admin backfill assembly-gene-call --execute --limit 5
  ```
  Once those five tickets complete, check each run in `make lake-shell`, with `PS`/`RUN`
  from the lines the `--limit 5` run printed. Expect no NULL `genome_idx` and a non-zero
  gene count:
  `SELECT (SELECT count(*) FROM qiita_lake.gene_call WHERE prep_sample_idx = PS AND processing_idx = RUN), (SELECT count(*) FILTER (WHERE genome_idx IS NULL) FROM qiita_lake.assembly_membership WHERE prep_sample_idx = PS AND processing_idx = RUN), (SELECT count(*) FILTER (WHERE genome_idx IS NULL) FROM qiita_lake.bin_quality WHERE prep_sample_idx = PS AND processing_idx = RUN);`
  Measure each step's peak memory and elapsed time against its unmeasured baseline
  (gene_call 16 GB / 2 h; assembly_load 32 GB / 2 h), for every attempt:
  `sacct -j <job> -o MaxRSS,Elapsed`, with each `<job>` from
  `SELECT step_name, attempt, state, slurm_job_id FROM qiita.work_ticket_step WHERE work_ticket_idx = <T> AND slurm_job_id IS NOT NULL ORDER BY step_index, attempt`.
  Then submit the rest, paced:
  `qiita-admin backfill assembly-gene-call --execute --rate 10`.

  Each pass submits one ticket per prep_sample, so rerun it once the pass's tickets finish.
  You are done when the dry run counts no writable and no in-flight runs and every
  residue line has an explanation. `--retry-failed` resubmits runs whose recovery
  ticket ended without completing. If the dry run stops with a permission error, run it
  as an account that can read the ticket workspaces.

### Notes (no host action)

- (#896) `POST /study/lookup-by-accession`, `POST /biosample/lookup-by-accession` and `POST /biosample/lookup-by-matrix-tube-id` are deprecated but still served; a downstream client should move to `POST /study/lookup` and `POST /biosample/lookup`. A client generated from the OpenAPI spec sees those routes' and `POST /sequencing-run/lookup-by-instrument-run-id`'s response type renamed to `NaturalKeyLookupResponse`; the JSON is unchanged.
- (#863) `download-ena-study` 1.0.0 gains an optional `ena_run_accessions` through the normal `qiita-admin actions sync`, and the `held_not_downloaded` run status is no longer reported.
- `.env.compute-orchestrator.example`'s `MIINT_GPL_BOUNDARY_PATH` comment now names the tools behind the boundary (bowtie2, FastTree, Prodigal) and the 0.5.0 minimum; no live env change. (#888)
- New DuckLake tables `gene_call` / `gene_call_contig`, and a `genome_idx` column on `assembly_membership` and `bin_quality`, created by the data plane at start. long-read-assembly 1.0.2 (edited in place) and the new `assembly-gene-call-recover` 1.0.0 arrive through the deploy's usual `qiita-admin actions sync`; no new image. (#888)
- The staged miint must have `prodigal_proteins` (duckdb-miint `2b215901` or later); the miint stage step restages when the mirror's build changes, and compute-readiness's `miint-gpl-boundary` probe fails on a build without it. (#888)

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
