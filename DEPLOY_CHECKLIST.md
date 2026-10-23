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

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

- `[admin]` (#773) The SLURM partition allows a 72 h job (`local-reference-add`'s `load` can now run 48 h and retry at 72 h): `scontrol show partition <partition> | grep -o 'MaxTime=[^ ]*'` reads `UNLIMITED` or at least `3-00:00:00`, where `<partition>` is `SLURM_PARTITION` in `compute-orchestrator.env`. Below that, SLURM holds or refuses a `load` submitted with the longer walltime, so a genome-scale reference load cannot finish; raise the partition's MaxTime before running one.
- `[admin]` (#773) Every `fastq-to-parquet` version's `fastq` step reads `cpu` 8 / `mem_gb` 13: `SELECT version, s->'baseline_resources' FROM qiita.action, jsonb_array_elements(steps) s WHERE action_id = 'fastq-to-parquet' AND s->>'name' = 'fastq';`

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (#773) Parquet that jobs register into DuckLake is now written at zstd level 16 (was the default 3), and the data plane sets DuckLake's `parquet_compression_level` to 16 at boot, so new lake files are ~12–15% smaller (synthetic production-shaped tables; −14.6% on one real long-read prep_sample's `sequence1`) and the steps that write them take longer: read ingest (`fastq-to-parquet`, `bcl-convert`'s `ingest_reads`, `download-ena-study`, `golay-demux`, `bam-to-parquet`), host filtering, alignment, and the reference, amplicon and long-read-assembly loads. Existing lake files are untouched. Watch reference-load walltimes on the first loads after the deploy.
- (#773) `fastq-to-parquet` 1.0.0–1.3.0 are edited in place, resources only: the `fastq` step goes from cpu 2 / mem_gb 8 to cpu 8 / mem_gb 13, re-synced by `qiita-admin actions sync` in `activate.sh`. `bam-to-parquet` 1.0.0 and `local-reference-add` 1.0.0 are edited in place the same way, for walltime: the `bam` step goes from PT4H to PT6H (its description now says prep_sample), and `local-reference-add`'s `load` step from PT36H to PT48H under an action ceiling raised from PT48H to PT72H. `golay-demux` now runs DuckDB on all 8 of the cpu its YAML already allocates (code only).

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
