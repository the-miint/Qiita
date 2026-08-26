# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

**Control plane.** `PATH_INGEST_ROOTS` bounds which host paths a submitted `action_context`
may name (`bcl_input_dir`, `bam_path`, `fastq_path`, the `local-*-reference-add` `*_path`
set). Required — `from_env()` refuses to boot without it, deliberately: an unset value would
mean every absolute path the orchestrator can open is nameable through the API. Colon-separated
absolute dirs; `/` is refused.

```bash
# (#484)
sudo bash -c '
F=/etc/qiita/control-plane.env
grep -q "^PATH_INGEST_ROOTS=" "$F" && exit 0
s=$(grep "^PATH_SCRATCH=" "$F" | tail -1 | cut -d= -f2-); s=${s%\"}; s=${s#\"}; s=${s%/}
: "${s:?PATH_SCRATCH is not set in control-plane.env; set it first}"
[ -n "$(tail -c1 "$F")" ] && echo >> "$F"   # the file may not end in a newline
echo "PATH_INGEST_ROOTS=/sequencing:$s/references/staging" >> "$F"
'
```

Set it to every mount a submitted path may live under, not the sequencing mount alone.
Reference sources are the second one: `qiita reference load --local` names its manifest and
companions as raw `*_path` keys, and they are staged at
`${PATH_SCRATCH}/references/staging/{name}/{version}/`
([`reference-data-staging.md`](docs/reference-data-staging.md)) — a scratch mount, not a
sequencing one. Omit it and every `local-reference-add` / `local-host-reference-add` submit
422s. The roots bound the manifest path itself, not the FASTA paths listed inside it —
`stage_local_fasta._read_manifest` requires each entry be absolute and existing, and checks
nothing else — so this fence narrows what a submit can name, not what a reference can read.
The command above reads `PATH_SCRATCH` back out of the same env file and refuses to write
anything if it is unset — otherwise it would append a plausible-looking `/references/staging`
that `_parse_ingest_roots` accepts (it checks absolute and not-`/`, never existence), leaving a
control plane that boots clean and refuses every reference. Check the line it appends against
where reference sources are actually staged on this host. **The gate itself
needs no grant for
`qiita-api`:** it is written for the account split (CP runs as `qiita-api`, steps as
`qiita-job`, different groups) — it treats a permission error as "cannot tell" and admits, so a
run folder only `qiita-job` can read still submits. Reading a run folder through
`POST /run-folder/inspect` does need one; that is bucket 2. Add a second root by extending the
value with `:` rather than adding a line. (#484)

### 2. One-time host setup

**`qiita-api` needs read+traverse on the PacBio run tree**, or `POST /run-folder/inspect`
answers 403 for those folders and `submit-pacbio-ingest` still has to run from a node that
mounts `/sequencing`. Illumina needs nothing — `/sequencing/igm_runs/**` is world-readable the
whole way down, measured. `/sequencing/gcore_runs/**` is group `kl-seq-rw`, which `qiita-job`
is in and `qiita-api` is not.

Granted as an ACL on directories, not group membership: the route lists directories and opens
no BAM (`index_run_bams` globs `*/hifi_reads/*.bam` for filenames), and `gcore_runs` is
`drwxrwsr-x root kl-seq-rw` — adding `qiita-api` to that group would also give the public API's
service account write on the raw drop directory.

```bash
ROOT=/sequencing/gcore_runs
# Access entry + default entry on every existing directory; new project / run / well /
# hifi_reads dirs inherit both from their parent as they are created.
sudo find "$ROOT" -type d -exec setfacl -m u:qiita-api:rx,d:u:qiita-api:rx {} +
# Runs sit at two depths under $ROOT: most directly (`r84137_.../<well>/hifi_reads`), some
# under a project dir (`Knightlab/<run>/<well>/hifi_reads`). The find above covers every
# depth; check both here or a pass on one layout hides a failure on the other.
sudo -u qiita-api ls -d "$ROOT"/*/*/hifi_reads "$ROOT"/*/*/*/hifi_reads 2>/dev/null | head -3
```

That must print paths. **No output is a failure, not a pass** — it is what an unapplied grant
looks like, since the shell expanding the glob is your account, not `qiita-api`. Before the
grant, `sudo -u qiita-api ls /sequencing/gcore_runs/` is `Permission denied` on every entry;
after it, the line above lists. If it still denies, find the component that refuses with
`sudo -u qiita-api namei -l "$ROOT"` rather than re-running `setfacl`.

If `setfacl` reports `Operation not supported`, the mount has no ACL support: fall back to
`sudo usermod -aG kl-seq-rw qiita-api`, which also grants group write on `gcore_runs`, and note
that a supplementary group is read at process start — the bucket-4 restart is what picks it up.
(#484)

### 3. Migrations

Plain `make migrate` — no out-of-band setup.

- `20260824000000_upload_source_filename.sql` — adds nullable `qiita.upload.source_filename`
  (the client's basename, so the fastq filename-prefix rule applies to an upload-fed
  submission). Existing rows keep NULL, which the gate skips.
  (#484)
- `20260819000001_assembly_sample.sql` — plain `make migrate`, no out-of-band setup. One
  empty table and its index, `qiita.assembly_sample`: the per-`(processing_idx,
  prep_sample)` completion gate for `long-read-assembly`, alongside the existing
  `qiita.mask_sample` and `qiita.alignment_sample`. The index is created with the table, so
  it is a plain `CREATE INDEX` over zero rows — no `CONCURRENTLY`, nothing to lock.
  **No backfill**: assemblies already completed on this host get no gate row, so they read
  as not-assembled. No code reads the gate yet; whether to backfill them is a separate
  decision. The migrate→restart window has the same shape — an
  assembly ticket completing between bucket 3 and the bucket-4 restart runs under old code
  that writes no gate row. Re-submitting such a sample after the restart is admitted and
  re-writes it (no disallow-without-delete site applies to `long-read-assembly`). (#467)

- `20260825000000_sample_field_comment_corrections.sql` — plain `make migrate`, no
  out-of-band setup. Four `COMMENT` statements on the sample-field tables and columns; no
  DDL, no data touched, nothing locked beyond the momentary catalog write. Ordering versus
  the bucket-4 restart is irrelevant — no code reads these comments. (#485)

### 4. Deploy

_None yet._

### 5. Verify

Beyond `sudo make verify-deploy QIITA_HOSTNAME=<fqdn>`:

- **The ingest-root gate is live and bounded.** A path outside the roots must be refused at
  submit rather than accepted and failed inside a job. As a wet-lab admin or system admin,
  with a PAT (see [`user-cli-quickstart.md`](docs/runbooks/user-cli-quickstart.md)):
  ```bash
  qiita ticket submit --base-url https://<fqdn> \
    --action-id fastq-to-parquet --action-version 1.3.0 \
    --prep-sample-idx <idx> --context-json '{"fastq_path": "/tmp/not-a-root_R1.fastq"}'
  ```
  Expect exit 1 and a 422 whose detail carries `outside every configured ingest root` and an
  `ingest_roots` list matching what bucket 1 set. A 500, or a 202, means the var is wrong.
  Then confirm the roots are not too *narrow*, which fails the other way — a legitimate
  submit refused: `ingest_roots` in that same body must contain the reference staging root,
  or `qiita reference load --local` 422s for every reference. (#484)
- **A submit can now run off the cluster — both platforms.** `POST /run-folder/inspect`
  reads the run folder as `qiita-api`, a NARROWER account than the `qiita-job` that runs
  the jobs: it reaches the IGM folders through their world bits, and the gcore folders
  only through the ACL bucket 2 grants. Confirm one real folder of each kind:
  ```bash
  curl -sS -X POST https://<fqdn>/api/v1/run-folder/inspect \
    -H "Authorization: Bearer $QIITA_TOKEN" -H 'Content-Type: application/json' \
    -d '{"path": "/sequencing/igm_runs/<a-real-run>", "platform": "illumina"}'
  curl -sS -X POST https://<fqdn>/api/v1/run-folder/inspect \
    -H "Authorization: Bearer $QIITA_TOKEN" -H 'Content-Type: application/json' \
    -d '{"path": "/sequencing/gcore_runs/<project>/<a-real-run>", "platform": "pacbio_smrt"}'
  ```
  Illumina: 200 with `illumina.instrument_run_id` / `instrument_model` — measured working
  for `/sequencing/igm_runs/*`, no grant involved. PacBio: 200 with a non-empty
  `pacbio.hifi_bam_by_barcode`. A **403** means the ACL did not take, and its `reason` says
  whether the denial is at the run folder (or a parent) or at a directory below it — a
  partial grant is refused rather than reported as an empty index.
  (#484)
- **The widened read-ingest schemas synced.** `qiita-admin actions list` must still show
  `fastq-to-parquet 1.3.0` and `bam-to-parquet 1.0.0` enabled — the change is a
  `context_schema` widening in place, not a version bump, so no new version appears and none
  should have been auto-deprecated. (#484)
- **`long-read-assembly` 1.0.0 is edited in place, not versioned** — `activate.sh`'s
  `qiita-admin actions sync` re-syncs it, so the `qiita.action` list check `make
  verify-deploy` already runs is the confirmation it landed. One edit rides this deploy,
  adding no bind mount, resource, or env var: a terminal `finalize-assembly-sample` entry
  appended after `register-files` — an in-process control-plane primitive writing the
  `qiita.assembly_sample` gate, not a SLURM step. Confirm it landed: all three
  `qiita.assembly_sample` writes are gated on the terminal entry being present in the
  synced `steps`, so under a stale copy no gate row is written at all and the table stays
  empty — which reads like a migration that did not apply rather than a sync that did not
  land.
  ```bash
  sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -Atc "SELECT steps::text LIKE '\''%finalize-assembly-sample%'\'' FROM qiita.action WHERE action_id = '\''long-read-assembly'\'' AND version = '\''1.0.0'\'';"'
  ```
  Expect `t`. `f` is the stale copy. **Empty output** is a third outcome, not a pass: `-Atc`
  prints nothing for zero rows, so it means no `long-read-assembly` 1.0.0 row matched at
  all. Re-run `qiita-admin actions sync` for either. (#467)

- **The staged miint build must carry `circular_query_coverage`** — `qiita feature-table
  build --circular-gate` calls it. There is no capability probe: an absent function
  surfaces as a bare `Catalog Error` naming the function, so check it once here rather
  than letting a user discover it. Run as the CP service account, against the staged
  extension directory the CP already LOADs from:
  ```bash
  sudo -u qiita-api env MIINT_EXTENSION_DIRECTORY="$(grep -oP '(?<=^MIINT_EXTENSION_DIRECTORY=).*' /etc/qiita/control-plane.env)" \
    python3 -c "import duckdb, os; c=duckdb.connect(':memory:', config={'extension_directory': os.environ['MIINT_EXTENSION_DIRECTORY'], 'allow_unsigned_extensions': 'true'}); c.execute('LOAD miint'); print(c.execute(\"SELECT count(*) FROM duckdb_functions() WHERE function_name='circular_query_coverage'\").fetchone()[0])"
  ```
  Expect `1`. A `0` means the staged build predates the function — re-stage the extension
  before telling anyone `--circular-gate` works. (#475)

### 6. After the deploy verifies green

- **Collapse the 24 reference features the 2026-08-21 collapse left ambiguous** (#479). That run reported them and stopped: their copies were not byte-identical, so it had no basis to pick a survivor, and its entry told the operator to re-run the producing load. That is not the remedy — measured 2026-08-23, 23 of the 24 differ only in soft-masking case (46,327 of 46,624 chunk positions disagree byte-wise, **0** after `upper()`), and only `feature_idx` 127 is a true reverse complement, settled separately below. With the split now storing upper case, the 23 are byte-identical under normalization and collapse unambiguously.

  Scope is the `reference_sequences` / `reference_sequence_chunks` pair **only** — measured 2026-08-24, the assembly pair carries 0 duplicated features (the archived backfill resolved it), and `reference_sequences` itself has 0 duplicate rows, so this is a chunk-table repair.

  **Before.** Read-only; records what the collapse has to fix.

  ```bash
  bash scripts/lake-shell.sh -c "
    SELECT feature_idx, count(*) AS duplicated_positions FROM (
      SELECT feature_idx, chunk_index
        FROM qiita_lake.reference_sequence_chunks
       GROUP BY feature_idx, chunk_index HAVING count(*) > 1)
     GROUP BY feature_idx ORDER BY feature_idx"
  ```

  Expect 24 features: `feature_idx` 1-9, 11-24 and 127.

  **Feature 127 — keep the Read 2 orientation, delete the other.** Its two
  rows at `chunk_index` 0 are exact reverse complements, 33 bp each; they share one
  `feature_idx` because `canonical_sequence_hash_expr` folds strand, and both persisted
  because they predate the `register_files` replace-by-key the **One-off** note below
  names. Keep `AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT` — the orientation the FASTA reference 13
  was loaded from declares under `>Illumina_TruSeq_Adapter_Read_2` (`fastp_truseq_adapters.fna`
  on the deploy host, read there 2026-08-24; it is not in this tree), and 13 is the
  configured `QIITA_DEFAULT_ADAPTER_REFERENCE_IDX`. This is not a tie-break: which
  orientation a read carries follows the library protocol, so the survivor has to be the one
  the source FASTA declares.

  Read the two rows first and confirm which is which — the delete matches on the literal,
  so a mistyped one silently removes nothing:

  ```bash
  bash scripts/lake-shell.sh -c "
    SELECT chunk_index, chunk_data, length(chunk_data)
      FROM qiita_lake.reference_sequence_chunks
     WHERE feature_idx = 127 ORDER BY chunk_index, chunk_data"
  ```

  Then, under the **Collapse** scaffolding below and before `collapse.sql`:

  ```sql
  DELETE FROM qiita_lake.reference_sequence_chunks
   WHERE feature_idx = 127
     AND chunk_data = 'ACACTCTTTCCCTACACGACGCTCTTCCGATCT';
  ```

  Expect **1** row deleted; re-running the before-query then returns 23 features, and 127
  never reaches the collapse. Because it leaves `dup_feature`, it also skips the collapse's
  own convergence assertion (archived §6, `sum(length(chunk_data)) <> sequence_length_bp`)
  and its `upper()`. Assert the first here instead:

  ```bash
  bash scripts/lake-shell.sh -c "
    SELECT s.sequence_length_bp, sum(length(c.chunk_data)) AS chunk_bp
      FROM qiita_lake.reference_sequences s
      JOIN qiita_lake.reference_sequence_chunks c USING (feature_idx)
     WHERE s.feature_idx = 127 GROUP BY 1"
  ```

  Expect `33, 33`. Measured 2026-08-25 before the repair: `sequence_length_bp` is already 33
  against 66 bytes across the two rows, and both rows are upper case — so the delete
  restores the invariant, and 127 needs no `upper()` of its own. The same measurement over
  the whole lake finds 24 features whose `sequence_length_bp` disagrees with their chunk
  bytes: exactly the 24 in the before-query, so this bucket resolves all of them and none
  are left behind.

  **No mask is re-run for this.** Measured 2026-08-24 over 2,113,320 reads on 600 prep
  samples: 0 retain R2 adapter after trimming and 15 retain R1 adapter — the R1 count is the
  control showing the detection works — and of 1,994 asymmetrically-trimmed pairs, 0 carry
  adapter. Those counts are paired-end; the single-end path, which has no overlap-analysis
  arm to fall back on and so rests entirely on the adapter set, was checked separately
  2026-08-25 and is likewise unaffected. For scale, the host carries 9 masks over 3,678 prep
  samples (measured 2026-08-25). The stored adapter set is wrong; the masks derived from it
  are not.

  **Hold `qc` submissions until this runs.** `_write_adapter_parquet` now refuses a repeated
  chunk position instead of joining the two rows, so from the bucket-4 restart until this
  delete a `qc` ticket fails at input preparation with a BAD_INPUT naming the position.
  Both adapter references reach 127 — measured 2026-08-25, `reference_membership` carries it
  for 10 and 13 — so pointing `QIITA_DEFAULT_ADAPTER_REFERENCE_IDX` at the other one is not
  a way around the window. Do not run the delete early to
  close that window — it is irreversible, which is why it sits in this bucket. (#494)

  **A hand re-load can undo the choice.** The loader's survivor rule is
  `DISTINCT ON (sequence_hash) … ORDER BY sequence_hash, read_id`
  (`qiita-compute-orchestrator/.../jobs/hash_sequences.py`) — lex-smallest record name, not
  orientation. Re-loading a FASTA that declares both orientations therefore stores whichever
  record sorts first, and leaves one row, so there is no repeated position for
  `_write_adapter_parquet` to catch. Reference 10 declares both (`Trans2` / `Trans2_rc`);
  reference 13, read on the host 2026-08-24, declares only Read 2. Nothing re-loads a
  reference on its own — this is a caveat on doing it by hand.

  **Collapse.** Run [`docs/deploy-archive/2026-08-21-0d771b79.md`](docs/deploy-archive/2026-08-21-0d771b79.md) §6 for its invocation scaffolding — the `qiita-data` run-as, the `PGPASSFILE` handling, `SET home_directory`, `-bail` (the CLI flag, not the dot-command), and the quiesce requirement all still apply unchanged. Two statements in its `collapse.sql` differ; use these instead of the archived ones, verbatim:

  ```sql
  -- ambiguous_feature, chunk arm: a case-only disagreement is no longer ambiguous.
  SELECT feature_idx FROM qiita_lake.reference_sequence_chunks
   SEMI JOIN dup_feature USING (feature_idx)
   GROUP BY feature_idx, chunk_index HAVING count(DISTINCT upper(chunk_data)) > 1;

  -- keep_chunk: normalize, and name the columns in the table's own order
  -- (feature_idx, chunk_index, chunk_data) because the INSERT below is positional.
  CREATE OR REPLACE TEMP TABLE keep_chunk AS
    SELECT DISTINCT feature_idx, chunk_index, upper(chunk_data) AS chunk_data
      FROM qiita_lake.reference_sequence_chunks
      SEMI JOIN dup_feature USING (feature_idx);
  ```

  Expect `collapsible_features` **23**, `ambiguous_features` **0** — the delete above removed `feature_idx` 127 from `dup_feature`, so nothing reaches the ambiguous arm. Measured against the live lake 2026-08-24: 24 ambiguous under the archived test, 1 under this one. Anything else, stop and re-measure rather than widening the normalization.

  **After.** The same before-query, expecting **no rows**.

  **One-off.** Nothing after this deploy can create these rows: `register_files` replaces `reference_sequence_chunks` on `feature_idx`, pinned by `register_files_replaces_sequences_shared_across_references` in `qiita-data-plane/src/flight_service.rs`. This repairs rows written before that landed; it is not tooling to keep.

  The collapse rewrites Parquet, so `scripts/lake-gc.sh` has more to reclaim afterwards.


### Notes (no host action)

- **A `user` role gains `ticket:doput` on the next deploy.** No host action — role
  ceilings are code, applied by the restart. **Existing user PATs do not gain it**: token
  auth resolves to `frozen mint-time scopes & current role ceiling` (`auth/principal.py`),
  and widening the ceiling cannot add what the token never carried — a user wanting the
  new capability mints a fresh token.
  The scope buys a staging slot the caller owns; `reference:write` stays admin-only, so
  this does not let a user load a reference database. (#484)

- **A `user` can no longer name a host path in `action_context`; it is now wet_lab_admin+.**
  A `fastq_path` / `bam_path` submission from a `user` role returns 403 naming the
  `*_upload_idx` handle to use instead. They load reads with the new `qiita submit-reads`,
  which streams the file to the data plane and submits against the upload — the same workflow
  either way, since the runner resolves the handle to the same step input. Existing
  wet-lab-admin and system-admin path submissions are unaffected apart from the root bound.
  (#484)
- **`submit-bcl-convert` / `submit-pacbio-ingest` no longer have to run on a machine that
  mounts the cluster.** Both used to read the run folder locally — RunInfo.xml for
  Illumina, the `*/hifi_reads/*.bam` glob for PacBio — which is why the runbook told
  operators which machine to type on. Both reads moved to `POST /run-folder/inspect`. The
  path still has to be the one the CLUSTER sees; what changed is that it is now checked
  and read where it exists, so a wrong path fails at the terminal instead of inside a job.
  (#484)
- **`qiita submit-reads` needs `--data-plane-url`**, like `qiita reference load` — the reads go
  over Flight. From off the host that is the public TLS edge
  (`grpc+tls://<fqdn>:443`). (#484)
- **`qiita reference export` stops reproducing soft-masking** (#479). Chunks are stored upper case from this deploy on, so an exported FASTA is upper case for anything loaded after it — and for the 23 collapsed in bucket 6. Case is not recoverable from the lake. A reference loaded earlier and never re-loaded keeps its submitted casing indefinitely, since nothing re-loads one on its own; that is only visible through export, because the four index builders that read `chunk_data` all discard case (measured, see `normalized_sequence_expr`). Strand is unchanged: it still follows load order, as the existing caveat on `_write_genome_fasta` says.

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
