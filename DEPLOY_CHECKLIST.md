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

- **Re-key the 6 legacy `mask_definition` rows onto the sequence-derived adapter identity** (#497). `qiita-admin backfill mask-adapter-hash` converts rows only when they carry ONE distinct stored `adapter_set_hash`; this host carries **two**, so the command reports and writes nothing, and the contract phase that removes the byte-derived fallback stays unreachable. `--attribute-all` is the designed way out — the operator asserting which reference a named set of rows was minted from — and the assignment is settled by the record, not a guess: reference 13 (`truseq-adapters`) was created 2026-06-27 12:39, and reference 7 (`qc-adapters`) has 0 `reference_membership` rows so it was never loadable as an adapter set, leaving reference 10 the only `active` `artifact_sequence_set` a mask could resolve before that timestamp. Masks 1, 2, 4 predate it and carry `d89caa61…`; masks 5, 6, 7 follow it and carry `48cd564d…`. Measured 2026-08-26.

  Dry-run first (the default — it writes nothing) and confirm **3 / 0 / 0** for re-key / unattributable / collided in each group:

  ```bash
  sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a
    /opt/qiita/control-plane/.venv/bin/qiita-admin backfill mask-adapter-hash \
      --reference-idx 10 --mask-idx 1 --mask-idx 2 --mask-idx 4 --attribute-all
    /opt/qiita/control-plane/.venv/bin/qiita-admin backfill mask-adapter-hash \
      --reference-idx 13 --mask-idx 5 --mask-idx 6 --mask-idx 7 --attribute-all'
  ```

  Then re-run both with `--execute`. `mask_idx` does not move, so nothing re-masks.

  **After.** Expect **0 rows** — this is what the contract phase reads as its go-ahead:

  ```sql
  SELECT count(*) FROM qiita.mask_definition
   WHERE adapter_hash_scheme IS NULL
     AND params->'resolved_qc'->>'adapter_set_hash' IS NOT NULL;
  ```

  Bucket 6 because it is irreversible: the byte digest is not recomputable without the adapter Parquet, so `migrate:down` cannot restore a converted row (see the `migrate:down` note in `20260804000000_mask_definition_adapter_hash_scheme.sql`).


### Notes (no host action)

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
