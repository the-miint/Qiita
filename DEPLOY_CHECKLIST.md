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

- **Collapse the 24 reference features the 2026-08-21 collapse left ambiguous** (#479). That run reported them and stopped: their copies were not byte-identical, so it had no basis to pick a survivor, and its entry told the operator to re-run the producing load. That is not the remedy — measured 2026-08-23, 23 of the 24 differ only in soft-masking case (46,327 of 46,624 chunk positions disagree byte-wise, **0** after `upper()`), and only `feature_idx` 127 is a true reverse complement. With the split now storing upper case, the 23 are byte-identical under normalization and collapse unambiguously.

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

  Expect `collapsible_features` **23**, `ambiguous_features` **1** — `feature_idx` 127, the fastp/truseq adapter, a real strand disagreement left alone by design. Measured against the live lake 2026-08-24: 24 ambiguous under the archived test, 1 under this one. Anything else, stop and re-measure rather than widening the normalization.

  **After.** The same before-query, expecting one row (`feature_idx` 127).

  **One-off.** Nothing after this deploy can create these rows: `register_files` replaces `reference_sequence_chunks` on `feature_idx`, pinned by `register_files_replaces_sequences_shared_across_references` in `qiita-data-plane/src/flight_service.rs`. This repairs rows written before that landed; it is not tooling to keep.

  The collapse rewrites Parquet, so `scripts/lake-gc.sh` has more to reclaim afterwards.


### Notes (no host action)

- **`qiita reference export` stops reproducing soft-masking** (#479). Chunks are stored upper case from this deploy on, so an exported FASTA is upper case for anything loaded after it — and for the 23 collapsed in bucket 6. Case is not recoverable from the lake. A reference loaded earlier and never re-loaded keeps its submitted casing indefinitely, since nothing re-loads one on its own; that is only visible through export, because the four index builders that read `chunk_data` all discard case (measured, see `normalized_sequence_expr`). Strand is unchanged: it still follows load order, as the existing caveat on `_write_genome_fasta` says.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
