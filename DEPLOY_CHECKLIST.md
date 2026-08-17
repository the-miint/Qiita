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

- `20260813000000_exported_feature.sql` and `20260813000001_exported_processing.sql` —
  plain `make migrate`, no out-of-band setup. Two empty mint tables
  (`qiita.exported_feature`, `qiita.exported_processing`), each with the same
  retire-on-detach trigger `qiita.exported_identifier` already carries. That behaviour now
  applies on three more delete paths: deleting a **genome**, a **reference**, or an
  `alignment_definition` that has published handles detaches and auto-retires them rather
  than failing or removing them, and the retirement records which identifier was severed.
  (#448)

### 4. Deploy

_None yet._

### 5. Verify

- **The staged miint build must carry `shear_tree`** — `qiita feature-table build --tree`
  calls it, and it is absent from older builds (`d0336e9` does not have it; the mirror's
  current `1ad7fb4` does). There is no capability probe: an absent function surfaces as a
  bare `Catalog Error` naming the function, so check it once here rather than letting a
  user discover it. Run as the CP service account, against the staged extension directory
  the CP already LOADs from:
  ```bash
  sudo -u qiita-api env MIINT_EXTENSION_DIRECTORY="$(grep -oP '(?<=^MIINT_EXTENSION_DIRECTORY=).*' /etc/qiita/control-plane.env)" \
    python3 -c "import duckdb, os; c=duckdb.connect(':memory:', config={'extension_directory': os.environ['MIINT_EXTENSION_DIRECTORY'], 'allow_unsigned_extensions': 'true'}); c.execute('LOAD miint'); print(c.execute(\"SELECT count(*) FROM duckdb_functions() WHERE function_name='shear_tree'\").fetchone()[0])"
  ```
  Expect `1`. A `0` means the staged build predates the function — re-stage the extension
  before telling anyone `--tree` works. (#448)

### 6. After the deploy verifies green

- **Collapse the sequence rows duplicated before this deploy.** `register_files` now
  replaces `assembled_sequence` / `assembled_sequence_chunks` / `reference_sequences` /
  `reference_sequence_chunks` on `feature_idx` instead of appending, but it does not touch
  rows already there. Measured 2026-08-13: 53,698 of 129,290 assembly features had two
  copies, so their `string_agg(chunk_data, '' ORDER BY chunk_index)` is twice
  `sequence_length_bp`. Run AFTER bucket 5 — before the new build is live the next load
  re-duplicates. One-off: nothing after this deploy can create these rows.

  Run it as `qiita-data`, the only account that can write the lake data path. It is a
  non-login account, so `sudo -u qiita-data`, and give `duckdb` an absolute path — nothing
  is on that account's `PATH`, and it has no home to install into. Use the same DuckDB the
  data plane links; `scripts/lake-shell.sh` pins the version and carries an install recipe.

  `scripts/lake-shell.sh` already derives everything this needs from
  `/etc/qiita/data-plane.env` and is worth reading first for the two traps it encodes:
  `DATA_PATH` is `$PATH_PERSISTENT/ducklake` **verbatim**, trailing slash included (DuckLake
  rejects an attach differing by a slash), and the catalog password must not reach a file or
  argv. Take the password out of `DUCKLAKE_CATALOG_CONNSTR` before pasting it below and hand
  it to libpq instead:

  ```bash
  umask 077   # collapse.sql is read by duckdb only; do not leave it group-readable
  printf '*:*:*:%s:%s\n' "$LAKE_USER" "$LAKE_PASSWORD" > ~/pgpass && chmod 600 ~/pgpass
  sudo -u qiita-data env PGPASSFILE=~/pgpass /usr/local/bin/duckdb -bail -f collapse.sql
  rm -f ~/pgpass collapse.sql
  ```

  `SET home_directory` in the block is not optional: `INSTALL` resolves against
  `$HOME/.duckdb`, and `qiita-data`'s `$HOME` is `/dev/null`, which fails with `Can't find
  the home directory`.

  The `collapse.sql` the command above runs is below. Run it **once per table pair** —
  substitute `assembled_sequence` / `assembled_sequence_chunks`, then `reference_sequences` /
  `reference_sequence_chunks`. `-bail` is the CLI flag, not the `.bail on` dot-command: a
  dot-command is only recognised at column 0, so a copy-paste that picks up this block's
  indentation would silently run on without it.

  ```sql
  -- Substitute <CONNSTR>, <DATA_PATH>, <SEQ>, <CHUNKS>.
  SET home_directory='/tmp';
  INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;
  SET memory_limit='32GB'; SET temp_directory='/tmp';
  ATTACH 'ducklake:postgres:<CONNSTR>' AS qiita_lake (DATA_PATH '<DATA_PATH>');

  -- Features held more than once.
  CREATE OR REPLACE TEMP TABLE dup_feature AS
  SELECT feature_idx FROM (
      SELECT feature_idx FROM qiita_lake.<SEQ> GROUP BY feature_idx HAVING count(*) > 1
      UNION
      SELECT feature_idx FROM qiita_lake.<CHUNKS>
       GROUP BY feature_idx, chunk_index HAVING count(*) > 1);

  -- Of those, the ones whose copies are NOT byte-identical. A sequence and its
  -- reverse complement share one feature_idx, and nothing records which chunk came
  -- from which load, so collapsing these could splice two strands into one
  -- sequence. They are reported and left alone.
  CREATE OR REPLACE TEMP TABLE ambiguous_feature AS
  SELECT feature_idx FROM qiita_lake.<SEQ> SEMI JOIN dup_feature USING (feature_idx)
   GROUP BY feature_idx
  HAVING count(DISTINCT sequence_hash) > 1 OR count(DISTINCT sequence_length_bp) > 1
  UNION
  SELECT feature_idx FROM qiita_lake.<CHUNKS> SEMI JOIN dup_feature USING (feature_idx)
   GROUP BY feature_idx, chunk_index HAVING count(DISTINCT chunk_data) > 1;
  DELETE FROM dup_feature WHERE feature_idx IN (SELECT feature_idx FROM ambiguous_feature);

  SELECT (SELECT count(*) FROM dup_feature)       AS collapsible_features,
         (SELECT count(*) FROM ambiguous_feature) AS ambiguous_features;
  SELECT feature_idx FROM ambiguous_feature ORDER BY 1 LIMIT 50;

  BEGIN TRANSACTION;
  -- DISTINCT, not DISTINCT ON: every copy left in dup_feature is byte-identical, so
  -- this picks nothing, it deduplicates. The DELETE removes EVERY copy of each
  -- duplicated feature; the INSERT puts one back from the snapshot taken above, in
  -- this same transaction. Features not in dup_feature are never touched.
  CREATE OR REPLACE TEMP TABLE keep_sequence AS
    SELECT DISTINCT * FROM qiita_lake.<SEQ> SEMI JOIN dup_feature USING (feature_idx);
  CREATE OR REPLACE TEMP TABLE keep_chunk AS
    SELECT DISTINCT * FROM qiita_lake.<CHUNKS> SEMI JOIN dup_feature USING (feature_idx);
  DELETE FROM qiita_lake.<SEQ> WHERE feature_idx IN (SELECT feature_idx FROM dup_feature);
  -- ORDER BY keeps the feature_idx clustering the load path builds for row-group
  -- and file pruning.
  INSERT INTO qiita_lake.<SEQ> SELECT * FROM keep_sequence ORDER BY feature_idx;
  DELETE FROM qiita_lake.<CHUNKS> WHERE feature_idx IN (SELECT feature_idx FROM dup_feature);
  INSERT INTO qiita_lake.<CHUNKS>
    SELECT * FROM keep_chunk ORDER BY feature_idx, chunk_index;

  -- Validate BEFORE committing: a collapse that did not converge must roll back,
  -- not report a lake that is already wrong. `-bail` turns this into exit 1.
  SELECT CASE WHEN dups + mismatches > 0
              THEN error(format('collapse did not converge: {} duplicated, {} length '
                                || 'mismatches', dups, mismatches))
              ELSE 'converged' END
  FROM (SELECT
    (SELECT count(*) FROM (SELECT feature_idx FROM qiita_lake.<SEQ>
       SEMI JOIN dup_feature USING (feature_idx)
       GROUP BY feature_idx HAVING count(*) > 1))                       AS dups,
    -- sum(length(...)) rather than length(string_agg(...)): same number, without
    -- sorting and rebuilding every sequence just to measure it. Winnowed to the
    -- collapsed features before the join, not after.
    (SELECT count(*) FROM (
       SELECT s.feature_idx
         FROM (SELECT * FROM qiita_lake.<SEQ> SEMI JOIN dup_feature USING (feature_idx)) s
         JOIN (SELECT * FROM qiita_lake.<CHUNKS> SEMI JOIN dup_feature USING (feature_idx)) c
           USING (feature_idx)
        GROUP BY s.feature_idx, s.sequence_length_bp
       HAVING sum(length(c.chunk_data)) <> s.sequence_length_bp))       AS mismatches);
  COMMIT;
  ```

  `ambiguous_features` is expected to be **0**. If any appear, re-run the producing load
  (which now replaces on the key) rather than picking a copy by hand.

  **Quiesce loads while this runs.** This collapse does not take the `registration_lock` a
  `register-files` does, so the two are not serialized against each other; where they touch
  the same rows one will abort with a DuckLake transaction conflict, and the one that loses
  may be the load — which cannot be retried from the top, because its staging files have
  already been moved. The collapse itself is safe to re-run. (#457)

### Notes (no host action)

- **`register-files` now REPLACES the four content-addressed sequence tables on
  `feature_idx` rather than appending.** A load that carries a feature the lake already
  holds deletes the lake's rows for that key in the same transaction, ahead of the
  registration — so the row count for those tables can now go DOWN across a load, and the
  control-plane log records what each one superseded (`register_files replaced rows in
  content-addressed tables`). Where a feature's two copies differ (a sequence and its
  reverse complement share one `feature_idx`), the newest load's bytes win; before this they
  were both kept and read back concatenated. Every other lake table is untouched. (#457)

- **`GET …/sequenced-pool/{pool}/alignment` gains a `params_hash` field, and the new
  `qiita feature-table build` requires it.** Additive, so an older client ignores it and
  nothing on the host changes. The direction that bites is the other one: the new CLI
  recomputes that digest and refuses to build against a server too old to report one, by
  design — a client cannot vouch for params it has no way to check. Anyone pointing this
  build's CLI at an older deployment gets that refusal, not a wrong table. Two new mint
  routes (`POST /exported-feature`, `POST /exported-processing`) ship alongside it under
  the scopes their siblings already use — no new scope, so no PAT re-mint. (#448)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
