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

- **[operator] `20260910000000_sample_field_unique_in_study.sql` blocks all reads and writes on
  the sample-metadata tables for as long as it runs, and `make migrate` runs before the restart,
  against live traffic.** dbmate runs the file as one transaction, so its very first
  `ALTER TABLE` takes an ACCESS EXCLUSIVE lock and every later one adds a table to the set, all
  held until commit: `biosample_study_field`, `prep_sample_study_field`, `biosample_metadata`,
  and `prep_sample_metadata`. The work under those locks is one `ADD COLUMN` per table (instant),
  then a CHECK-constraint validation scan and three partial unique index builds on each metadata
  table — roughly four passes over each. Requests reaching any of the four in that window do not
  queue behind the lock: the control-plane pool's 10s `command_timeout` turns a lock wait into a
  failed request, and a metadata read joins the field table, so the whole sample-metadata surface
  is affected rather than the writes alone.

  Size the window first — count the two metadata tables on the live DB (the field tables hold one
  row per field definition and never drive the duration):

  ```sql
  SELECT 'biosample_metadata' AS table_name, count(*) AS row_count
    FROM qiita.biosample_metadata
  UNION ALL
  SELECT 'prep_sample_metadata', count(*)
    FROM qiita.prep_sample_metadata;
  ```

  What the counts mean (an order-of-magnitude judgement, not a measured curve — nothing here has
  been timed against production hardware):

  - **Under ~1M rows in each table:** small. Four passes finish in seconds, well inside the
    timeout. Migrate whenever.
  - **~1M to ~10M:** migrate during a quiet period. The window plausibly exceeds 10s, so requests
    touching sample metadata can fail while it runs; every other route is unaffected.
  - **Over ~10M:** stop and reopen the question of restructuring before migrating. Splitting the
    index builds into `transaction:false` + `CREATE INDEX CONCURRENTLY` files is the fix, and it
    is one file per index — not a flag on this one.

- **[operator] `make migrate` can abort on duplicate owner biosample ids, and that is a data
  finding, not a migration bug.** `20260911000000_owner_biosample_id_unique_in_study.sql` makes
  every existing owner-biosample-id field unique within its study. It fails, and rolls back, for
  any study whose samples already share an owner id. List them with:

  ```sql
  SELECT sf.study_idx,
         sf.idx   AS study_field_idx,
         sf.display_name,
         m.value_text,
         count(*) AS biosample_count
    FROM qiita.biosample_study_field sf
    JOIN qiita.biosample_metadata m
      ON m.biosample_study_field_idx = sf.idx
     AND m.is_owner_biosample_id
   WHERE sf.biosample_global_field_idx IS NULL
     AND NOT sf.unique_in_study
   GROUP BY sf.study_idx, sf.idx, sf.display_name, m.value_text
  HAVING count(*) > 1
   ORDER BY sf.study_idx, m.value_text;
  ```

  Each row is two or more samples in one study answering to the same owner id, so at least one is
  mislabelled. Resolving that is the study's decision, not a deploy step: take it back to the
  study before re-running the migration. Deploying without this migration is not an option — the
  code refuses imports into any study whose owner-id field is still unflagged.

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

_None yet._

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
