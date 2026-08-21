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

- `20260818000000_assembly_membership_kind_comment.sql` — plain `make migrate`, no
  out-of-band setup. Comment only, no schema change: the `qiita.assembly_membership` table
  comment names the module that enumerates the `kind` value set
  (`qiita-common/src/qiita_common/assembly_constants.py`) instead of listing its members,
  which went stale when unbinned contigs became a third kind. (#460)

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

- **`long-read-assembly` 1.0.0 is edited in place, not versioned** — `activate.sh`'s
  `qiita-admin actions sync` re-syncs it, so the `qiita.action` list check `make
  verify-deploy` already runs is the confirmation it landed. It gained no step and no
  declared input: the `assembly_hash` step reads one more file (`noLCG.fa`) out of the
  `genomes_dir` it already binds, so there is no new bind mount, resource, or env var.
  (#460)

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

- **Backfill the unbinned residue onto already-assembled samples — AFTER the collapse
  above has finished.** The backfill is a series of `register-files` loads, and the collapse
  quiesce requirement is one-directional: a load that loses a DuckLake conflict is lost
  outright (its staging files are already moved), while the collapse is safe to re-run. So
  run the collapse to completion first, then the backfill, and never overlap them.

  **Volume.** Measured across the 57 samples below: 927,110 residue contigs, 44.26 Gbp of
  sequence new to the lake, plus the `qiita.feature` rows they mint and the
  `assembly_membership`, `assembled_sequence` and `assembled_sequence_chunks` rows that
  carry them.

  Features the collapse reported `ambiguous` in the `assembled_sequence` /
  `assembled_sequence_chunks` pair are resolved by this backfill where they belong to a
  backfilled sample: `register-files` deletes every lake row for a `feature_idx` it carries
  before adding its own, so the re-registered contig's bytes stand alone afterwards. It
  reaches only the tables its payload names, so a copy of the same feature in
  `reference_sequences` — a different pair, and one the collapse compares separately — is
  untouched, as are ambiguous features outside these samples.

  **This backfill re-runs the STORAGE TAIL only; it does not re-assemble.** The runner
  fast-forwards any entry that still has a `completed` row in `qiita.work_ticket_step` and
  rebuilds its outputs from the ticket workspace (`_reconstruct_completed_outputs` calls
  `result_step` for the attempt recorded on that row). The workflow has eleven entries;
  dropping the five tail rows keeps SIX — `assembly_run_config`, `assemble`,
  `assembly_coverage`, `binning`, `bin_refine`, `checkm` — and therefore replays
  `assembly_hash` → `mint-features` → `write-assembly-membership` → `assembly_load` →
  `register-files` against the SAME assembly bytes. All six are fast-forwarded the same way,
  the two native (`module:`) ones included: `assembly_run_config` at index 0 runs under SLURM
  like the rest, and `jobs/__main__.py` writes its `manifest.json` exactly as a container
  entrypoint does, so it is re-verified before anything else happens.
  A full re-run would instead re-execute `assemble` (baseline 32 CPU / 192 GB / PT16H)
  and everything after it, and nothing establishes that the assembler and the binners
  re-derive the same contigs — so it would change the stored `LCG` / `MAG` sequence rather
  than only add the residue.

  **The retained rows ARE the attempt lineage.** 40 of the 57 candidates have more than one
  attempt directory on at least one retained step (`assemble` up to 4; also `binning` and
  `assembly_coverage`). Which of them the fast-forward reads comes from the `attempt` column
  on the retained `work_ticket_step` row, so keep those rows exactly as they are and never
  identify a directory by attempt number, mtime, or "the highest one".

  **What the fast-forward re-verifies is the whole step contract, not just a directory.** On
  a COMPLETED status `result_step` re-runs `verify_container_output` over the attempt's
  `output/`: `manifest.json` present, under the size cap, valid JSON, a dict with a `files`
  array and an `outputs` object; every `outputs.<name>` resolving inside `output/` and
  existing; every `files[]` entry present AT ITS DECLARED `size_bytes`; and every file under
  `output/` listed in the manifest and mode `0440`. A partially reaped output, or one whose
  modes drifted, fails there with `CONTRACT_VIOLATION` even though its directory is intact —
  so read a failure as "the contract broke", not "the directory vanished".

  **Coverage is bounded by what survived on scratch, and the window is closing.** Surveyed on
  the deploy host: of 7,234 ticket workspaces under `PATH_SCRATCH/ticket`, 57 carry both an
  `assemble` and a `bin_refine` workspace; all 57 carry all six retained steps, and all 342
  of those workspaces passed the full contract above — no size drift, no undeclared files, no
  mode drift. So the backfillable population was 57 of 57, with no per-ticket exception list.
  That held because nothing in the set had been reaped yet: the oldest surviving workspace
  was 28 days old at survey time (2026-07-22). **Re-run the readiness check immediately
  before the backfill rather than trusting those numbers** — a workspace reaped in between
  turns a redrive into the recovery case below.

  Enumerate the candidates first; each directory name is the `work_ticket_idx`:

  ```bash
  for d in "$PATH_SCRATCH"/ticket/*/; do
    complete=1
    for step in assembly_run_config assemble assembly_coverage binning bin_refine checkm; do
      [ -d "$d/$step" ] || complete=0
    done
    [ "$complete" = 1 ] && basename "$d"
  done
  ```

  Then check the contract on the attempt each candidate will actually re-read, using the
  orchestrator's own verifier rather than a hand-rolled shell equivalent. The psql half is
  the operator's; the verifier half runs as `qiita-orch`, which owns those files. `<IDXS>` is
  the comma-separated candidate list:

  ```bash
  psql "$DATABASE_URL" -At -F' ' -c "
    SELECT work_ticket_idx, step_name, attempt
      FROM qiita.work_ticket_step
     WHERE work_ticket_idx IN (<IDXS>) AND state = 'completed'
       AND step_name IN ('assembly_run_config','assemble','assembly_coverage',
                         'binning','bin_refine','checkm')
     ORDER BY work_ticket_idx, step_index" \
  | sudo -u qiita-orch env PATH_SCRATCH="$PATH_SCRATCH" \
      /opt/qiita/compute-orchestrator/.venv/bin/python -c '
import os, sys
from pathlib import Path
from qiita_compute_orchestrator.slurm.verify import verify_container_output

root = Path(os.environ["PATH_SCRATCH"]) / "ticket"
bad = 0
for line in sys.stdin:
    if not line.strip():
        continue
    idx, step, attempt = line.split()
    out = root / idx / step / f"attempt-{attempt}" / "output"
    for failure in verify_container_output(out):
        bad += 1
        print(idx, step, f"attempt-{attempt}:", failure.reason, failure.detail or "")
print(bad, "failing workspaces")
sys.exit(1 if bad else 0)'
  ```

  Expect `0 failing workspaces`. Anything else names the ticket to drop from the run list.

  Both terminal outcomes are backfillable and take the same route: `completed` (the sample
  stored LCG/MAG rows and is missing only the residue) and `no_data` (`assembly_hash` raised
  StepNoData because every contig went unbinned — those samples gain everything). Per ticket,
  as the operator (`DATABASE_URL` from `/etc/qiita/control-plane.env`), confirm it is the
  assembly ticket, then flip it and drop the tail rows in ONE transaction. `qiita-admin
  ticket force-fail` cannot do this flip — it refuses a terminal ticket by design
  (`_FORCE_FAIL_ELIGIBLE_STATES` is the non-terminal set), so the UPDATE is by hand and must
  satisfy `work_ticket_failure_consistent` (`failure_type`, `failure_stage`, `failure_reason`
  all NOT NULL when `state='failed'`) and `work_ticket_failure_step_name_consistent`
  (`failure_step_name` NULL unless `failure_stage='step_run'`):

  ```sql
  -- Substitute <IDX>. Run one ticket at a time. RECORD the `state` this SELECT prints
  -- before going further — it is what the restore below puts back, and the
  -- `work_ticket_failure_consistent` CHECK guarantees the failure_* columns are all NULL
  -- alongside it, so there is nothing else to capture.
  BEGIN;
  SELECT work_ticket_idx, action_id, action_version, state, prep_sample_idx
    FROM qiita.work_ticket WHERE work_ticket_idx = <IDX> FOR UPDATE;
  -- Expect action_id='long-read-assembly' and state IN ('completed','no_data').
  -- Anything else — in particular a non-terminal state: ROLLBACK.

  -- The flip and the tail-row drop are ONE statement: the DELETE reads the
  -- work_ticket_idx the UPDATE returned, so it can only touch a ticket the UPDATE just
  -- flipped. `mint-features` and `register-files` are generic entries other workflows run
  -- too, so a DELETE keyed on work_ticket_idx and step_name alone would strip a mistyped
  -- ticket's progress and report a clean COMMIT.
  --
  -- The tail entries are named, not indexed, so a renumbered workflow cannot silently
  -- clear the wrong ones. Dropping a row makes the runner re-run that entry; the orphaned
  -- attempt dir left on disk is stepped over into a fresh attempt dir, never reused. This
  -- also clears the still-live `assembly_hash` row a StepNoData leaves behind (that path
  -- records no terminal step state), which `/run` keeps on a FAILED redrive and would
  -- otherwise try to re-attach to its dead SLURM job.
  WITH flipped AS (
    UPDATE qiita.work_ticket
       SET state = 'failed', failure_type = 'permanent', failure_stage = 'finalize',
           failure_step_name = NULL,
           failure_reason = 'operator redrive: replay the storage tail for the unbinned residue'
     WHERE work_ticket_idx = <IDX>
       AND action_id = 'long-read-assembly'
       AND state IN ('completed', 'no_data')
    RETURNING work_ticket_idx
  )
  DELETE FROM qiita.work_ticket_step s
   USING flipped f
   WHERE s.work_ticket_idx = f.work_ticket_idx
     AND s.step_name IN ('assembly_hash', 'mint-features', 'write-assembly-membership',
                         'assembly_load', 'register-files')
  RETURNING s.step_index, s.step_name, s.attempt;
  -- ZERO rows back means the UPDATE matched nothing (wrong idx, wrong action, or a state
  -- outside completed/no_data) and nothing was dropped: ROLLBACK and re-read the SELECT.
  COMMIT;
  ```

  Then redrive it and watch it to `completed`:

  ```bash
  qiita ticket run <IDX>
  qiita ticket status <IDX>
  ```

  **Watch the first few tickets for a resource escalation on `assembly_hash`.** Its
  baseline (2 CPU / 8 GB / PT1H) predates the residue: the step now reads `noLCG.fa` as well,
  in both of its passes (per-contig metadata, then chunking). An OOM or TIMEOUT there is
  retried with a grown floor, but the retry budget is TICKET-wide (`max_retries`, default 3),
  so an escalation spends a retry the rest of the redrive may need. If the first tickets
  escalate, raise
  `baseline_resources` on the step (a workflow edit + `qiita-admin actions sync`) before
  running the remaining ones, rather than paying it per ticket.

  **If a redrive fails, put the ticket back.** A workspace that fails the contract above —
  reaped, partially reaped, or mode-drifted — surfaces as a permanent `CONTRACT_VIOLATION`
  at the entry that re-reads it, and the ticket stays `failed`: a different terminal state
  than it started in, which the pool run-preflight gate reads differently for a `no_data`
  sample (see the Notes bucket). Restore it with the `state` captured above:

  ```sql
  -- <CAPTURED> is the state the SELECT printed: 'completed' or 'no_data'.
  UPDATE qiita.work_ticket
     SET state = '<CAPTURED>', failure_type = NULL, failure_stage = NULL,
         failure_step_name = NULL, failure_reason = NULL
   WHERE work_ticket_idx = <IDX>
     AND action_id = 'long-read-assembly'
     AND state = 'failed'
  RETURNING work_ticket_idx, state;
  ```

  This restores the ticket, not the dropped `work_ticket_step` rows — a later redrive of the
  same ticket replays the tail from `assembly_hash` again, which is what the backfill wants
  anyway. Any rows a partial redrive already wrote stay: `mint-features` mints against the
  shared `qiita.feature` (deduped by sequence hash) and `write-assembly-membership` inserts
  `ON CONFLICT DO NOTHING`, so re-running either adds nothing a second time.

  `processing_idx` is re-minted before the step loop from `{workflow, version, mask_idx,
  assembler}` and upserts on that hash, so the redrive resolves to the SAME identity the
  original run used — which is what makes the replace-by-key supersede that sample's
  `assembly_membership` / `bin_quality` rows instead of doubling them. The Postgres
  `qiita.assembly_membership` is the asymmetric half: it is written `ON CONFLICT DO NOTHING`,
  so the redrive ADDS the `UNBINNED` rows and removes nothing. Reusing the stored assembly
  bytes is what keeps that additive write correct — a redrive over re-assembled contigs would
  leave the superseded rows behind there. (#460)

### Notes (no host action)

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
  that principal now reaches contigs too. (#TBD)

- **`qiita.assembly_membership.kind` gains a third value, `UNBINNED`.** A
  long-read-assembly run also records the contigs no refined bin claimed, with the contig
  id as `bin_id` — the same `(kind, bin_id)` shape `LCG` uses. A consumer filtering on
  `kind IN ('LCG','MAG')` keeps working (the new value does not match that filter), and
  until a run is backfilled it keeps seeing exactly the rows it saw; a backfilled run's
  rows are replaced wholesale by the re-run's (bucket 6). A consumer that reads every row
  of a run sees more of them, and finds no `bin_quality` row for the new kind (CheckM
  covers refined bins only, as it already did for `LCG`). A sample whose contigs all went
  unbinned now completes with stored sequence where it previously ended as a no-data
  ticket. (#460)

- **Already-assembled samples do not gain the new kind until they are backfilled**, which
  is the bucket-6 step above, not something the deploy does. Two things that outlive the
  backfill itself: `qiita ticket run` alone cannot redrive a terminal ticket (`POST
  /work-ticket/{idx}/run` applies to `pending` / `failed` / `cancelled`, so it 409s on
  `completed` and on `no_data`) — which is why the backfill flips the state by hand rather
  than submitting a fresh ticket, since a fresh ticket gets a fresh workspace and would
  re-assemble. And a sample moving `no_data` → `completed` starts blocking a run-preflight
  edit on its pool: that gate counts the in-flight and `completed` tickets scoped to the
  pool or its samples, and `no_data` is deliberately outside that set so edit-then-retry
  stays possible. Correct any preflight that still needs it before backfilling its samples.
  (#460)

- **`register-files` now REPLACES the four content-addressed sequence tables on
  `feature_idx` rather than appending.** A load that carries a feature the lake already
  holds deletes the lake's rows for that key in the same transaction, ahead of the
  registration — so the row count for those tables can now go DOWN across a load, and the
  control-plane log records what each one superseded (`register_files superseded rows on the
  load's replace key`, with the per-table counts). Where a feature's two copies differ (a
  sequence and its reverse complement share one `feature_idx`), the newest load's bytes win;
  before this they were both kept and read back concatenated. (#457) `assembly_membership`
  and `bin_quality` are replaced too, on the composite `(prep_sample_idx, processing_idx)`:
  a re-run supersedes that sample's rows for that run, while a row agreeing on only one half
  of the key — another sample of the same run, or the same sample under a different
  `processing_idx` — is left alone. A run with no refined MAG writes `bin_quality` empty and
  still clears the previous run's rows there, so the log line can name a table the load
  itself contributed no rows to. Every other lake table is untouched. (#460)

- **`GET …/sequenced-pool/{pool}/alignment` gains a `params_hash` field, and the new
  `qiita feature-table build` requires it.** Additive, so an older client ignores it and
  nothing on the host changes. The direction that bites is the other one: the new CLI
  recomputes that digest and refuses to build against a server too old to report one, by
  design — a client cannot vouch for params it has no way to check. Anyone pointing this
  build's CLI at an older deployment gets that refusal, not a wrong table. Two new mint
  routes (`POST /exported-feature`, `POST /exported-processing`) ship alongside it under
  the scopes their siblings already use — no new scope, so no PAT re-mint. (#448)

- **`POST /exported-feature` labels a `source='qiita'` genome `QF<idx>`, not its
  `source_id`.** A genome whose `source` is an external repository (`genbank`, `refseq`)
  still publishes its `source_id` as `export_feature_id`, unchanged. Only a genome derived
  from one of our own prep_samples is affected — its `source_id` has no external authority
  behind it, so it now comes back with `accession: null` and `accession_published: false`.
  Such a genome is reachable today only through a `qiita reference load` genome map
  declaring `genome_source='qiita'` with a `prep_sample_idx`. No handle predates this
  behaviour: `qiita.exported_feature` is created by `20260813000000_exported_feature.sql`
  in bucket 3 of this same wave. That migration must not go out in a wave without this fix
  — `accession` and `accession_published` are immutable (the
  `exported_feature_retire_on_detach` trigger rejects any UPDATE touching either), so a
  handle minted in the interim cannot be corrected in place; the only remediation is
  deleting the genome, whose `ON DELETE SET NULL` detaches and retires the identifier.
  (#462)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
