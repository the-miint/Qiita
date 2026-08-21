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

- **`scripts/lake-gc.sh` reports against the live catalog.** Its default mode is inert —
  it acts only under `--reclaim` — so it is safe to run as a check, and its output sizes
  the reclaimable pile for the bucket-6 decision. Not read-only, though: DuckLake needs to
  write the data path even for the dry-run orphan scan, so run it as the account that owns
  that path, not merely one with group read. `--help` explains the flags.
  ```bash
  sudo -u qiita-data /usr/local/bin/duckdb --version   # 1.5.4, reachable by that account
  sudo -u qiita-data bash /home/qiita/qiita-miint/scripts/lake-gc.sh
  ```
  Expect three counted rows and `Nothing was removed`. A `DATA_PATH parameter … does not
  match` means the derivation drifted from `PATH_PERSISTENT`; a missing `duckdb` means the
  CLI isn't on a path that account can traverse (its home is not readable to it).
  (#472)

### 6. After the deploy verifies green

- **Needs the data plane from this wave — do not run the backfill below on an older
  build.** It redrives each ticket under its existing `work_ticket_idx`, which used to
  fail at the file move: `lake_dest_filename` minted `wt<work_ticket_idx>-<basename>`,
  unique across tickets but not across two loads from one ticket, so `move_file` refused
  to overwrite the file that ticket's original run had registered. Observed 2026-08-21 on
  work_ticket 6939 — `refusing to overwrite existing lake file
  <PATH_PERSISTENT>/ducklake/assembled_sequence_chunks/wt6939-part_00000.parquet` — and all
  57 candidate prep_samples collide the same way. The name now also carries a digest of the
  registration's `staging_dir`, so a redrive whose producer step re-ran lands on its own
  path; the procedure below drops `assembly_load`'s row, so it does. 6939 was restored to
  `completed` and the 16,395 `UNBINNED` `assembly_membership` rows its partial run wrote
  were deleted, so no prep_sample is left half-backfilled.
  (#472)

- **Readiness was re-verified 2026-08-21 and the candidate set is intact**: 7,234 ticket
  workspaces, 57 carrying all six retained steps, and all 342 of those workspaces passing
  `verify_container_output`. Re-run it again before the backfill anyway — the window
  closes as workspaces are reaped.

- **738 assembly and 24 reference features were left uncollapsed** by the collapse step
  archived in [`docs/deploy-archive/2026-08-21-0d771b79.md`](docs/deploy-archive/2026-08-21-0d771b79.md).
  All are reverse-complement pairs: identical `sequence_hash` and `sequence_length_bp`,
  differing `chunk_data`. The backfill below resolves the assembly ones for the samples it
  covers. Nothing resolves the 24 in `reference_sequence_chunks` — those need their
  producing reference load re-run.

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

- **Lake data files registered from this build carry an extra name segment.** The shape
  goes from `wt<work_ticket_idx>-<basename>` to
  `wt<work_ticket_idx>-<12 hex>-<basename>`; the hex is a digest of the registration's
  staging dir, which is what lets one ticket register twice (a redrive). Files already on
  disk are not renamed, so both shapes coexist. Anything matching lake filenames should key
  on the `wt<idx>-` prefix, not on the whole name.
  (#472)

- **Nothing has ever reclaimed superseded lake files, and `scripts/lake-gc.sh` is the first
  thing that can.** Every `register_files` replace-by-key and every `delete_reference` /
  `delete_mask` / `delete_pool_reads` / `delete_alignment` leaves its Parquet on disk. The
  script reports by default and acts only under `--reclaim`, behind a typed confirmation;
  running it is an operator decision, not a deploy step, because `--reclaim` expires
  snapshot history (7 days kept unless `--older-than` says otherwise) and that is not
  reversible. Quiesce registrations first — the script header says why the cutoff alone
  does not make a concurrent load safe.
  (#472)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
