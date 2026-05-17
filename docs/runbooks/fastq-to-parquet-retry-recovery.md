# fastq_to_parquet retry recovery

> Recovery path for a failed `fastq_to_parquet` work_ticket where the
> sequence-range was already minted by the failed attempt. Avoids
> destroying the prep_sample (the heavy-handed alternative).

## When this applies

The job ran past phase 3 (CP-minted sequence_idx range) and then failed
in phase 4 (rewrite the intermediate Parquet with the assigned
sequence_idx values) — typical causes: disk full, OOM, DuckDB crash,
SLURM node failure mid-write.

Symptoms:
- `qiita.work_ticket` row is in `state = FAILED`.
- `failure_step_name = 'fastq'`, `failure_stage = 'step_run'`.
- `failure_reason` does NOT contain "already has a sequence_range" —
  this recovery path is for failures AFTER phase 3, not failures at
  the mint call.
- `qiita.sequence_range` has a row for `prep_sample_idx` (i.e., phase
  3 succeeded before the crash).

If the failure was AT phase 3 (`SequenceRangeAlreadyExists` from a
prior even earlier failure), there's no recovery via this path —
follow the DELETE-prep_sample path instead.

## Recovery sequence

1. **Identify the prep_sample_idx** from the failed work_ticket:

   ```sql
   SELECT
     w.idx                AS work_ticket_idx,
     w.scope_target_value AS prep_sample_idx,
     w.failure_reason
   FROM qiita.work_ticket w
   WHERE w.idx = $FAILED_WORK_TICKET_IDX;
   ```

2. **Confirm the sequence-range row exists** for that prep_sample:

   ```sql
   SELECT sequence_idx_start, sequence_idx_stop
   FROM qiita.sequence_range
   WHERE prep_sample_idx = $PREP_SAMPLE_IDX;
   ```

   If this returns zero rows, the failure was at or before phase 3 —
   this runbook does not apply; follow the DELETE-prep_sample path.

3. **Confirm read count matches.** The orchestrator validates that the
   recovery range covers exactly the FASTQ's read count
   (`stop - start + 1`). If you somehow have a different FASTQ (e.g.,
   the operator re-uploaded a corrected file), the orchestrator will
   reject the recovery with `BAD_INPUT`. In that case, fall back to
   the DELETE-prep_sample path so a fresh mint sizes correctly.

4. **Resubmit the work_ticket** with the recovery range populated in
   the action inputs. The exact admin path depends on your CP's
   retry-policy surface — minimally, the resubmission payload's
   `inputs` block carries:

   ```json
   {
     "fastq_path": "/scratch/.../sample.fastq.gz",
     "prep_sample_idx": 42,
     "work_ticket_idx": <new_work_ticket_idx>,
     "pre_minted_range": {
       "sequence_idx_start": 1000,
       "sequence_idx_stop":  1099
     }
   }
   ```

   The orchestrator skips phase 3's HTTP mint call entirely when
   `pre_minted_range` is set; phases 1, 2, and 4 run as on the first
   attempt.

5. **Verify** on success: the Parquet at `reads.parquet` contains
   `sequence_idx` values in `[start, stop]` and the data plane
   registers the file into DuckLake without "sequence_idx range
   mismatch" errors.

## Why not just DELETE the prep_sample and start over?

That path works but destroys the prep_sample row. If anything else
already references the sample (biosample link, metadata, another
prep_sample sibling under the same biosample), you'd need to recreate
all of that. The recovery range path skips the destructive step
entirely for the common case of "phase 4 hit a transient I/O fault."

## Invariants preserved

- All identifiers still minted exclusively by the CP (the recovery
  range was minted on the original attempt; the retry reuses it).
- `qiita.sequence_range.UNIQUE(prep_sample_idx)` stays unviolated —
  the retry doesn't call mint.
- Compute service-account scope-minimal at `sequence_range:mint` is
  preserved (no new HTTP calls from the orchestrator on the retry
  path).
- Mint endpoint contract unchanged (still 409 on duplicate).

## Future automation

The runner could detect this scenario and auto-inject the
`pre_minted_range` without operator action: query
`qiita.sequence_range` for the prep_sample, find the existing row,
populate the inputs block. Tracked as #40 (section (a)); this runbook
documents the manual path that ships today.
