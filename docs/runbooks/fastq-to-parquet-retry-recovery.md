# Reads-ingest retry recovery (fastq_to_parquet / bam_to_parquet / ingest_reads)

A reads job mints a `sequence_range` and **then** does its heavy durable write. The
window between the two is exactly where an OOM or walltime kill lands, which leaves an
orphaned range: the reads never reached the lake, but the sample's one-shot mint is
spent.

**This recovers itself now — there is almost nothing for an operator to do.**

Audience: operators and `system_admin`s.

## The common case: nothing to do

Every reads job pairs the mint with a read-back (`mint_or_reuse_sequence_range`). On a
409 it reads the existing range back and, **if its own ticket minted it**, reuses it and
carries on. So:

- The runner's in-place retry (including the OOM memory-escalation) recovers by itself.
- A ticket that ended `FAILED` is re-driven with `qiita ticket run <idx>` — same ticket,
  so the range is still its own, so the reuse applies.

No DB reads, no hand-built resubmission payload. (The old `pre_minted_range` input that
this page used to describe has been removed: it bypassed the ownership check below, and
nothing could set it — the step binder drops unknown `action_context` keys.)

## The refusal you might see, and what it means

> `prep_sample N already has a sequence_range minted by work_ticket M, not by this one
> (work_ticket K) — its reads are already loaded, and re-ingesting would duplicate them`

Reuse is deliberately restricted to the **minting** ticket. A range minted by a
*different* ticket means the sample's reads are **already registered in the lake**, and
reusing the range would register them a second time. DuckLake has no uniqueness, so that
duplication would be silent and permanent — hence a hard, permanent refusal.

The same refusal fires when the minter is **unknown** (`minted_by_work_ticket_idx IS
NULL` — a row the migration's backfill could not attribute unambiguously). Read it the
same way: assume the sample is already loaded.

**If you see this, the sample is already ingested. Do not force it through.** A
deliberate re-ingest means destroying what is there first:

- one sample → `DELETE` the `prep_sample` (its `sequence_range` goes with it via
  `ON DELETE CASCADE`), then resubmit;
- a whole pool → `qiita delete-sequenced-pool`, then resubmit.

Deleting the `prep_sample` is the **only** thing that clears a `sequence_range` — no CLI
or route deletes one on its own.

Confirm what you're about to destroy first:

```sql
SELECT sr.prep_sample_idx,
       sr.sequence_idx_start,
       sr.sequence_idx_stop,
       sr.minted_by_work_ticket_idx
  FROM qiita.sequence_range sr
 WHERE sr.prep_sample_idx = $PREP_SAMPLE_IDX;
```

## The other manual case: a width mismatch

> `… but its input now has N reads — the range must match the prior mint count exactly`

The range's width no longer matches the input's read count, which means the **input file
changed between attempts**. That is a data-integrity problem, not a retry problem:
inputs are required to be immutable between work_ticket submission and step execution.
Establish which file is correct before doing anything else; if the new file is the
intended one, delete the prep_sample so a fresh mint sizes correctly.

## Force-failing a stuck ticket

If a `pending`, `queued`, or `processing` ticket needs to be terminally failed (operator
triage, blocked-by-unrelated-bug, etc.), use `qiita-admin ticket force-fail` rather than
writing the UPDATE by hand. It mirrors the `work_ticket_failure_step_name_consistent`
CHECK constraint client-side and refuses to overwrite an already-terminal ticket:

```bash
# [admin] — DATABASE_URL sourced from /etc/qiita/control-plane.env
qiita-admin ticket force-fail \
    --idx 42 \
    --stage step_run \
    --step-name fastq \
    --reason "manual triage: stuck mid-step"
```

`--step-name` is required when `--stage=step_run` and rejected when `--stage` is
`submission` or `finalize`.

## Invariants preserved

- All identifiers are still minted exclusively by the control plane; a retry reuses the
  range its own attempt minted rather than allocating a new one.
- `qiita.sequence_range.UNIQUE(prep_sample_idx)` is never violated — the reuse path does
  not re-mint.
- The compute service account stays scope-minimal: the read-back
  (`GET /sequence-range/{idx}`) is deliberately gated on the same `sequence_range:mint`
  scope the mint uses, so no `prep_sample:read` grant is needed.
- The mint endpoint's contract is unchanged (still 409 on duplicate); what changed is
  that the *caller* now recovers from that 409 when — and only when — the range is its
  own.
