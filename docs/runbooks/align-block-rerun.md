# Re-running finished align blocks

For align blocks whose `alignment` rows are known to be wrong after the ticket
completed. `qiita-admin alignment rerun-blocks` re-arms a batch of those tickets so
the align workflow runs again and replaces each block's rows; you compare each batch
against a lake snapshot taken before the rollout before starting the next.

Requires a PAT carrying `alignment_definition:lifecycle` (system_admin) and
`prep_sample:write`, and `make lake-shell` on the deploy host for the checks.
Everything below reads the lake through `lake-shell` only.

## While a rollout is in progress

- **No `lake-gc`.** Expiring snapshots removes the baseline the comparisons read,
  and cannot be undone (`scripts/lake-gc.sh`).
- **No deploy whose migration UPDATEs `qiita.work_ticket`.** It changes every
  ticket's `updated_at`, and the CLI then refuses every remaining ticket as already
  changed.
- **No `abandon-plan` on the alignment.** A failed re-run leaves its block
  `pending` and its prep_samples' gates `pending`; abandon-plan would delete those
  gates and purge the prep_samples' rows from every block of the alignment. Retry
  the failed re-run instead (below). `AlignResubmitError`'s message suggests
  abandon-plan for pending gates; it does not apply here.
- **No feature tables, cohort downloads or exports on the alignment.** A prep_sample
  split across blocks goes back to `completed` when one of its blocks finishes,
  while a sibling block may still hold the bad rows.

## 1. Baseline

In `lake-shell`, record the snapshot and the tickets' state before anything runs:

```sql
SELECT max(snapshot_id) FROM ducklake_snapshots('qiita_lake');

COPY (SELECT work_ticket_idx, state::VARCHAR AS state, updated_at
        FROM qiita_cp.qiita.work_ticket
       WHERE work_ticket_idx IN (SELECT column0::BIGINT
                                   FROM read_csv('<tickets.txt>', header = false)))
  TO '<baseline.csv>' (HEADER);
```

`<baseline.csv>` is the CLI's `--baseline-file`; keep it, and the snapshot id, until
the rollout is finished. Also check that every ticket has been notified
(`notified_at IS NOT NULL`): the notification sweeper updates `updated_at` too.

## 2. Pre-flight

The re-run must not reuse the attempt directory whose output was registered, or it
reuses the registered file's lake name. `rerun-block` refuses such a ticket
(`registered_attempt_missing`); checking first saves a refused batch:

```sql
SELECT work_ticket_idx, attempt FROM qiita_cp.qiita.work_ticket_step
 WHERE step_name = 'align_sharded' AND state = 'completed'
   AND work_ticket_idx IN (<tickets>);
```

then, on the host, `PATH_SCRATCH/ticket/<work_ticket_idx>/align_sharded/attempt-<attempt>`
must exist for each row, and each ticket must have exactly one row.

## 3. Batches

Group the tickets into batches of at most 6, no two of which share a prep_sample
(`qiita_cp.qiita.block_member` joined to `block`). One alignment per batch. Put a
single ticket in the first batch, as the pilot.

## 4. Run a batch

```bash
qiita-admin alignment rerun-blocks <alignment_idx> \
  --work-ticket-idx <idx> [<idx> …] --reason "<why>" --baseline-file <baseline.csv>
```

prints the pre-check; add `--execute` to run it. Before the pilot's `--execute`, check
that its dry run shows `in_baseline: true` and `changed_since_baseline: false`: that
confirms the baseline's `updated_at` reads back as the API's. A ticket on a
since-deprecated mask needs `--allow-deprecated-mask` (refused on an active mask). The
CLI prints each response as it comes, waits until every re-armed ticket is terminal,
and prints a JSON report. Exit 0 means every ticket was re-armed and completed.

- **Refused locally** (`not_in_baseline`, `changed_since_baseline`): not POSTed.
- **Refused** (409, or 503 with `deadline_passed`, `statement_timeout` or
  `scratch_timeout`, each with a `reason`): nothing changed. Fix the cause or move the ticket to a later batch.
- **`unknown`** (any other 5xx, a timeout or a dropped connection): the batch
  stopped. Wait `unknown_decide_after_s` (in the report) after the request, then
  decide in `lake-shell`: the re-arm landed if the ticket's state is no longer the
  `completed` or `failed` it had before the POST. If it landed, wait for it with
  `--wait-only` (a landed ticket still `pending` with no step rows may never have
  been dispatched: see "Long `pending`"); if not, put it in a later batch. The bound
  counts from when the route starts, after authentication; a re-arm that commits
  later than that is refused a second time anyway, by the baseline check and because
  the route only re-arms a `completed` or `failed` ticket.
- **Long `pending`**: a re-armed ticket may be queued for one of the control plane's
  dispatch slots. Check the control-plane log for "queued behind the dispatch cap"
  before running `qiita ticket run`; a second dispatch of a queued ticket can fail it.
- **The CLI died or stopped mid-wait** (Ctrl-C, a 4xx on a GET, or GETs failing for
  `--wait-deadline-h`, each printing the report with `wait_stopped`; or a dropped
  session, which prints none): run it again with `--wait-only` and the same tickets.
  A ticket it lists under `not_rearmed` is unchanged since the baseline, so its POST
  never landed: put it in a later batch.

## 5. Check the batch

On the deploy host, against the baseline snapshot:

```bash
bash scripts/align-rerun-compare.sh <alignment_idx> <snapshot_id> <work_ticket_idx> …
```

prints one row per ticket; its header says what each column must be. Any difference
in `only_before` or `only_now` stops the rollout until it is understood: list the
rows of the differing reads, paired on `(sequence_idx, feature_idx)` (not `position`,
which can itself be damaged), with unpaired rows on their own. When
`zeroed_at_snapshot` is above 0, the ticket's original file held rows under a zeroed
`prep_sample_idx`. Those rows are outside the footprint at the snapshot, so compare
them by hand, as the prep_sample they belong to, and expect them in
`old_file_live_rows` until step 7 deletes them. Start the next batch only after every
ticket in this one passes.

## 6. Retrying a failed re-run

A failed re-run keeps its block `pending`, so its sibling blocks are refused
(`sibling_incomplete`) until it completes. Retry it before the batch that holds its
siblings:

- under an active mask: `qiita ticket run <idx>`, then
  `rerun-blocks <alignment_idx> --wait-only --work-ticket-idx <idx> …` to wait and report;
- under a deprecated mask: `rerun-blocks … --work-ticket-idx <idx> --allow-deprecated-mask
  --force-rearm --execute` (`--force-rearm` because the failed run changed `updated_at`).

Then check it as in step 5.

## 7. Rows under a prep_sample no gate tracks

```bash
qiita-admin alignment purge-sample-rows <alignment_idx> --prep-sample-idx <P> --reason "<why>"
```

deletes every row of that pair from `alignment` and `alignment_origin_spanning`;
refused when the pair has a gate. Count the rows of both in `lake-shell` before and
after (the response counts `alignment` only). After a timeout the count decides; the
delete is idempotent, so calling again is safe.

## 8. Finish

For every ticket of the rollout, not only those that went through a batch: the
original file has no live rows and a newer `wt<idx>-` file is registered. Scan the
alignment for `\0` in `cigar` / `tag_md`, and re-run whatever consistency checks
found the damage, over the new files. Refused, skipped or not-landed tickets go into
a further batch, rebuilt with the same no-shared-prep_sample rule; repeat this step
until every ticket passes. Then rebuild any feature table built from the alignment,
and only then run `lake-gc`.
