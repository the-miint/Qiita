### Fixed

- **A redrive or restart after a workflow edit skips the steps that ran, not the
  steps at their old positions (#NNN).** A ticket's step progress is matched to the
  workflow's steps by step name and occurrence (how many earlier steps share that
  name), so inserting, removing or reordering steps in a workflow YAML no longer makes
  `qiita ticket run` or a control-plane restart fast-forward the wrong step. A ticket
  whose progress names a step the workflow no longer has (removed, renamed, or changed
  between a `step:` and an `action:`) is refused by `POST /work-ticket/{idx}/run` with a
  409 naming that step, and fails at `submission` if a restart resumes it; submit a new
  ticket for it instead. `qiita.work_ticket_step` gains a `step_occurrence` column,
  backfilled by the migration; where the backfill cannot tell which entry a row was
  written for (a repeated step name on a ticket whose workflow has changed since), the
  row gets a negative occurrence and the ticket is refused the same way. The step
  indexes a ticket's status and step logs show follow the current workflow after a
  redrive, not the positions the steps had when they ran.
- **A resumed ticket no longer fails at a completed step whose files
  `register-files` moved (#NNN).** On a resume or redrive, a completed SLURM step's
  outputs are re-read checking its manifest and declared outputs, not every file the
  manifest lists, so the files a later `register-files` moved into the lake no longer
  fail the step with `declared output file missing` (#325's first defect).
- **`register-files` can be run again after it registered, or after it failed
  (#NNN).** A replay of the data plane's `register_files` — the control plane restarted
  after the registration committed but before it recorded the step complete — returns
  the files the first call registered instead of failing with `contains no Parquet
  files`. The control plane records the payload it sends beside the step's attempt
  directories and resends it on a replay, since the first call's move empties the
  staging dir. A registration that fails after its moves returns the files to staging
  (unless the catalog lists any of them, or cannot be read), so a redrive starts it
  over. A replay that finds the files moved but not registered still fails, since the
  first call may still be running.

### Changed

- **`register-files` is re-run on an unavailable data plane (#NNN).** It is re-run on
  the same transient data-plane failures as every other action entry, now that a
  re-run resends the recorded payload rather than what is left in staging.
- **`POST /work-ticket/{idx}/run` refuses a ticket under a deprecated mask (#NNN).** A
  ticket whose `mask_idx` names a deprecated `mask_definition` gets a 409 and stays as
  it was: a redrive fast-forwards the completed mint, so the mint's own refusal never
  ran.

### Added

- **`qiita ticket redrive` (#NNN).** Selects FAILED (or `--state cancelled`) tickets
  with the `qiita ticket list` filters plus `--failure-type`, prints the selection, and
  with `--execute` redrives each through `POST /work-ticket/{idx}/run`, one at a time,
  `--rate` seconds apart and with at most `--max-inflight` of them (default 8, the
  control plane's default fan-out cap) still pending, queued or processing. A ticket `/run`
  refuses (404 or 409) is reported and the run goes on; any other answer stops it and
  reports the tickets not attempted. A selection larger than `--limit` is refused
  rather than redriven in part.
- **`GET /work-ticket?failure_type=` (#NNN).** Filters the ticket list to FAILED
  tickets whose failure was `retriable` or `permanent`.
