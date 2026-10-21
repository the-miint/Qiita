### Fixed

- **A redrive or restart after a workflow edit skips the steps that ran, not the
  steps at their old positions (#NNN).** A ticket's step progress is matched to the
  workflow's steps by step name and occurrence (how many earlier steps share that
  name), so inserting, removing or reordering steps in a workflow YAML no longer makes
  `qiita ticket run` or a control-plane restart fast-forward the wrong step. A ticket
  whose progress names a step the workflow no longer has (removed, renamed, or changed
  between a `step:` and an `action:`) now fails at `submission`, naming that step; submit
  a new ticket for it instead of redriving. `qiita.work_ticket_step` gains a
  `step_occurrence` column, backfilled by the migration.
- **`register-files` can be run again after it registered (#NNN).** A replay of the
  data plane's `register_files` — the control plane restarted after the registration
  committed but before it recorded the step complete — returns the files the first call
  registered instead of failing with `staging file not found`. A replay that finds the
  files moved but not registered still fails, since the first call may still be
  registering them.

### Added

- **`qiita ticket redrive` (#NNN).** Selects FAILED (or `--state cancelled`) tickets
  with the `qiita ticket list` filters plus `--failure-type`, prints the selection, and
  with `--execute` redrives each through `POST /work-ticket/{idx}/run`, one at a time,
  `--rate` seconds apart. A ticket `/run` refuses (404 or 409) is reported and the run
  goes on; any other answer stops it and reports the tickets not attempted. A selection
  larger than `--limit` is refused rather than redriven in part.
