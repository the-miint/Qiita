### Fixed

- **A redrive or restart after a workflow edit no longer skips a step that did not
  run (#756).** A ticket's step progress was matched to the workflow's steps by
  position alone, so inserting, removing or reordering steps in a workflow YAML made
  `qiita ticket run` or a control-plane restart fast-forward whichever step now sat
  at a completed step's old position. Each progress row must now still name the step
  at its position, by name and by kind (`step:` or `action:`). A ticket with a row
  that does not is refused by `POST /work-ticket/{idx}/run` with a 409 naming that
  row, and fails at `submission` if a restart resumes it; submit a new ticket for it
  instead, or for a `read-mask-block` ticket, re-mask its prep_samples under a new
  mask version. An edit that puts another step of the same name at a recorded position is
  not caught.
- **A resumed ticket no longer fails at a completed step whose files
  `register-files` moved (#756).** On a resume or redrive, a completed SLURM step's
  outputs are re-read checking its manifest and declared outputs, not every file the
  manifest lists, so the files a later `register-files` moved into the lake no longer
  fail the step with `declared output file missing` (#325's first defect).
- **`register-files` can be run again after it registered, or after it failed
  (#756).** A replay of the data plane's `register_files` — the control plane restarted
  after the registration committed but before it recorded the step complete — returns
  the files the first call registered instead of failing with `contains no Parquet
  files`. The control plane records the payload it sends beside the step's attempt
  directories and resends it on a replay, since the first call's move empties the
  staging dir. A registration that fails after its moves returns the files to staging
  (unless the catalog lists any of them, or cannot be read), so a redrive starts it
  over. A replay that finds the files moved but not registered still fails, since the
  first call may still be running.

- **A lake registration whose commit answer is lost no longer registers its files
  twice (#756).** When the data plane's connection to the DuckLake catalog drops
  after the catalog committed, the COMMIT reports an error although the rows are
  there, and the data plane's commit retry re-ran the registration, adding every
  file's rows a second time. Before re-running it now reads the catalog, and returns
  the first attempt's result when its commit is there. A reference delete does the
  same, so it reports the rows it deleted rather than zeros.
- **Two `register-files` calls placing the same lake file can no longer both move
  it (#756).** The data plane refused an existing destination by checking before the
  move, so two calls could both pass the check and the second replace the first's
  file; the move itself now refuses it.

### Changed

- **`register-files` is re-run on an unavailable data plane (#756).** It is re-run on
  the same transient data-plane failures as every other action entry, now that a
  re-run resends the recorded payload rather than what is left in staging.
- **`POST /work-ticket/{idx}/run` refuses a ticket under a deprecated mask (#756).** A
  ticket whose `mask_idx` names a deprecated `mask_definition` gets a 409 and stays as
  it was: a redrive fast-forwards the completed mint, so the mint's own refusal never
  ran.

### Added

- **`qiita ticket redrive` (#756).** Selects FAILED (or `--state cancelled`) tickets
  with the `qiita ticket list` filters plus `--failure-type`, prints the selection, and
  with `--execute` redrives each through `POST /work-ticket/{idx}/run`, one at a time,
  `--rate` seconds apart and with at most `--max-inflight` of them (default 8, the
  control plane's default fan-out cap) still pending, queued or processing. A ticket `/run`
  refuses (404 or 409) is reported and the run goes on; any other answer stops it and
  reports the tickets not attempted. A selection larger than `--limit` is refused
  rather than redriven in part. It prints a JSON report on stdout, or on stderr with
  exit 1 when any selected ticket was not redriven.
- **`GET /work-ticket?failure_type=` (#756).** Filters the ticket list to FAILED
  tickets whose failure was `retriable` or `permanent`.
