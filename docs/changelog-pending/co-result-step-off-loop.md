### Fixed

- **The orchestrator keeps answering requests while it checks a step's outputs
  (#TBD).** `POST /api/v1/step/result` checks every file in a completed step's output
  directory. That check ran on the orchestrator's only event loop, so every other
  request waited for it, including `/health`, status polls and submits. On the deploy
  host's Lustre scratch a gene_call output of 1,154 files took 2.85 s to check, longer
  than the control plane's 1 s `/health` probe, which then reported the orchestrator
  `unreachable`. `result_step` now runs in a worker thread.
- **Checking a step's outputs resolves each path only when a symlink is involved
  (#TBD).** The check resolved every output file's path three times; it now lists the
  output directory once and resolves only symlinks and paths containing `..` or an
  absolute root. Apart from the next entry, which outputs pass or fail is unchanged.
- **A step output with a directory or entry the orchestrator cannot read fails
  verification (#TBD).** It was skipped, so files in or behind it were never checked
  against the manifest or for mode `0440`. The failure is `output entry cannot be
  read`, naming the path; a declared file or `outputs` path that cannot be read is
  reported as `cannot be read`, no longer as missing. All 40,872 step output
  directories on the deploy host's scratch could be read in full by `qiita-orch`.
- **`POST /api/v1/step/submit` does its file-system work off the event loop
  (#TBD).** Creating the workspace directories, writing `params.json`, and checking
  each container input to pick its bind directory run in a worker thread.
