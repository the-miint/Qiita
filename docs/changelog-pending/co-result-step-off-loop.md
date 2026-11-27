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
  absolute root. Which outputs pass or fail is unchanged.
