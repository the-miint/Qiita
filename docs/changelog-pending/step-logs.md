### Added

- **Native jobs write a readable step log (#852).** The SLURM launcher writes each
  native step's stderr as timestamped `<LEVEL> <step>: …` narrative lines — its own
  started/ended lines, plus whatever the job logs at INFO — and its stdout as one JSON
  receipt: `{"kind": "completed", "step_name", "summary": {…}}`, or the no-data /
  failure kind with a `reason`. The step-logs route and `qiita ticket logs` serve both
  files. A job narrates by logging at INFO, and may fill the receipt's `summary` via
  `summarize()`.
