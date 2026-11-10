### Added

- **Native jobs write a readable step log.** A native job's stderr is a few
  timestamped `<LEVEL> <step>: …` lines (the settings it runs with, what each stage
  kept or dropped, timings), and its stdout is one JSON receipt — `{"kind":
  "completed", "step_name", "summary": {…}}`, or the no-data / failure kind with a
  `reason`. The SLURM launcher writes both; a job narrates by logging at INFO and
  calling `summarize()` with the counts that describe its result.
