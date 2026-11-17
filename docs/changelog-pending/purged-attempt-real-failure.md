### Fixed

- **A step attempt whose Slurm job slurmrestd had purged no longer fails as `manifest.json missing` (#900).**
  With no manifest, the attempt is now classified from its stderr: a launcher failure line keeps its kind, a
  `TIME LIMIT` kill is `timeout_before_start`, a Slurm signal kill (scancel) is `exit_nonzero`, an OOM signature is `oom_killed`,
  and an attempt with no recognizable cause is the retriable `job_purged`. A manifest that is present but broken
  is still a `contract_violation`.
