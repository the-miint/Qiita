### Fixed

- **A step attempt whose Slurm job slurmrestd had purged no longer fails as `manifest.json missing` (#900).**
  When slurmrestd answers 404 for the job and the attempt has no manifest, it is now classified from its stderr
  (any other 4xx on the status read keeps the manifest-only verdict): a launcher failure line keeps its kind, a
  `TIME LIMIT` kill is `timeout_before_start`, a Slurm signal kill (scancel) is `exit_nonzero`, an OOM signature is `oom_killed`,
  and an attempt with no recognizable cause is the retriable `job_purged`. A manifest that is present but broken
  is still a `contract_violation`, and so is a manifest that cannot be stat'ed (permission or stale-handle error).
