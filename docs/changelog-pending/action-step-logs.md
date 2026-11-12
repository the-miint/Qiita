### Added

- **Control-plane `action:` steps write a readable step log.** An in-process action
  (a library primitive such as mint-features or register-files) now writes the same two
  files a SLURM step does: a few timestamped narrative lines on stderr and one JSON
  receipt on stdout when it ends — a success summary, or a failure kind with a reason.
  Actions previously wrote no logs at all.
