### Added

- **ENA download concurrency is configurable.** `download-ena-study` accepts an optional
  `run_concurrency` (1-8, default 4) for how many runs a ticket fetches at once, and the
  control plane sets it on batch-submitted tickets from `ENA_DOWNLOAD_RUN_CONCURRENCY`.
