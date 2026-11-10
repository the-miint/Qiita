### Fixed

- **Registrations into the replace-keyed tables no longer fail on each other's commits
  (#TBD).** DuckLake detects a conflict per table, so overlapping registrations into
  these tables lost their commits to each other and re-ran, and under steady overlap one
  could lose every attempt and fail with `failed to commit the registration transaction
  within 120s`, as 55 of a 1,062-ticket myloasm backfill did on qiita-miint. Now a
  registration of runs the lake does not hold yet (myloasm provenance, `amplicon_membership`,
  `assembly_membership` and `bin_quality` alone) takes a Postgres advisory lock per run and
  only inserts, and does not conflict with other such registrations. One that deletes —
  any registration with a `feature_idx`-keyed table, a run the lake already holds, or a
  reference delete — waits for a global advisory lock first and reaches COMMIT alone. The
  waits hold no pooled connection, are bounded at 120 s each, and a registration that times
  out moves its files back to staging and fails with a retriable status, so its step is run
  again.

### Changed

- **A data plane declares its lake lock sessions in its catalog connection name (#TBD).**
  `qiita-data-plane:<port>:pool=<n>` now counts the pool plus two lock sessions, so the
  `verify-deploy` connection budget counts the extra catalog backends they may open.

### Added

- **The data plane logs each lake transaction attempt (#TBD).** One `lake-write:` line per
  attempt names the operation and its `work_ticket_idx` or `reference_idx`, the attempt
  number, how long the body and the COMMIT took and how it ended; a registration's line
  also gives its mode (`insert-only`, `replace` or `unlocked`), its lock waits, and the time
  spent deleting, adding files and writing the ledger.
