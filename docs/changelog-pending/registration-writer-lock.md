### Fixed

- **Registrations into the replace-keyed tables no longer fail on each other's commits
  (#TBD).** DuckLake detects a conflict per table, so overlapping registrations into
  these tables lost their commits to each other and re-ran, and under steady overlap one
  could lose every attempt and fail with `failed to commit the registration transaction
  within 120s`, as 55 of a 1,062-ticket myloasm backfill did on qiita-miint. Now a
  registration of runs the lake does not hold yet, whose tables are all run-keyed
  (myloasm provenance, `amplicon_membership`, `assembly_membership`, `bin_quality`),
  takes a Postgres advisory lock per run, holds a global advisory lock shared, and only
  inserts, so such registrations overlap each other. One that deletes — any registration with a `feature_idx`-keyed table, a run the
  lake already holds, or a reference delete — holds the global lock exclusively, so no
  insert or delete into those tables commits beside it. The waits hold no pooled
  connection and end after 120 s in all; a registration that gives up, or cannot open its
  lock session, moves its files back to staging and fails with a retriable status, so its
  step is run again.

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
