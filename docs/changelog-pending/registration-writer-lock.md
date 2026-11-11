### Fixed

- **Registrations into the replace-keyed tables no longer fail on each other's commits
  (#851).** DuckLake detects a conflict per table, so overlapping registrations into
  these tables lost their commits to each other and re-ran, and under steady overlap one
  could lose every attempt and fail with `failed to commit the registration transaction
  within 120s`, as 55 of a 1,062-ticket myloasm backfill did on qiita-miint. Now every
  registration into those tables, and every reference delete, holds one Postgres advisory
  lock in the catalog's database exclusively through its COMMIT, so they commit one at a
  time. The wait holds no pooled connection and ends after 120 s; a registration that
  gives up, cannot open its lock session, or finds before its COMMIT that the session
  lost the lock, moves its files back to staging and fails with a retriable status, so
  its step is run again. A lock session sets the catalog server's TCP keepalives for its
  connection (30 s idle, then 3 probes 10 s apart), so the server can end a session whose
  data plane it can no longer reach, releasing the lock.

### Changed

- **Lake writers no longer update `qiita_lake.registration_lock` (#851).** Every writer that
  updated it now holds the advisory lock exclusively. In local runs the row changed
  neither how many commits overlapping writers lost nor how many copies of a key registered
  concurrently were left (DuckDB 2.0.0, ducklake 417a9117). The data plane no longer
  creates it; the table stays in an existing catalog, unused.

- **A data plane declares its lake lock sessions in its catalog connection name (#851).**
  `qiita-data-plane:<port>:pool=<n>` now counts the pool plus two lock sessions, so the
  `verify-deploy` connection budget counts the extra catalog backends they may open.

### Added

- **The data plane logs each lake transaction attempt (#851).** One `lake-write:` line per
  attempt names the operation and its `work_ticket_idx` or `reference_idx`, the attempt
  number, how long the body and the COMMIT took and how it ended; a registration's line
  also gives its mode (`replace` or `unlocked`), its lock waits, and the time
  spent deleting, adding files and writing the ledger.
