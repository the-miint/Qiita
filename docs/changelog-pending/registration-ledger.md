### Added

- **The lake records every registration in `qiita_lake.registered_file` (#818).** Each
  registration writes a row per file (its lake path, table, work ticket and time)
  in the transaction that registers the files, and nothing deletes from the table. The
  data plane creates it at boot, and at each start moves the rows registered since the
  last one out of the catalog's Postgres into a Parquet file under the lake's data path.
  A data plane from before this change ignores the table, so rolling back keeps working.

### Fixed

- **A `register_files` replay is answered with the earlier registration even after its
  rows were deleted (#818).** A replay (the control plane resends a registration whose
  reply it lost) is now recognised from `registered_file`, so one that arrives after every
  row of a file was deleted, or after `scripts/lake-gc.sh --reclaim` removed the file,
  returns the earlier registration and writes nothing. Since the move to DuckDB 2.0 (#801)
  such a replay was refused; once lake GC had removed the file, it failed on the missing
  staging file or, if that file was still in staging, registered the deleted rows again
  (#800). A registration made before the ledger existed is still recognised as before,
  while its files are registered.
- **A registration whose COMMIT reported an error is answered as committed when it
  landed (#818).** The data plane already checked the lake after such an error. When that
  check could not be read, the call failed, and the files stayed in the lake. Now it reads
  the ledger once more, and a registration recorded there succeeds instead of failing its
  step.
