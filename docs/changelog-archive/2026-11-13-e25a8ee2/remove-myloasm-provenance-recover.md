### Removed

- **The `myloasm-provenance-recover` workflow, its job and `qiita-admin backfill
  myloasm-provenance` (#849).** Every myloasm run of
  long-read-assembly 1.0.0/1.0.1 now has its `myloasm_*` provenance tables, so the
  one-off has nothing left to recover. long-read-assembly 1.0.2 writes those tables
  itself. The optional `columns` on a `read_masked` DoGet ticket stays.
