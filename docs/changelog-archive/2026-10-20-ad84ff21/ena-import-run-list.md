### Added

- **ENA import takes a chosen list of runs.** `POST /api/v1/ena-import-batch` and
  `qiita submit-ena-import` accept run accessions (`SRR`/`ERR`/`DRR`) alongside study
  accessions. The runs are resolved in the background, grouped by study into one item
  per study, and only those runs are registered. Only pools holding them get a download
  ticket, which also fetches runs a failed earlier download of that pool never stored.
  A run ENA does not return, or returns non-public, fails the item as
  `run(s) unavailable at ENA: …` with nothing registered or flagged. A run list wholly
  covered by whole-study accessions in the same batch follows those items' downloads.
  Batch items now carry `idx` and `requested_run_accessions`, and `ena_study_accession`
  is null on a run-list item while it is not grouped by study. See the runbook's
  "Importing chosen runs".
