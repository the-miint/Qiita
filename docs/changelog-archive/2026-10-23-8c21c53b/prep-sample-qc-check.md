### Added

- **A prep_sample's QC check results can be stored and read back (#729).** A new
  `qiita.prep_sample_qc_check` table holds one result per check and producer, and the
  `qiita.qc_verdict` view reduces a prep_sample's counting rows to its worst severity and
  the check that set it. `GET /api/v1/qc-check/{prep_sample_idx}` returns both to anyone
  who can read the prep_sample. The reason codes each check may carry are listed in
  `qiita_common/data/qc_reasons.yml`.
