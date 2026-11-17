### Fixed

- **`fastq-to-parquet` now stores its raw QC report (#NNN).** 1.3.0 computed `raw_qc_report` and never persisted it, so the pool QC report had nothing for those samples. The new 1.4.0 adds `persist-qc-report` after `persist-read-metrics`, and `persist-qc-report` accepts the raw report alone, leaving `filtered_qc_report` as it was. `qiita submit-reads` now submits 1.4.0.
