### Fixed

- **`fastq-to-parquet` 1.3.0 now stores its raw QC report (#906).** 1.3.0 computed `raw_qc_report` and never persisted it, so the pool QC report had nothing for those prep_samples. It is edited in place to end with a `persist-qc-report` step, which accepts the raw report alone and stores NULL for `filtered_qc_report`; in-flight tickets resume and run it, and prep_samples already processed keep NULL reports until recovery (#904). A comment-only migration refreshes the `sequenced_sample` QC-report column comments to match (#905).
