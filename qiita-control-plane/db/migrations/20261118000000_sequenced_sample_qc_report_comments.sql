-- migrate:up

-- Comment-only: the columns' writers changed since they were described. Raw is
-- written by every run that persists it; filtered by none.
COMMENT ON COLUMN qiita.sequenced_sample.raw_qc_report IS
  'fastqc-equivalent qc_report.json for the RAW reads (the fastq output, before '
  'qc trims/filters). Written by the persist-qc-report primitive from the '
  'qc_report_raw step sidecar. NULL for a prep_sample no run has persisted it for.';
COMMENT ON COLUMN qiita.sequenced_sample.filtered_qc_report IS
  'fastqc-equivalent qc_report.json for the host-filtered reads, from a '
  'qc_report_filtered step sidecar. NULL when the workflow that last wrote the row '
  'has no filtered report, which persist-qc-report stores as NULL.';

-- migrate:down

COMMENT ON COLUMN qiita.sequenced_sample.raw_qc_report IS
  'fastqc-equivalent qc_report.json for the RAW reads (the fastq output, before '
  'qc trims/filters). Written by the persist-qc-report primitive from the '
  'qc_report_raw step sidecar. NULL until processed by fastq-to-parquet/1.2.0.';
COMMENT ON COLUMN qiita.sequenced_sample.filtered_qc_report IS
  'fastqc-equivalent qc_report.json for the host-filtered reads (the host_filter '
  'output). Written by the persist-qc-report primitive from the qc_report_filtered '
  'step sidecar. NULL until processed.';
