-- Per-sequenced_sample QC-report JSONB.
--
-- Two nullable JSONB columns holding the fastqc-equivalent qc_report.json that
-- the native `qc_report` job emits at each report point, written by the
-- `persist-qc-report` action primitive from the report sidecars:
--   raw_qc_report      -- report on the raw reads (the fastq output, before qc)
--   filtered_qc_report -- report on the host-filtered reads (the host_filter output)
-- Each blob is the per-mate (r1/r2) summary + the per-sequence quality / GC /
-- length histograms straight from the job's output document; the pool-level
-- merged report (GET .../sequenced-pool/{idx}/qc-report) sums these across the
-- pool's samples on read. Nothing is stored at the pool level.
--
-- Additive and backfill-free: every existing row reads NULL until its sample is
-- (re)processed by fastq-to-parquet/1.2.0. Left as free-form JSONB (no CHECK) —
-- the shape is owned by the Python `qc_report` job and the Pydantic merge
-- models, not enforced in the database.

-- migrate:up
ALTER TABLE qiita.sequenced_sample
  ADD COLUMN raw_qc_report      jsonb,
  ADD COLUMN filtered_qc_report jsonb;

COMMENT ON COLUMN qiita.sequenced_sample.raw_qc_report IS
  'fastqc-equivalent qc_report.json for the RAW reads (the fastq output, before '
  'qc trims/filters). Written by the persist-qc-report primitive from the '
  'qc_report_raw step sidecar. NULL until processed by fastq-to-parquet/1.2.0.';
COMMENT ON COLUMN qiita.sequenced_sample.filtered_qc_report IS
  'fastqc-equivalent qc_report.json for the host-filtered reads (the host_filter '
  'output). Written by the persist-qc-report primitive from the qc_report_filtered '
  'step sidecar. NULL until processed.';

-- migrate:down
ALTER TABLE qiita.sequenced_sample
  DROP COLUMN filtered_qc_report,
  DROP COLUMN raw_qc_report;
