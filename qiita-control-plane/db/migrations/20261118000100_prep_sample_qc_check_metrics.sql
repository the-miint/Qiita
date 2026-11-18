-- Per-check measurements behind a QC row; existing rows read as NULL. The CHECK keeps a
-- jsonb `null` from posing as SQL NULL.

-- migrate:up
ALTER TABLE qiita.prep_sample_qc_check
  ADD COLUMN metrics JSONB,
  ADD CONSTRAINT prep_sample_qc_check_metrics_is_object CHECK (
      metrics IS NULL
      OR jsonb_typeof(metrics) = 'object'
  );

COMMENT ON COLUMN qiita.prep_sample_qc_check.metrics IS
    'Validated per-check measurements; the model per check lives in '
    'qiita_common.qc_metrics. NULL when the check has none.';

-- migrate:down

ALTER TABLE qiita.prep_sample_qc_check
  DROP CONSTRAINT prep_sample_qc_check_metrics_is_object,
  DROP COLUMN metrics;
