-- Per-run download failure on a sequenced_sample.

-- migrate:up
ALTER TABLE qiita.sequenced_sample
  ADD COLUMN download_failure_reason TEXT,
  ADD COLUMN download_failure_type   qiita.failure_type,
  ADD CONSTRAINT sequenced_sample_download_failure_pair CHECK (
    (download_failure_reason IS NULL) = (download_failure_type IS NULL)
  );

COMMENT ON COLUMN qiita.sequenced_sample.download_failure_reason IS
  'Why the run''s last download failed. Meaningful only while the run has no '
  'sequence_range row. Set together with download_failure_type.';
COMMENT ON COLUMN qiita.sequenced_sample.download_failure_type IS
  'Whether that failure was permanent or retriable. Set together with '
  'download_failure_reason.';

-- migrate:down
ALTER TABLE qiita.sequenced_sample
  DROP CONSTRAINT sequenced_sample_download_failure_pair,
  DROP COLUMN download_failure_type,
  DROP COLUMN download_failure_reason;
