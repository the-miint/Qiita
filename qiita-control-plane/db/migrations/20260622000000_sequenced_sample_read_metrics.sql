-- Per-sequenced_sample read-count metrics.
--
-- Three nullable BIGINT counts recording how many reads survive each stage of
-- the fastq -> qc -> host_filter pipeline, written by the `persist-read-metrics`
-- action primitive from the read_count.json sidecars. Each count is
-- the both-mates total (R1 + R2; the `*_r1r2` convention), so a paired-end pair
-- contributes 2 and a single-end read contributes 1:
--   raw              -- reads out of bcl-convert, before any filtering (fastq)
--   biological       -- after adapter + quality/length filtering (qc)
--   quality_filtered -- after host depletion (host_filter; == biological on a
--                       host-filter pass-through)
-- fraction_passing_quality_filter is NOT stored — it is computed on read
-- (quality_filtered / raw) in SequencedSampleResponse, so it can never drift
-- from the counts.
--
-- Additive and backfill-free: every existing row reads NULL until its sample is
-- (re)processed by fastq-to-parquet/1.2.0. The monotonic CHECK encodes the
-- pipeline invariant (each stage only DROPS reads) and is satisfied vacuously
-- while any count is NULL.

-- migrate:up
ALTER TABLE qiita.sequenced_sample
  ADD COLUMN raw_read_count_r1r2              BIGINT,
  ADD COLUMN biological_read_count_r1r2       BIGINT,
  ADD COLUMN quality_filtered_read_count_r1r2 BIGINT,
  ADD CONSTRAINT sequenced_sample_read_counts_nonneg CHECK (
    (raw_read_count_r1r2 IS NULL OR raw_read_count_r1r2 >= 0)
    AND (biological_read_count_r1r2 IS NULL OR biological_read_count_r1r2 >= 0)
    AND (quality_filtered_read_count_r1r2 IS NULL OR quality_filtered_read_count_r1r2 >= 0)
  ),
  -- Each stage only drops reads, so quality_filtered <= biological <= raw.
  -- NULL comparisons evaluate to unknown, so a partially-populated row (which
  -- the primitive never writes — it sets all three together) still passes.
  ADD CONSTRAINT sequenced_sample_read_counts_monotonic CHECK (
    quality_filtered_read_count_r1r2 <= biological_read_count_r1r2
    AND biological_read_count_r1r2 <= raw_read_count_r1r2
  );

COMMENT ON COLUMN qiita.sequenced_sample.raw_read_count_r1r2 IS
  'Total reads (R1+R2) out of bcl-convert, before filtering. Written by the '
  'persist-read-metrics primitive from the fastq stage read_count.json. NULL '
  'until processed by fastq-to-parquet/1.2.0.';
COMMENT ON COLUMN qiita.sequenced_sample.biological_read_count_r1r2 IS
  'Total reads (R1+R2) after adapter + quality/length filtering (qc stage), '
  'before host depletion. NULL until processed.';
COMMENT ON COLUMN qiita.sequenced_sample.quality_filtered_read_count_r1r2 IS
  'Total reads (R1+R2) after host depletion (host_filter stage); equals '
  'biological_read_count_r1r2 when host filtering is disabled. NULL until '
  'processed.';

-- migrate:down
ALTER TABLE qiita.sequenced_sample
  DROP CONSTRAINT sequenced_sample_read_counts_monotonic,
  DROP CONSTRAINT sequenced_sample_read_counts_nonneg,
  DROP COLUMN quality_filtered_read_count_r1r2,
  DROP COLUMN biological_read_count_r1r2,
  DROP COLUMN raw_read_count_r1r2;
