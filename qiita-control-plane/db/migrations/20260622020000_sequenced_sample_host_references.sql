-- Per-sequenced_sample host-filter references.
--
-- Two nullable references naming the host this sample is depleted against, set
-- at sample-creation time (submit-bcl-convert maps the run preflight's
-- per-project `human_filtering` flag: 1 -> the operator's host reference(s),
-- 0 -> NULL). They drive the per-sample host-filter decision when the pool fans
-- out fastq-to-parquet, and map 1:1 onto that workflow's action_context keys
-- (host_rype_reference_idx / host_minimap2_reference_idx), so the fan-out is a
-- pass-through:
--   host_rype_reference_idx     -- the host database (a reference carrying a
--                                  rype .ryxdi index); stage 1 of host_filter.
--   host_minimap2_reference_idx -- optional second-pass reference (a minimap2
--                                  .mmi index), run on rype's survivors.
-- Both NULL -> no host filtering (pass-through). minimap2 only ever accompanies
-- rype (the workflow's "rype required, minimap2 optional" rule), enforced by the
-- CHECK below.
--
-- The reference is (name, version), so pinning host_rype_reference_idx records
-- the exact host build per sample; a new build is a new reference_idx. A
-- non-human host (mouse, horse, ...) is just a different reference here, not a
-- schema change. ON DELETE RESTRICT mirrors the other reference FKs: a reference
-- a sample is filtered against cannot be hard-deleted out from under it.
--
-- Additive and backfill-free: every existing row reads NULL/NULL until its
-- sample is (re)created by submit-bcl-convert.

-- migrate:up
ALTER TABLE qiita.sequenced_sample
  ADD COLUMN host_rype_reference_idx     BIGINT REFERENCES qiita.reference (reference_idx) ON DELETE RESTRICT,
  ADD COLUMN host_minimap2_reference_idx BIGINT REFERENCES qiita.reference (reference_idx) ON DELETE RESTRICT,
  -- minimap2 is the optional second stage; it never runs without rype.
  ADD CONSTRAINT sequenced_sample_host_minimap2_requires_rype CHECK (
    host_minimap2_reference_idx IS NULL OR host_rype_reference_idx IS NOT NULL
  );

COMMENT ON COLUMN qiita.sequenced_sample.host_rype_reference_idx IS
  'Host database this sample is depleted against (a reference carrying a rype '
  '.ryxdi index), set at creation from the run preflight (human_filtering 1 -> '
  'operator host reference, 0 -> NULL). NULL -> no host filtering. Flows into '
  'fastq-to-parquet host_rype_reference_idx in the pool fan-out.';
COMMENT ON COLUMN qiita.sequenced_sample.host_minimap2_reference_idx IS
  'Optional second-pass host reference (carrying a minimap2 .mmi index), run on '
  'rype survivors. Only set alongside host_rype_reference_idx (see CHECK). Flows '
  'into fastq-to-parquet host_minimap2_reference_idx in the pool fan-out.';

-- migrate:down
ALTER TABLE qiita.sequenced_sample
  DROP CONSTRAINT sequenced_sample_host_minimap2_requires_rype,
  DROP COLUMN host_minimap2_reference_idx,
  DROP COLUMN host_rype_reference_idx;
