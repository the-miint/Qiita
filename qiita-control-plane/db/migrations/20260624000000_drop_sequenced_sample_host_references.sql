-- Drop the per-sequenced_sample host-filter references.
--
-- Host references are a filtering-config choice, not a sample property: which
-- host a sample's reads are depleted against is decided at human-filter
-- submission (it parameterizes the read mask), so two filtering configs can
-- coexist over the same reads. Pinning one host reference per sample at
-- creation forbade that. The references now travel as human-filter submission
-- arguments into the work-ticket action_context, where the runner reads them to
-- mint the mask and drive host_filter — the sequenced_sample row no longer
-- carries them.
--
-- A single drop (no expand/contract): the deploy wipes all legacy
-- sequenced/pool samples first (their reads predate the lake-read model and
-- were never registered into DuckLake), so there is no data to preserve and no
-- rolling-deploy window to protect. prep_protocol_idx stays on the sample (a
-- wet-lab property set at prep creation); only the host-filter references move.
--
-- The FK constraints and the minimap2-requires-rype CHECK drop with their
-- columns. migrate:down re-adds the columns, their ON DELETE RESTRICT FKs to
-- qiita.reference, the CHECK, and the column COMMENTs exactly as
-- 20260622020000_sequenced_sample_host_references.sql defined them, so the
-- migration is reversible.

-- migrate:up
ALTER TABLE qiita.sequenced_sample
  DROP COLUMN host_minimap2_reference_idx,
  DROP COLUMN host_rype_reference_idx;

-- migrate:down
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
