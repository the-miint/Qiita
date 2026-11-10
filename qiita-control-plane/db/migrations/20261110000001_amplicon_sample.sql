-- migrate:up
-- =============================================================================
-- AMPLICON SAMPLE (per-(processing, sample) completion gate)
-- =============================================================================
-- The completion gate for amplicon denoising, the twin of qiita.assembly_sample
-- (and of qiita.mask_sample / qiita.alignment_sample). Without it the only record
-- of which samples an amplicon run counted is DuckLake `amplicon_membership`,
-- which a Postgres consumer — the study lineage — cannot read cheaply, and which
-- cannot say "this sample was denoised and yielded nothing".
--
-- Keyed on the RUN identity (qiita.processing): processing_idx is the
-- canonical-params hash that amplicon_membership is stamped with, so the gate
-- and the data it gates share one key.
--
-- An amplicon ticket covers a whole sequenced pool, so the gate is written for
-- every prep_sample of the pool at once:
--
--   'pending'   materialized by the runner for the pool's population when it
--               mints the run's processing_idx, before the step loop.
--   'completed' written by the terminal `finalize-amplicon-sample` action for
--               each sample denoise counted ASVs for, with that count.
--   'no_data'   written by the same action for each of the run's pending
--               samples denoise counted nothing for.
--
-- A failed ticket leaves its rows 'pending'; a re-run under the same params
-- resolves to the same processing_idx and closes them.
--
-- `state` is TEXT + CHECK, not an ENUM: the gate has no wire surface of its own
-- (the TEXT/CHECK carve-out in CLAUDE.md), as for qiita.assembly_sample.
CREATE TABLE qiita.amplicon_sample (
    -- RESTRICT for the reason on qiita.assembly_sample.processing_idx: one
    -- processing row is shared by every pool denoised under the same params.
    processing_idx   BIGINT NOT NULL
        REFERENCES qiita.processing(processing_idx) ON DELETE RESTRICT,
    prep_sample_idx  BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,
    state            TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'completed', 'no_data')),
    -- How many ASVs the run counted in this sample; set with 'completed' only.
    asv_count        INTEGER,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (processing_idx, prep_sample_idx),
    CONSTRAINT amplicon_sample_asv_count_with_completed CHECK (
        (state = 'completed') = (asv_count IS NOT NULL)
    ),
    CONSTRAINT amplicon_sample_asv_count_positive CHECK (asv_count IS NULL OR asv_count > 0)
);

COMMENT ON TABLE qiita.amplicon_sample IS
    'Per-(processing_idx, prep_sample) completion gate for amplicon denoising. '
    'Materialized ''pending'' for the pool''s population when the runner mints the '
    'run identity, then written ''completed'' (with asv_count) or ''no_data'' by '
    'the terminal finalize-amplicon-sample action. Twin of qiita.assembly_sample.';

-- The lineage reads by prep_sample; the PRIMARY KEY leads with processing_idx.
CREATE INDEX amplicon_sample_prep_sample_idx ON qiita.amplicon_sample (prep_sample_idx);

CREATE TRIGGER amplicon_sample_set_updated_at
    BEFORE UPDATE ON qiita.amplicon_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();

-- migrate:down
DROP TABLE IF EXISTS qiita.amplicon_sample;
