-- migrate:up
-- =============================================================================
-- SYNDNA READ COUNT (reads aligned to each SynDNA insert, per masked sample)
-- =============================================================================
-- The read-mask `syndna` step aligns a sample's raw reads to the mask's SynDNA
-- reference, but the mask it emits keeps only a per-read verdict: which insert a
-- read hit is not in `read_mask`. This table keeps that breakdown — the number of
-- reads with a mapped primary alignment to each insert, ungated by the step's
-- identity / aligned-fraction cut — written by the `persist-syndna-read-count`
-- action before the sample's mask_sample gate flips to 'completed'.
--
-- One row per insert of the mask's SynDNA reference, zeros included, so a
-- (mask_idx, prep_sample_idx) with no rows means "not counted", never "no
-- SynDNA reads".
--
-- References mask_definition and prep_sample rather than mask_sample: the action
-- writes before finalize-mask-sample creates the gate row. Completion is still
-- the gate's value; a reader checks mask_sample, not the presence of rows here.
CREATE TABLE qiita.syndna_read_count (
    mask_idx         BIGINT NOT NULL REFERENCES qiita.mask_definition(mask_idx) ON DELETE CASCADE,
    -- CASCADE, as assembly_membership: derived per-sample output, removed with the
    -- sample by the pool force-delete rather than blocking it.
    prep_sample_idx  BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE CASCADE,
    -- The insert: a member of the mask's SynDNA reference.
    feature_idx      BIGINT NOT NULL REFERENCES qiita.feature(feature_idx) ON DELETE RESTRICT,
    read_count       BIGINT NOT NULL CHECK (read_count >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mask_idx, prep_sample_idx, feature_idx)
);

COMMENT ON TABLE qiita.syndna_read_count IS
    'Per-(mask, prep_sample, SynDNA insert) count of reads with a mapped primary '
    'alignment to the insert, ungated. One row per insert of the mask''s SynDNA '
    'reference, zeros included: no rows means not counted. Completion is the '
    'mask_sample gate, not the presence of rows.';

-- The PK leads with mask_idx; this serves the prep_sample CASCADE.
CREATE INDEX syndna_read_count_prep_sample_idx ON qiita.syndna_read_count (prep_sample_idx);

-- migrate:down
DROP TABLE IF EXISTS qiita.syndna_read_count;
