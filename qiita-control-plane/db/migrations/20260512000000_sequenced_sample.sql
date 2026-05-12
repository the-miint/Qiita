-- migrate:up

-- =============================================================================
-- SEQUENCED SAMPLES (subtype of prep_sample where processing_kind = 'sequenced')
-- =============================================================================

CREATE TABLE qiita.sequenced_sample (
    idx                             BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    prep_sample_idx                 BIGINT NOT NULL,
    -- Pinned to a single enum value; participates in the composite FK to
    -- prep_sample (idx, processing_kind) so this row can only attach to a
    -- prep_sample whose processing_kind matches. The GENERATED form makes
    -- the constant un-writeable from outside, removing the failure mode
    -- where a caller passes a non-matching kind explicitly.
    processing_kind                 qiita.processing_kind
                                    GENERATED ALWAYS AS
                                    ('sequenced'::qiita.processing_kind) STORED,

    sequenced_pool_idx              BIGINT REFERENCES qiita.sequenced_pool(idx) ON DELETE RESTRICT,
    sequenced_pool_item_id          TEXT,

    ena_experiment_accession        VARCHAR(50),
    ena_run_accession               VARCHAR(50),

    -- Submission tracking. last_submission_at is NULL until the submission
    -- subsystem first attempts a submission, otherwise it holds the time of
    -- the most recent attempt. submission_error is NULL on success and
    -- carries the error message when the most recent attempt failed.
    last_submission_at              TIMESTAMPTZ,
    submission_error                TEXT,

    created_by_idx                  BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger; used as the
    -- ETag for optimistic-concurrency control on PATCH.
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- 1:1 with prep_sample.
    CONSTRAINT sequenced_sample_prep_sample_idx_unique UNIQUE (prep_sample_idx),

    -- Composite FK: drags processing_kind along so the parent's kind must
    -- match the literal-pinned kind on this row. Disjointness from any
    -- future processing_kind subtype (e.g., mass_specd_sample) is enforced
    -- by this FK plus the GENERATED ALWAYS AS literal on each subtype.
    CONSTRAINT sequenced_sample_prep_sample_fk
        FOREIGN KEY (prep_sample_idx, processing_kind)
        REFERENCES qiita.prep_sample (idx, processing_kind)
        ON DELETE RESTRICT,

    CONSTRAINT sequenced_sample_ena_experiment_accession_unique UNIQUE (ena_experiment_accession),
    CONSTRAINT sequenced_sample_ena_run_accession_unique UNIQUE (ena_run_accession),

    CONSTRAINT sequenced_sample_run_accession_requires_run CHECK (
        ena_run_accession IS NULL OR sequenced_pool_idx IS NOT NULL
    ),

    -- The pool reference and the pool's per-item id are co-populated: either
    -- the sample is attached to a pool (both set) or it is not (both null).
    -- A half-populated pair would mean either an item id without a pool
    -- (orphan) or a pool reference without the samplesheet's Sample_ID
    -- (which is required to locate the row inside the samplesheet).
    CONSTRAINT sequenced_sample_pool_pair_consistent CHECK (
        (sequenced_pool_idx IS NULL) = (sequenced_pool_item_id IS NULL)
    ),

    CONSTRAINT sequenced_sample_pool_item_id_unique
        UNIQUE (sequenced_pool_idx, sequenced_pool_item_id)
);

COMMENT ON TABLE qiita.sequenced_sample IS
    'A prep_sample routed down the sequencing pathway; one of the disjoint '
    'subtypes of prep_sample. Carries the sequencing-run linkage and the '
    'ENA EXPERIMENT / RUN submission state. 1:1 with prep_sample; mutually '
    'exclusive with any future processing_kind subtype via the composite '
    '(prep_sample_idx, processing_kind) FK plus the GENERATED ALWAYS AS '
    'pinned processing_kind on this row.';

COMMENT ON COLUMN qiita.sequenced_sample.ena_experiment_accession IS
    'ENA-assigned experiment accession. NULL until the experiment '
    'submission has succeeded and ENA has returned an accession. Parallels '
    'biosample.biosample_accession in role.';

COMMENT ON COLUMN qiita.sequenced_sample.ena_run_accession IS
    'ENA-assigned run accession. NULL until a sequencing run has been '
    'assigned and its submission has succeeded. The schema enforces that a '
    'run accession can only exist when sequenced_pool_idx is also populated.';

CREATE INDEX sequenced_sample_sequenced_pool_idx
    ON qiita.sequenced_sample (sequenced_pool_idx)
    WHERE sequenced_pool_idx IS NOT NULL;

CREATE TRIGGER sequenced_sample_set_updated_at
    BEFORE UPDATE ON qiita.sequenced_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- When a new submission attempt is recorded (last_submission_at changes),
-- any stale submission_error is cleared -- unless the caller also set
-- submission_error in the same UPDATE, in which case the caller's value is
-- kept. This lets a failed-attempt caller record both fields in one UPDATE
-- without the trigger overwriting the freshly-set error.
CREATE OR REPLACE FUNCTION qiita.sequenced_sample_clear_submission_error_on_new_attempt()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.last_submission_at IS DISTINCT FROM OLD.last_submission_at
       AND NEW.submission_error IS NOT DISTINCT FROM OLD.submission_error THEN
        NEW.submission_error := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_clear_submission_error_on_new_attempt
    BEFORE UPDATE OF last_submission_at ON qiita.sequenced_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_clear_submission_error_on_new_attempt();


-- migrate:down

DROP TABLE IF EXISTS qiita.sequenced_sample;

DROP FUNCTION IF EXISTS qiita.sequenced_sample_clear_submission_error_on_new_attempt();
