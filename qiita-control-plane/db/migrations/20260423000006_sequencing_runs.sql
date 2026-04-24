-- migrate:up

-- =============================================================================
-- SEQUENCING RUNS
-- =============================================================================

CREATE TABLE qiita.sequencing_run (
    idx                  BIGSERIAL PRIMARY KEY,
    instrument_run_id    VARCHAR(255) NOT NULL,
    instrument_model     TEXT,
    instrument_serial    TEXT,
    run_performed_at     TIMESTAMPTZ,
    extra_metadata       JSONB,
    created_by_idx       BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    retired              BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx       BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at           TIMESTAMPTZ,
    retire_reason        TEXT,

    CONSTRAINT sequencing_run_instrument_run_id_unique UNIQUE (instrument_run_id),
    CONSTRAINT sequencing_run_retirement_consistent CHECK (
        (retired = false
            AND retired_at IS NULL
            AND retired_by_idx IS NULL
            AND retire_reason IS NULL)
        OR
        (retired = true
            AND retired_at IS NOT NULL
            AND retired_by_idx IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.sequencing_run IS
    'One physical run of a sequencing instrument. Multiple flowcells loaded '
    'together on the same instrument, or multiple deliveries of data from a '
    'single run, share one sequencing_run row; two instrument runs delivered '
    'at the same time are two separate rows. instrument_run_id is the '
    'instrument-assigned identifier and the UNIQUE constraint on it is for '
    'collision detection only; idx is the surrogate primary key used as the '
    'foreign-key target elsewhere in the schema. Lab-prep batches and '
    'sequencing libraries are LIMS concerns and are not modeled here.';

COMMENT ON COLUMN qiita.sequencing_run.retired IS
    'When true, this sequencing_run record has been withdrawn. Used for '
    'mistakenly-created run records and runs that turned out worthless '
    '(e.g., operational issues like a forgotten reagent).';

CREATE INDEX sequencing_run_active_idx
    ON qiita.sequencing_run (run_performed_at DESC NULLS LAST)
    WHERE retired = false;

CREATE INDEX sequencing_run_instrument_model_idx
    ON qiita.sequencing_run (instrument_model)
    WHERE instrument_model IS NOT NULL;


-- migrate:down

DROP TABLE IF EXISTS qiita.sequencing_run;
