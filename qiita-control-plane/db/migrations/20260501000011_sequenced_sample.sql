-- migrate:up

-- =============================================================================
-- SEQUENCED SAMPLES
-- =============================================================================

CREATE TABLE qiita.sequenced_sample (
    idx                             BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    biosample_idx                   BIGINT NOT NULL REFERENCES qiita.biosample(idx) ON DELETE RESTRICT,
    owner_idx                       BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    prep_protocol_idx               BIGINT NOT NULL REFERENCES qiita.prep_protocol(idx) ON DELETE RESTRICT,
    metadata_checklist_idx          BIGINT NOT NULL REFERENCES qiita.metadata_checklist(idx) ON DELETE RESTRICT,
    sequencing_run_idx              BIGINT REFERENCES qiita.sequencing_run(idx) ON DELETE RESTRICT,

    ena_experiment_accession        VARCHAR(50),
    ena_run_accession               VARCHAR(50),

    -- Submission tracking. last_submission_at is NULL until the submission
    -- subsystem first attempts a submission, otherwise it holds the time of the
    -- most recent attempt. submission_error is NULL on success and carries the
    -- error message when the most recent attempt failed.
    last_submission_at              TIMESTAMPTZ,
    submission_error                TEXT,
    last_metadata_change_at         TIMESTAMPTZ,

    created_by_idx                  BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger; used as the
    -- ETag for optimistic-concurrency control on PATCH.
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),

    retired                         BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx                  BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at                      TIMESTAMPTZ,
    retire_reason                   TEXT,

    CONSTRAINT sequenced_sample_ena_experiment_accession_unique UNIQUE (ena_experiment_accession),
    CONSTRAINT sequenced_sample_ena_run_accession_unique UNIQUE (ena_run_accession),

    CONSTRAINT sequenced_sample_run_accession_requires_run CHECK (
        ena_run_accession IS NULL OR sequencing_run_idx IS NOT NULL
    ),

    CONSTRAINT sequenced_sample_retirement_consistent CHECK (
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

COMMENT ON TABLE qiita.sequenced_sample IS
    'A specific biosample as prepared under a specific protocol, optionally '
    'sequenced on a specific run; the closest analog to an ENA EXPERIMENT. '
    'Structurally parallel to biosample: has an owner and many-to-many study '
    'relationships through sequenced_sample_to_study. Can be deposited to ENA '
    'as an EXPERIMENT and, once a sequencing run has been assigned, also as '
    'an ENA RUN; both accessions live on this row, shared by a single '
    'submission-tracking pair (last_submission_at, submission_error) that '
    'records the most recent attempt. Technical replicates are supported: '
    '(biosample_idx, prep_protocol_idx) is not UNIQUE, so the same biosample '
    'may be prepared multiple times under the same protocol. Lab-prep batches '
    'are LIMS concerns and are not modeled here.';

COMMENT ON COLUMN qiita.sequenced_sample.ena_experiment_accession IS
    'ENA-assigned experiment accession. NULL until the experiment '
    'submission has succeeded and ENA has returned an accession. Parallels '
    'biosample.biosample_accession in role.';

COMMENT ON COLUMN qiita.sequenced_sample.ena_run_accession IS
    'ENA-assigned run accession. NULL until a sequencing run has been '
    'assigned and its submission has succeeded. The schema enforces that a '
    'run accession can only exist when sequencing_run_idx is also populated.';

COMMENT ON COLUMN qiita.sequenced_sample.owner_idx IS
    'The principal who owns this sequenced sample. Parallel to '
    'biosample.owner_idx but usually the lab here.';

CREATE INDEX sequenced_sample_biosample_idx ON qiita.sequenced_sample (biosample_idx);
CREATE INDEX sequenced_sample_owner_idx ON qiita.sequenced_sample (owner_idx);
CREATE INDEX sequenced_sample_prep_protocol_idx
    ON qiita.sequenced_sample (prep_protocol_idx);
CREATE INDEX sequenced_sample_sequencing_run_idx
    ON qiita.sequenced_sample (sequencing_run_idx)
    WHERE sequencing_run_idx IS NOT NULL;
CREATE INDEX sequenced_sample_active_idx
    ON qiita.sequenced_sample (idx)
    WHERE retired = false;

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


-- =============================================================================
-- SEQUENCED-SAMPLE-TO-STUDY LINKS
-- =============================================================================

CREATE TABLE qiita.sequenced_sample_to_study (
    sequenced_sample_idx  BIGINT NOT NULL REFERENCES qiita.sequenced_sample(idx) ON DELETE RESTRICT,
    study_idx             BIGINT NOT NULL REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    created_by_idx        BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired               BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx        BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at            TIMESTAMPTZ,
    retire_reason         TEXT,

    PRIMARY KEY (sequenced_sample_idx, study_idx),

    CONSTRAINT sequenced_sample_to_study_retirement_consistent CHECK (
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

COMMENT ON TABLE qiita.sequenced_sample_to_study IS
    'Many-to-many link between sequenced samples and studies. One row per '
    '(sequenced_sample, study) pair. Parallel to biosample_to_study. A row can '
    'be created only if the underlying biosample is also linked (non-retired) '
    'to the same study -- enforced by the reject_without_biosample_link '
    'trigger further down.';

COMMENT ON COLUMN qiita.sequenced_sample_to_study.retired IS
    'When true, this study has lost permission to use this sequenced sample. '
    'Distinct from sequenced_sample.retired, which withdraws the sample '
    'everywhere.';

CREATE INDEX sequenced_sample_to_study_study_idx
    ON qiita.sequenced_sample_to_study (study_idx);
CREATE INDEX sequenced_sample_to_study_active_idx
    ON qiita.sequenced_sample_to_study (sequenced_sample_idx, study_idx)
    WHERE retired = false;


-- =============================================================================
-- SEQUENCED SAMPLE METADATA (the EAV)
-- =============================================================================

CREATE TABLE qiita.sequenced_sample_metadata (
    idx                                BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    sequenced_sample_idx               BIGINT NOT NULL REFERENCES qiita.sequenced_sample(idx) ON DELETE RESTRICT,
    sequenced_sample_study_field_idx   BIGINT NOT NULL REFERENCES qiita.sequenced_sample_study_field(idx) ON DELETE RESTRICT,
    -- Maintained by trigger; see comment.
    global_field_idx                   BIGINT REFERENCES qiita.sequenced_sample_global_field(idx) ON DELETE RESTRICT,
    value_text                         TEXT,
    value_numeric                      NUMERIC,
    value_boolean                      BOOLEAN,
    value_date                         DATE,
    value_terminology_term_idx         BIGINT REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT,
    value_missing_reason_idx           BIGINT REFERENCES qiita.missing_value_reason(idx) ON DELETE RESTRICT,
    created_by_idx                     BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Each (sequenced_sample, study field) pair has at most one metadata row. This
    -- is the natural-key constraint that PUT-on-natural-key operations rely on.
    CONSTRAINT sequenced_sample_metadata_unique_per_field
        UNIQUE (sequenced_sample_idx, sequenced_sample_study_field_idx),

    -- Exactly one value column (or a missing-reason) must be populated.
    CONSTRAINT sequenced_sample_metadata_exactly_one_value CHECK (
        (CASE WHEN value_text IS NOT NULL                 THEN 1 ELSE 0 END
       + CASE WHEN value_numeric IS NOT NULL              THEN 1 ELSE 0 END
       + CASE WHEN value_boolean IS NOT NULL              THEN 1 ELSE 0 END
       + CASE WHEN value_date IS NOT NULL                 THEN 1 ELSE 0 END
       + CASE WHEN value_terminology_term_idx IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN value_missing_reason_idx IS NOT NULL   THEN 1 ELSE 0 END) = 1
    )
);

COMMENT ON COLUMN qiita.sequenced_sample_metadata.global_field_idx IS
    'Maintained by trigger from sequenced_sample_study_field.sequenced_sample_global_field_idx. '
    'NULL when the source field is purely study-local, and ALSO NULL when the '
    'sequenced sample''s link to the source field''s owning study has been '
    'retired (set to true on sequenced_sample_to_study.retired): retirement '
    'of the contributing link demotes the row from globally-linked to '
    'study-local and releases the cross-study uniqueness slot so another '
    'study may later claim it. Powers the partial unique index that enforces '
    'one value per (sequenced_sample, global concept) pair across all '
    'studies, so cross-study reads through the global field always return a '
    'single canonical value.';

CREATE INDEX sequenced_sample_metadata_field_idx
    ON qiita.sequenced_sample_metadata (sequenced_sample_study_field_idx);
CREATE INDEX sequenced_sample_metadata_terminology_value_idx
    ON qiita.sequenced_sample_metadata (value_terminology_term_idx)
    WHERE value_terminology_term_idx IS NOT NULL;

-- Cross-study uniqueness for globally-linked values: a given sequenced sample
-- has at most one metadata row per global concept, even if multiple studies
-- have local fields linked to that concept. Parallel to
-- biosample_metadata_one_value_per_global_concept.
CREATE UNIQUE INDEX sequenced_sample_metadata_one_value_per_global_concept
    ON qiita.sequenced_sample_metadata (sequenced_sample_idx, global_field_idx)
    WHERE global_field_idx IS NOT NULL;


-- =============================================================================
-- SEQUENCED SAMPLE FIELD EXCEPTIONS (per-(sequenced_sample, field) visibility overrides)
-- =============================================================================

CREATE TABLE qiita.sequenced_sample_field_exception (
    idx                                BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    sequenced_sample_idx               BIGINT NOT NULL REFERENCES qiita.sequenced_sample(idx) ON DELETE RESTRICT,
    -- Dual-keyed; see table comment.
    sequenced_sample_study_field_idx   BIGINT REFERENCES qiita.sequenced_sample_study_field(idx) ON DELETE RESTRICT,
    global_field_idx                   BIGINT REFERENCES qiita.sequenced_sample_global_field(idx) ON DELETE RESTRICT,
    tier_override                      qiita.tier NOT NULL,
    reason                             TEXT,
    created_by_idx                     BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger below; used as the
    -- ETag for optimistic-concurrency control on upsert PUT.
    updated_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT sequenced_sample_field_exception_exactly_one_key CHECK (
        (sequenced_sample_study_field_idx IS NOT NULL AND global_field_idx IS NULL)
        OR
        (sequenced_sample_study_field_idx IS NULL AND global_field_idx IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.sequenced_sample_field_exception IS
    'Per-(sequenced_sample, field) visibility overrides. Used to restrict '
    'the audience for specific metadata values that need narrower visibility '
    'than their field''s general policy. An exception downgrades visibility '
    'to a specified tier_override. Exceptions on globally-linked metadata '
    'are keyed on global_field_idx so they follow the value across studies; '
    'exceptions on purely study-local metadata are keyed on '
    'sequenced_sample_study_field_idx.';

COMMENT ON COLUMN qiita.sequenced_sample_field_exception.tier_override IS
    'While most tier_override columns in the schema are nullable, '
    'field_exception tier_overrides are not because there would be '
    'no point in registering an exception if you were not overriding '
    'the expected value.';

CREATE UNIQUE INDEX sequenced_sample_field_exception_unique_local
    ON qiita.sequenced_sample_field_exception (sequenced_sample_idx, sequenced_sample_study_field_idx)
    WHERE sequenced_sample_study_field_idx IS NOT NULL;
CREATE UNIQUE INDEX sequenced_sample_field_exception_unique_global
    ON qiita.sequenced_sample_field_exception (sequenced_sample_idx, global_field_idx)
    WHERE global_field_idx IS NOT NULL;

CREATE INDEX sequenced_sample_field_exception_study_field_idx
    ON qiita.sequenced_sample_field_exception (sequenced_sample_study_field_idx)
    WHERE sequenced_sample_study_field_idx IS NOT NULL;
CREATE INDEX sequenced_sample_field_exception_global_field_idx
    ON qiita.sequenced_sample_field_exception (global_field_idx)
    WHERE global_field_idx IS NOT NULL;

CREATE TRIGGER sequenced_sample_field_exception_set_updated_at
    BEFORE UPDATE ON qiita.sequenced_sample_field_exception
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- =============================================================================
-- TRIGGER: maintain sequenced_sample_metadata.global_field_idx
--
-- Denormalization of sequenced_sample_study_field.sequenced_sample_global_field_idx
-- onto sequenced_sample_metadata.global_field_idx. Populated on insert/update
-- of metadata rows, and propagated on updates to the source study_field's
-- global link. Parallel to the biosample-side pair.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.sequenced_sample_metadata_set_global_field_idx()
RETURNS TRIGGER AS $$
BEGIN
    SELECT sequenced_sample_global_field_idx
      INTO NEW.global_field_idx
      FROM qiita.sequenced_sample_study_field
     WHERE idx = NEW.sequenced_sample_study_field_idx;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_metadata_set_global_field_idx_insert
    BEFORE INSERT ON qiita.sequenced_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_metadata_set_global_field_idx();

CREATE TRIGGER sequenced_sample_metadata_set_global_field_idx_update
    BEFORE UPDATE OF sequenced_sample_study_field_idx ON qiita.sequenced_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_metadata_set_global_field_idx();


CREATE OR REPLACE FUNCTION qiita.propagate_global_field_link_to_sequenced_sample_metadata()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.sequenced_sample_global_field_idx IS DISTINCT FROM OLD.sequenced_sample_global_field_idx THEN
        UPDATE qiita.sequenced_sample_metadata
           SET global_field_idx = NEW.sequenced_sample_global_field_idx
         WHERE sequenced_sample_study_field_idx = NEW.idx;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_study_field_propagate_global_link
    AFTER UPDATE OF sequenced_sample_global_field_idx ON qiita.sequenced_sample_study_field
    FOR EACH ROW EXECUTE FUNCTION qiita.propagate_global_field_link_to_sequenced_sample_metadata();


-- =============================================================================
-- TRIGGER: demote globally-linked metadata on sequenced-sample-to-study retirement
--
-- When a sequenced_sample_to_study link is retired (false -> true), any
-- sequenced_sample_metadata row contributed through the retiring link loses
-- its global linkage (global_field_idx set to NULL), releasing the cross-study
-- uniqueness slot. On un-retirement, per-row best-effort restoration attempts
-- to re-populate global_field_idx; rows where restoration would collide with
-- another study's claim silently remain study-local. Parallel to the
-- biosample-side trigger.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.sequenced_sample_to_study_retirement_demote_globals()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.retired IS DISTINCT FROM OLD.retired THEN
        IF NEW.retired = true THEN
            UPDATE qiita.sequenced_sample_metadata ssm
               SET global_field_idx = NULL
              FROM qiita.sequenced_sample_study_field sssf
             WHERE ssm.sequenced_sample_study_field_idx = sssf.idx
               AND ssm.sequenced_sample_idx = NEW.sequenced_sample_idx
               AND sssf.study_idx = NEW.study_idx
               AND ssm.global_field_idx IS NOT NULL;
        ELSE
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN
                    SELECT ssm.idx AS metadata_idx,
                           sssf.sequenced_sample_global_field_idx AS target_global
                      FROM qiita.sequenced_sample_metadata ssm
                      JOIN qiita.sequenced_sample_study_field sssf
                        ON ssm.sequenced_sample_study_field_idx = sssf.idx
                     WHERE ssm.sequenced_sample_idx = NEW.sequenced_sample_idx
                       AND sssf.study_idx = NEW.study_idx
                       AND ssm.global_field_idx IS NULL
                       AND sssf.sequenced_sample_global_field_idx IS NOT NULL
                LOOP
                    BEGIN
                        UPDATE qiita.sequenced_sample_metadata
                           SET global_field_idx = r.target_global
                         WHERE idx = r.metadata_idx;
                    EXCEPTION WHEN unique_violation THEN
                        -- Slot has been claimed by another study; leave the
                        -- row study-local. Not an error.
                        NULL;
                    END;
                END LOOP;
            END;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_to_study_retirement_demote_globals
    AFTER UPDATE OF retired ON qiita.sequenced_sample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_to_study_retirement_demote_globals();


-- =============================================================================
-- TRIGGER: enforce non-retired-link invariant on sequenced_sample_metadata inserts
--
-- A sequenced_sample_metadata row cannot exist for a (sequenced_sample, study)
-- pair whose sequenced_sample_to_study link is retired. Row-repointing is
-- separately forbidden by the immutability trigger further down, so only the
-- INSERT path needs guarding here.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.sequenced_sample_metadata_reject_if_link_retired()
RETURNS TRIGGER AS $$
DECLARE
    link_retired BOOLEAN;
    field_study_idx BIGINT;
BEGIN
    SELECT sssf.study_idx
      INTO field_study_idx
      FROM qiita.sequenced_sample_study_field sssf
     WHERE sssf.idx = NEW.sequenced_sample_study_field_idx;

    SELECT ssts.retired
      INTO link_retired
      FROM qiita.sequenced_sample_to_study ssts
     WHERE ssts.sequenced_sample_idx = NEW.sequenced_sample_idx
       AND ssts.study_idx = field_study_idx;

    IF link_retired IS NULL THEN
        RAISE EXCEPTION 'sequenced_sample_metadata refers to (sequenced_sample=%, study=%) but no sequenced_sample_to_study row exists',
            NEW.sequenced_sample_idx, field_study_idx;
    END IF;

    IF link_retired = true THEN
        RAISE EXCEPTION 'sequenced_sample_metadata cannot be written: sequenced_sample_to_study(%, %) is retired',
            NEW.sequenced_sample_idx, field_study_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_metadata_reject_if_link_retired_insert
    BEFORE INSERT ON qiita.sequenced_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_metadata_reject_if_link_retired();


-- =============================================================================
-- TRIGGER: enforce immutability of sequenced_sample_metadata key columns
--
-- A sequenced_sample_metadata row's (sequenced_sample_idx,
-- sequenced_sample_study_field_idx) pair identifies which sequenced sample and
-- which study field the value is FOR. If either was wrong, the correct flow is
-- to DELETE the row and INSERT a new one for the right pair. Parallel to the
-- biosample-side immutability trigger.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.sequenced_sample_metadata_reject_key_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.sequenced_sample_idx IS DISTINCT FROM OLD.sequenced_sample_idx THEN
        RAISE EXCEPTION 'sequenced_sample_metadata.sequenced_sample_idx is immutable; delete and re-insert instead';
    END IF;
    IF NEW.sequenced_sample_study_field_idx IS DISTINCT FROM OLD.sequenced_sample_study_field_idx THEN
        RAISE EXCEPTION 'sequenced_sample_metadata.sequenced_sample_study_field_idx is immutable; delete and re-insert instead';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_metadata_reject_key_update
    BEFORE UPDATE OF sequenced_sample_idx, sequenced_sample_study_field_idx ON qiita.sequenced_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_metadata_reject_key_update();


-- =============================================================================
-- TRIGGER: bump sequenced_sample.last_metadata_change_at on
-- sequenced_sample_metadata writes
--
-- A sequenced sample's last_metadata_change_at is set to now() whenever a
-- sequenced_sample_metadata row for it is inserted or updated.
-- sequenced_sample_idx is immutable on UPDATE (enforced above), so only one
-- sequenced sample is ever touched per firing.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.sequenced_sample_metadata_touch_sequenced_sample()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE qiita.sequenced_sample
       SET last_metadata_change_at = now()
     WHERE idx = NEW.sequenced_sample_idx;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_metadata_touch_sequenced_sample
    AFTER INSERT OR UPDATE ON qiita.sequenced_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_metadata_touch_sequenced_sample();


-- =============================================================================
-- TRIGGER: sequenced_sample_to_study row requires a non-retired
-- biosample_to_study link for the same (biosample, study) pair
--
-- Without this guard, a study could be linked to the sequenced sample while
-- failing the biosample-level access check in the two-gate visibility rule,
-- producing an inert link. Enforcing at the schema layer makes inert links
-- impossible.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.sequenced_sample_to_study_reject_without_biosample_link()
RETURNS TRIGGER AS $$
DECLARE
    biosample_link_exists_active BOOLEAN;
    biosample_idx_for_sequenced_sample BIGINT;
BEGIN
    SELECT biosample_idx
      INTO biosample_idx_for_sequenced_sample
      FROM qiita.sequenced_sample
     WHERE idx = NEW.sequenced_sample_idx;

    SELECT EXISTS (
        SELECT 1 FROM qiita.biosample_to_study
         WHERE biosample_idx = biosample_idx_for_sequenced_sample
           AND study_idx = NEW.study_idx
           AND retired = false
    ) INTO biosample_link_exists_active;

    IF NOT biosample_link_exists_active THEN
        RAISE EXCEPTION
            'sequenced_sample_to_study(sequenced_sample=%, study=%) requires a non-retired biosample_to_study(biosample=%, study=%) link',
            NEW.sequenced_sample_idx, NEW.study_idx, biosample_idx_for_sequenced_sample, NEW.study_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_to_study_reject_without_biosample_link_insert
    BEFORE INSERT ON qiita.sequenced_sample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_to_study_reject_without_biosample_link();

-- Belt-and-suspenders: the columns below form the PK, so updating either on
-- an existing row is unusual. This trigger enforces the invariant anyway for
-- out-of-band paths (migrations, admin ALTERs).
CREATE TRIGGER sequenced_sample_to_study_reject_without_biosample_link_update
    BEFORE UPDATE OF sequenced_sample_idx, study_idx ON qiita.sequenced_sample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_to_study_reject_without_biosample_link();


-- migrate:down

-- Drop the trigger that lives on a previous migration's table
-- (sequenced_sample_study_field). Dropping the tables below takes their own
-- triggers with them, but this one would otherwise orphan against an empty
-- target.
DROP TRIGGER IF EXISTS sequenced_sample_study_field_propagate_global_link ON qiita.sequenced_sample_study_field;

DROP TABLE IF EXISTS qiita.sequenced_sample_field_exception;
DROP TABLE IF EXISTS qiita.sequenced_sample_metadata;
DROP TABLE IF EXISTS qiita.sequenced_sample_to_study;
DROP TABLE IF EXISTS qiita.sequenced_sample;

DROP FUNCTION IF EXISTS qiita.sequenced_sample_to_study_reject_without_biosample_link();
DROP FUNCTION IF EXISTS qiita.sequenced_sample_metadata_reject_key_update();
DROP FUNCTION IF EXISTS qiita.sequenced_sample_metadata_reject_if_link_retired();
DROP FUNCTION IF EXISTS qiita.sequenced_sample_to_study_retirement_demote_globals();
DROP FUNCTION IF EXISTS qiita.propagate_global_field_link_to_sequenced_sample_metadata();
DROP FUNCTION IF EXISTS qiita.sequenced_sample_metadata_set_global_field_idx();
DROP FUNCTION IF EXISTS qiita.sequenced_sample_clear_submission_error_on_new_attempt();
DROP FUNCTION IF EXISTS qiita.sequenced_sample_metadata_touch_sequenced_sample();
