-- migrate:up

-- =============================================================================
-- PROCESSING KIND ENUM
-- =============================================================================
--
-- Closed set of disjoint downstream-measurement specializations a prep_sample
-- may flow into. Today only 'sequenced'; future values (e.g., 'mass_specd')
-- add new subtype tables that FK back to prep_sample via the composite
-- (idx, processing_kind) key. The composite-FK + GENERATED ALWAYS AS pattern
-- enforces mutual exclusion declaratively: each subtype table pins its own
-- processing_kind column to a single value, so a prep_sample row can satisfy
-- the FK from at most one subtype.
CREATE TYPE qiita.processing_kind AS ENUM ('sequenced');


-- =============================================================================
-- PREP SAMPLES
-- =============================================================================

CREATE TABLE qiita.prep_sample (
    idx                             BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    biosample_idx                   BIGINT NOT NULL REFERENCES qiita.biosample(idx) ON DELETE RESTRICT,
    owner_idx                       BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    prep_protocol_idx               BIGINT NOT NULL REFERENCES qiita.prep_protocol(idx) ON DELETE RESTRICT,
    metadata_checklist_idx          BIGINT REFERENCES qiita.metadata_checklist(idx) ON DELETE RESTRICT,

    -- The downstream specialization this prep is/will be routed into.
    -- Required at insert time; once a subtype row exists for this prep,
    -- the subtype's composite FK pins this column to the matching value.
    processing_kind                 qiita.processing_kind NOT NULL,

    -- Bumped by the prep_sample_metadata_touch_prep_sample trigger whenever
    -- a prep_sample_metadata row for this prep is inserted or updated. The
    -- submission subsystem reads this against the matching subtype row's
    -- last_submission_at to decide whether re-submission is needed.
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

    -- Target of the composite FK from every subtype table. Each subtype
    -- carries a processing_kind column pinned by GENERATED ALWAYS AS to a
    -- single enum value and references (idx, processing_kind) here. The
    -- target tuple's uniqueness combined with the subtype's pinned kind
    -- makes mutual exclusion a pure declarative-DDL guarantee.
    CONSTRAINT prep_sample_idx_processing_kind_unique UNIQUE (idx, processing_kind),

    CONSTRAINT prep_sample_retirement_consistent CHECK (
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

COMMENT ON TABLE qiita.prep_sample IS
    'A specific biosample as prepared under a specific protocol; the '
    'supertype of the downstream-measurement hierarchy. Structurally '
    'parallel to biosample: has an owner and many-to-many study '
    'relationships through prep_sample_to_study. Downstream-measurement '
    'data (sequencing run + ENA submission for the sequenced case; future '
    'mass-spec metadata; etc.) lives on disjoint subtype tables that FK '
    'back via the composite (idx, processing_kind) key. Technical '
    'replicates are supported: (biosample_idx, prep_protocol_idx) is not '
    'UNIQUE, so the same biosample may be prepared multiple times under '
    'the same protocol. Lab-prep batches are LIMS concerns and are not '
    'modeled here.';

COMMENT ON COLUMN qiita.prep_sample.owner_idx IS
    'The principal who owns this prep sample. Parallel to '
    'biosample.owner_idx but usually the lab here.';

COMMENT ON COLUMN qiita.prep_sample.processing_kind IS
    'The kind of downstream measurement this prep is routed into. NOT NULL: '
    'every prep must commit to a processing path at insert time. Once a '
    'subtype row (e.g., sequenced_sample) exists for this prep, the '
    'subtype''s composite FK pins this column to the matching value, and '
    'any change here that would orphan the subtype row will be rejected '
    'by the FK.';

CREATE INDEX prep_sample_biosample_idx ON qiita.prep_sample (biosample_idx);
CREATE INDEX prep_sample_owner_idx ON qiita.prep_sample (owner_idx);
CREATE INDEX prep_sample_prep_protocol_idx
    ON qiita.prep_sample (prep_protocol_idx);
CREATE INDEX prep_sample_active_idx
    ON qiita.prep_sample (idx)
    WHERE retired = false;

CREATE TRIGGER prep_sample_set_updated_at
    BEFORE UPDATE ON qiita.prep_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- =============================================================================
-- PREP-SAMPLE-TO-STUDY LINKS
-- =============================================================================

CREATE TABLE qiita.prep_sample_to_study (
    prep_sample_idx  BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,
    study_idx             BIGINT NOT NULL REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    created_by_idx        BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired               BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx        BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at            TIMESTAMPTZ,
    retire_reason         TEXT,

    PRIMARY KEY (prep_sample_idx, study_idx),

    CONSTRAINT prep_sample_to_study_retirement_consistent CHECK (
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

COMMENT ON TABLE qiita.prep_sample_to_study IS
    'Many-to-many link between prep samples and studies. One row per '
    '(prep_sample, study) pair. Parallel to biosample_to_study. A row can '
    'be created only if the underlying biosample is also linked (non-retired) '
    'to the same study -- enforced by the reject_without_biosample_link '
    'trigger further down.';

COMMENT ON COLUMN qiita.prep_sample_to_study.retired IS
    'When true, this study has lost permission to use this prep sample. '
    'Distinct from prep_sample.retired, which withdraws the sample '
    'everywhere.';

CREATE INDEX prep_sample_to_study_study_idx
    ON qiita.prep_sample_to_study (study_idx);
CREATE INDEX prep_sample_to_study_active_idx
    ON qiita.prep_sample_to_study (prep_sample_idx, study_idx)
    WHERE retired = false;


-- =============================================================================
-- PREP SAMPLE METADATA (the EAV)
-- =============================================================================

CREATE TABLE qiita.prep_sample_metadata (
    idx                                BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    prep_sample_idx               BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,
    prep_sample_study_field_idx   BIGINT NOT NULL REFERENCES qiita.prep_sample_study_field(idx) ON DELETE RESTRICT,
    -- Maintained by trigger; see comment.
    global_field_idx                   BIGINT REFERENCES qiita.prep_sample_global_field(idx) ON DELETE RESTRICT,
    value_text                         TEXT,
    value_numeric                      NUMERIC,
    value_boolean                      BOOLEAN,
    value_date                         DATE,
    value_terminology_term_idx         BIGINT REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT,
    value_missing_reason_idx           BIGINT REFERENCES qiita.missing_value_reason(idx) ON DELETE RESTRICT,
    created_by_idx                     BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Each (prep_sample, study field) pair has at most one metadata row. This
    -- is the natural-key constraint that PUT-on-natural-key operations rely on.
    CONSTRAINT prep_sample_metadata_unique_per_field
        UNIQUE (prep_sample_idx, prep_sample_study_field_idx),

    -- Exactly one value column (or a missing-reason) must be populated.
    CONSTRAINT prep_sample_metadata_exactly_one_value CHECK (
        (CASE WHEN value_text IS NOT NULL                 THEN 1 ELSE 0 END
       + CASE WHEN value_numeric IS NOT NULL              THEN 1 ELSE 0 END
       + CASE WHEN value_boolean IS NOT NULL              THEN 1 ELSE 0 END
       + CASE WHEN value_date IS NOT NULL                 THEN 1 ELSE 0 END
       + CASE WHEN value_terminology_term_idx IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN value_missing_reason_idx IS NOT NULL   THEN 1 ELSE 0 END) = 1
    )
);

COMMENT ON COLUMN qiita.prep_sample_metadata.global_field_idx IS
    'Maintained by trigger from prep_sample_study_field.prep_sample_global_field_idx. '
    'NULL when the source field is purely study-local, and ALSO NULL when the '
    'prep sample''s link to the source field''s owning study has been '
    'retired (set to true on prep_sample_to_study.retired): retirement '
    'of the contributing link demotes the row from globally-linked to '
    'study-local and releases the cross-study uniqueness slot so another '
    'study may later claim it. Powers the partial unique index that enforces '
    'one value per (prep_sample, global concept) pair across all '
    'studies, so cross-study reads through the global field always return a '
    'single canonical value.';

CREATE INDEX prep_sample_metadata_field_idx
    ON qiita.prep_sample_metadata (prep_sample_study_field_idx);
CREATE INDEX prep_sample_metadata_terminology_value_idx
    ON qiita.prep_sample_metadata (value_terminology_term_idx)
    WHERE value_terminology_term_idx IS NOT NULL;

-- Cross-study uniqueness for globally-linked values: a given prep sample
-- has at most one metadata row per global concept, even if multiple studies
-- have local fields linked to that concept. Parallel to
-- biosample_metadata_one_value_per_global_concept.
CREATE UNIQUE INDEX prep_sample_metadata_one_value_per_global_concept
    ON qiita.prep_sample_metadata (prep_sample_idx, global_field_idx)
    WHERE global_field_idx IS NOT NULL;


-- =============================================================================
-- PREP SAMPLE FIELD EXCEPTIONS (per-(prep_sample, field) visibility overrides)
-- =============================================================================

CREATE TABLE qiita.prep_sample_field_exception (
    idx                                BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    prep_sample_idx               BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,
    -- Dual-keyed; see table comment.
    prep_sample_study_field_idx   BIGINT REFERENCES qiita.prep_sample_study_field(idx) ON DELETE RESTRICT,
    global_field_idx                   BIGINT REFERENCES qiita.prep_sample_global_field(idx) ON DELETE RESTRICT,
    tier_override                      qiita.tier NOT NULL,
    reason                             TEXT,
    created_by_idx                     BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger below; used as the
    -- ETag for optimistic-concurrency control on upsert PUT.
    updated_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT prep_sample_field_exception_exactly_one_key CHECK (
        (prep_sample_study_field_idx IS NOT NULL AND global_field_idx IS NULL)
        OR
        (prep_sample_study_field_idx IS NULL AND global_field_idx IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.prep_sample_field_exception IS
    'Per-(prep_sample, field) visibility overrides. Used to restrict '
    'the audience for specific metadata values that need narrower visibility '
    'than their field''s general policy. An exception downgrades visibility '
    'to a specified tier_override. Exceptions on globally-linked metadata '
    'are keyed on global_field_idx so they follow the value across studies; '
    'exceptions on purely study-local metadata are keyed on '
    'prep_sample_study_field_idx.';

COMMENT ON COLUMN qiita.prep_sample_field_exception.tier_override IS
    'While most tier_override columns in the schema are nullable, '
    'field_exception tier_overrides are not because there would be '
    'no point in registering an exception if you were not overriding '
    'the expected value.';

CREATE UNIQUE INDEX prep_sample_field_exception_unique_local
    ON qiita.prep_sample_field_exception (prep_sample_idx, prep_sample_study_field_idx)
    WHERE prep_sample_study_field_idx IS NOT NULL;
CREATE UNIQUE INDEX prep_sample_field_exception_unique_global
    ON qiita.prep_sample_field_exception (prep_sample_idx, global_field_idx)
    WHERE global_field_idx IS NOT NULL;

CREATE INDEX prep_sample_field_exception_study_field_idx
    ON qiita.prep_sample_field_exception (prep_sample_study_field_idx)
    WHERE prep_sample_study_field_idx IS NOT NULL;
CREATE INDEX prep_sample_field_exception_global_field_idx
    ON qiita.prep_sample_field_exception (global_field_idx)
    WHERE global_field_idx IS NOT NULL;

CREATE TRIGGER prep_sample_field_exception_set_updated_at
    BEFORE UPDATE ON qiita.prep_sample_field_exception
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- =============================================================================
-- TRIGGER: prep_sample_metadata_apply_field_contract
--
-- Parallel to biosample_metadata_apply_field_contract; see that trigger's
-- comment for the full responsibilities. One SELECT against the source
-- prep_sample_study_field row drives both:
--
--   1. CHECK that the populated value_* column matches the field's data_type
--      (resolved via COALESCE across study- and global-field rows).
--   2. SET NEW.global_field_idx from the source study_field's
--      prep_sample_global_field_idx.
--
-- Missing-reason rows are exempt from the data_type match.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_apply_field_contract()
RETURNS TRIGGER AS $$
DECLARE
    expected_data_type qiita.field_data_type;
    populated_ok      BOOLEAN;
BEGIN
    -- Single SELECT covers both responsibilities.
    SELECT ssf.prep_sample_global_field_idx,
           COALESCE(ssf.data_type, sgf.data_type)
      INTO NEW.global_field_idx, expected_data_type
      FROM qiita.prep_sample_study_field ssf
      LEFT JOIN qiita.prep_sample_global_field sgf
        ON sgf.idx = ssf.prep_sample_global_field_idx
     WHERE ssf.idx = NEW.prep_sample_study_field_idx;

    -- Missing-reason rows are exempt from the value/data_type match.
    IF NEW.value_missing_reason_idx IS NOT NULL THEN
        RETURN NEW;
    END IF;

    -- Verify the populated value column matches the field's data_type.
    -- ELSE NULL + IS NOT TRUE so an unrecognized or NULL data_type fails
    -- loudly rather than passing through (which a bare CASE + IF NOT
    -- populated_ok would do, since NOT NULL is NULL is not TRUE).
    populated_ok := CASE expected_data_type
        WHEN 'text'        THEN NEW.value_text IS NOT NULL
        WHEN 'numeric'     THEN NEW.value_numeric IS NOT NULL
        WHEN 'boolean'     THEN NEW.value_boolean IS NOT NULL
        WHEN 'date'        THEN NEW.value_date IS NOT NULL
        WHEN 'terminology' THEN NEW.value_terminology_term_idx IS NOT NULL
        ELSE NULL
    END;
    IF populated_ok IS NOT TRUE THEN
        RAISE EXCEPTION
            'prep_sample_metadata value column does not match field data_type % for prep_sample_study_field_idx %',
            expected_data_type, NEW.prep_sample_study_field_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_metadata_apply_field_contract_insert
    BEFORE INSERT ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_apply_field_contract();

-- The UPDATE trigger fires when any value_* column or the source field
-- changes — both are inputs to the data_type check.
CREATE TRIGGER prep_sample_metadata_apply_field_contract_update
    BEFORE UPDATE OF prep_sample_study_field_idx, value_text, value_numeric,
                     value_boolean, value_date, value_terminology_term_idx,
                     value_missing_reason_idx
        ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_apply_field_contract();


CREATE OR REPLACE FUNCTION qiita.propagate_global_field_link_to_prep_sample_metadata()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.prep_sample_global_field_idx IS DISTINCT FROM OLD.prep_sample_global_field_idx THEN
        UPDATE qiita.prep_sample_metadata
           SET global_field_idx = NEW.prep_sample_global_field_idx
         WHERE prep_sample_study_field_idx = NEW.idx;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_study_field_propagate_global_link
    AFTER UPDATE OF prep_sample_global_field_idx ON qiita.prep_sample_study_field
    FOR EACH ROW EXECUTE FUNCTION qiita.propagate_global_field_link_to_prep_sample_metadata();


-- =============================================================================
-- TRIGGER: demote globally-linked metadata on prep-sample-to-study retirement
--
-- When a prep_sample_to_study link is retired (false -> true), any
-- prep_sample_metadata row contributed through the retiring link loses
-- its global linkage (global_field_idx set to NULL), releasing the cross-study
-- uniqueness slot. On un-retirement, per-row best-effort restoration attempts
-- to re-populate global_field_idx; rows where restoration would collide with
-- another study's claim silently remain study-local. Parallel to the
-- biosample-side trigger.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_to_study_retirement_demote_globals()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.retired IS DISTINCT FROM OLD.retired THEN
        IF NEW.retired = true THEN
            UPDATE qiita.prep_sample_metadata ssm
               SET global_field_idx = NULL
              FROM qiita.prep_sample_study_field sssf
             WHERE ssm.prep_sample_study_field_idx = sssf.idx
               AND ssm.prep_sample_idx = NEW.prep_sample_idx
               AND sssf.study_idx = NEW.study_idx
               AND ssm.global_field_idx IS NOT NULL;
        ELSE
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN
                    SELECT ssm.idx AS metadata_idx,
                           sssf.prep_sample_global_field_idx AS target_global
                      FROM qiita.prep_sample_metadata ssm
                      JOIN qiita.prep_sample_study_field sssf
                        ON ssm.prep_sample_study_field_idx = sssf.idx
                     WHERE ssm.prep_sample_idx = NEW.prep_sample_idx
                       AND sssf.study_idx = NEW.study_idx
                       AND ssm.global_field_idx IS NULL
                       AND sssf.prep_sample_global_field_idx IS NOT NULL
                LOOP
                    BEGIN
                        UPDATE qiita.prep_sample_metadata
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

CREATE TRIGGER prep_sample_to_study_retirement_demote_globals
    AFTER UPDATE OF retired ON qiita.prep_sample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_to_study_retirement_demote_globals();


-- =============================================================================
-- TRIGGER: enforce non-retired-link invariant on prep_sample_metadata inserts
--
-- A prep_sample_metadata row cannot exist for a (prep_sample, study)
-- pair whose prep_sample_to_study link is retired. Row-repointing is
-- separately forbidden by the immutability trigger further down, so only the
-- INSERT path needs guarding here.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_reject_if_link_retired()
RETURNS TRIGGER AS $$
DECLARE
    link_retired BOOLEAN;
    field_study_idx BIGINT;
BEGIN
    SELECT sssf.study_idx
      INTO field_study_idx
      FROM qiita.prep_sample_study_field sssf
     WHERE sssf.idx = NEW.prep_sample_study_field_idx;

    SELECT ssts.retired
      INTO link_retired
      FROM qiita.prep_sample_to_study ssts
     WHERE ssts.prep_sample_idx = NEW.prep_sample_idx
       AND ssts.study_idx = field_study_idx;

    IF link_retired IS NULL THEN
        RAISE EXCEPTION 'prep_sample_metadata refers to (prep_sample=%, study=%) but no prep_sample_to_study row exists',
            NEW.prep_sample_idx, field_study_idx;
    END IF;

    IF link_retired = true THEN
        RAISE EXCEPTION 'prep_sample_metadata cannot be written: prep_sample_to_study(%, %) is retired',
            NEW.prep_sample_idx, field_study_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_metadata_reject_if_link_retired_insert
    BEFORE INSERT ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_reject_if_link_retired();


-- =============================================================================
-- TRIGGER: enforce immutability of prep_sample_metadata key columns
--
-- A prep_sample_metadata row's (prep_sample_idx,
-- prep_sample_study_field_idx) pair identifies which prep sample and
-- which study field the value is FOR. If either was wrong, the correct flow is
-- to DELETE the row and INSERT a new one for the right pair. Parallel to the
-- biosample-side immutability trigger.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_reject_key_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.prep_sample_idx IS DISTINCT FROM OLD.prep_sample_idx THEN
        RAISE EXCEPTION 'prep_sample_metadata.prep_sample_idx is immutable; delete and re-insert instead';
    END IF;
    IF NEW.prep_sample_study_field_idx IS DISTINCT FROM OLD.prep_sample_study_field_idx THEN
        RAISE EXCEPTION 'prep_sample_metadata.prep_sample_study_field_idx is immutable; delete and re-insert instead';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_metadata_reject_key_update
    BEFORE UPDATE OF prep_sample_idx, prep_sample_study_field_idx ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_reject_key_update();


-- =============================================================================
-- TRIGGER: bump prep_sample.last_metadata_change_at on
-- prep_sample_metadata writes
--
-- A prep sample's last_metadata_change_at is set to now() whenever a
-- prep_sample_metadata row for it is inserted or updated.
-- prep_sample_idx is immutable on UPDATE (enforced above), so only one
-- prep sample is ever touched per firing.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_touch_prep_sample()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE qiita.prep_sample
       SET last_metadata_change_at = now()
     WHERE idx = NEW.prep_sample_idx;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_metadata_touch_prep_sample
    AFTER INSERT OR UPDATE ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_touch_prep_sample();


-- =============================================================================
-- TRIGGER: prep_sample_to_study row requires a non-retired
-- biosample_to_study link for the same (biosample, study) pair
--
-- Without this guard, a study could be linked to the prep sample while
-- failing the biosample-level access check in the two-gate visibility rule,
-- producing an inert link. Enforcing at the schema layer makes inert links
-- impossible.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_to_study_reject_without_biosample_link()
RETURNS TRIGGER AS $$
DECLARE
    biosample_link_exists_active BOOLEAN;
    biosample_idx_for_prep_sample BIGINT;
BEGIN
    SELECT biosample_idx
      INTO biosample_idx_for_prep_sample
      FROM qiita.prep_sample
     WHERE idx = NEW.prep_sample_idx;

    SELECT EXISTS (
        SELECT 1 FROM qiita.biosample_to_study
         WHERE biosample_idx = biosample_idx_for_prep_sample
           AND study_idx = NEW.study_idx
           AND retired = false
    ) INTO biosample_link_exists_active;

    IF NOT biosample_link_exists_active THEN
        RAISE EXCEPTION
            'prep_sample_to_study(prep_sample=%, study=%) requires a non-retired biosample_to_study(biosample=%, study=%) link',
            NEW.prep_sample_idx, NEW.study_idx, biosample_idx_for_prep_sample, NEW.study_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_to_study_reject_without_biosample_link_insert
    BEFORE INSERT ON qiita.prep_sample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_to_study_reject_without_biosample_link();

-- Belt-and-suspenders: the columns below form the PK, so updating either on
-- an existing row is unusual. This trigger enforces the invariant anyway for
-- out-of-band paths (migrations, admin ALTERs).
CREATE TRIGGER prep_sample_to_study_reject_without_biosample_link_update
    BEFORE UPDATE OF prep_sample_idx, study_idx ON qiita.prep_sample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_to_study_reject_without_biosample_link();


-- migrate:down

-- Drop the trigger that lives on a previous migration's table
-- (prep_sample_study_field). Dropping the tables below takes their own
-- triggers with them, but this one would otherwise orphan against an empty
-- target.
DROP TRIGGER IF EXISTS prep_sample_study_field_propagate_global_link ON qiita.prep_sample_study_field;

DROP TABLE IF EXISTS qiita.prep_sample_field_exception;
DROP TABLE IF EXISTS qiita.prep_sample_metadata;
DROP TABLE IF EXISTS qiita.prep_sample_to_study;
DROP TABLE IF EXISTS qiita.prep_sample;

DROP FUNCTION IF EXISTS qiita.prep_sample_to_study_reject_without_biosample_link();
DROP FUNCTION IF EXISTS qiita.prep_sample_metadata_reject_key_update();
DROP FUNCTION IF EXISTS qiita.prep_sample_metadata_reject_if_link_retired();
DROP FUNCTION IF EXISTS qiita.prep_sample_to_study_retirement_demote_globals();
DROP FUNCTION IF EXISTS qiita.propagate_global_field_link_to_prep_sample_metadata();
DROP FUNCTION IF EXISTS qiita.prep_sample_metadata_apply_field_contract();
DROP FUNCTION IF EXISTS qiita.prep_sample_metadata_touch_prep_sample();

DROP TYPE IF EXISTS qiita.processing_kind;
