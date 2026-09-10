-- migrate:up

-- =============================================================================
-- STUDY-LOCAL UNIQUENESS (unique_in_study)
-- =============================================================================
--
-- Opt-in policy on a purely-local study field: the values written through it
-- must be distinct within the study, and none of them may be a missing-value
-- marker. The two guarantees are deliberately carried by one flag -- a field
-- whose job is to tell a study's samples apart cannot have samples that
-- decline to be told apart.
--
-- Enforced against the study field's own idx rather than a study_idx column:
-- a purely-local field belongs to exactly one study, so uniqueness per field
-- IS uniqueness per study. That equivalence is why the flag is refused on a
-- globally-linked row below -- there, one metadata row is shared across every
-- study linked to the global field, and the row's study field is whichever
-- study wrote first, so grouping by it would not describe any single study.
--
-- Applied to the biosample and prep_sample stacks alike.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- biosample_study_field
-- -----------------------------------------------------------------------------

ALTER TABLE qiita.biosample_study_field
    ADD COLUMN unique_in_study BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN qiita.biosample_study_field.unique_in_study IS
    'When true, values written through this field must be distinct within the '
    'study and none may be a missing-value marker. Settable only on a purely-'
    'local row whose data_type is text, numeric, or date; boolean and '
    'terminology are refused because a closed value set caps the study at as '
    'many samples as it has values. Says nothing about identity outside this '
    'study: two studies may each hold the same value through their own fields.';

-- Recreated rather than altered: PostgreSQL has no ALTER CONSTRAINT for a
-- CHECK. Identical to the original except for the unique_in_study term on the
-- globally-linked arm.
ALTER TABLE qiita.biosample_study_field
    DROP CONSTRAINT biosample_study_field_inheritance_consistent;

ALTER TABLE qiita.biosample_study_field
    ADD CONSTRAINT biosample_study_field_inheritance_consistent
        CHECK (
            (biosample_global_field_idx IS NULL
                AND data_type IS NOT NULL
                AND required IS NOT NULL
                AND (data_type = 'terminology') = (terminology_idx IS NOT NULL))
            OR
            (biosample_global_field_idx IS NOT NULL
                AND data_type IS NULL
                AND terminology_idx IS NULL
                AND tier_override IS NULL
                AND required IS NULL
                AND unique_in_study = false)
        );

-- Separately named from the inheritance CHECK so a rejection states which
-- rule was broken. Holds trivially on a globally-linked row, where the arm
-- above already pins the flag false.
ALTER TABLE qiita.biosample_study_field
    ADD CONSTRAINT biosample_study_field_unique_in_study_data_type_eligible
        CHECK (NOT unique_in_study OR data_type IN ('text', 'numeric', 'date'));


-- -----------------------------------------------------------------------------
-- prep_sample_study_field
-- -----------------------------------------------------------------------------

ALTER TABLE qiita.prep_sample_study_field
    ADD COLUMN unique_in_study BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN qiita.prep_sample_study_field.unique_in_study IS
    'When true, values written through this field must be distinct within the '
    'study and none may be a missing-value marker. Settable only on a purely-'
    'local row whose data_type is text, numeric, or date; boolean and '
    'terminology are refused because a closed value set caps the study at as '
    'many samples as it has values. Says nothing about identity outside this '
    'study: two studies may each hold the same value through their own fields.';

ALTER TABLE qiita.prep_sample_study_field
    DROP CONSTRAINT prep_sample_study_field_inheritance_consistent;

ALTER TABLE qiita.prep_sample_study_field
    ADD CONSTRAINT prep_sample_study_field_inheritance_consistent
        CHECK (
            (prep_sample_global_field_idx IS NULL
                AND data_type IS NOT NULL
                AND required IS NOT NULL
                AND (data_type = 'terminology') = (terminology_idx IS NOT NULL))
            OR
            (prep_sample_global_field_idx IS NOT NULL
                AND data_type IS NULL
                AND terminology_idx IS NULL
                AND tier_override IS NULL
                AND required IS NULL
                AND unique_in_study = false)
        );

ALTER TABLE qiita.prep_sample_study_field
    ADD CONSTRAINT prep_sample_study_field_unique_in_study_data_type_eligible
        CHECK (NOT unique_in_study OR data_type IN ('text', 'numeric', 'date'));


-- -----------------------------------------------------------------------------
-- biosample_metadata
-- -----------------------------------------------------------------------------

ALTER TABLE qiita.biosample_metadata
    ADD COLUMN unique_in_study BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN qiita.biosample_metadata.unique_in_study IS
    'Maintained by trigger from biosample_study_field.unique_in_study. Powers '
    'the partial unique indexes below and the no-missing-value CHECK, neither '
    'of which can reach into the study field table from an index predicate or '
    'a row constraint. Never written by a caller.';

ALTER TABLE qiita.biosample_metadata
    ADD CONSTRAINT biosample_metadata_unique_in_study_no_missing_value
        CHECK (NOT unique_in_study OR value_missing_reason_idx IS NULL);


-- -----------------------------------------------------------------------------
-- prep_sample_metadata
-- -----------------------------------------------------------------------------

ALTER TABLE qiita.prep_sample_metadata
    ADD COLUMN unique_in_study BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN qiita.prep_sample_metadata.unique_in_study IS
    'Maintained by trigger from prep_sample_study_field.unique_in_study. Powers '
    'the partial unique indexes below and the no-missing-value CHECK, neither '
    'of which can reach into the study field table from an index predicate or '
    'a row constraint. Never written by a caller.';

ALTER TABLE qiita.prep_sample_metadata
    ADD CONSTRAINT prep_sample_metadata_unique_in_study_no_missing_value
        CHECK (NOT unique_in_study OR value_missing_reason_idx IS NULL);


-- =============================================================================
-- TRIGGER: biosample_metadata_apply_field_contract (third responsibility)
--
-- Gains a third responsibility alongside the data_type check and the
-- global-link denormalization: SET NEW.unique_in_study from the source study
-- field. It rides the SELECT the trigger already issues, so the added
-- responsibility costs no extra query.
--
-- The assignment deliberately precedes the missing-reason early exit: a
-- missing-reason row is exempt from the data_type match but is NOT exempt
-- from the no-missing-value CHECK, which can only see a value the trigger
-- has already assigned.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.biosample_metadata_apply_field_contract()
RETURNS TRIGGER AS $$
DECLARE
    expected_data_type qiita.field_data_type;
    populated_ok      BOOLEAN;
BEGIN
    -- Single SELECT covers all three responsibilities: the global link, the
    -- study-local uniqueness policy, and the resolved data_type for the
    -- source field row.
    SELECT bsf.biosample_global_field_idx,
           bsf.unique_in_study,
           COALESCE(bsf.data_type, bgf.data_type)
      INTO NEW.global_field_idx, NEW.unique_in_study, expected_data_type
      FROM qiita.biosample_study_field bsf
      LEFT JOIN qiita.biosample_global_field bgf
        ON bgf.idx = bsf.biosample_global_field_idx
     WHERE bsf.idx = NEW.biosample_study_field_idx;

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
            'biosample_metadata value column does not match field data_type % for biosample_study_field_idx %',
            expected_data_type, NEW.biosample_study_field_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- The prep_sample twin; see the banner above.
CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_apply_field_contract()
RETURNS TRIGGER AS $$
DECLARE
    expected_data_type qiita.field_data_type;
    populated_ok      BOOLEAN;
BEGIN
    -- Single SELECT covers all three responsibilities.
    SELECT ssf.prep_sample_global_field_idx,
           ssf.unique_in_study,
           COALESCE(ssf.data_type, sgf.data_type)
      INTO NEW.global_field_idx, NEW.unique_in_study, expected_data_type
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


-- =============================================================================
-- PARTIAL UNIQUE INDEXES
--
-- One per eligible value column, because a multi-column index over the whole
-- value_* set would be inert: under the default NULLS DISTINCT, rows carrying
-- a NULL in any indexed column never conflict, and every metadata row has
-- NULLs in all but one value column.
--
-- The value IS NOT NULL arm is what keeps a missing-reason row out of the
-- index; the no-missing-value CHECK above is what makes such a row impossible
-- on a flagged field in the first place. The two are not redundant -- the
-- index arm is about which rows the index describes, the CHECK is about which
-- rows may exist.
-- =============================================================================

CREATE UNIQUE INDEX biosample_metadata_unique_in_study_text
    ON qiita.biosample_metadata (biosample_study_field_idx, value_text)
    WHERE unique_in_study AND value_text IS NOT NULL;

CREATE UNIQUE INDEX biosample_metadata_unique_in_study_numeric
    ON qiita.biosample_metadata (biosample_study_field_idx, value_numeric)
    WHERE unique_in_study AND value_numeric IS NOT NULL;

CREATE UNIQUE INDEX biosample_metadata_unique_in_study_date
    ON qiita.biosample_metadata (biosample_study_field_idx, value_date)
    WHERE unique_in_study AND value_date IS NOT NULL;

CREATE UNIQUE INDEX prep_sample_metadata_unique_in_study_text
    ON qiita.prep_sample_metadata (prep_sample_study_field_idx, value_text)
    WHERE unique_in_study AND value_text IS NOT NULL;

CREATE UNIQUE INDEX prep_sample_metadata_unique_in_study_numeric
    ON qiita.prep_sample_metadata (prep_sample_study_field_idx, value_numeric)
    WHERE unique_in_study AND value_numeric IS NOT NULL;

CREATE UNIQUE INDEX prep_sample_metadata_unique_in_study_date
    ON qiita.prep_sample_metadata (prep_sample_study_field_idx, value_date)
    WHERE unique_in_study AND value_date IS NOT NULL;


-- migrate:down

DROP INDEX IF EXISTS qiita.prep_sample_metadata_unique_in_study_date;
DROP INDEX IF EXISTS qiita.prep_sample_metadata_unique_in_study_numeric;
DROP INDEX IF EXISTS qiita.prep_sample_metadata_unique_in_study_text;
DROP INDEX IF EXISTS qiita.biosample_metadata_unique_in_study_date;
DROP INDEX IF EXISTS qiita.biosample_metadata_unique_in_study_numeric;
DROP INDEX IF EXISTS qiita.biosample_metadata_unique_in_study_text;

-- Restored to the bodies that stood before this migration: two
-- responsibilities, no unique_in_study assignment.
CREATE OR REPLACE FUNCTION qiita.biosample_metadata_apply_field_contract()
RETURNS TRIGGER AS $$
DECLARE
    expected_data_type qiita.field_data_type;
    populated_ok      BOOLEAN;
BEGIN
    -- Single SELECT covers both responsibilities: the global link and the
    -- resolved data_type for the source field row.
    SELECT bsf.biosample_global_field_idx,
           COALESCE(bsf.data_type, bgf.data_type)
      INTO NEW.global_field_idx, expected_data_type
      FROM qiita.biosample_study_field bsf
      LEFT JOIN qiita.biosample_global_field bgf
        ON bgf.idx = bsf.biosample_global_field_idx
     WHERE bsf.idx = NEW.biosample_study_field_idx;

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
            'biosample_metadata value column does not match field data_type % for biosample_study_field_idx %',
            expected_data_type, NEW.biosample_study_field_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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

ALTER TABLE qiita.prep_sample_metadata
    DROP CONSTRAINT IF EXISTS prep_sample_metadata_unique_in_study_no_missing_value;
ALTER TABLE qiita.prep_sample_metadata DROP COLUMN IF EXISTS unique_in_study;

ALTER TABLE qiita.biosample_metadata
    DROP CONSTRAINT IF EXISTS biosample_metadata_unique_in_study_no_missing_value;
ALTER TABLE qiita.biosample_metadata DROP COLUMN IF EXISTS unique_in_study;

ALTER TABLE qiita.prep_sample_study_field
    DROP CONSTRAINT IF EXISTS prep_sample_study_field_unique_in_study_data_type_eligible;
ALTER TABLE qiita.prep_sample_study_field
    DROP CONSTRAINT IF EXISTS prep_sample_study_field_inheritance_consistent;
ALTER TABLE qiita.prep_sample_study_field
    ADD CONSTRAINT prep_sample_study_field_inheritance_consistent
        CHECK (
            (prep_sample_global_field_idx IS NULL
                AND data_type IS NOT NULL
                AND required IS NOT NULL
                AND (data_type = 'terminology') = (terminology_idx IS NOT NULL))
            OR
            (prep_sample_global_field_idx IS NOT NULL
                AND data_type IS NULL
                AND terminology_idx IS NULL
                AND tier_override IS NULL
                AND required IS NULL)
        );
ALTER TABLE qiita.prep_sample_study_field DROP COLUMN IF EXISTS unique_in_study;

ALTER TABLE qiita.biosample_study_field
    DROP CONSTRAINT IF EXISTS biosample_study_field_unique_in_study_data_type_eligible;
ALTER TABLE qiita.biosample_study_field
    DROP CONSTRAINT IF EXISTS biosample_study_field_inheritance_consistent;
ALTER TABLE qiita.biosample_study_field
    ADD CONSTRAINT biosample_study_field_inheritance_consistent
        CHECK (
            (biosample_global_field_idx IS NULL
                AND data_type IS NOT NULL
                AND required IS NOT NULL
                AND (data_type = 'terminology') = (terminology_idx IS NOT NULL))
            OR
            (biosample_global_field_idx IS NOT NULL
                AND data_type IS NULL
                AND terminology_idx IS NULL
                AND tier_override IS NULL
                AND required IS NULL)
        );
ALTER TABLE qiita.biosample_study_field DROP COLUMN IF EXISTS unique_in_study;
