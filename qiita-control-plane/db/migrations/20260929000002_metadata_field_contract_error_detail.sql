-- migrate:up

-- =============================================================================
-- Tag the field-contract rejections on *_metadata with a structured DETAIL so a
-- route can identify them.
--
-- The MESSAGE stays human-readable; everything a route acts on goes in DETAIL as
-- comma-separated key=value pairs, in the manner of the retired-link rejections.
-- The `trigger` key carries the raising function's name, so a route tells this
-- rejection from the other P0001 raisers on these tables -- which it could not
-- before, this one having carried no DETAIL at all -- and the field idx and the
-- declared type let it say which field and what was expected.
--
-- Only the two functions change. Both the INSERT and the UPDATE triggers execute
-- them, so both statement kinds gain the DETAIL at once, and the ERRCODE stays
-- 'P0001' -- what already catches these errors keeps catching them.
--
-- same-pattern-ok: NEW carries a different FK column name per entity, so one
-- function would have to read it through to_jsonb on every metadata write.
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
            expected_data_type, NEW.biosample_study_field_idx
          USING ERRCODE = 'P0001', DETAIL = format(
              'trigger=biosample_metadata_apply_field_contract, biosample_study_field_idx=%s, data_type=%s',
              NEW.biosample_study_field_idx, expected_data_type);
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
            expected_data_type, NEW.prep_sample_study_field_idx
          USING ERRCODE = 'P0001', DETAIL = format(
              'trigger=prep_sample_metadata_apply_field_contract, prep_sample_study_field_idx=%s, data_type=%s',
              NEW.prep_sample_study_field_idx, expected_data_type);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- migrate:down

-- The bodies as they stood before the DETAIL was added.

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
