-- migrate:up

-- Guarded rebind of one or more global biosample fields to a new data_type.
-- The metadata-side field-contract trigger fires on biosample_metadata DML,
-- not on a field-definition UPDATE, so rows already stored against these
-- fields would survive silently misaligned under the new data_type; the
-- rebind is refused whenever any such row exists. When p_data_type is
-- 'terminology' the named terminology is resolved and bound, otherwise the
-- binding is cleared, keeping the data_type / terminology_idx pair consistent.
CREATE FUNCTION qiita.rebind_biosample_global_field_data_type(
    p_fields           TEXT[],
    p_data_type        qiita.field_data_type,
    p_terminology_name TEXT DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    n_existing        int;
    v_terminology_idx bigint;
    n_updated         int;
BEGIN
    -- Refuse the flip if any stored metadata references these fields.
    SELECT COUNT(*) INTO n_existing
      FROM qiita.biosample_metadata m
      JOIN qiita.biosample_study_field bsf
        ON bsf.idx = m.biosample_study_field_idx
      JOIN qiita.biosample_global_field bgf
        ON bgf.idx = bsf.biosample_global_field_idx
     WHERE bgf.internal_name = ANY(p_fields);
    IF n_existing > 0 THEN
        RAISE EXCEPTION
            'fields % have % biosample_metadata rows; data_type flip to % unsafe',
            p_fields, n_existing, p_data_type;
    END IF;

    -- Resolve the terminology binding required by the target data_type.
    IF p_data_type = 'terminology' THEN
        SELECT idx INTO v_terminology_idx
          FROM qiita.terminology WHERE name = p_terminology_name;
        IF v_terminology_idx IS NULL THEN
            RAISE EXCEPTION 'terminology % not found', p_terminology_name;
        END IF;
    ELSE
        v_terminology_idx := NULL;
    END IF;

    -- Apply the rebind and confirm every named field actually existed.
    UPDATE qiita.biosample_global_field
       SET data_type = p_data_type,
           terminology_idx = v_terminology_idx
     WHERE internal_name = ANY(p_fields);
    GET DIAGNOSTICS n_updated = ROW_COUNT;
    IF n_updated <> COALESCE(array_length(p_fields, 1), 0) THEN
        RAISE EXCEPTION
            'expected to rebind % fields, updated %',
            COALESCE(array_length(p_fields, 1), 0), n_updated;
    END IF;
END $$;


-- migrate:down

DROP FUNCTION IF EXISTS qiita.rebind_biosample_global_field_data_type(
    TEXT[], qiita.field_data_type, TEXT);
