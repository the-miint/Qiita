-- migrate:up

-- Abort loudly if any existing row holds a matrix_tube_id that is not exactly
-- ten digits; there is no safe automated transform of a shorter id, so the
-- operator must remediate the listed rows by hand before re-running migrate.
DO $$
DECLARE
    bad_count integer;
    bad_rows text;
BEGIN
    SELECT
        count(*),
        string_agg(
            format('biosample_idx=%s matrix_tube_id=%s',
                   idx, matrix_tube_id),
            ', ')
    INTO bad_count, bad_rows
    FROM qiita.biosample
    WHERE matrix_tube_id IS NOT NULL
      AND matrix_tube_id !~ '^[0-9]{10}$';

    IF bad_count > 0 THEN
        RAISE EXCEPTION
            'Cannot tighten matrix_tube_id to exactly 10 digits: % row(s) violate the new format and need manual remediation: %',
            bad_count, bad_rows;
    END IF;
END $$;

-- This constraint is deliberately duplicated in the application layer in the models.
-- If changing here, also change there in the same PR.
ALTER TABLE qiita.biosample
    DROP CONSTRAINT IF EXISTS biosample_matrix_tube_id_format;
ALTER TABLE qiita.biosample
    ADD CONSTRAINT biosample_matrix_tube_id_format
        CHECK (matrix_tube_id IS NULL OR matrix_tube_id ~ '^[0-9]{10}$');

-- migrate:down

ALTER TABLE qiita.biosample
    DROP CONSTRAINT IF EXISTS biosample_matrix_tube_id_format;
ALTER TABLE qiita.biosample
    ADD CONSTRAINT biosample_matrix_tube_id_format
        CHECK (matrix_tube_id IS NULL OR matrix_tube_id ~ '^[0-9]{8,10}$');
