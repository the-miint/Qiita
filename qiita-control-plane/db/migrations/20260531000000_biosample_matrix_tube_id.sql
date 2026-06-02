-- migrate:up

-- matrix_tube_id holds a digit-only identifier (per local convention) that
-- may carry leading zeros, so the column type is VARCHAR rather than a
-- numeric type.
ALTER TABLE qiita.biosample
    ADD COLUMN matrix_tube_id VARCHAR(50);

ALTER TABLE qiita.biosample
    ADD CONSTRAINT biosample_matrix_tube_id_unique UNIQUE (matrix_tube_id);


-- This constraint is deliberately duplicated in the application layer in the models.
-- If changing here, also change there in the same PR.
ALTER TABLE qiita.biosample
    ADD CONSTRAINT biosample_matrix_tube_id_format
        CHECK (matrix_tube_id IS NULL OR matrix_tube_id ~ '^[0-9]{8,10}$');

-- migrate:down

ALTER TABLE qiita.biosample
    DROP CONSTRAINT IF EXISTS biosample_matrix_tube_id_format;
ALTER TABLE qiita.biosample
    DROP CONSTRAINT IF EXISTS biosample_matrix_tube_id_unique;
ALTER TABLE qiita.biosample
    DROP COLUMN IF EXISTS matrix_tube_id;
