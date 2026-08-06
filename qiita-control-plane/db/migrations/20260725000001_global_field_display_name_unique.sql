-- migrate:up

-- internal_name is already unique; display_name was not. Enforce uniqueness
-- within each global-field table so a display_name maps to at most one row.
-- See docs/architecture.md (Global vs. study-local fields and multi-alias
-- linkage) for why non-unique global display_names were rejected.

ALTER TABLE qiita.biosample_global_field
    ADD CONSTRAINT biosample_global_field_display_name_unique UNIQUE (display_name);

ALTER TABLE qiita.prep_sample_global_field
    ADD CONSTRAINT prep_sample_global_field_display_name_unique UNIQUE (display_name);

-- migrate:down

ALTER TABLE qiita.prep_sample_global_field
    DROP CONSTRAINT prep_sample_global_field_display_name_unique;

ALTER TABLE qiita.biosample_global_field
    DROP CONSTRAINT biosample_global_field_display_name_unique;
