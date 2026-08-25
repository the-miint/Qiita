-- migrate:up

-- Comments only; no schema change. Two corrections, applied to both entities:
--
-- The study_field table comments listed tier_override among the properties
-- "owned by the global field". A global field has no tier_override — it carries
-- default_tier, a distinct concept — so a linked row's NULL tier_override is
-- what the inheritance CHECK requires, not a value it inherits.
--
-- The global_field.internal_name comments said the column is never displayed to
-- end users, which is no longer true.

COMMENT ON TABLE qiita.biosample_study_field IS
    'Per-study field definitions for biosamples. Parallel to prep_sample_study_field. '
    'May be linked to a biosample_global_field, in which '
    'case data_type, terminology_idx, and required are all owned by the global field '
    'and must be NULL on this row, as must tier_override — the global field carries '
    'default_tier instead, so a linked row has no per-study tier to override. Only '
    'display_name and description may be overridden per-study on linked rows, as '
    'cosmetic presentation for that study''s own users. Unlinked rows are purely '
    'study-local and carry their own type, terminology, tier, and required policy.';

COMMENT ON TABLE qiita.prep_sample_study_field IS
    'Per-study field definitions for prep samples. Parallel to '
    'biosample_study_field. May be linked to a prep_sample_global_field, '
    'in which case data_type, terminology_idx, and required are all owned by '
    'the global field and must be NULL on this row, as must tier_override — the '
    'global field carries default_tier instead, so a linked row has no per-study '
    'tier to override. Only display_name and description may be overridden '
    'per-study on linked rows, as cosmetic presentation for that study''s own '
    'users. Unlinked rows are purely study-local and carry their own type, '
    'terminology, tier, and required policy.';

COMMENT ON COLUMN qiita.biosample_global_field.internal_name IS
    'Globally unique snake_case identifier used in cross-study queries.';

COMMENT ON COLUMN qiita.prep_sample_global_field.internal_name IS
    'Globally unique snake_case identifier used in cross-study queries.';


-- migrate:down

COMMENT ON TABLE qiita.biosample_study_field IS
    'Per-study field definitions. May be linked to a biosample_global_field, in which '
    'case data_type, terminology_idx, tier_override, and required are all owned by the '
    'global field and must be NULL on this row. Only display_name and description '
    'may be overridden per-study on linked rows, as cosmetic presentation for that '
    'study''s own users. Unlinked rows are purely study-local and carry their own type, '
    'terminology, tier, and required policy.';

COMMENT ON TABLE qiita.prep_sample_study_field IS
    'Per-study field definitions for prep samples. Parallel to '
    'biosample_study_field. May be linked to a prep_sample_global_field, '
    'in which case data_type, terminology_idx, tier_override, and required '
    'are all owned by the global field and must be NULL on this row. Only '
    'display_name and description may be overridden per-study on linked rows, '
    'as cosmetic presentation for that study''s own users. Unlinked rows are '
    'purely study-local and carry their own type, terminology, tier, and '
    'required policy.';

COMMENT ON COLUMN qiita.biosample_global_field.internal_name IS
    'Globally unique snake_case identifier used in cross-study queries. Stable; '
    'never displayed to end users in normal workflows.';

COMMENT ON COLUMN qiita.prep_sample_global_field.internal_name IS
    'Globally unique snake_case identifier used in cross-study queries. '
    'Stable; never displayed to end users in normal workflows.';
