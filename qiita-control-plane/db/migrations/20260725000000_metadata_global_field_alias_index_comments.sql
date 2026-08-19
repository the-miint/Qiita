-- migrate:up

-- Record, in the catalog itself, why the per-global metadata uniqueness is
-- keyed on (entity, global field) and must stay that way. No actual changes
-- to indexes; this only attaches an explanatory COMMENT so future work surfaces
-- the intended multi-alias invariant without reading architecture.md first.

COMMENT ON INDEX qiita.biosample_metadata_one_value_per_global_field IS
    'At most one metadata value per (biosample, global field). Deliberately '
    'keyed on (biosample_idx, global_field_idx), NOT (study_idx, '
    'global_field_idx): one study may hold several study-local fields that '
    'each link to the same global field (e.g. different contributor-supplied '
    'column names for one concept), each populated for a *disjoint* set of '
    'biosamples and reconciled through global_field_idx. Do not tighten this '
    'to a per-study uniqueness constraint — it would forbid that intended '
    'aliasing. See docs/architecture.md (Biosample and Prep Sample Metadata).';

COMMENT ON INDEX qiita.prep_sample_metadata_one_value_per_global_field IS
    'At most one metadata value per (prep_sample, global field). Deliberately '
    'keyed on (prep_sample_idx, global_field_idx), NOT (study_idx, '
    'global_field_idx): one study may hold several study-local fields that '
    'each link to the same global field (e.g. different contributor-supplied '
    'column names for one concept), each populated for a *disjoint* set of '
    'prep_samples and reconciled through global_field_idx. Do not tighten this '
    'to a per-study uniqueness constraint — it would forbid that intended '
    'aliasing. See docs/architecture.md (Biosample and Prep Sample Metadata).';


-- migrate:down

COMMENT ON INDEX qiita.biosample_metadata_one_value_per_global_field IS NULL;
COMMENT ON INDEX qiita.prep_sample_metadata_one_value_per_global_field IS NULL;
