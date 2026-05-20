"""Prep-sample arm of the shared metadata stack.

Holds PREP_SAMPLE_METADATA_SPEC (consumed by the cross-entity helpers in
_sample_helpers). The typed-value INSERT path for every prep_sample
metadata row is driven by this spec via the shared writers. There is
no prep_sample-specific inserter analogous to
biosample_metadata.insert_owner_biosample_id_metadata — the owner-id
flag has no prep_sample counterpart.
"""

from ._sample_helpers import (
    EntityMetadataSpec,
    SampleEntityKind,
)

# ---------------------------------------------------------------------------
# EntityMetadataSpec for prep_sample (consumed by _sample_helpers shared writers)
# ---------------------------------------------------------------------------

PREP_SAMPLE_METADATA_SPEC = EntityMetadataSpec(
    entity_kind=SampleEntityKind.PREP_SAMPLE,
    metadata_table="qiita.prep_sample_metadata",
    global_field_table="qiita.prep_sample_global_field",
    entity_key_column="prep_sample_idx",
    study_field_table="qiita.prep_sample_study_field",
    study_field_idx_column="prep_sample_study_field_idx",
    study_field_global_fk_column="prep_sample_global_field_idx",
    global_field_unique_index_name="prep_sample_metadata_one_value_per_global_field",
    local_unique_per_field_index_name="prep_sample_metadata_unique_per_field",
    link_table="qiita.prep_sample_to_study",
    link_entity_key_column="prep_sample_idx",
)
