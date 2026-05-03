-- migrate:up

-- =============================================================================
-- METADATA CHECKLISTS (metadata field sets / templates)
-- =============================================================================
--
-- A metadata checklist is a published external specification (e.g., MIxS,
-- MIMS, MIMARKS) that lists fields expected for samples claiming conformance
-- to it. Checklists describe required fields on both the biosample side
-- (collection metadata, environmental context) and the sequenced-study side
-- (library construction, sequencing parameters). metadata_checklist_field is
-- defined in a later migration because it references both
-- biosample_global_field and sequenced_sample_global_field, which are not
-- declared until after this one.

CREATE TABLE qiita.metadata_checklist (
    idx                            BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name                           VARCHAR(255) NOT NULL,
    description                    TEXT,
    parent_metadata_checklist_idx  BIGINT REFERENCES qiita.metadata_checklist(idx) ON DELETE SET NULL,

    CONSTRAINT metadata_checklist_name_unique UNIQUE (name),
    CONSTRAINT metadata_checklist_no_self_parent
        CHECK (parent_metadata_checklist_idx IS NULL OR parent_metadata_checklist_idx <> idx)
);

COMMENT ON TABLE qiita.metadata_checklist IS
    'A published external metadata specification (e.g., MIxS, MIMARKS, MIMS) '
    'that describes which fields a sample should carry to be considered '
    'compliant. A metadata checklist''s required-field list is declared by '
    'the metadata_checklist_field table. Checklists are not enforced at '
    'write time; they are checked at query time when a sample claims '
    'conformance. Metadata checklists are not retired: conformance against '
    'a currently-defined checklist is always meaningful, even for samples '
    'created long ago. A metadata_checklist row does not carry creation-audit '
    'columns (created_by_idx, created_at) because the checklist is a mutable '
    'curated list: metadata_checklist_field rows may be added and removed '
    'over time.';


-- migrate:down

DROP TABLE IF EXISTS qiita.metadata_checklist;
