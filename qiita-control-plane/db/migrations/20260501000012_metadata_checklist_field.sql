-- migrate:up

-- =============================================================================
-- METADATA CHECKLIST FIELDS (dual-keyed; biosample-side and prep-sample-side)
-- =============================================================================

CREATE TABLE qiita.metadata_checklist_field (
    idx                                BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    metadata_checklist_idx             BIGINT NOT NULL REFERENCES qiita.metadata_checklist(idx) ON DELETE RESTRICT,
    -- Dual-keyed; see table comment.
    biosample_global_field_idx         BIGINT REFERENCES qiita.biosample_global_field(idx) ON DELETE RESTRICT,
    prep_sample_global_field_idx  BIGINT REFERENCES qiita.prep_sample_global_field(idx) ON DELETE RESTRICT,

    CONSTRAINT metadata_checklist_field_exactly_one_target CHECK (
        (biosample_global_field_idx IS NOT NULL AND prep_sample_global_field_idx IS NULL)
        OR
        (biosample_global_field_idx IS NULL AND prep_sample_global_field_idx IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.metadata_checklist_field IS
    'Required fields for a metadata checklist, dual-keyed to either a global '
    'biosample field or a global prep-sample field. A checklist like MIxS '
    'describes requirements at both layers (biosample collection metadata and '
    'prep-sample sequencing/library metadata), so the table needs to '
    'address both global registries. Checklists are conformance descriptions, '
    'not operational gates: a write that omits a checklist-required field is '
    'not rejected at write time. Conformance is checked when asked, by joining '
    'through this table to find the required fields and verifying that the '
    'sample has values for them.';

-- At most one row per (metadata_checklist, global biosample field) pair.
CREATE UNIQUE INDEX metadata_checklist_field_unique_biosample
    ON qiita.metadata_checklist_field (metadata_checklist_idx, biosample_global_field_idx)
    WHERE biosample_global_field_idx IS NOT NULL;

-- At most one row per (metadata_checklist, global prep-sample field) pair.
CREATE UNIQUE INDEX metadata_checklist_field_unique_sequenced
    ON qiita.metadata_checklist_field (metadata_checklist_idx, prep_sample_global_field_idx)
    WHERE prep_sample_global_field_idx IS NOT NULL;

CREATE INDEX metadata_checklist_field_biosample_idx
    ON qiita.metadata_checklist_field (biosample_global_field_idx)
    WHERE biosample_global_field_idx IS NOT NULL;
CREATE INDEX metadata_checklist_field_sequenced_idx
    ON qiita.metadata_checklist_field (prep_sample_global_field_idx)
    WHERE prep_sample_global_field_idx IS NOT NULL;


-- migrate:down

DROP TABLE IF EXISTS qiita.metadata_checklist_field;
