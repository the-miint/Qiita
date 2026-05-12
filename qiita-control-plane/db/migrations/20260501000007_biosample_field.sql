-- migrate:up

-- =============================================================================
-- FIELD DATA TYPE ENUM
-- =============================================================================
--
-- Closed set of value kinds a field may carry. The members map 1:1 to the
-- value_* columns on the EAV metadata tables (biosample_metadata,
-- prep_sample_metadata): a field declared as a given data_type must have
-- its value written into the matching value_* column. Adding a new member is
-- a coordinated schema change (new value_* column on every metadata table,
-- new arm in the trigger that checks the match), not a per-row decision.
CREATE TYPE qiita.field_data_type AS ENUM (
    'text',
    'numeric',
    'boolean',
    'date',
    'terminology'
);


-- =============================================================================
-- BIOSAMPLE FIELDS (the field registry, split global vs. study-scoped)
-- =============================================================================

CREATE TABLE qiita.biosample_global_field (
    idx               BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    internal_name     TEXT NOT NULL,
    display_name      TEXT NOT NULL,
    description       TEXT,
    data_type         qiita.field_data_type NOT NULL,
    default_tier      qiita.tier NOT NULL DEFAULT 'public',
    required          BOOLEAN NOT NULL DEFAULT false,
    terminology_idx   BIGINT REFERENCES qiita.terminology(idx) ON DELETE RESTRICT,
    created_by_idx    BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT biosample_global_field_internal_name_unique UNIQUE (internal_name),
    CONSTRAINT biosample_global_field_internal_name_format
        CHECK (internal_name ~ '^[a-z][a-z0-9_]*$'),

    -- terminology_idx is set iff data_type = 'terminology'. A terminology
    -- field stores controlled-vocabulary references in
    -- biosample_metadata.value_terminology_term_idx; non-terminology fields
    -- never carry a terminology source.
    CONSTRAINT biosample_global_field_terminology_data_type_consistent
        CHECK ((data_type = 'terminology') = (terminology_idx IS NOT NULL))
);

COMMENT ON TABLE qiita.biosample_global_field IS
    'Global concept registry. internal_name is the stable cross-study query identifier; '
    'display_name is the default human-readable label used as a fallback when a study '
    'has not provided its own label. data_type, terminology_idx, default_tier, and '
    'required are owned at the global level and apply uniformly to every study that '
    'links to this concept; per-study disagreement on these properties is forbidden '
    'so that cross-study reads remain sound.';

COMMENT ON COLUMN qiita.biosample_global_field.internal_name IS
    'Globally unique snake_case identifier used in cross-study queries. Stable; '
    'never displayed to end users in normal workflows.';

COMMENT ON COLUMN qiita.biosample_global_field.display_name IS
    'Human-readable label for UI and downloads. May contain spaces, punctuation, '
    'and parentheses. Intentionally not derived from internal_name: the label '
    'can evolve (typo fixes, terminology updates) without breaking queries that '
    'pin against internal_name.';


CREATE TABLE qiita.biosample_study_field (
    idx                         BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    study_idx                   BIGINT NOT NULL REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    biosample_global_field_idx  BIGINT REFERENCES qiita.biosample_global_field(idx) ON DELETE RESTRICT,
    display_name                TEXT NOT NULL,
    description                 TEXT,
    data_type                   qiita.field_data_type,
    required                    BOOLEAN,
    terminology_idx             BIGINT REFERENCES qiita.terminology(idx) ON DELETE RESTRICT,
    tier_override               qiita.tier,
    created_by_idx              BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- display_name must be unique within a study so that downloads and the user's
    -- mental model of "their columns" stay consistent.
    CONSTRAINT biosample_study_field_display_name_unique
        UNIQUE (study_idx, display_name),

    -- Inheritance rules for linked vs unlinked fields; see table comment.
    -- The unlinked branch additionally enforces the same
    -- terminology_idx ↔ data_type='terminology' coupling as the global table.
    CONSTRAINT biosample_study_field_inheritance_consistent
        CHECK (
            (biosample_global_field_idx IS NULL
                AND data_type IS NOT NULL
                AND required IS NOT NULL
                AND (data_type = 'terminology') = (terminology_idx IS NOT NULL))
            OR
            (biosample_global_field_idx IS NOT NULL
                AND data_type IS NULL
                AND terminology_idx IS NULL
                AND tier_override IS NULL
                AND required IS NULL)
        )
);

COMMENT ON TABLE qiita.biosample_study_field IS
    'Per-study field definitions. May be linked to a biosample_global_field, in which '
    'case data_type, terminology_idx, tier_override, and required are all owned by the '
    'global concept and must be NULL on this row. Only display_name and description '
    'may be overridden per-study on linked rows, as cosmetic presentation for that '
    'study''s own users. Unlinked rows are purely study-local and carry their own type, '
    'terminology, tier, and required policy.';

CREATE INDEX biosample_study_field_study_idx ON qiita.biosample_study_field (study_idx);
CREATE INDEX biosample_study_field_global_link_idx
    ON qiita.biosample_study_field (biosample_global_field_idx)
    WHERE biosample_global_field_idx IS NOT NULL;


-- =============================================================================
-- BIOSAMPLE GLOBAL FIELD SEED
-- =============================================================================
--
-- Bootstraps the cross-study biosample concept registry with the minimum set
-- of fields every biosample needs in order to be submittable to BioSample.
-- created_by_idx = 1 names the seeded system principal (SYSTEM_PRINCIPAL_IDX).
-- ON CONFLICT DO NOTHING keeps the seed re-runnable: if dbmate's tracking row
-- is ever lost (manual rollback, restored snapshot) and this migration is
-- reapplied while the seed rows still exist, both unique constraints
-- (internal_name and display_name) absorb the conflict instead of erroring.
INSERT INTO qiita.biosample_global_field
    (internal_name, display_name, data_type, required, created_by_idx)
VALUES
    ('collection_date', 'collection date', 'date', true, 1),
    ('geographic_location_country_or_sea', 'geographic location (country and/or sea)', 'text', true, 1),
    ('geographic_location_latitude', 'geographic location (latitude)', 'numeric', true, 1),
    ('geographic_location_longitude', 'geographic location (longitude)', 'numeric', true, 1),
    ('broad_scale_environmental_context', 'broad-scale environmental context', 'text', true, 1),
    ('local_environmental_context', 'local environmental context', 'text', true, 1),
    ('environmental_medium', 'environmental medium', 'text', true, 1),
    ('taxon_id', 'taxon id', 'text', true, 1)
ON CONFLICT DO NOTHING;


-- migrate:down

DROP TABLE IF EXISTS qiita.biosample_study_field;
DROP TABLE IF EXISTS qiita.biosample_global_field;
DROP TYPE IF EXISTS qiita.field_data_type;
