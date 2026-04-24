-- migrate:up

-- =============================================================================
-- BIOSAMPLE FIELDS (the field registry, split global vs. study-scoped)
-- =============================================================================

CREATE TABLE qiita.biosample_global_field (
    idx               BIGSERIAL PRIMARY KEY,
    internal_name     TEXT NOT NULL,
    display_name      TEXT NOT NULL,
    description       TEXT,
    data_type         TEXT NOT NULL,
    default_tier      qiita.tier NOT NULL DEFAULT 'public',
    required          BOOLEAN NOT NULL DEFAULT false,
    terminology_idx   BIGINT REFERENCES qiita.terminology(idx) ON DELETE RESTRICT,
    created_by_idx    BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT biosample_global_field_internal_name_unique UNIQUE (internal_name),
    CONSTRAINT biosample_global_field_internal_name_format
        CHECK (internal_name ~ '^[a-z][a-z0-9_]*$')
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


CREATE TABLE qiita.biosample_study_field (
    idx                         BIGSERIAL PRIMARY KEY,
    study_idx                   BIGINT NOT NULL REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    biosample_global_field_idx  BIGINT REFERENCES qiita.biosample_global_field(idx) ON DELETE RESTRICT,
    display_name                TEXT NOT NULL,
    description                 TEXT,
    data_type                   TEXT,
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
    CONSTRAINT biosample_study_field_inheritance_consistent
        CHECK (
            (biosample_global_field_idx IS NULL
                AND data_type IS NOT NULL
                AND required IS NOT NULL)
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


-- migrate:down

DROP TABLE IF EXISTS qiita.biosample_study_field;
DROP TABLE IF EXISTS qiita.biosample_global_field;
