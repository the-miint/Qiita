-- migrate:up

-- =============================================================================
-- PREP PROTOCOLS
-- =============================================================================

CREATE TABLE qiita.prep_protocol (
    idx             BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name            TEXT NOT NULL,
    description     TEXT,
    created_by_idx  BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    retired         BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx  BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at      TIMESTAMPTZ,
    retire_reason   TEXT,

    CONSTRAINT prep_protocol_name_unique UNIQUE (name),
    CONSTRAINT prep_protocol_name_format CHECK (name ~ '^[a-z][a-z0-9_]*$'),

    CONSTRAINT prep_protocol_retirement_consistent CHECK (
        (retired = false
            AND retired_at IS NULL
            AND retired_by_idx IS NULL
            AND retire_reason IS NULL)
        OR
        (retired = true
            AND retired_at IS NOT NULL
            AND retired_by_idx IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.prep_protocol IS
    'Curated registry of prep protocols (amplicon, shotgun metagenomics, etc.). '
    'A prep protocol classifies a sequenced sample by the laboratory procedure '
    'used to prepare it. Via the prep_protocol_field join table (defined below), '
    'it declares which sequenced_sample fields are allowed (and which are '
    'required) for sequenced samples following that protocol; writes are '
    'rejected if a field not associated with the protocol is named. This is '
    'operational gating, enforced at write time, distinct from the '
    'conformance-claim role of metadata_checklist (checked at query time). '
    'System-admin curated.';

COMMENT ON COLUMN qiita.prep_protocol.retired IS
    'When true, this prep protocol is soft-hidden. Sequenced samples already '
    'referencing it continue to function; new sequenced samples cannot be '
    'created with a retired protocol, and the protocol''s prep_protocol_field '
    'association list is frozen. Prep protocols cannot be hard-deleted because '
    'sequenced_sample.prep_protocol_idx uses ON DELETE RESTRICT.';

CREATE INDEX prep_protocol_active_idx
    ON qiita.prep_protocol (name)
    WHERE retired = false;


-- =============================================================================
-- SEQUENCED SAMPLE FIELDS (the field registry, split global vs. study-scoped)
-- =============================================================================

CREATE TABLE qiita.sequenced_sample_global_field (
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

    CONSTRAINT sequenced_sample_global_field_internal_name_unique UNIQUE (internal_name),
    CONSTRAINT sequenced_sample_global_field_internal_name_format
        CHECK (internal_name ~ '^[a-z][a-z0-9_]*$'),

    -- terminology_idx is set iff data_type = 'terminology'. Mirrors the
    -- biosample_global_field constraint.
    CONSTRAINT sequenced_sample_global_field_terminology_data_type_consistent
        CHECK ((data_type = 'terminology') = (terminology_idx IS NOT NULL))
);

COMMENT ON TABLE qiita.sequenced_sample_global_field IS
    'Global concept registry for sequenced-sample fields. Parallel to '
    'biosample_global_field. internal_name is the stable cross-study query '
    'identifier; display_name is the default human-readable label used as a '
    'fallback when a study has not provided its own. data_type, '
    'terminology_idx, default_tier, and required are owned at the global '
    'level and apply uniformly across every study that links to this concept; '
    'per-study disagreement on these properties is forbidden so that '
    'cross-study reads remain sound.';

COMMENT ON COLUMN qiita.sequenced_sample_global_field.internal_name IS
    'Globally unique snake_case identifier used in cross-study queries. '
    'Stable; never displayed to end users in normal workflows.';


CREATE TABLE qiita.sequenced_sample_study_field (
    idx                                BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    study_idx                          BIGINT NOT NULL REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    sequenced_sample_global_field_idx  BIGINT REFERENCES qiita.sequenced_sample_global_field(idx) ON DELETE RESTRICT,
    display_name                       TEXT NOT NULL,
    description                        TEXT,
    data_type                          qiita.field_data_type,
    required                           BOOLEAN,
    terminology_idx                    BIGINT REFERENCES qiita.terminology(idx) ON DELETE RESTRICT,
    tier_override                      qiita.tier,
    created_by_idx                     BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- display_name must be unique within a study so that downloads and the user's
    -- mental model of "their columns" stay consistent.
    CONSTRAINT sequenced_sample_study_field_display_name_unique
        UNIQUE (study_idx, display_name),

    -- Inheritance rules for linked vs unlinked fields; see table comment.
    -- The unlinked branch additionally enforces the same
    -- terminology_idx ↔ data_type='terminology' coupling as the global table.
    CONSTRAINT sequenced_sample_study_field_inheritance_consistent
        CHECK (
            (sequenced_sample_global_field_idx IS NULL
                AND data_type IS NOT NULL
                AND required IS NOT NULL
                AND (data_type = 'terminology') = (terminology_idx IS NOT NULL))
            OR
            (sequenced_sample_global_field_idx IS NOT NULL
                AND data_type IS NULL
                AND terminology_idx IS NULL
                AND tier_override IS NULL
                AND required IS NULL)
        )
);

COMMENT ON TABLE qiita.sequenced_sample_study_field IS
    'Per-study field definitions for sequenced samples. Parallel to '
    'biosample_study_field. May be linked to a sequenced_sample_global_field, '
    'in which case data_type, terminology_idx, tier_override, and required '
    'are all owned by the global concept and must be NULL on this row. Only '
    'display_name and description may be overridden per-study on linked rows, '
    'as cosmetic presentation for that study''s own users. Unlinked rows are '
    'purely study-local and carry their own type, terminology, tier, and '
    'required policy.';

CREATE INDEX sequenced_sample_study_field_study_idx
    ON qiita.sequenced_sample_study_field (study_idx);
CREATE INDEX sequenced_sample_study_field_global_link_idx
    ON qiita.sequenced_sample_study_field (sequenced_sample_global_field_idx)
    WHERE sequenced_sample_global_field_idx IS NOT NULL;


-- =============================================================================
-- PREP PROTOCOL FIELDS (prep_protocol x sequenced_sample_global_field association)
-- =============================================================================

CREATE TABLE qiita.prep_protocol_field (
    prep_protocol_idx                  BIGINT NOT NULL REFERENCES qiita.prep_protocol(idx) ON DELETE RESTRICT,
    sequenced_sample_global_field_idx  BIGINT NOT NULL REFERENCES qiita.sequenced_sample_global_field(idx) ON DELETE RESTRICT,
    required                           BOOLEAN NOT NULL DEFAULT false,
    created_by_idx                     BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (prep_protocol_idx, sequenced_sample_global_field_idx)
);

COMMENT ON TABLE qiita.prep_protocol_field IS
    'Many-to-many between prep protocols and sequenced-sample global fields. '
    'Each row says "this global field is allowed (and possibly required) for '
    'sequenced samples following this protocol." Protocol requirements are '
    'cross-study governance -- the protocol is defined once and used by many '
    'studies -- so requirements reference only globally-known concepts, not '
    'study-local fields. A study that wishes to require a study-local field '
    'for its own use of a protocol must first promote the field to a global '
    'concept via the field-promotion flow.';

CREATE INDEX prep_protocol_field_field_idx
    ON qiita.prep_protocol_field (sequenced_sample_global_field_idx);


-- =============================================================================
-- SEQUENCED-SAMPLE SUBMISSION GLOBAL FIELDS (seed)
-- =============================================================================

-- Globally-known sequenced-sample fields required for ENA Experiment
-- submission. Seeded here so the orchestration can write per-study local
-- field rows linked to these globals from the start.
-- created_by_idx=1 references the system principal seeded earlier.

INSERT INTO qiita.sequenced_sample_global_field (
    internal_name, display_name, data_type, default_tier, required, created_by_idx
) VALUES
    ('alias',                         'Alias',                         'text', 'public', true, 1),
    ('title',                         'Title',                         'text', 'public', true, 1),
    ('design_description',            'Design description',            'text', 'public', true, 1),
    ('library_name',                  'Library name',                  'text', 'public', true, 1),
    ('library_strategy',              'Library strategy',              'text', 'public', true, 1),
    ('library_source',                'Library source',                'text', 'public', true, 1),
    ('library_selection',             'Library selection',             'text', 'public', true, 1),
    ('library_layout',                'Library layout',                'text', 'public', true, 1),
    ('library_construction_protocol', 'Library construction protocol', 'text', 'public', true, 1);


-- migrate:down

DROP TABLE IF EXISTS qiita.prep_protocol_field;
DROP TABLE IF EXISTS qiita.sequenced_sample_study_field;
DROP TABLE IF EXISTS qiita.sequenced_sample_global_field;
DROP TABLE IF EXISTS qiita.prep_protocol;
