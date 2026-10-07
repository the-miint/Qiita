-- migrate:up

-- =============================================================================
-- EXPORTED ENTITY (the public handle for one curated entity)
-- =============================================================================
-- Mints the name a study or a biosample is referred to by outside Qiita:
-- 'QS<idx>' for a study, 'QB<idx>' for a biosample.
--
-- It exists because an `*_idx` should not leave this system. This table is the
-- alternative, and its identifiers can be used generally outside this system
-- (as in ENA submissions, exported filenames, etc.) with the expectation that
-- they will be permanent and stable.
--
-- WHAT COUNTS AS AN ENTITY HERE. Study and biosample each satisfy the conditions
-- below, and the rest of this table's shape follows from them:
--
--   1. Standalone identity — named by a single FK column, never by a tuple, because
--      the uniqueness constraints below key one column per kind.
--   2. No purge path — nothing hard-DELETEs it and every inbound FK is RESTRICT,
--      so a handle never has to outlive the thing it names.
--   3. Its lifecycle (e.g., retirement) is recorded on the entity itself, so a
--      reader joins the entity for that state instead of reading a copy kept here.
--   4. Mintable without an accession — the handle must not wait on an external
--      authority, because having a name BEFORE one exists is part of the point.
--
-- Because of condition 3, this table deliberately has NO RETIREMENT COLUMNS.

CREATE TABLE qiita.exported_entity (
    idx               BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- The published handle. GENERATED, not written: Postgres refuses an INSERT
    -- that supplies a value, so it cannot be forged by a caller and cannot be
    -- edited after publication. No code path composes this string.
    --
    -- NOT NULL is load-bearing rather than decorative. The CASE has no ELSE, so a
    -- new kind added without extending here composes NULL and the INSERT fails loudly.
    -- Note that all prefixes must be the same length (2 characters) and composed
    -- only of letters.
    export_entity_id  VARCHAR NOT NULL GENERATED ALWAYS AS (
        CASE
            WHEN study_idx     IS NOT NULL THEN 'QS'
            WHEN biosample_idx IS NOT NULL THEN 'QB'
        END || idx
    ) STORED,

    -- The entity. Exactly one of these is non-null; see the CHECK below.
    --
    -- RESTRICT on both as neither parent has a hard-delete path to accommodate.
    study_idx         BIGINT REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    biosample_idx     BIGINT REFERENCES qiita.biosample(idx) ON DELETE RESTRICT,

    created_by_idx    BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Exactly one entity per row, unconditionally. This is what makes the prefix above
    -- total: with one column set, the CASE always has a branch to take.
    CONSTRAINT exported_entity_one_entity
        CHECK (num_nonnulls(study_idx, biosample_idx) = 1),

    -- One handle per entity, for that entity's whole life. Plain UNIQUE. NULLs are
    -- distinct, so the rows of one kind do not collide on the other kind's column.
    CONSTRAINT exported_entity_study_unique     UNIQUE (study_idx),
    CONSTRAINT exported_entity_biosample_unique UNIQUE (biosample_idx)
);

COMMENT ON TABLE qiita.exported_entity IS
    'Public handle (export_entity_id, ''QS<idx>'' for a study and ''QB<idx>'' for a '
    'biosample) for one curated entity, so nothing that crosses the Qiita boundary '
    'has to carry an internal *_idx. An entity qualifies here when it has a '
    'standalone single-column identity, no hard-delete path, its own lifecycle '
    'columns, and no dependency on an external accession existing first. '
    'Deliberately carries no retirement columns: a handle is permanent and its '
    'entity''s retirement is read from the entity.';

COMMENT ON COLUMN qiita.exported_entity.export_entity_id IS
    'The published handle. GENERATED ALWAYS: unforgeable by a caller, immutable '
    'after publication, and NOT NULL so a kind added without extending the prefix '
    'expression fails at insert rather than minting a wrong handle.';

-- The published handle is the lookup key for anyone resolving an external usage.
-- A collision is already impossible while all prefixes follow the requirements
-- recorded above, but this index enforces uniqueness at the database level and
-- keeps the guarantee attached to the published column rather than left implicit
-- in the expression.
CREATE UNIQUE INDEX exported_entity_export_entity_id_unique
    ON qiita.exported_entity (export_entity_id);


-- migrate:down

DROP TABLE IF EXISTS qiita.exported_entity;
