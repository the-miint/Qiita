-- migrate:up

-- =============================================================================
-- EXPORTED ENTITY (the public handle for one curated entity)
-- =============================================================================
-- Mints the name a study or a biosample is referred to by outside Qiita:
-- 'QS<n>' for a study, 'QB<n>' for a biosample, where <n> is this table's own idx
-- (one sequence shared by both kinds), never the entity's.
--
-- These identifiers stand in for an `*_idx` wherever one would leave this system
-- (see CLAUDE.md on opaque identifiers) — ENA submissions, exported filenames,
-- etc. — and are permanent and stable.
--
-- WHAT COUNTS AS AN ENTITY HERE. Study and biosample each satisfy the conditions
-- below, and the rest of this table's shape follows from them:
--
--   1. Standalone identity — named by a single FK column, never by a tuple, because
--      the uniqueness constraints below key one column per kind.
--   2. No purge path — nothing hard-DELETEs it, so a handle never has to outlive
--      the thing it names.
--   3. Its lifecycle (e.g., retirement) is recorded on the entity itself, so a
--      reader joins the entity for that state instead of reading a copy kept here.
--   4. Mintable without an accession — the handle must not wait on an external
--      authority, since it is needed before one exists.
--
-- Because of condition 3, this table has no retirement columns.

CREATE TABLE qiita.exported_entity (
    idx               BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    -- The published handle. GENERATED, not written: Postgres refuses an INSERT
    -- that supplies a value, so it cannot be forged by a caller and cannot be
    -- edited after publication. No code path composes this string.
    --
    -- NOT NULL turns a missing CASE branch into an error: the CASE has no ELSE, so a
    -- new kind added without extending it composes NULL and the INSERT fails loudly.
    -- Every prefix is letters only, so with a digits-only idx after it each handle
    -- splits into prefix and idx one way only.
    export_entity_id  VARCHAR NOT NULL GENERATED ALWAYS AS (
        CASE
            WHEN study_idx     IS NOT NULL THEN 'QS'
            WHEN biosample_idx IS NOT NULL THEN 'QB'
        END || idx
    ) STORED,

    -- The entity. Exactly one of these is non-null; see the CHECK below.
    --
    -- RESTRICT on both: see condition 2.
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
    'Public handle (export_entity_id, ''QS<n>'' for a study and ''QB<n>'' for a '
    'biosample, <n> being this table''s own idx) for one curated entity, so nothing '
    'that crosses the Qiita boundary '
    'has to carry an internal *_idx. An entity qualifies here when it has a '
    'standalone single-column identity, no hard-delete path, its own lifecycle '
    'columns, and no dependency on an external accession existing first. '
    'Carries no retirement columns: a handle is permanent and its '
    'entity''s retirement is read from the entity.';

COMMENT ON COLUMN qiita.exported_entity.export_entity_id IS
    'The published handle. GENERATED ALWAYS: unforgeable by a caller and immutable '
    'after publication.';

-- The published handle is the lookup key for anyone resolving an external usage.
-- A collision is already impossible while all prefixes follow the requirements
-- recorded above, but this index enforces uniqueness at the database level and
-- keeps the guarantee attached to the published column rather than left implicit
-- in the expression.
CREATE UNIQUE INDEX exported_entity_export_entity_id_unique
    ON qiita.exported_entity (export_entity_id);


-- =============================================================================
-- TRIGGER: mint the handle when its entity is inserted
--
-- Every study and biosample holds a handle from the transaction that creates it,
-- whichever path creates it. The handle is attributed to the entity's creator,
-- which may be a service account. Writes only to exported_entity, never to the
-- entity row: an UPDATE there would bump its updated_at and, on a biosample,
-- meet its publication lock.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.tg_mint_exported_entity()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_TABLE_NAME = 'study' THEN
        INSERT INTO qiita.exported_entity (study_idx, created_by_idx)
        VALUES (NEW.idx, NEW.created_by_idx);
    ELSIF TG_TABLE_NAME = 'biosample' THEN
        INSERT INTO qiita.exported_entity (biosample_idx, created_by_idx)
        VALUES (NEW.idx, NEW.created_by_idx);
    ELSE
        RAISE EXCEPTION 'tg_mint_exported_entity has no entity column for qiita.%', TG_TABLE_NAME;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER study_mint_exported_entity
    AFTER INSERT ON qiita.study
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_mint_exported_entity();

CREATE TRIGGER biosample_mint_exported_entity
    AFTER INSERT ON qiita.biosample
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_mint_exported_entity();


-- =============================================================================
-- BACKFILL: a handle for every entity that predates the trigger
--
-- Runs after the triggers are created, in the same transaction: creating them
-- waits out every in-flight insert and blocks new ones until commit, so each
-- entity is either visible here or minted by the trigger.
-- =============================================================================

INSERT INTO qiita.exported_entity (study_idx, created_by_idx)
SELECT idx, created_by_idx FROM qiita.study;

INSERT INTO qiita.exported_entity (biosample_idx, created_by_idx)
SELECT idx, created_by_idx FROM qiita.biosample;

-- Fail the migration, and so roll all of it back, if any entity was left without
-- a handle.
DO $$
DECLARE
    unhandled BIGINT;
BEGIN
    SELECT (SELECT count(*) FROM qiita.study s
             WHERE NOT EXISTS (SELECT 1 FROM qiita.exported_entity e
                                WHERE e.study_idx = s.idx))
         + (SELECT count(*) FROM qiita.biosample b
             WHERE NOT EXISTS (SELECT 1 FROM qiita.exported_entity e
                                WHERE e.biosample_idx = b.idx))
      INTO unhandled;
    IF unhandled > 0 THEN
        RAISE EXCEPTION 'exported_entity backfill left % entities without a handle', unhandled;
    END IF;
END $$;


-- migrate:down

DROP TRIGGER IF EXISTS study_mint_exported_entity ON qiita.study;
DROP TRIGGER IF EXISTS biosample_mint_exported_entity ON qiita.biosample;
DROP FUNCTION IF EXISTS qiita.tg_mint_exported_entity();
DROP TABLE IF EXISTS qiita.exported_entity;
