-- migrate:up

CREATE SCHEMA IF NOT EXISTS qiita;

CREATE EXTENSION IF NOT EXISTS citext;


-- =============================================================================
-- SHARED TRIGGER HELPERS
-- =============================================================================

-- Touches updated_at on UPDATE. Attach as a BEFORE UPDATE FOR EACH ROW trigger
-- on any table that carries an updated_at column; the resulting timestamp is used
-- as the ETag for optimistic-concurrency-controlled PATCH/PUT.
-- If a row's updated_at is advanced explicitly by application code
-- (e.g., touching a row to bump its ETag without changing any other column),
-- the trigger respects that: it only advances updated_at when the caller did
-- not already set it to a strictly later value in the same UPDATE.
CREATE OR REPLACE FUNCTION qiita.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.updated_at IS NULL OR NEW.updated_at <= OLD.updated_at THEN
        NEW.updated_at := now();
    END IF;
    RETURN NEW;
END;
$$;


-- migrate:down

DROP FUNCTION IF EXISTS qiita.set_updated_at();
-- citext extension intentionally not dropped: may be used elsewhere.
DROP SCHEMA IF EXISTS qiita;
