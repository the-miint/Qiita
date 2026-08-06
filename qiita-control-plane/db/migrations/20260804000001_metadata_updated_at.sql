-- migrate:up

-- =============================================================================
-- METADATA updated_at
-- =============================================================================
--
-- Both *_metadata tables carried created_by_idx / created_at but no
-- updated_at, so an overwritten value left no record of when the overwrite
-- happened. This adds the column and the same set_updated_at() trigger the
-- rest of the schema uses.
--
-- Existing rows are NOT backfilled to their created_at. The backfill UPDATE
-- would be rejected outright by the publication-lock trigger on any published
-- row (aborting the migration), and would fire the touch trigger for every
-- row, bumping each parent's last_metadata_change_at and ETag for a write
-- that changed no value. ADD COLUMN with a non-volatile default fires no row
-- triggers and takes the fast-default path, so existing rows uniformly carry
-- the migration timestamp instead.

ALTER TABLE qiita.biosample_metadata
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE qiita.prep_sample_metadata
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

COMMENT ON COLUMN qiita.biosample_metadata.updated_at IS
    'Bumped on every UPDATE by the set_updated_at() trigger. The trigger is '
    'unscoped, so a global_field_idx denormalization written by the '
    'local-to-global field upgrade bumps it too: the column tracks any change '
    'to the row, not only a change of value. Rows that predate this column '
    'carry the timestamp of the migration that added it, not their created_at.';

COMMENT ON COLUMN qiita.prep_sample_metadata.updated_at IS
    'Bumped on every UPDATE by the set_updated_at() trigger. The trigger is '
    'unscoped, so a global_field_idx denormalization written by the '
    'local-to-global field upgrade bumps it too: the column tracks any change '
    'to the row, not only a change of value. Rows that predate this column '
    'carry the timestamp of the migration that added it, not their created_at.';

CREATE TRIGGER biosample_metadata_set_updated_at
    BEFORE UPDATE ON qiita.biosample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();

CREATE TRIGGER prep_sample_metadata_set_updated_at
    BEFORE UPDATE ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- migrate:down

DROP TRIGGER IF EXISTS biosample_metadata_set_updated_at ON qiita.biosample_metadata;
DROP TRIGGER IF EXISTS prep_sample_metadata_set_updated_at ON qiita.prep_sample_metadata;

ALTER TABLE qiita.biosample_metadata
    DROP COLUMN IF EXISTS updated_at;

ALTER TABLE qiita.prep_sample_metadata
    DROP COLUMN IF EXISTS updated_at;
