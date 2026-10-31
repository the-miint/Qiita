-- migrate:up

-- A metadata write records itself on its parent biosample or prep_sample in
-- last_metadata_change_at alone. The touch triggers' UPDATE of that column
-- also fired the parent's set_updated_at trigger, so updated_at, and the ETag
-- read from it, moved on every metadata write: a reader who may not see the
-- written value could still tell that it exists and when it changed.
--
-- Two changes close this leak. Each set_updated_at trigger skips an UPDATE
-- that moves last_metadata_change_at, which only the touch functions set. And
-- each touch function skips its UPDATE when last_metadata_change_at already
-- holds now(), as it does from the second metadata write of one transaction
-- on: that UPDATE would move nothing, so the WHEN clause would let
-- set_updated_at run.
-- updated_at then describes the record's own columns.
--
-- The touch's UPDATE takes the parent's row lock, so a metadata write waits for
-- a PATCH holding the parent FOR UPDATE, and none lands between the PATCH's
-- reads. The touch skips its UPDATE only when last_metadata_change_at already
-- equals this transaction's start time, which means this transaction's earlier
-- touch already took the lock, unless another transaction started in the same
-- microsecond.

CREATE OR REPLACE FUNCTION qiita.biosample_metadata_touch_biosample()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE qiita.biosample
       SET last_metadata_change_at = now()
     WHERE idx = NEW.biosample_idx
       AND last_metadata_change_at IS DISTINCT FROM now();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_touch_prep_sample()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE qiita.prep_sample
       SET last_metadata_change_at = now()
     WHERE idx = NEW.prep_sample_idx
       AND last_metadata_change_at IS DISTINCT FROM now();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER biosample_set_updated_at ON qiita.biosample;
CREATE TRIGGER biosample_set_updated_at
    BEFORE UPDATE ON qiita.biosample
    FOR EACH ROW
    WHEN (NEW.last_metadata_change_at IS NOT DISTINCT FROM OLD.last_metadata_change_at)
    EXECUTE FUNCTION qiita.set_updated_at();

DROP TRIGGER prep_sample_set_updated_at ON qiita.prep_sample;
CREATE TRIGGER prep_sample_set_updated_at
    BEFORE UPDATE ON qiita.prep_sample
    FOR EACH ROW
    WHEN (NEW.last_metadata_change_at IS NOT DISTINCT FROM OLD.last_metadata_change_at)
    EXECUTE FUNCTION qiita.set_updated_at();

COMMENT ON COLUMN qiita.biosample.updated_at IS
    'When the biosample''s own columns last changed. A metadata write moves '
    'last_metadata_change_at instead.';
COMMENT ON COLUMN qiita.biosample.last_metadata_change_at IS
    'When any biosample_metadata row of this biosample was last written, '
    'whatever its tier. Not returned to callers; see '
    'docs/architecture/data-model.md, "Metadata visibility tiers".';
COMMENT ON COLUMN qiita.prep_sample.updated_at IS
    'When the prep_sample''s own columns last changed. A metadata write moves '
    'last_metadata_change_at instead.';
COMMENT ON COLUMN qiita.prep_sample.last_metadata_change_at IS
    'When any prep_sample_metadata row of this prep_sample was last written, '
    'whatever its tier. Not returned to callers; see '
    'docs/architecture/data-model.md, "Metadata visibility tiers".';

-- migrate:down

COMMENT ON COLUMN qiita.prep_sample.last_metadata_change_at IS NULL;
COMMENT ON COLUMN qiita.prep_sample.updated_at IS NULL;
COMMENT ON COLUMN qiita.biosample.last_metadata_change_at IS NULL;
COMMENT ON COLUMN qiita.biosample.updated_at IS NULL;

DROP TRIGGER prep_sample_set_updated_at ON qiita.prep_sample;
CREATE TRIGGER prep_sample_set_updated_at
    BEFORE UPDATE ON qiita.prep_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();

DROP TRIGGER biosample_set_updated_at ON qiita.biosample;
CREATE TRIGGER biosample_set_updated_at
    BEFORE UPDATE ON qiita.biosample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_touch_prep_sample()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE qiita.prep_sample
       SET last_metadata_change_at = now()
     WHERE idx = NEW.prep_sample_idx;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION qiita.biosample_metadata_touch_biosample()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE qiita.biosample
       SET last_metadata_change_at = now()
     WHERE idx = NEW.biosample_idx;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
