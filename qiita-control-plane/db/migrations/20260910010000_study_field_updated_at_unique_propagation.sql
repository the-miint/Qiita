-- migrate:up

-- =============================================================================
-- STUDY FIELD updated_at
-- =============================================================================
--
-- Both *_study_field tables carried created_by_idx / created_at but no
-- updated_at, so an edited field definition left no record of when it was
-- edited and nothing to build an optimistic-concurrency tag from. This adds
-- the column and the same set_updated_at() trigger the rest of the schema
-- uses.
--
-- Existing rows are NOT backfilled to their created_at, following the
-- migration that added updated_at to the *_metadata tables: ADD COLUMN with a
-- non-volatile default takes the fast-default path and fires no row triggers,
-- and the value's only consumer treats it as opaque.

ALTER TABLE qiita.biosample_study_field
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE qiita.prep_sample_study_field
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

COMMENT ON COLUMN qiita.biosample_study_field.updated_at IS
    'Bumped on every UPDATE by the set_updated_at() trigger; used as the ETag '
    'for optimistic-concurrency control on PATCH. The trigger is unscoped, so '
    'it tracks any change to the row, not only a change a caller asked for. '
    'Rows that predate this column carry the timestamp of the migration that '
    'added it, not their created_at.';

COMMENT ON COLUMN qiita.prep_sample_study_field.updated_at IS
    'Bumped on every UPDATE by the set_updated_at() trigger; used as the ETag '
    'for optimistic-concurrency control on PATCH. The trigger is unscoped, so '
    'it tracks any change to the row, not only a change a caller asked for. '
    'Rows that predate this column carry the timestamp of the migration that '
    'added it, not their created_at.';

CREATE TRIGGER biosample_study_field_set_updated_at
    BEFORE UPDATE ON qiita.biosample_study_field
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();

CREATE TRIGGER prep_sample_study_field_set_updated_at
    BEFORE UPDATE ON qiita.prep_sample_study_field
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- =============================================================================
-- TRIGGER: tg_propagate_unique_in_study
--
-- Mirrors a study field's unique_in_study into the denormalized column on
-- every metadata row written through that field, so the partial unique indexes
-- and the no-missing-value CHECK keep describing the field's current policy
-- rather than the policy it was minted with.
--
-- One function for both stacks, taking its target metadata table and that
-- table's study-field FK column through TG_ARGV, in the manner of
-- tg_principal_must_be_user: the two stacks differ only in those identifiers,
-- which arrive as literals from the CREATE TRIGGER statements below and never
-- from caller input.
--
-- Both directions propagate, and neither is rejected here:
--
--   false -> true: the UPDATE below is gated by the partial unique indexes and
--     the no-missing-value CHECK. A study that already holds a duplicate, or a
--     missing-value marker, through this field has the flag change rejected by
--     those constraints, which rolls back the UPDATE on the study field. That
--     rejection is the intended answer, so this function adds no check of its
--     own -- a second copy would drift from the constraints that decide.
--
--   true -> false: relaxing a constraint cannot violate one, so it propagates
--     unconditionally.
--
-- Concurrent metadata INSERTs are not serialized against this. A row whose
-- field-contract trigger read the old flag before this statement's snapshot
-- commits after it, keeping the stale value -- and so staying outside the
-- indexes and the CHECK -- until the next write through the field. An UPDATE is
-- unaffected: it collides with this statement's row lock and re-reads the
-- committed field row. Closing the INSERT window means a shared lock on the
-- field row for every metadata write, permanently, which this write volume does
-- not justify.
--
-- The propagation bumps each metadata row's own updated_at and, through the
-- touch trigger, its parent entity's last_metadata_change_at and ETag: a policy
-- change is a change to the row so a flip invalidates outstanding ETags in the
-- study. The global-link propagation already behaves this way.
-- =============================================================================

CREATE FUNCTION qiita.tg_propagate_unique_in_study() RETURNS trigger AS $$
DECLARE
    metadata_table    TEXT := TG_ARGV[0];
    study_field_column TEXT := TG_ARGV[1];
BEGIN
    -- AFTER UPDATE OF filters by column-touched, but the column may sit in the
    -- SET clause without a real value change.
    IF NEW.unique_in_study IS NOT DISTINCT FROM OLD.unique_in_study THEN
        RETURN NEW;
    END IF;

    EXECUTE format(
        'UPDATE qiita.%I SET unique_in_study = $1 WHERE %I = $2',
        metadata_table, study_field_column
    ) USING NEW.unique_in_study, NEW.idx;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_study_field_propagate_unique_in_study
    AFTER UPDATE OF unique_in_study ON qiita.biosample_study_field
    FOR EACH ROW EXECUTE FUNCTION
        qiita.tg_propagate_unique_in_study('biosample_metadata', 'biosample_study_field_idx');

CREATE TRIGGER prep_sample_study_field_propagate_unique_in_study
    AFTER UPDATE OF unique_in_study ON qiita.prep_sample_study_field
    FOR EACH ROW EXECUTE FUNCTION
        qiita.tg_propagate_unique_in_study('prep_sample_metadata', 'prep_sample_study_field_idx');


-- migrate:down

DROP TRIGGER IF EXISTS prep_sample_study_field_propagate_unique_in_study
    ON qiita.prep_sample_study_field;
DROP TRIGGER IF EXISTS biosample_study_field_propagate_unique_in_study
    ON qiita.biosample_study_field;
DROP FUNCTION IF EXISTS qiita.tg_propagate_unique_in_study();

DROP TRIGGER IF EXISTS prep_sample_study_field_set_updated_at ON qiita.prep_sample_study_field;
DROP TRIGGER IF EXISTS biosample_study_field_set_updated_at ON qiita.biosample_study_field;

ALTER TABLE qiita.prep_sample_study_field DROP COLUMN IF EXISTS updated_at;
ALTER TABLE qiita.biosample_study_field DROP COLUMN IF EXISTS updated_at;
