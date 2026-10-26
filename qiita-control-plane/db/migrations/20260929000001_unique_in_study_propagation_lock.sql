-- migrate:up

-- =============================================================================
-- SERIALIZE unique_in_study PROPAGATION AGAINST METADATA WRITERS
-- =============================================================================
--
-- The propagation takes a table lock that conflicts with every concurrent
-- writer, so a metadata INSERT either commits before the propagation reads
-- the field's values -- and is caught by the partial unique indexes or the
-- no-missing-value CHECK, rejecting the flip -- or cannot start until the
-- flip's transaction has ended, and reads the new policy. Without it such an
-- INSERT lands carrying the pre-flip flag, outside both, and stays that way
-- until that row itself is written again.
--
-- Only false -> true is locked. Relaxing a constraint cannot violate one, so
-- nothing has to be serialized against it.
--
-- The lock mode, its dependence on the writer running at READ COMMITTED, its
-- table-wide cost, and why the bound is the caller's rather than a value
-- chosen here are all stated on the same lock in
-- qiita.widen_study_field_to_text and are not repeated here. The timeout the
-- API path supplies lives on the constant in routes/_helpers.py. A migration
-- that tightens fields in bulk is such a caller and must set its own bound;
-- dbmate supplies none.

CREATE OR REPLACE FUNCTION qiita.tg_propagate_unique_in_study() RETURNS trigger AS $$
DECLARE
    metadata_table    TEXT := TG_ARGV[0];
    study_field_column TEXT := TG_ARGV[1];
    lock_timeout_ms   BIGINT;
BEGIN
    -- AFTER UPDATE OF filters by column-touched, but the column may sit in the
    -- SET clause without a real value change.
    IF NEW.unique_in_study IS NOT DISTINCT FROM OLD.unique_in_study THEN
        RETURN NEW;
    END IF;

    IF NEW.unique_in_study THEN
        -- same-pattern-ok: the bound is read the same way in
        -- widen_study_field_to_text, where why it is read this way is stated.
        SELECT setting::BIGINT INTO lock_timeout_ms
          FROM pg_settings WHERE name = 'lock_timeout';

        IF lock_timeout_ms = 0 THEN
            RAISE EXCEPTION 'unique_in_study propagation requires a bounded lock_timeout'
              USING ERRCODE = '55000',
                    HINT = 'SET LOCAL lock_timeout in the same transaction as the UPDATE';
        END IF;

        -- same-pattern-ok: widen_study_field_to_text locks the same tables for
        -- the same reason; the mode is the invariant and must not differ.
        EXECUTE format(
            'LOCK TABLE qiita.%I IN SHARE ROW EXCLUSIVE MODE', metadata_table);
    END IF;

    EXECUTE format(
        'UPDATE qiita.%I SET unique_in_study = $1 WHERE %I = $2',
        metadata_table, study_field_column
    ) USING NEW.unique_in_study, NEW.idx;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION qiita.tg_propagate_unique_in_study() IS
    'Mirrors a study field''s unique_in_study into the denormalized column on '
    'every metadata row written through that field. Tightening locks the '
    'metadata table against concurrent writers for the caller''s transaction, '
    'and refuses to take that lock without a bounded lock_timeout, raising '
    'SQLSTATE 55000 in its place; the row lock the triggering UPDATE takes is '
    'already held by then and is the caller''s to bound. Relaxing takes no '
    'lock. A flip a study''s existing data cannot satisfy is rejected by the '
    'partial unique indexes or the no-missing-value CHECK, which rolls the '
    'flip back.';


-- migrate:down

COMMENT ON FUNCTION qiita.tg_propagate_unique_in_study() IS NULL;

CREATE OR REPLACE FUNCTION qiita.tg_propagate_unique_in_study() RETURNS trigger AS $$
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
