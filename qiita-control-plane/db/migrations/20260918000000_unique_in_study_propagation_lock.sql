-- migrate:up

-- =============================================================================
-- SERIALIZE unique_in_study PROPAGATION AGAINST METADATA WRITERS
-- =============================================================================
--
-- The propagation trigger previously left a window open: a metadata INSERT in
-- flight read the pre-flip flag through the contract trigger and landed
-- carrying it -- outside the partial unique indexes and the no-missing-value
-- CHECK -- and stayed that way until that row itself was written again. The
-- trigger now takes a table lock that conflicts with every concurrent writer,
-- so an INSERT either commits before the propagation reads (and is caught by
-- the indexes, rejecting the flip) or cannot start until the flip's
-- transaction has ended (and reads the new policy).
--
-- 20260910010000_study_field_updated_at_unique_propagation.sql weighs that
-- same window against a FOR SHARE on the field row and decides to leave it
-- open. It is closed here, by a different mechanism, so that file's reasoning
-- is superseded rather than current; it is not edited because it has already
-- been applied.
--
-- Only false -> true is locked. Relaxing a constraint cannot violate one, so
-- nothing has to be serialized against it.
--
-- The lock mode, its dependence on the writer running at READ COMMITTED, its
-- table-wide cost, and why the bound is the caller's rather than a value
-- chosen here are all stated on the same lock in
-- 20260915000000_sample_field_widen_fn.sql and are not repeated here. The
-- timeout the API path supplies, and why it is the size it is, live on the
-- constant in routes/_helpers.py. A migration that tightens fields in bulk is
-- such a caller and must set its own bound; dbmate supplies none.

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
    'metadata table against concurrent writers for the caller''s transaction '
    'and refuses to run without a bounded lock_timeout, raising SQLSTATE 55000; '
    'relaxing takes no lock. A flip a study''s existing data cannot satisfy is '
    'rejected by the partial unique indexes or the no-missing-value CHECK, '
    'which rolls the flip back.';


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
