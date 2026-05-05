-- migrate:up

-- =============================================================================
-- ROLE-TYPED FK TRIGGERS
-- =============================================================================
-- The principal/subtype model uses a single principal(idx) identifier and
-- 0..1 subtype rows in qiita.user / qiita.service_account. Consumer FK
-- columns therefore can't directly express "must reference a user-kind
-- principal" with a plain REFERENCES — every consumer points at
-- qiita.principal(idx). This module adds two trigger functions that
-- together close that gap at the DB level:
--
--   tg_principal_must_be_user      — fires BEFORE INSERT/UPDATE on each
--                                    consumer's role-typed column; raises
--                                    if qiita.user has no row for the new
--                                    value.
--
--   tg_user_role_ref_blocks_delete — fires BEFORE DELETE on qiita.user;
--                                    raises if any registered consumer
--                                    column still references the
--                                    departing principal.
--
-- Both functions take their target (column name on the consumer side; or
-- (schema, table, column) tuple on the qiita.user side) via TG_ARGV so
-- one function services every role-typed column. Adding a new role-typed
-- column is therefore symmetric: one trigger on the consumer for INSERT/
-- UPDATE and one on qiita.user for DELETE.
--
-- Both triggers acquire pg_advisory_xact_lock(principal_idx) so a
-- concurrent INSERT-on-consumer / DELETE-on-user pair cannot pass-pass:
-- whichever arrives second blocks until the first commits, then re-checks
-- against committed state. The lock key is the bare principal_idx — the
-- same single-arg form used by tg_principal_subtype_exclusion — so all
-- subtype-shaped invariants share one lock space per principal.


CREATE FUNCTION qiita.tg_principal_must_be_user() RETURNS trigger AS $$
DECLARE
    column_name TEXT := TG_ARGV[0];
    new_value   BIGINT;
BEGIN
    EXECUTE format('SELECT ($1).%I', column_name) INTO new_value USING NEW;

    -- Nullable role columns: a NULL is allowed here; column NOT NULL is
    -- enforced separately by the column declaration where wanted.
    IF new_value IS NULL THEN
        RETURN NEW;
    END IF;

    -- Skip if the column value didn't change on UPDATE (ON UPDATE OF
    -- already filters by column-touched, but the column may be in the
    -- SET clause without a real value change). The lookup is cheap; the
    -- early return saves the lock + EXISTS pair.
    IF TG_OP = 'UPDATE' THEN
        DECLARE
            old_value BIGINT;
        BEGIN
            EXECUTE format('SELECT ($1).%I', column_name) INTO old_value USING OLD;
            IF old_value IS NOT DISTINCT FROM new_value THEN
                RETURN NEW;
            END IF;
        END;
    END IF;

    PERFORM pg_advisory_xact_lock(new_value);

    IF NOT EXISTS (SELECT 1 FROM qiita.user WHERE principal_idx = new_value) THEN
        RAISE EXCEPTION
            '%.% must reference a user-kind principal; principal_idx=% has no qiita.user row',
            TG_TABLE_NAME, column_name, new_value;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION qiita.tg_user_role_ref_blocks_delete() RETURNS trigger AS $$
DECLARE
    consumer_schema TEXT := TG_ARGV[0];
    consumer_table  TEXT := TG_ARGV[1];
    consumer_column TEXT := TG_ARGV[2];
    blocking_count  BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(OLD.principal_idx);

    EXECUTE format(
        'SELECT count(*) FROM %I.%I WHERE %I = $1',
        consumer_schema, consumer_table, consumer_column
    )
      INTO blocking_count
      USING OLD.principal_idx;

    IF blocking_count > 0 THEN
        RAISE EXCEPTION
            'cannot delete qiita.user(principal_idx=%): % rows in %.%(%) still reference it',
            OLD.principal_idx, blocking_count, consumer_schema, consumer_table, consumer_column;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- REGISTRATIONS — qiita.study
-- =============================================================================
-- owner_idx and principal_investigator_idx are user-only (humans).
-- created_by_idx is intentionally NOT registered: bulk imports and admin
-- tools legitimately set it to a service account or the system principal.

CREATE TRIGGER study_owner_must_be_user
    BEFORE INSERT OR UPDATE OF owner_idx ON qiita.study
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_principal_must_be_user('owner_idx');

CREATE TRIGGER study_pi_must_be_user
    BEFORE INSERT OR UPDATE OF principal_investigator_idx ON qiita.study
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_principal_must_be_user('principal_investigator_idx');

CREATE TRIGGER user_no_delete_if_study_owner
    BEFORE DELETE ON qiita.user
    FOR EACH ROW
    EXECUTE FUNCTION qiita.tg_user_role_ref_blocks_delete('qiita', 'study', 'owner_idx');

CREATE TRIGGER user_no_delete_if_study_pi
    BEFORE DELETE ON qiita.user
    FOR EACH ROW
    EXECUTE FUNCTION qiita.tg_user_role_ref_blocks_delete('qiita', 'study', 'principal_investigator_idx');


-- Column comments document the otherwise-invisible constraint, since
-- triggers don't show up in \d like a real FK does.

COMMENT ON COLUMN qiita.study.owner_idx IS
    'Principal that owns the study. Must be a user-kind principal (enforced by '
    'study_owner_must_be_user trigger). qiita.user DELETE is blocked while a '
    'study still references this column (user_no_delete_if_study_owner trigger).';

COMMENT ON COLUMN qiita.study.principal_investigator_idx IS
    'Principal investigator. Must be a user-kind principal (enforced by '
    'study_pi_must_be_user trigger). qiita.user DELETE is blocked while a '
    'study still references this column (user_no_delete_if_study_pi trigger).';


-- =============================================================================
-- REGISTRATIONS — qiita.biosample
-- =============================================================================
-- owner_idx is user-only (humans). created_by_idx and retired_by_idx are
-- intentionally NOT registered: bulk imports and admin tools legitimately
-- set them to a service account or the system principal.

CREATE TRIGGER biosample_owner_must_be_user
    BEFORE INSERT OR UPDATE OF owner_idx ON qiita.biosample
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_principal_must_be_user('owner_idx');

CREATE TRIGGER user_no_delete_if_biosample_owner
    BEFORE DELETE ON qiita.user
    FOR EACH ROW
    EXECUTE FUNCTION qiita.tg_user_role_ref_blocks_delete('qiita', 'biosample', 'owner_idx');


COMMENT ON COLUMN qiita.biosample.owner_idx IS
    'Principal that owns the biosample. Must be a user-kind principal (enforced by '
    'biosample_owner_must_be_user trigger). qiita.user DELETE is blocked while a '
    'biosample still references this column (user_no_delete_if_biosample_owner trigger).';


-- migrate:down

COMMENT ON COLUMN qiita.biosample.owner_idx IS NULL;
COMMENT ON COLUMN qiita.study.principal_investigator_idx IS NULL;
COMMENT ON COLUMN qiita.study.owner_idx IS NULL;

DROP TRIGGER IF EXISTS user_no_delete_if_biosample_owner ON qiita.user;
DROP TRIGGER IF EXISTS biosample_owner_must_be_user ON qiita.biosample;
DROP TRIGGER IF EXISTS user_no_delete_if_study_pi ON qiita.user;
DROP TRIGGER IF EXISTS user_no_delete_if_study_owner ON qiita.user;
DROP TRIGGER IF EXISTS study_pi_must_be_user ON qiita.study;
DROP TRIGGER IF EXISTS study_owner_must_be_user ON qiita.study;

DROP FUNCTION IF EXISTS qiita.tg_user_role_ref_blocks_delete();
DROP FUNCTION IF EXISTS qiita.tg_principal_must_be_user();
