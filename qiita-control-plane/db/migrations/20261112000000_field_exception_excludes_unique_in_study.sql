-- migrate:up

-- =============================================================================
-- A VALUE-LEVEL EXCEPTION AND unique_in_study EXCLUDE EACH OTHER
-- =============================================================================
--
-- A unique_in_study field refuses a duplicate of any value stored through it,
-- including one the writer may not read. With an exception on that value, the
-- refusal would tell a writer below the exception's tier that the hidden value
-- equals the one they sent. Without one, every value through the field
-- requires the field's own tier, which anyone allowed to write the field
-- reaches, so the refusal tells them nothing they could not read.
--
-- Refused both ways: an exception keyed on a unique_in_study field, and
-- declaring unique_in_study over a field that exceptions key on. An exception
-- keyed on a global field never meets the flag, which only a purely-local
-- field carries.
--
-- The two serialize on the study field's row: the exception side reads it
-- FOR SHARE, which waits behind an uncommitted declaration and then reads its
-- outcome, and the declaration's UPDATE waits behind that share lock and then,
-- at READ COMMITTED, sees the committed exception. Each raises check_violation
-- naming the rule as its constraint, which the routes map to a 409.
-- docs/architecture/data-model.md, "Metadata visibility tiers", states the rule.

CREATE FUNCTION qiita.tg_field_exception_not_on_unique_field() RETURNS trigger AS $$
DECLARE
    study_field_table  TEXT := TG_ARGV[0];
    study_field_column TEXT := TG_ARGV[1];
    rule_name          TEXT := TG_ARGV[2];
    keyed_field_idx    BIGINT;
    is_unique          BOOLEAN;
BEGIN
    EXECUTE format('SELECT ($1).%I', study_field_column) INTO keyed_field_idx USING NEW;
    IF keyed_field_idx IS NULL THEN
        RETURN NEW;
    END IF;

    EXECUTE format(
        'SELECT unique_in_study FROM qiita.%I WHERE idx = $1 FOR SHARE', study_field_table
    ) INTO is_unique USING keyed_field_idx;

    IF is_unique THEN
        RAISE EXCEPTION 'a value-level exception cannot key on a unique_in_study field'
          USING ERRCODE = 'check_violation',
                CONSTRAINT = rule_name,
                DETAIL = format('%s %s', study_field_table, keyed_field_idx);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION qiita.tg_study_field_unique_without_exceptions() RETURNS trigger AS $$
DECLARE
    exception_table    TEXT := TG_ARGV[0];
    study_field_column TEXT := TG_ARGV[1];
    rule_name          TEXT := TG_ARGV[2];
    has_exception      BOOLEAN;
BEGIN
    IF NOT NEW.unique_in_study OR OLD.unique_in_study THEN
        RETURN NEW;
    END IF;

    EXECUTE format(
        'SELECT EXISTS (SELECT 1 FROM qiita.%I WHERE %I = $1)',
        exception_table, study_field_column
    ) INTO has_exception USING NEW.idx;

    IF has_exception THEN
        RAISE EXCEPTION 'unique_in_study cannot be declared on a field that value-level exceptions key on'
          USING ERRCODE = 'check_violation',
                CONSTRAINT = rule_name,
                DETAIL = format('%s %s', TG_TABLE_NAME, NEW.idx);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_field_exception_not_on_unique_field
    BEFORE INSERT OR UPDATE OF biosample_study_field_idx ON qiita.biosample_field_exception
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_field_exception_not_on_unique_field(
        'biosample_study_field', 'biosample_study_field_idx',
        'biosample_field_exception_not_on_unique_field');

CREATE TRIGGER prep_sample_field_exception_not_on_unique_field
    BEFORE INSERT OR UPDATE OF prep_sample_study_field_idx ON qiita.prep_sample_field_exception
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_field_exception_not_on_unique_field(
        'prep_sample_study_field', 'prep_sample_study_field_idx',
        'prep_sample_field_exception_not_on_unique_field');

CREATE TRIGGER biosample_study_field_unique_without_exceptions
    BEFORE UPDATE OF unique_in_study ON qiita.biosample_study_field
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_study_field_unique_without_exceptions(
        'biosample_field_exception', 'biosample_study_field_idx',
        'biosample_study_field_unique_without_exceptions');

CREATE TRIGGER prep_sample_study_field_unique_without_exceptions
    BEFORE UPDATE OF unique_in_study ON qiita.prep_sample_study_field
    FOR EACH ROW EXECUTE FUNCTION qiita.tg_study_field_unique_without_exceptions(
        'prep_sample_field_exception', 'prep_sample_study_field_idx',
        'prep_sample_study_field_unique_without_exceptions');

-- migrate:down

DROP TRIGGER IF EXISTS prep_sample_study_field_unique_without_exceptions
    ON qiita.prep_sample_study_field;
DROP TRIGGER IF EXISTS biosample_study_field_unique_without_exceptions
    ON qiita.biosample_study_field;
DROP TRIGGER IF EXISTS prep_sample_field_exception_not_on_unique_field
    ON qiita.prep_sample_field_exception;
DROP TRIGGER IF EXISTS biosample_field_exception_not_on_unique_field
    ON qiita.biosample_field_exception;
DROP FUNCTION IF EXISTS qiita.tg_study_field_unique_without_exceptions();
DROP FUNCTION IF EXISTS qiita.tg_field_exception_not_on_unique_field();
