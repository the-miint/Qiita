-- migrate:up

-- Study gains the submission-tracking pair, matching biosample and
-- sequenced_sample. last_submission_at is NULL until the submission subsystem
-- first attempts an ENA study submission, otherwise it holds the time of the
-- most recent attempt. submission_error is NULL on success and carries the
-- error message when the most recent attempt failed.
ALTER TABLE qiita.study
    ADD COLUMN last_submission_at TIMESTAMPTZ,
    ADD COLUMN submission_error   TEXT;

COMMENT ON COLUMN qiita.study.last_submission_at IS
    'NULL until the submission subsystem first attempts an ENA study '
    'submission, otherwise the time of the most recent attempt.';
COMMENT ON COLUMN qiita.study.submission_error IS
    'NULL on success; the error message from the most recent failed ENA '
    'study submission attempt.';

-- One generic clear-on-new-attempt trigger function, shared by every table
-- carrying (last_submission_at, submission_error). When a new attempt time is
-- recorded, a stale submission_error is cleared -- unless the same UPDATE also
-- sets submission_error, in which case the caller's value is kept. This lets a
-- failed-attempt caller record both fields in one UPDATE without the trigger
-- overwriting the freshly-set error.
CREATE OR REPLACE FUNCTION qiita.clear_submission_error_on_new_attempt()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.last_submission_at IS DISTINCT FROM OLD.last_submission_at
       AND NEW.submission_error IS NOT DISTINCT FROM OLD.submission_error THEN
        NEW.submission_error := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER study_clear_submission_error_on_new_attempt
    BEFORE UPDATE OF last_submission_at ON qiita.study
    FOR EACH ROW EXECUTE FUNCTION qiita.clear_submission_error_on_new_attempt();

-- Converge biosample and sequenced_sample onto the shared function, dropping
-- their byte-identical per-table copies.
DROP TRIGGER biosample_clear_submission_error_on_new_attempt ON qiita.biosample;
CREATE TRIGGER biosample_clear_submission_error_on_new_attempt
    BEFORE UPDATE OF last_submission_at ON qiita.biosample
    FOR EACH ROW EXECUTE FUNCTION qiita.clear_submission_error_on_new_attempt();
DROP FUNCTION qiita.biosample_clear_submission_error_on_new_attempt();

DROP TRIGGER sequenced_sample_clear_submission_error_on_new_attempt ON qiita.sequenced_sample;
CREATE TRIGGER sequenced_sample_clear_submission_error_on_new_attempt
    BEFORE UPDATE OF last_submission_at ON qiita.sequenced_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.clear_submission_error_on_new_attempt();
DROP FUNCTION qiita.sequenced_sample_clear_submission_error_on_new_attempt();

-- migrate:down

-- Recreate the per-table functions, re-point biosample/sequenced_sample
-- triggers back to them, then drop study's trigger, the shared function, and
-- the study columns.
CREATE OR REPLACE FUNCTION qiita.biosample_clear_submission_error_on_new_attempt()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.last_submission_at IS DISTINCT FROM OLD.last_submission_at
       AND NEW.submission_error IS NOT DISTINCT FROM OLD.submission_error THEN
        NEW.submission_error := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER biosample_clear_submission_error_on_new_attempt ON qiita.biosample;
CREATE TRIGGER biosample_clear_submission_error_on_new_attempt
    BEFORE UPDATE OF last_submission_at ON qiita.biosample
    FOR EACH ROW EXECUTE FUNCTION qiita.biosample_clear_submission_error_on_new_attempt();

CREATE OR REPLACE FUNCTION qiita.sequenced_sample_clear_submission_error_on_new_attempt()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.last_submission_at IS DISTINCT FROM OLD.last_submission_at
       AND NEW.submission_error IS NOT DISTINCT FROM OLD.submission_error THEN
        NEW.submission_error := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER sequenced_sample_clear_submission_error_on_new_attempt ON qiita.sequenced_sample;
CREATE TRIGGER sequenced_sample_clear_submission_error_on_new_attempt
    BEFORE UPDATE OF last_submission_at ON qiita.sequenced_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_clear_submission_error_on_new_attempt();

DROP TRIGGER study_clear_submission_error_on_new_attempt ON qiita.study;
DROP FUNCTION qiita.clear_submission_error_on_new_attempt();

ALTER TABLE qiita.study
    DROP COLUMN submission_error,
    DROP COLUMN last_submission_at;
