-- migrate:up

-- =============================================================================
-- PUBLICATION LOCK
-- =============================================================================
--
-- A prep_sample participates in a "public record" via its
-- prep_sample_to_study links. Each link carries an is_published flag:
-- when TRUE, the prep_sample (with its 1:1 sequenced_sample subtype, its
-- prep_sample_metadata rows, the published link itself, and the underlying
-- biosample plus its metadata / study-links) is frozen against mutation.
--
-- WHAT SETS is_published. One path: an owner-driven publish action
-- (future PR) explicitly flips a prep_sample_to_study.is_published from
-- FALSE to TRUE. Publication is a deliberate act.
--
-- ENA accessions do NOT set is_published, and this migration wires no
-- trigger from them. An ENA accession (biosample.ena_sample_accession,
-- sequenced_sample.ena_experiment_accession / ena_run_accession) records
-- that the row was submitted to ENA -- and an ENA submission can sit
-- under embargo, accession already assigned but the data not yet
-- publicly released. "Has an accession" is a submission-tracking fact,
-- not a publication fact; the two are independent. Conflating them would
-- freeze records that are merely submitted (and still legitimately
-- editable before release) and would also miss owner-published records
-- that never went to ENA at all.
--
-- DIRECTION. Publication freezes UPWARD, never downward. A published
-- prep freezes its own rows AND the biosample beneath it (the biosample
-- sits under the published prep's record). It does NOT freeze downward:
-- a biosample that is frozen because one prep on it is published leaves
-- every OTHER, unpublished prep on that same biosample freely mutable --
-- a specimen can carry preps that were never published. The per-prep
-- is_prep_sample_published check gives this for free; the biosample is
-- reached only via is_biosample_reaching_published_prep.
--
-- The lock-trigger family is BEFORE UPDATE on every row type a published
-- prep transitively reaches:
--     prep_sample, sequenced_sample, prep_sample_metadata,
--     prep_sample_to_study (the published link), biosample,
--     biosample_metadata, biosample_to_study.
-- Each lock trigger asks "is OLD already published?" (via the relevant
-- relationship) and rejects the UPDATE if so. The publish action's own
-- FALSE -> TRUE UPDATE on the link is not blocked: pre-write
-- OLD.is_published is still FALSE, so the lock lets the publication
-- through; every UPDATE after that sees TRUE and is rejected.
--
-- The retire path is an UPDATE (sets retired = TRUE), which the lock
-- trigger catches when the row is published -- correct behavior, since
-- retiring a published row would silently strand the public-facing
-- pointer. A separate always-reject BEFORE DELETE trigger on prep_sample
-- and sequenced_sample is deferred to a follow-up: the existing
-- test-cleanup helper uses DELETE FROM, and adding the never-DELETE
-- trigger here without a session-var bypass would break every test that
-- seeds a prep_sample.
--
-- TRIPWIRE -- no system_admin / wet_lab_admin bypass is wired in this
-- migration. If future operations need to mutate published data, the
-- right shape is a session-scoped GUC (set_config(
-- 'qiita.bypass_publication_lock', 'on', TRUE)) that each lock trigger
-- checks first. There are SEVEN lock triggers (publication_lock_*
-- below); a GUC implementer must add the bypass check to EVERY one or
-- the lock becomes selectively porous. The COMMENT ON TRIGGER strings
-- at the bottom of this file repeat this note so a `\d+` reader sees it.
--
-- The is_published column itself lives in the prep_sample_to_study
-- CREATE TABLE (20260501000011_prep_sample.sql), not here -- this
-- migration owns only the lock-trigger behavior over it.


-- =============================================================================
-- LOOKUP HELPERS (used by the lock triggers)
-- =============================================================================

-- "Is any prep_sample_to_study link for this prep published?"
CREATE OR REPLACE FUNCTION qiita.is_prep_sample_published(p_prep_sample_idx BIGINT)
RETURNS BOOLEAN AS $$
    SELECT EXISTS (
        SELECT 1 FROM qiita.prep_sample_to_study
         WHERE prep_sample_idx = p_prep_sample_idx
           AND is_published = TRUE
    );
$$ LANGUAGE sql STABLE;

-- "Does this biosample reach ANY published prep_sample (via prep_sample
-- ownership of this biosample)?" The join lives in the function rather
-- than in each caller so the lock triggers stay short. This is the
-- upward direction: a published prep freezes the biosample beneath it.
CREATE OR REPLACE FUNCTION qiita.is_biosample_reaching_published_prep(p_biosample_idx BIGINT)
RETURNS BOOLEAN AS $$
    SELECT EXISTS (
        SELECT 1 FROM qiita.prep_sample ps
          JOIN qiita.prep_sample_to_study pts ON pts.prep_sample_idx = ps.idx
         WHERE ps.biosample_idx = p_biosample_idx
           AND pts.is_published = TRUE
    );
$$ LANGUAGE sql STABLE;


-- =============================================================================
-- LOCK TRIGGERS (BEFORE UPDATE on every row a published prep reaches)
-- =============================================================================
--
-- Each lock trigger raises with SQLSTATE 'P0001' so callers can
-- distinguish "publication-lock rejection" from unrelated trigger
-- failures by SQLSTATE rather than by parsing the message text.

CREATE OR REPLACE FUNCTION qiita.publication_lock_prep_sample()
RETURNS TRIGGER AS $$
BEGIN
    IF qiita.is_prep_sample_published(OLD.idx) THEN
        RAISE EXCEPTION
            'prep_sample % is published (via prep_sample_to_study.is_published) and is immutable',
            OLD.idx
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_publication_lock
    BEFORE UPDATE ON qiita.prep_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.publication_lock_prep_sample();


-- sequenced_sample shares the prep_sample's lock state via the back-
-- pointing OLD.prep_sample_idx column: it is mutable until one of the
-- prep's links is published, and frozen against every UPDATE afterwards.
CREATE OR REPLACE FUNCTION qiita.publication_lock_sequenced_sample()
RETURNS TRIGGER AS $$
BEGIN
    IF qiita.is_prep_sample_published(OLD.prep_sample_idx) THEN
        RAISE EXCEPTION
            'sequenced_sample % (prep_sample %) is on a published prep_sample and is immutable',
            OLD.idx, OLD.prep_sample_idx
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_publication_lock
    BEFORE UPDATE ON qiita.sequenced_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.publication_lock_sequenced_sample();


CREATE OR REPLACE FUNCTION qiita.publication_lock_prep_sample_metadata()
RETURNS TRIGGER AS $$
BEGIN
    IF qiita.is_prep_sample_published(OLD.prep_sample_idx) THEN
        RAISE EXCEPTION
            'prep_sample_metadata % refers to published prep_sample % and is immutable',
            OLD.idx, OLD.prep_sample_idx
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_metadata_publication_lock
    BEFORE UPDATE ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.publication_lock_prep_sample_metadata();


-- The link itself: the publish action's FALSE -> TRUE UPDATE passes
-- because OLD.is_published is still FALSE at BEFORE-UPDATE time; once
-- TRUE, every further UPDATE on the row -- retire (retired = TRUE) or
-- unpublish (is_published back to FALSE) -- is rejected.
CREATE OR REPLACE FUNCTION qiita.publication_lock_prep_sample_to_study()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.is_published = TRUE THEN
        RAISE EXCEPTION
            'prep_sample_to_study(%, %) is published and is immutable (cannot retire or unpublish)',
            OLD.prep_sample_idx, OLD.study_idx
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_to_study_publication_lock
    BEFORE UPDATE ON qiita.prep_sample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.publication_lock_prep_sample_to_study();


CREATE OR REPLACE FUNCTION qiita.publication_lock_biosample()
RETURNS TRIGGER AS $$
BEGIN
    IF qiita.is_biosample_reaching_published_prep(OLD.idx) THEN
        RAISE EXCEPTION
            'biosample % is referenced by a published prep_sample and is immutable',
            OLD.idx
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_publication_lock
    BEFORE UPDATE ON qiita.biosample
    FOR EACH ROW EXECUTE FUNCTION qiita.publication_lock_biosample();


CREATE OR REPLACE FUNCTION qiita.publication_lock_biosample_metadata()
RETURNS TRIGGER AS $$
BEGIN
    IF qiita.is_biosample_reaching_published_prep(OLD.biosample_idx) THEN
        RAISE EXCEPTION
            'biosample_metadata % refers to biosample % which is referenced by a published prep_sample and is immutable',
            OLD.idx, OLD.biosample_idx
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_metadata_publication_lock
    BEFORE UPDATE ON qiita.biosample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.publication_lock_biosample_metadata();


CREATE OR REPLACE FUNCTION qiita.publication_lock_biosample_to_study()
RETURNS TRIGGER AS $$
BEGIN
    IF qiita.is_biosample_reaching_published_prep(OLD.biosample_idx) THEN
        RAISE EXCEPTION
            'biosample_to_study(%, %): biosample % is referenced by a published prep_sample and the link is immutable',
            OLD.biosample_idx, OLD.study_idx, OLD.biosample_idx
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_to_study_publication_lock
    BEFORE UPDATE ON qiita.biosample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.publication_lock_biosample_to_study();


-- The never-DELETE guardrail on prep_sample and sequenced_sample
-- (always-reject BEFORE DELETE trigger) is deferred to a follow-up
-- migration that ships alongside a test-cleanup bypass. The current
-- test-suite cleanup helper DELETEs prep_sample rows; adding the
-- guardrail here without a coordinated bypass would break every
-- existing test that seeds a prep_sample.


-- =============================================================================
-- TRIGGER COMMENTS
-- =============================================================================
-- Surfaced via `\d+ <table>` / pg_description so the publication-lock
-- policy is visible from the catalog, not only by reading this file.

COMMENT ON TRIGGER prep_sample_publication_lock ON qiita.prep_sample IS
    'Publication lock: rejects UPDATE when the prep has any published link. '
    'TRIPWIRE: a qiita.bypass_publication_lock GUC must be checked here too.';
COMMENT ON TRIGGER sequenced_sample_publication_lock ON qiita.sequenced_sample IS
    'Publication lock: rejects UPDATE when the parent prep is published. '
    'TRIPWIRE: a qiita.bypass_publication_lock GUC must be checked here too.';
COMMENT ON TRIGGER prep_sample_metadata_publication_lock ON qiita.prep_sample_metadata IS
    'Publication lock: rejects UPDATE when the owning prep is published. '
    'TRIPWIRE: a qiita.bypass_publication_lock GUC must be checked here too.';
COMMENT ON TRIGGER prep_sample_to_study_publication_lock ON qiita.prep_sample_to_study IS
    'Publication lock: rejects UPDATE of a published link (retire/unpublish). '
    'TRIPWIRE: a qiita.bypass_publication_lock GUC must be checked here too.';
COMMENT ON TRIGGER biosample_publication_lock ON qiita.biosample IS
    'Publication lock: rejects UPDATE when the biosample reaches a published prep. '
    'TRIPWIRE: a qiita.bypass_publication_lock GUC must be checked here too.';
COMMENT ON TRIGGER biosample_metadata_publication_lock ON qiita.biosample_metadata IS
    'Publication lock: rejects UPDATE when the biosample reaches a published prep. '
    'TRIPWIRE: a qiita.bypass_publication_lock GUC must be checked here too.';
COMMENT ON TRIGGER biosample_to_study_publication_lock ON qiita.biosample_to_study IS
    'Publication lock: rejects UPDATE when the biosample reaches a published prep. '
    'TRIPWIRE: a qiita.bypass_publication_lock GUC must be checked here too.';


-- migrate:down

DROP TRIGGER IF EXISTS biosample_to_study_publication_lock ON qiita.biosample_to_study;
DROP TRIGGER IF EXISTS biosample_metadata_publication_lock ON qiita.biosample_metadata;
DROP TRIGGER IF EXISTS biosample_publication_lock ON qiita.biosample;
DROP TRIGGER IF EXISTS prep_sample_to_study_publication_lock ON qiita.prep_sample_to_study;
DROP TRIGGER IF EXISTS prep_sample_metadata_publication_lock ON qiita.prep_sample_metadata;
DROP TRIGGER IF EXISTS sequenced_sample_publication_lock ON qiita.sequenced_sample;
DROP TRIGGER IF EXISTS prep_sample_publication_lock ON qiita.prep_sample;

DROP FUNCTION IF EXISTS qiita.publication_lock_biosample_to_study();
DROP FUNCTION IF EXISTS qiita.publication_lock_biosample_metadata();
DROP FUNCTION IF EXISTS qiita.publication_lock_biosample();
DROP FUNCTION IF EXISTS qiita.publication_lock_prep_sample_to_study();
DROP FUNCTION IF EXISTS qiita.publication_lock_prep_sample_metadata();
DROP FUNCTION IF EXISTS qiita.publication_lock_sequenced_sample();
DROP FUNCTION IF EXISTS qiita.publication_lock_prep_sample();
DROP FUNCTION IF EXISTS qiita.is_biosample_reaching_published_prep(BIGINT);
DROP FUNCTION IF EXISTS qiita.is_prep_sample_published(BIGINT);

-- The is_published column and its partial index are owned by
-- 20260501000011_prep_sample.sql (folded into the CREATE TABLE); this
-- migration's down step drops only the trigger + function behavior.
