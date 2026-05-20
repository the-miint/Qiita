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
-- Two paths set is_published = TRUE on a link:
--   (a) Cascade from ENA accession transitions: when sequenced_sample's
--       ena_experiment_accession or ena_run_accession goes NULL -> set,
--       OR biosample's ena_sample_accession goes NULL -> set, ALL of the
--       prep's links flip to is_published = TRUE. ENA submission is a
--       globally-observable publication event; trying to keep a prep
--       "private in some secondary study" while its data is downloadable
--       from ENA would be a fiction. The cascade reconciles the model
--       with reality.
--   (b) Owner-driven publish action (future PR): the owner explicitly
--       flips a single prep_sample_to_study.is_published from FALSE to
--       TRUE. Other links of the same prep keep their state.
--
-- The lock-trigger family is BEFORE UPDATE on every row type a published
-- prep transitively reaches:
--     prep_sample, sequenced_sample, prep_sample_metadata,
--     prep_sample_to_study (the published link), biosample,
--     biosample_metadata, biosample_to_study.
-- Each lock trigger asks "is OLD already published?" (via the relevant
-- relationship) and rejects the UPDATE if so. The first ENA-set UPDATE
-- itself is *not* blocked because pre-write the row is still
-- pre-publication (is_published is FALSE on every link); the cascade then
-- flips the flag AFTER the row is in place, so future UPDATEs see TRUE
-- and are rejected.
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
-- No system_admin / wet_lab_admin bypass is wired in this migration. If
-- future operations need to mutate published data, the right shape is a
-- session-scoped GUC (set_config('qiita.bypass_publication_lock', 'on',
-- TRUE)) that each lock trigger checks first; deferred until an actual
-- humans-facing PATCH endpoint requires it. The current PATCH routes
-- are admin-only and out of scope here.


-- =============================================================================
-- IS_PUBLISHED COLUMN
-- =============================================================================

ALTER TABLE qiita.prep_sample_to_study
    ADD COLUMN is_published BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN qiita.prep_sample_to_study.is_published IS
    'TRUE when this (prep_sample, study) link is part of the public record. '
    'Set by the ENA-accession cascade triggers (on sequenced_sample or '
    'biosample going NULL -> non-NULL on any ENA accession) or by a future '
    'owner-driven publish action. Once TRUE, the link itself plus the '
    'prep_sample, its 1:1 sequenced_sample subtype, its prep_sample_metadata '
    'rows, the underlying biosample, its metadata, and its biosample_to_study '
    'links are frozen against UPDATE via the publication_lock_* trigger '
    'family. Distinct from prep_sample_to_study.retired: retirement removes '
    'a study''s permission to use a prep; is_published locks the prep''s '
    'shape because its bytes are out in the world.';

-- Partial index for the lock-trigger lookup; the EXISTS query only cares
-- about published rows, so the partial index keeps the working set small
-- as published rows accumulate.
CREATE INDEX prep_sample_to_study_published_idx
    ON qiita.prep_sample_to_study (prep_sample_idx)
    WHERE is_published = TRUE;


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
-- than in each caller so the lock triggers stay short.
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
-- CASCADE TRIGGERS (ENA accession -> is_published)
-- =============================================================================

-- sequenced_sample side. The 1:1 subtype carries its own GENERATED-ALWAYS
-- idx plus the foreign-keyed prep_sample_idx column that points back at
-- the supertype; use NEW.prep_sample_idx (not NEW.idx) for the join.
CREATE OR REPLACE FUNCTION qiita.cascade_publish_from_sequenced_sample_ena()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.ena_experiment_accession IS NOT NULL
            AND OLD.ena_experiment_accession IS NULL)
       OR (NEW.ena_run_accession IS NOT NULL
            AND OLD.ena_run_accession IS NULL) THEN
        UPDATE qiita.prep_sample_to_study
           SET is_published = TRUE
         WHERE prep_sample_idx = NEW.prep_sample_idx
           AND is_published = FALSE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sequenced_sample_cascade_publish
    AFTER UPDATE OF ena_experiment_accession, ena_run_accession
    ON qiita.sequenced_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.cascade_publish_from_sequenced_sample_ena();


-- biosample side. ENA-publishing the biosample propagates to every prep
-- that references it. Cross-prep / cross-study span is intentional:
-- once a biosample is in ENA, every downstream prep / link it appears in
-- is observationally public too.
CREATE OR REPLACE FUNCTION qiita.cascade_publish_from_biosample_ena()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.ena_sample_accession IS NOT NULL
       AND OLD.ena_sample_accession IS NULL THEN
        UPDATE qiita.prep_sample_to_study pts
           SET is_published = TRUE
          FROM qiita.prep_sample ps
         WHERE ps.idx = pts.prep_sample_idx
           AND ps.biosample_idx = NEW.idx
           AND pts.is_published = FALSE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_cascade_publish
    AFTER UPDATE OF ena_sample_accession
    ON qiita.biosample
    FOR EACH ROW EXECUTE FUNCTION qiita.cascade_publish_from_biosample_ena();


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
-- pointing OLD.prep_sample_idx column. The first ENA-set UPDATE precedes
-- the cascade, so this trigger sees OLD where is_published is still
-- FALSE and lets the publication happen; subsequent UPDATEs are rejected.
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


-- The link itself: once is_published is TRUE, no further UPDATE on the
-- row is allowed -- including retire (sets retired = TRUE) or
-- unpublish (sets is_published back to FALSE). Cascade-driven flips
-- pass through because they target rows where OLD.is_published = FALSE
-- (the WHERE clause in both cascade functions filters those out).
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


-- migrate:down

DROP TRIGGER IF EXISTS biosample_to_study_publication_lock ON qiita.biosample_to_study;
DROP TRIGGER IF EXISTS biosample_metadata_publication_lock ON qiita.biosample_metadata;
DROP TRIGGER IF EXISTS biosample_publication_lock ON qiita.biosample;
DROP TRIGGER IF EXISTS prep_sample_to_study_publication_lock ON qiita.prep_sample_to_study;
DROP TRIGGER IF EXISTS prep_sample_metadata_publication_lock ON qiita.prep_sample_metadata;
DROP TRIGGER IF EXISTS sequenced_sample_publication_lock ON qiita.sequenced_sample;
DROP TRIGGER IF EXISTS prep_sample_publication_lock ON qiita.prep_sample;
DROP TRIGGER IF EXISTS biosample_cascade_publish ON qiita.biosample;
DROP TRIGGER IF EXISTS sequenced_sample_cascade_publish ON qiita.sequenced_sample;

DROP FUNCTION IF EXISTS qiita.publication_lock_biosample_to_study();
DROP FUNCTION IF EXISTS qiita.publication_lock_biosample_metadata();
DROP FUNCTION IF EXISTS qiita.publication_lock_biosample();
DROP FUNCTION IF EXISTS qiita.publication_lock_prep_sample_to_study();
DROP FUNCTION IF EXISTS qiita.publication_lock_prep_sample_metadata();
DROP FUNCTION IF EXISTS qiita.publication_lock_sequenced_sample();
DROP FUNCTION IF EXISTS qiita.publication_lock_prep_sample();
DROP FUNCTION IF EXISTS qiita.cascade_publish_from_biosample_ena();
DROP FUNCTION IF EXISTS qiita.cascade_publish_from_sequenced_sample_ena();
DROP FUNCTION IF EXISTS qiita.is_biosample_reaching_published_prep(BIGINT);
DROP FUNCTION IF EXISTS qiita.is_prep_sample_published(BIGINT);

DROP INDEX IF EXISTS qiita.prep_sample_to_study_published_idx;
ALTER TABLE qiita.prep_sample_to_study DROP COLUMN IF EXISTS is_published;
