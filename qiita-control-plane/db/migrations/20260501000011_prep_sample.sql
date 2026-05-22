-- migrate:up

-- =============================================================================
-- PROCESSING KIND ENUM
-- =============================================================================
--
-- Closed set of disjoint downstream-measurement specializations a prep_sample
-- may flow into. Today only 'sequenced'; future values (e.g., 'mass_specd')
-- add new subtype tables that FK back to prep_sample via the composite
-- (idx, processing_kind) key. The composite-FK + GENERATED ALWAYS AS pattern
-- enforces mutual exclusion declaratively: each subtype table pins its own
-- processing_kind column to a single value, so a prep_sample row can satisfy
-- the FK from at most one subtype.
CREATE TYPE qiita.processing_kind AS ENUM ('sequenced');


-- =============================================================================
-- PREP SAMPLES
-- =============================================================================

CREATE TABLE qiita.prep_sample (
    idx                             BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    biosample_idx                   BIGINT NOT NULL REFERENCES qiita.biosample(idx) ON DELETE RESTRICT,
    owner_idx                       BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    prep_protocol_idx               BIGINT NOT NULL REFERENCES qiita.prep_protocol(idx) ON DELETE RESTRICT,
    metadata_checklist_idx          BIGINT REFERENCES qiita.metadata_checklist(idx) ON DELETE RESTRICT,

    -- The downstream specialization this prep is routed into.
    -- Required at insert time; once a subtype row exists for this prep,
    -- the subtype's composite FK pins this column to the matching value.
    processing_kind                 qiita.processing_kind NOT NULL,

    -- Bumped by the prep_sample_metadata_touch_prep_sample trigger whenever
    -- a prep_sample_metadata row for this prep is inserted or updated. The
    -- submission subsystem reads this against the matching subtype row's
    -- last_submission_at to decide whether re-submission is needed.
    last_metadata_change_at         TIMESTAMPTZ,

    created_by_idx                  BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger; used as the
    -- ETag for optimistic-concurrency control on PATCH.
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),

    retired                         BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx                  BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at                      TIMESTAMPTZ,
    retire_reason                   TEXT,

    -- Target of the composite FK from every subtype table. Each subtype
    -- carries a processing_kind column pinned by GENERATED ALWAYS AS to a
    -- single enum value and references (idx, processing_kind) here. The
    -- target tuple's uniqueness combined with the subtype's pinned kind
    -- makes mutual exclusion a pure declarative-DDL guarantee.
    CONSTRAINT prep_sample_idx_processing_kind_unique UNIQUE (idx, processing_kind),

    CONSTRAINT prep_sample_retirement_consistent CHECK (
        (retired = false
            AND retired_at IS NULL
            AND retired_by_idx IS NULL
            AND retire_reason IS NULL)
        OR
        (retired = true
            AND retired_at IS NOT NULL
            AND retired_by_idx IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.prep_sample IS
    'A specific biosample as prepared under a specific protocol; the '
    'supertype of the downstream-measurement hierarchy. Structurally '
    'parallel to biosample: has an owner and many-to-many study '
    'relationships through prep_sample_to_study. Downstream-measurement '
    'data (sequencing run + ENA submission for the sequenced case; future '
    'mass-spec metadata; etc.) lives on disjoint subtype tables that FK '
    'back via the composite (idx, processing_kind) key. Technical '
    'replicates are supported: (biosample_idx, prep_protocol_idx) is not '
    'UNIQUE, so the same biosample may be prepared multiple times under '
    'the same protocol. Lab-prep batches are LIMS concerns and are not '
    'modeled here.';

COMMENT ON COLUMN qiita.prep_sample.owner_idx IS
    'The principal who owns this prep sample. Parallel to '
    'biosample.owner_idx but usually the lab here.';

COMMENT ON COLUMN qiita.prep_sample.processing_kind IS
    'The kind of downstream measurement this prep is routed into. NOT NULL: '
    'every prep must commit to a processing path at insert time. Once a '
    'subtype row (e.g., sequenced_sample) exists for this prep, the '
    'subtype''s composite FK pins this column to the matching value, and '
    'any change here that would orphan the subtype row will be rejected '
    'by the FK.';

CREATE INDEX prep_sample_biosample_idx ON qiita.prep_sample (biosample_idx);
CREATE INDEX prep_sample_owner_idx ON qiita.prep_sample (owner_idx);
CREATE INDEX prep_sample_prep_protocol_idx
    ON qiita.prep_sample (prep_protocol_idx);
CREATE INDEX prep_sample_active_idx
    ON qiita.prep_sample (idx)
    WHERE retired = false;

CREATE TRIGGER prep_sample_set_updated_at
    BEFORE UPDATE ON qiita.prep_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- =============================================================================
-- PREP-SAMPLE-TO-STUDY LINKS
-- =============================================================================

CREATE TABLE qiita.prep_sample_to_study (
    prep_sample_idx       BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,
    study_idx             BIGINT NOT NULL REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    created_by_idx        BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired               BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx        BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at            TIMESTAMPTZ,
    retire_reason         TEXT,
    -- Publication-lock flag. The lock triggers that act on it are
    -- defined in 20260520000000_publication_lock.sql; the column lives
    -- here so prep_sample_to_study has a single CREATE-TABLE source of
    -- truth.
    is_published          BOOLEAN NOT NULL DEFAULT false,

    PRIMARY KEY (prep_sample_idx, study_idx),

    CONSTRAINT prep_sample_to_study_retirement_consistent CHECK (
        (retired = false
            AND retired_at IS NULL
            AND retired_by_idx IS NULL
            AND retire_reason IS NULL)
        OR
        (retired = true
            AND retired_at IS NOT NULL
            AND retired_by_idx IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.prep_sample_to_study IS
    'Many-to-many link between prep samples and studies. One row per '
    '(prep_sample, study) pair. Parallel to biosample_to_study. A row can '
    'be created only if the underlying biosample is also linked (non-retired) '
    'to the same study -- enforced by the reject_without_biosample_link '
    'trigger further down.';

COMMENT ON COLUMN qiita.prep_sample_to_study.retired IS
    'When true, this study has lost permission to use this prep sample. '
    'Distinct from prep_sample.retired, which withdraws the sample '
    'everywhere.';

COMMENT ON COLUMN qiita.prep_sample_to_study.is_published IS
    'TRUE when this (prep_sample, study) link has been published. Set only '
    'by the owner-driven publish action (future PR). ENA accessions do NOT '
    'set it: an ENA accession records a submission (which may sit under '
    'embargo), not a publication. Once TRUE, the link itself plus the '
    'prep_sample, its 1:1 sequenced_sample subtype, its prep_sample_metadata '
    'rows, the underlying biosample, its metadata, and its biosample_to_study '
    'links are frozen against UPDATE via the publication_lock_* trigger '
    'family (see 20260520000000_publication_lock.sql). Distinct from '
    'prep_sample_to_study.retired: retirement removes a study''s permission '
    'to use a prep; is_published locks the prep''s shape because it has been '
    'published.';

CREATE INDEX prep_sample_to_study_study_idx
    ON qiita.prep_sample_to_study (study_idx);
-- study_idx-leading (mirrors biosample_to_study_active_idx): serves the
-- study-scoped active-link roster read with retired rows pruned at the
-- index rather than filtered post-scan. Do not reorder to lead with
-- prep_sample_idx -- that re-introduces the asymmetry this index closes.
CREATE INDEX prep_sample_to_study_active_idx
    ON qiita.prep_sample_to_study (study_idx, prep_sample_idx)
    WHERE retired = false;
-- Partial index for the publication-lock trigger lookup
-- (qiita.is_prep_sample_published, defined in the publication-lock
-- migration). The EXISTS probe only cares about published rows, so the
-- partial predicate keeps the working set small as published rows
-- accumulate.
CREATE INDEX prep_sample_to_study_published_idx
    ON qiita.prep_sample_to_study (prep_sample_idx)
    WHERE is_published = true;


-- =============================================================================
-- PREP SAMPLE METADATA (the EAV)
-- =============================================================================

CREATE TABLE qiita.prep_sample_metadata (
    idx                                BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    prep_sample_idx                    BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,
    prep_sample_study_field_idx        BIGINT NOT NULL REFERENCES qiita.prep_sample_study_field(idx) ON DELETE RESTRICT,
    -- Maintained by trigger; see comment.
    global_field_idx                   BIGINT REFERENCES qiita.prep_sample_global_field(idx) ON DELETE RESTRICT,
    value_text                         TEXT,
    value_numeric                      NUMERIC,
    value_boolean                      BOOLEAN,
    value_date                         DATE,
    value_terminology_term_idx         BIGINT REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT,
    value_missing_reason_idx           BIGINT REFERENCES qiita.missing_value_reason(idx) ON DELETE RESTRICT,
    created_by_idx                     BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Each (prep_sample, study field) pair has at most one metadata row. This
    -- is the natural-key constraint that PUT-on-natural-key operations rely on.
    CONSTRAINT prep_sample_metadata_unique_per_field
        UNIQUE (prep_sample_idx, prep_sample_study_field_idx),

    -- Exactly one value column (or a missing-reason) must be populated.
    CONSTRAINT prep_sample_metadata_exactly_one_value CHECK (
        (CASE WHEN value_text IS NOT NULL                 THEN 1 ELSE 0 END
       + CASE WHEN value_numeric IS NOT NULL              THEN 1 ELSE 0 END
       + CASE WHEN value_boolean IS NOT NULL              THEN 1 ELSE 0 END
       + CASE WHEN value_date IS NOT NULL                 THEN 1 ELSE 0 END
       + CASE WHEN value_terminology_term_idx IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN value_missing_reason_idx IS NOT NULL   THEN 1 ELSE 0 END) = 1
    )
);

COMMENT ON COLUMN qiita.prep_sample_metadata.global_field_idx IS
    'Maintained by trigger from prep_sample_study_field.prep_sample_global_field_idx. '
    'NULL when the source field is purely study-local, non-NULL when the source '
    'field is bound to a global field. Powers the partial unique index that '
    'enforces one value per (prep_sample, global field) pair across all '
    'studies, so cross-study reads through the global field always return a '
    'single canonical value. The slot is permanently held by whichever study '
    'first wrote through the global field; retiring that study''s '
    'prep_sample_to_study link does not release the slot (the canonical value '
    'persists for other studies that retained their link). Per-study read '
    'access on retired links is governed by the study_access predicate at '
    'read time, not by this column.';

CREATE INDEX prep_sample_metadata_field_idx
    ON qiita.prep_sample_metadata (prep_sample_study_field_idx);
CREATE INDEX prep_sample_metadata_terminology_value_idx
    ON qiita.prep_sample_metadata (value_terminology_term_idx)
    WHERE value_terminology_term_idx IS NOT NULL;

-- Cross-study uniqueness for globally-linked values: a given prep sample
-- has at most one metadata row per global field, even if multiple studies
-- have local fields linked to that global field. Parallel to
-- biosample_metadata_one_value_per_global_field.
CREATE UNIQUE INDEX prep_sample_metadata_one_value_per_global_field
    ON qiita.prep_sample_metadata (prep_sample_idx, global_field_idx)
    WHERE global_field_idx IS NOT NULL;


-- =============================================================================
-- PREP SAMPLE FIELD EXCEPTIONS (per-(prep_sample, field) visibility overrides)
-- =============================================================================

CREATE TABLE qiita.prep_sample_field_exception (
    idx                                BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    prep_sample_idx                    BIGINT NOT NULL REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,
    -- Dual-keyed; see table comment.
    prep_sample_study_field_idx        BIGINT REFERENCES qiita.prep_sample_study_field(idx) ON DELETE RESTRICT,
    global_field_idx                   BIGINT REFERENCES qiita.prep_sample_global_field(idx) ON DELETE RESTRICT,
    tier_override                      qiita.tier NOT NULL,
    reason                             TEXT,
    created_by_idx                     BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger below; used as the
    -- ETag for optimistic-concurrency control on upsert PUT.
    updated_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT prep_sample_field_exception_exactly_one_key CHECK (
        (prep_sample_study_field_idx IS NOT NULL AND global_field_idx IS NULL)
        OR
        (prep_sample_study_field_idx IS NULL AND global_field_idx IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.prep_sample_field_exception IS
    'Per-(prep_sample, field) visibility overrides. Used to restrict '
    'the audience for specific metadata values that need narrower visibility '
    'than their field''s general policy. An exception downgrades visibility '
    'to a specified tier_override. Exceptions on globally-linked metadata '
    'are keyed on global_field_idx so they follow the value across studies; '
    'exceptions on purely study-local metadata are keyed on '
    'prep_sample_study_field_idx.';

COMMENT ON COLUMN qiita.prep_sample_field_exception.tier_override IS
    'While most tier_override columns in the schema are nullable, '
    'field_exception tier_overrides are not because there would be '
    'no point in registering an exception if you were not overriding '
    'the expected value.';

CREATE UNIQUE INDEX prep_sample_field_exception_unique_local
    ON qiita.prep_sample_field_exception (prep_sample_idx, prep_sample_study_field_idx)
    WHERE prep_sample_study_field_idx IS NOT NULL;
CREATE UNIQUE INDEX prep_sample_field_exception_unique_global
    ON qiita.prep_sample_field_exception (prep_sample_idx, global_field_idx)
    WHERE global_field_idx IS NOT NULL;

CREATE INDEX prep_sample_field_exception_study_field_idx
    ON qiita.prep_sample_field_exception (prep_sample_study_field_idx)
    WHERE prep_sample_study_field_idx IS NOT NULL;
CREATE INDEX prep_sample_field_exception_global_field_idx
    ON qiita.prep_sample_field_exception (global_field_idx)
    WHERE global_field_idx IS NOT NULL;

CREATE TRIGGER prep_sample_field_exception_set_updated_at
    BEFORE UPDATE ON qiita.prep_sample_field_exception
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- =============================================================================
-- TRIGGER: prep_sample_metadata_apply_field_contract
--
-- Parallel to biosample_metadata_apply_field_contract; see that trigger's
-- comment for the full responsibilities. One SELECT against the source
-- prep_sample_study_field row drives both:
--
--   1. CHECK that the populated value_* column matches the field's data_type
--      (resolved via COALESCE across study- and global-field rows).
--   2. SET NEW.global_field_idx from the source study_field's
--      prep_sample_global_field_idx.
--
-- Missing-reason rows are exempt from the data_type match.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_apply_field_contract()
RETURNS TRIGGER AS $$
DECLARE
    expected_data_type qiita.field_data_type;
    populated_ok      BOOLEAN;
BEGIN
    -- Single SELECT covers both responsibilities.
    SELECT ssf.prep_sample_global_field_idx,
           COALESCE(ssf.data_type, sgf.data_type)
      INTO NEW.global_field_idx, expected_data_type
      FROM qiita.prep_sample_study_field ssf
      LEFT JOIN qiita.prep_sample_global_field sgf
        ON sgf.idx = ssf.prep_sample_global_field_idx
     WHERE ssf.idx = NEW.prep_sample_study_field_idx;

    -- Missing-reason rows are exempt from the value/data_type match.
    IF NEW.value_missing_reason_idx IS NOT NULL THEN
        RETURN NEW;
    END IF;

    -- Verify the populated value column matches the field's data_type.
    -- ELSE NULL + IS NOT TRUE so an unrecognized or NULL data_type fails
    -- loudly rather than passing through (which a bare CASE + IF NOT
    -- populated_ok would do, since NOT NULL is NULL is not TRUE).
    populated_ok := CASE expected_data_type
        WHEN 'text'        THEN NEW.value_text IS NOT NULL
        WHEN 'numeric'     THEN NEW.value_numeric IS NOT NULL
        WHEN 'boolean'     THEN NEW.value_boolean IS NOT NULL
        WHEN 'date'        THEN NEW.value_date IS NOT NULL
        WHEN 'terminology' THEN NEW.value_terminology_term_idx IS NOT NULL
        ELSE NULL
    END;
    IF populated_ok IS NOT TRUE THEN
        RAISE EXCEPTION
            'prep_sample_metadata value column does not match field data_type % for prep_sample_study_field_idx %',
            expected_data_type, NEW.prep_sample_study_field_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_metadata_apply_field_contract_insert
    BEFORE INSERT ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_apply_field_contract();

-- The UPDATE trigger fires when any value_* column or the source field
-- changes — both are inputs to the data_type check.
CREATE TRIGGER prep_sample_metadata_apply_field_contract_update
    BEFORE UPDATE OF prep_sample_study_field_idx, value_text, value_numeric,
                     value_boolean, value_date, value_terminology_term_idx,
                     value_missing_reason_idx
        ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_apply_field_contract();


-- =============================================================================
-- TRIGGER: propagate prep_sample_study_field global link changes to metadata
--
-- Parallel to the biosample-side trigger. Recognises three transition
-- shapes on prep_sample_study_field.prep_sample_global_field_idx:
--
--   NULL -> non-NULL (upgrade local to global): propagate the new link to
--     every existing metadata row through this field. The partial unique
--     index prep_sample_metadata_one_value_per_global_field gates
--     collisions and rolls back the propagation if any row's slot is
--     already claimed by another study.
--
--   non-NULL -> NULL (unlink), no metadata exists: propagate (no-op);
--     the field becomes local for future writes.
--
--   non-NULL -> NULL (unlink), metadata exists: REJECTED. Unlinking
--     would strand globally-linked metadata; caller must delete those
--     rows first to make the data loss explicit and deliberate.
--
--   non-NULL -> different non-NULL (rebind): REJECTED unconditionally.
--     Rebinding mutates the field's identity, changing the semantic
--     meaning of every existing metadata row. The correct flow is to
--     create a new study_field bound to the desired global field.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.propagate_global_field_link_to_prep_sample_metadata()
RETURNS TRIGGER AS $$
DECLARE
    metadata_row_count BIGINT;
BEGIN
    -- Short-circuit: no actual change in the global link.
    IF NEW.prep_sample_global_field_idx IS NOT DISTINCT FROM OLD.prep_sample_global_field_idx THEN
        RETURN NEW;
    END IF;

    -- Reject rebind (non-NULL -> different non-NULL) unconditionally.
    IF OLD.prep_sample_global_field_idx IS NOT NULL
       AND NEW.prep_sample_global_field_idx IS NOT NULL THEN
        RAISE EXCEPTION
            'cannot rebind prep_sample_study_field idx=% from prep_sample_global_field_idx=% to %; '
            'rebinding changes the semantic meaning of every metadata row through this field. '
            'Create a new prep_sample_study_field bound to the desired global field instead.',
            NEW.idx, OLD.prep_sample_global_field_idx, NEW.prep_sample_global_field_idx;
    END IF;

    -- Reject unlink (non-NULL -> NULL) when metadata rows exist through
    -- this field.
    IF OLD.prep_sample_global_field_idx IS NOT NULL
       AND NEW.prep_sample_global_field_idx IS NULL THEN
        SELECT COUNT(*) INTO metadata_row_count
          FROM qiita.prep_sample_metadata
         WHERE prep_sample_study_field_idx = NEW.idx;
        IF metadata_row_count > 0 THEN
            RAISE EXCEPTION
                'cannot unlink prep_sample_study_field idx=% from prep_sample_global_field_idx=%; '
                '% metadata row(s) reference this field. Delete those rows first '
                'if the unlink is intentional.',
                NEW.idx, OLD.prep_sample_global_field_idx, metadata_row_count;
        END IF;
        RETURN NEW;
    END IF;

    -- Remaining case: NULL -> non-NULL (upgrade local to global). Propagate
    -- the new link; the partial unique index rolls back the UPDATE if any
    -- row's (prep_sample_idx, new_global_field_idx) slot is taken.
    UPDATE qiita.prep_sample_metadata
       SET global_field_idx = NEW.prep_sample_global_field_idx
     WHERE prep_sample_study_field_idx = NEW.idx;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_study_field_propagate_global_link
    AFTER UPDATE OF prep_sample_global_field_idx ON qiita.prep_sample_study_field
    FOR EACH ROW EXECUTE FUNCTION qiita.propagate_global_field_link_to_prep_sample_metadata();


-- =============================================================================
-- TRIGGER: enforce non-retired-link invariant on prep_sample_metadata inserts
--
-- A prep_sample_metadata row cannot exist for a (prep_sample, study)
-- pair whose prep_sample_to_study link is retired. Row-repointing is
-- separately forbidden by the immutability trigger further down, so only the
-- INSERT path needs guarding here.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_reject_if_link_retired()
RETURNS TRIGGER AS $$
DECLARE
    link_retired BOOLEAN;
    field_study_idx BIGINT;
BEGIN
    SELECT sssf.study_idx
      INTO field_study_idx
      FROM qiita.prep_sample_study_field sssf
     WHERE sssf.idx = NEW.prep_sample_study_field_idx;

    SELECT ssts.retired
      INTO link_retired
      FROM qiita.prep_sample_to_study ssts
     WHERE ssts.prep_sample_idx = NEW.prep_sample_idx
       AND ssts.study_idx = field_study_idx;

    IF link_retired IS NULL THEN
        RAISE EXCEPTION 'prep_sample_metadata refers to (prep_sample=%, study=%) but no prep_sample_to_study row exists',
            NEW.prep_sample_idx, field_study_idx;
    END IF;

    IF link_retired = true THEN
        RAISE EXCEPTION 'prep_sample_metadata cannot be written: prep_sample_to_study(%, %) is retired',
            NEW.prep_sample_idx, field_study_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_metadata_reject_if_link_retired_insert
    BEFORE INSERT ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_reject_if_link_retired();


-- =============================================================================
-- TRIGGER: enforce immutability of prep_sample_metadata key columns
--
-- A prep_sample_metadata row's (prep_sample_idx,
-- prep_sample_study_field_idx) pair identifies which prep sample and
-- which study field the value is FOR. If either was wrong, the correct flow is
-- to DELETE the row and INSERT a new one for the right pair. Parallel to the
-- biosample-side immutability trigger.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_reject_key_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.prep_sample_idx IS DISTINCT FROM OLD.prep_sample_idx THEN
        RAISE EXCEPTION 'prep_sample_metadata.prep_sample_idx is immutable; delete and re-insert instead';
    END IF;
    IF NEW.prep_sample_study_field_idx IS DISTINCT FROM OLD.prep_sample_study_field_idx THEN
        RAISE EXCEPTION 'prep_sample_metadata.prep_sample_study_field_idx is immutable; delete and re-insert instead';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_metadata_reject_key_update
    BEFORE UPDATE OF prep_sample_idx, prep_sample_study_field_idx ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_reject_key_update();


-- =============================================================================
-- TRIGGER: bump prep_sample.last_metadata_change_at on
-- prep_sample_metadata writes
--
-- A prep sample's last_metadata_change_at is set to now() whenever a
-- prep_sample_metadata row for it is inserted or updated.
-- prep_sample_idx is immutable on UPDATE (enforced above), so only one
-- prep sample is ever touched per firing.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_metadata_touch_prep_sample()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE qiita.prep_sample
       SET last_metadata_change_at = now()
     WHERE idx = NEW.prep_sample_idx;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_metadata_touch_prep_sample
    AFTER INSERT OR UPDATE ON qiita.prep_sample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_metadata_touch_prep_sample();


-- =============================================================================
-- TRIGGER: prep_sample_to_study row requires a non-retired
-- biosample_to_study link for the same (biosample, study) pair
--
-- Without this guard, a study could be linked to the prep sample while
-- failing the biosample-level access check in the two-gate visibility rule,
-- producing an inert link. Enforcing at the schema layer makes inert links
-- impossible.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.prep_sample_to_study_reject_without_biosample_link()
RETURNS TRIGGER AS $$
DECLARE
    biosample_link_exists_active BOOLEAN;
    biosample_idx_for_prep_sample BIGINT;
BEGIN
    SELECT biosample_idx
      INTO biosample_idx_for_prep_sample
      FROM qiita.prep_sample
     WHERE idx = NEW.prep_sample_idx;

    SELECT EXISTS (
        SELECT 1 FROM qiita.biosample_to_study
         WHERE biosample_idx = biosample_idx_for_prep_sample
           AND study_idx = NEW.study_idx
           AND retired = false
    ) INTO biosample_link_exists_active;

    IF NOT biosample_link_exists_active THEN
        -- MESSAGE stays human-readable; everything a route acts on goes
        -- in DETAIL as comma-separated key=value pairs. The `trigger`
        -- key carries this function's name -- a stable schema identifier
        -- -- so the route decides WHICH rejection this is without
        -- matching message prose; study_idx / biosample_idx let it name
        -- the exact failing study. Consumed by routes/_helpers.py
        -- detail_for_biosample_link_rejection and the RaiseError catch
        -- in routes/sequenced_sample.py -- keep the keys in sync.
        -- ERRCODE is pinned to 'P0001' to match the house style of the
        -- publication-lock triggers.
        RAISE EXCEPTION
            'prep_sample_to_study(prep_sample=%, study=%) requires a non-retired biosample_to_study(biosample=%, study=%) link',
            NEW.prep_sample_idx, NEW.study_idx, biosample_idx_for_prep_sample, NEW.study_idx
            USING
                ERRCODE = 'P0001',
                DETAIL = format(
                    'trigger=prep_sample_to_study_reject_without_biosample_link, study_idx=%s, biosample_idx=%s',
                    NEW.study_idx, biosample_idx_for_prep_sample
                );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prep_sample_to_study_reject_without_biosample_link_insert
    BEFORE INSERT ON qiita.prep_sample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_to_study_reject_without_biosample_link();

-- Belt-and-suspenders: the columns below form the PK, so updating either on
-- an existing row is unusual. This trigger enforces the invariant anyway for
-- out-of-band paths (migrations, admin ALTERs).
CREATE TRIGGER prep_sample_to_study_reject_without_biosample_link_update
    BEFORE UPDATE OF prep_sample_idx, study_idx ON qiita.prep_sample_to_study
    FOR EACH ROW EXECUTE FUNCTION qiita.prep_sample_to_study_reject_without_biosample_link();


-- =============================================================================
-- SEQUENCED SAMPLES (subtype of prep_sample where processing_kind = 'sequenced')
-- =============================================================================

CREATE TABLE qiita.sequenced_sample (
    idx                             BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    prep_sample_idx                 BIGINT NOT NULL,
    -- Pinned to a single enum value; participates in the composite FK to
    -- prep_sample (idx, processing_kind) so this row can only attach to a
    -- prep_sample whose processing_kind matches. The GENERATED form makes
    -- the constant un-writeable from outside, removing the failure mode
    -- where a caller passes a non-matching kind explicitly.
    processing_kind                 qiita.processing_kind
                                    GENERATED ALWAYS AS
                                    ('sequenced'::qiita.processing_kind) STORED,

    sequenced_pool_idx              BIGINT REFERENCES qiita.sequenced_pool(idx) ON DELETE RESTRICT,
    sequenced_pool_item_id          TEXT,

    ena_experiment_accession        VARCHAR(50),
    ena_run_accession               VARCHAR(50),

    -- Submission tracking. last_submission_at is NULL until the submission
    -- subsystem first attempts a submission, otherwise it holds the time of
    -- the most recent attempt. submission_error is NULL on success and
    -- carries the error message when the most recent attempt failed.
    last_submission_at              TIMESTAMPTZ,
    submission_error                TEXT,

    created_by_idx                  BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger; used as the
    -- ETag for optimistic-concurrency control on PATCH.
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- 1:1 with prep_sample.
    CONSTRAINT sequenced_sample_prep_sample_idx_unique UNIQUE (prep_sample_idx),

    -- Composite FK: drags processing_kind along so the parent's kind must
    -- match the literal-pinned kind on this row. Disjointness from any
    -- future processing_kind subtype (e.g., mass_specd_sample) is enforced
    -- by this FK plus the GENERATED ALWAYS AS literal on each subtype.
    CONSTRAINT sequenced_sample_prep_sample_fk
        FOREIGN KEY (prep_sample_idx, processing_kind)
        REFERENCES qiita.prep_sample (idx, processing_kind)
        ON DELETE RESTRICT,

    CONSTRAINT sequenced_sample_ena_experiment_accession_unique UNIQUE (ena_experiment_accession),
    CONSTRAINT sequenced_sample_ena_run_accession_unique UNIQUE (ena_run_accession),

    CONSTRAINT sequenced_sample_run_accession_requires_run CHECK (
        ena_run_accession IS NULL OR sequenced_pool_idx IS NOT NULL
    ),

    -- The pool reference and the pool's per-item id are co-populated: either
    -- the sample is attached to a pool (both set) or it is not (both null).
    CONSTRAINT sequenced_sample_pool_pair_consistent CHECK (
        (sequenced_pool_idx IS NULL) = (sequenced_pool_item_id IS NULL)
    ),

    CONSTRAINT sequenced_sample_pool_item_id_unique
        UNIQUE (sequenced_pool_idx, sequenced_pool_item_id)
);

COMMENT ON TABLE qiita.sequenced_sample IS
    'A prep_sample routed down the sequencing pathway; one of the disjoint '
    'subtypes of prep_sample. Carries the sequencing-run linkage and the '
    'ENA EXPERIMENT / RUN submission state. 1:1 with prep_sample; mutually '
    'exclusive with any future processing_kind subtype via the composite '
    '(prep_sample_idx, processing_kind) FK plus the GENERATED ALWAYS AS '
    'pinned processing_kind on this row.';

COMMENT ON COLUMN qiita.sequenced_sample.ena_experiment_accession IS
    'ENA-assigned experiment accession. NULL until the experiment '
    'submission has succeeded and ENA has returned an accession. Parallels '
    'biosample.biosample_accession in role.';

COMMENT ON COLUMN qiita.sequenced_sample.ena_run_accession IS
    'ENA-assigned run accession. NULL until a sequencing run has been '
    'assigned and its submission has succeeded. The schema enforces that a '
    'run accession can only exist when sequenced_pool_idx is also populated.';

CREATE INDEX sequenced_sample_sequenced_pool_idx
    ON qiita.sequenced_sample (sequenced_pool_idx)
    WHERE sequenced_pool_idx IS NOT NULL;

CREATE TRIGGER sequenced_sample_set_updated_at
    BEFORE UPDATE ON qiita.sequenced_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- When a new submission attempt is recorded (last_submission_at changes),
-- any stale submission_error is cleared -- unless the caller also set
-- submission_error in the same UPDATE, in which case the caller's value is
-- kept. This lets a failed-attempt caller record both fields in one UPDATE
-- without the trigger overwriting the freshly-set error.
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

CREATE TRIGGER sequenced_sample_clear_submission_error_on_new_attempt
    BEFORE UPDATE OF last_submission_at ON qiita.sequenced_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.sequenced_sample_clear_submission_error_on_new_attempt();


-- migrate:down

-- Drop the trigger that lives on a previous migration's table
-- (prep_sample_study_field). Dropping the tables below takes their own
-- triggers with them, but this one would otherwise orphan against an empty
-- target.
DROP TRIGGER IF EXISTS prep_sample_study_field_propagate_global_link ON qiita.prep_sample_study_field;

DROP TABLE IF EXISTS qiita.prep_sample_field_exception;
DROP TABLE IF EXISTS qiita.prep_sample_metadata;
DROP TABLE IF EXISTS qiita.prep_sample_to_study;
DROP TABLE IF EXISTS qiita.sequenced_sample;
DROP TABLE IF EXISTS qiita.prep_sample;

DROP FUNCTION IF EXISTS qiita.sequenced_sample_clear_submission_error_on_new_attempt();
DROP FUNCTION IF EXISTS qiita.prep_sample_to_study_reject_without_biosample_link();
DROP FUNCTION IF EXISTS qiita.prep_sample_metadata_reject_key_update();
DROP FUNCTION IF EXISTS qiita.prep_sample_metadata_reject_if_link_retired();
DROP FUNCTION IF EXISTS qiita.propagate_global_field_link_to_prep_sample_metadata();
DROP FUNCTION IF EXISTS qiita.prep_sample_metadata_apply_field_contract();
DROP FUNCTION IF EXISTS qiita.prep_sample_metadata_touch_prep_sample();

DROP TYPE IF EXISTS qiita.processing_kind;
