-- migrate:up

-- =============================================================================
-- BIOSAMPLES
-- =============================================================================

CREATE TABLE qiita.biosample (
    idx                      BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    owner_idx                BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    metadata_checklist_idx   BIGINT REFERENCES qiita.metadata_checklist(idx) ON DELETE RESTRICT,
    biosample_accession      VARCHAR(50),
    ena_sample_accession     VARCHAR(50),
    -- Submission tracking for NCBI BioSample deposit. last_submission_at is NULL
    -- until the submission subsystem first attempts a submission, otherwise it
    -- holds the time of the most recent attempt. submission_error is NULL on
    -- success and carries the error message when the most recent attempt failed.
    -- The scheduling predicate for new work is (last_submission_at IS NULL AND
    -- biosample_accession IS NULL and ena_sample_accession IS NULL);
    -- rows with an accession but no recorded attempt were submitted externally
    -- and are left alone.
    last_submission_at       TIMESTAMPTZ,
    submission_error         TEXT,
    last_metadata_change_at  TIMESTAMPTZ,
    created_by_idx           BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger; used as the
    -- ETag for optimistic-concurrency control on PATCH.
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    retired                  BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx           BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at               TIMESTAMPTZ,
    retire_reason            TEXT,

    -- external accessions are unique when present.
    CONSTRAINT biosample_accession_unique UNIQUE (biosample_accession),
    CONSTRAINT biosample_ena_sample_accession_unique UNIQUE (ena_sample_accession),

    -- Retirement audit fields must be consistent with the retired flag: when
    -- the row is retired, retired_at and retired_by_idx are both mandatory
    -- (the audit trail requires knowing when and by whom), and retire_reason
    -- is optional. When the row is not retired, all three audit fields must
    -- be NULL. The retired_by_idx FK uses ON DELETE RESTRICT: a principal
    -- who has ever retired a biosample cannot be hard-deleted until the
    -- retirement is reassigned or cleared at the DDL level.
    CONSTRAINT biosample_retirement_consistent CHECK (
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

COMMENT ON COLUMN qiita.biosample.retired IS
    'When true, this biosample has been withdrawn from use everywhere (e.g., sample-integrity / '
    'inadequate consenting). Distinct from biosample_to_study.retired, '
    'which is a per-study permission flag.';

CREATE INDEX biosample_owner_idx ON qiita.biosample (owner_idx);
CREATE INDEX biosample_active_idx
    ON qiita.biosample (idx)
    WHERE retired = false;

CREATE TRIGGER biosample_set_updated_at
    BEFORE UPDATE ON qiita.biosample
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- When a new submission attempt is recorded (last_submission_at changes),
-- any stale submission_error is cleared -- unless the caller also set
-- submission_error in the same UPDATE, in which case the caller's value is
-- kept. This lets a failed-attempt caller record both fields in one UPDATE
-- without the trigger overwriting the freshly-set error.
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

CREATE TRIGGER biosample_clear_submission_error_on_new_attempt
    BEFORE UPDATE OF last_submission_at ON qiita.biosample
    FOR EACH ROW EXECUTE FUNCTION qiita.biosample_clear_submission_error_on_new_attempt();


-- =============================================================================
-- BIOSAMPLE-TO-STUDY LINKS
-- =============================================================================

CREATE TABLE qiita.biosample_to_study (
    biosample_idx     BIGINT NOT NULL REFERENCES qiita.biosample(idx) ON DELETE RESTRICT,
    study_idx         BIGINT NOT NULL REFERENCES qiita.study(idx) ON DELETE RESTRICT,
    created_by_idx    BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired           BOOLEAN NOT NULL DEFAULT false,
    retired_by_idx    BIGINT REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    retired_at        TIMESTAMPTZ,
    retire_reason     TEXT,

    PRIMARY KEY (biosample_idx, study_idx),

    -- Note that the biosample and biosample-to-study retirement states are
    -- independent: a biosample-level retirement does not automatically
    -- retire its links, and a per-link retirement does not affect the
    -- biosample's overall availability in other studies.
    CONSTRAINT biosample_to_study_retirement_consistent CHECK (
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

COMMENT ON COLUMN qiita.biosample_to_study.retired IS
    'When true, this study has lost permission to use this biosample. '
    'Distinct from biosample.retired, which withdraws the sample everywhere.';

CREATE INDEX biosample_to_study_study_idx ON qiita.biosample_to_study (study_idx);
CREATE INDEX biosample_to_study_active_idx
    ON qiita.biosample_to_study (study_idx, biosample_idx)
    WHERE retired = false;


-- =============================================================================
-- BIOSAMPLE METADATA (the EAV)
-- =============================================================================

CREATE TABLE qiita.biosample_metadata (
    idx                          BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    biosample_idx                BIGINT NOT NULL REFERENCES qiita.biosample(idx) ON DELETE RESTRICT,
    biosample_study_field_idx    BIGINT NOT NULL REFERENCES qiita.biosample_study_field(idx) ON DELETE RESTRICT,
    -- Maintained by trigger; see comment.
    global_field_idx             BIGINT REFERENCES qiita.biosample_global_field(idx) ON DELETE RESTRICT,
    value_text                   TEXT,
    value_numeric                NUMERIC,
    value_boolean                BOOLEAN,
    value_date                   DATE,
    value_terminology_term_idx   BIGINT REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT,
    value_missing_reason_idx     BIGINT REFERENCES qiita.missing_value_reason(idx) ON DELETE RESTRICT,
    is_owner_biosample_id        BOOLEAN NOT NULL DEFAULT false,
    created_by_idx               BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Each (biosample, study field) pair has at most one metadata row. This is
    -- the natural-key constraint that PUT-on-natural-key operations rely on.
    CONSTRAINT biosample_metadata_unique_per_field
        UNIQUE (biosample_idx, biosample_study_field_idx),

    -- Exactly one value column (or a missing-reason) must be populated.
    CONSTRAINT biosample_metadata_exactly_one_value CHECK (
        (CASE WHEN value_text IS NOT NULL                 THEN 1 ELSE 0 END
       + CASE WHEN value_numeric IS NOT NULL              THEN 1 ELSE 0 END
       + CASE WHEN value_boolean IS NOT NULL              THEN 1 ELSE 0 END
       + CASE WHEN value_date IS NOT NULL                 THEN 1 ELSE 0 END
       + CASE WHEN value_terminology_term_idx IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN value_missing_reason_idx IS NOT NULL   THEN 1 ELSE 0 END) = 1
    )
);

COMMENT ON COLUMN qiita.biosample_metadata.global_field_idx IS
    'Maintained by trigger from biosample_study_field.biosample_global_field_idx. '
    'NULL when the source field is purely study-local, non-NULL when the source '
    'field is bound to a global field. Powers the partial unique index that '
    'enforces one value per (biosample, global field) pair across all studies, '
    'so cross-study reads through the global field always return a single '
    'canonical value. The slot is permanently held by whichever study first '
    'wrote through the global field; retiring that study''s biosample_to_study '
    'link does not release the slot (the canonical value persists for other '
    'studies that retained their link). Per-study read access on retired links '
    'is governed by the study_access predicate at read time, not by this column.';

COMMENT ON COLUMN qiita.biosample_metadata.is_owner_biosample_id IS
    'True iff this metadata row holds the owner''s identifier for this biosample. '
    'The biosample_metadata_unique_owner_biosample_id partial unique index '
    'enforces at most one true row per biosample_idx; rows with this flag false '
    'are unrestricted.';

CREATE INDEX biosample_metadata_field_idx
    ON qiita.biosample_metadata (biosample_study_field_idx);
CREATE INDEX biosample_metadata_terminology_value_idx
    ON qiita.biosample_metadata (value_terminology_term_idx)
    WHERE value_terminology_term_idx IS NOT NULL;

-- Cross-study uniqueness for globally-linked values: a given biosample has at
-- most one metadata row per global field, even if multiple studies have local
-- fields linked to that global field. Conflicting writes are caught here rather than
-- silently allowed to coexist. NULL global_field_idx (purely study-local fields)
-- is excluded from the constraint, since study-local values are scoped to their
-- study and cross-study uniqueness does not apply.
--
-- This partial UNIQUE index also serves as the primary lookup index for reads
-- driven by the global field (cross-study queries), since the UNIQUE index
-- covers the same column prefix.
CREATE UNIQUE INDEX biosample_metadata_one_value_per_global_field
    ON qiita.biosample_metadata (biosample_idx, global_field_idx)
    WHERE global_field_idx IS NOT NULL;

-- Each biosample has at most one metadata row flagged as the owner's
-- identifier-for-this-biosample. The flag is application-maintained;
-- the partial UNIQUE index makes a second 'true' row for the same
-- biosample fail at the schema layer rather than relying on caller
-- discipline. Rows with is_owner_biosample_id = false are unrestricted.
CREATE UNIQUE INDEX biosample_metadata_unique_owner_biosample_id
    ON qiita.biosample_metadata (biosample_idx)
    WHERE is_owner_biosample_id = true;


-- =============================================================================
-- BIOSAMPLE FIELD EXCEPTIONS (per-(biosample, field) visibility overrides)
-- =============================================================================

CREATE TABLE qiita.biosample_field_exception (
    idx                          BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    biosample_idx                BIGINT NOT NULL REFERENCES qiita.biosample(idx) ON DELETE RESTRICT,
    -- Dual-keyed; see table comment.
    biosample_study_field_idx    BIGINT REFERENCES qiita.biosample_study_field(idx) ON DELETE RESTRICT,
    global_field_idx             BIGINT REFERENCES qiita.biosample_global_field(idx) ON DELETE RESTRICT,
    tier_override                qiita.tier NOT NULL,
    reason                       TEXT,
    created_by_idx               BIGINT NOT NULL REFERENCES qiita.principal(idx) ON DELETE RESTRICT,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Bumped on every UPDATE by the set_updated_at() trigger; used as the
    -- ETag for optimistic-concurrency control on upsert PUT.
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Exactly one of the two key columns must be populated. A row keyed on
    -- both or neither would be ambiguous.
    CONSTRAINT biosample_field_exception_exactly_one_key CHECK (
        (biosample_study_field_idx IS NOT NULL AND global_field_idx IS NULL)
        OR
        (biosample_study_field_idx IS NULL AND global_field_idx IS NOT NULL)
    )
);

COMMENT ON TABLE qiita.biosample_field_exception IS
    'Per-(biosample, field) visibility overrides. Used to restrict the audience for '
    'specific metadata values that need narrower visibility than their field''s general '
    'policy (e.g., a description field where one biosample accidentally received PII). '
    'An exception downgrades visibility to a specified tier_override. Exceptions on '
    'globally-linked metadata are keyed on global_field_idx so they follow the value '
    'across studies; exceptions on purely study-local metadata are keyed on '
    'biosample_study_field_idx.';

COMMENT ON COLUMN qiita.biosample_field_exception.tier_override IS
    'While most tier_override columns in the schema are nullable, '
    'field_exception tier_overrides are not because there would be '
    'no point in registering an exception if you were not overriding '
    'the expected value.';

-- Uniqueness for study-local exceptions: at most one exception per
-- (biosample, study-local field) pair.
CREATE UNIQUE INDEX biosample_field_exception_unique_local
    ON qiita.biosample_field_exception (biosample_idx, biosample_study_field_idx)
    WHERE biosample_study_field_idx IS NOT NULL;

-- Uniqueness for globally-linked exceptions: at most one exception per
-- (biosample, global field) pair, regardless of which study's field row
-- happens to be the current source of the metadata value.
CREATE UNIQUE INDEX biosample_field_exception_unique_global
    ON qiita.biosample_field_exception (biosample_idx, global_field_idx)
    WHERE global_field_idx IS NOT NULL;

CREATE INDEX biosample_field_exception_study_field_idx
    ON qiita.biosample_field_exception (biosample_study_field_idx)
    WHERE biosample_study_field_idx IS NOT NULL;
CREATE INDEX biosample_field_exception_global_field_idx
    ON qiita.biosample_field_exception (global_field_idx)
    WHERE global_field_idx IS NOT NULL;

CREATE TRIGGER biosample_field_exception_set_updated_at
    BEFORE UPDATE ON qiita.biosample_field_exception
    FOR EACH ROW EXECUTE FUNCTION qiita.set_updated_at();


-- =============================================================================
-- TRIGGER: biosample_metadata_apply_field_contract
--
-- "Field contract" = the constraints the source biosample_study_field row
-- (and any linked biosample_global_field) imposes on every metadata row that
-- references it. Two responsibilities, both keyed off the source field row
-- (the FK target of biosample_metadata.biosample_study_field_idx) so they
-- share a single SELECT:
--
--   1. CHECK: the populated value_* column matches the field's data_type.
--      data_type is resolved via COALESCE(study_field.data_type,
--      global_field.data_type) so the same trigger covers both purely-local
--      study fields (which own data_type) and globally-linked ones (which
--      inherit it from the linked global field; see the
--      biosample_study_field_inheritance_consistent CHECK). Rows with
--      value_missing_reason_idx populated are exempt — a missing reason
--      applies to any data_type and does not occupy a typed value column.
--
--   2. SET: NEW.global_field_idx is denormalized from the source study
--      field's biosample_global_field_idx. Cannot be a GENERATED column
--      because PostgreSQL generated columns cannot reference other tables.
--
-- A separate trigger on biosample_study_field
-- (propagate_global_field_link_to_biosample_metadata) keeps the
-- denormalization consistent if a study field's global link is updated
-- after metadata has already been written.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.biosample_metadata_apply_field_contract()
RETURNS TRIGGER AS $$
DECLARE
    expected_data_type qiita.field_data_type;
    populated_ok      BOOLEAN;
BEGIN
    -- Single SELECT covers both responsibilities: the global link and the
    -- resolved data_type for the source field row.
    SELECT bsf.biosample_global_field_idx,
           COALESCE(bsf.data_type, bgf.data_type)
      INTO NEW.global_field_idx, expected_data_type
      FROM qiita.biosample_study_field bsf
      LEFT JOIN qiita.biosample_global_field bgf
        ON bgf.idx = bsf.biosample_global_field_idx
     WHERE bsf.idx = NEW.biosample_study_field_idx;

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
            'biosample_metadata value column does not match field data_type % for biosample_study_field_idx %',
            expected_data_type, NEW.biosample_study_field_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_metadata_apply_field_contract_insert
    BEFORE INSERT ON qiita.biosample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.biosample_metadata_apply_field_contract();

-- The UPDATE trigger fires when any value_* column or the source field
-- changes — both are inputs to the data_type check.
CREATE TRIGGER biosample_metadata_apply_field_contract_update
    BEFORE UPDATE OF biosample_study_field_idx, value_text, value_numeric,
                     value_boolean, value_date, value_terminology_term_idx,
                     value_missing_reason_idx
        ON qiita.biosample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.biosample_metadata_apply_field_contract();


-- =============================================================================
-- TRIGGER: propagate biosample_study_field global link changes to metadata
--
-- Mirrors the biosample_study_field.biosample_global_field_idx column into
-- the denormalized biosample_metadata.global_field_idx column whenever the
-- former changes. Three transition shapes are recognised:
--
--   NULL -> non-NULL (upgrade local to global): propagate the new link to
--     every existing metadata row through this field. The partial unique
--     index biosample_metadata_one_value_per_global_field will reject
--     the propagation if any row's (biosample_idx, new_global_field_idx)
--     slot is already claimed by another study; the rejection rolls back
--     the underlying UPDATE on biosample_study_field. Existing metadata
--     rows are unusual on a still-local field but possible (a missing-
--     reason row written through it, for example).
--
--   non-NULL -> NULL (unlink), no metadata exists: propagate (no-op). The
--     field becomes local for any future writes through it; nothing is
--     stranded.
--
--   non-NULL -> NULL (unlink), metadata exists: REJECTED. Unlinking would
--     strand globally-linked metadata rows (their global_field_idx column
--     would be cleared) and silently break cross-study reads that other
--     studies may depend on. Caller must explicitly delete those metadata
--     rows first; the per-row delete makes the data loss visible and
--     deliberate.
--
--   non-NULL -> different non-NULL (rebind): REJECTED unconditionally.
--     Rebinding changes the semantic meaning of every metadata row
--     through the field — the readers that pinned against the original
--     global field now see a different global field's value. There is no coherent
--     intent to mutate a field's identity in place; the correct flow is
--     to create a new study_field bound to the desired global field and write
--     against that.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.propagate_global_field_link_to_biosample_metadata()
RETURNS TRIGGER AS $$
DECLARE
    metadata_row_count BIGINT;
BEGIN
    -- Short-circuit: no actual change in the global link (the trigger
    -- fires on any UPDATE OF biosample_global_field_idx, including
    -- noop-self UPDATEs).
    IF NEW.biosample_global_field_idx IS NOT DISTINCT FROM OLD.biosample_global_field_idx THEN
        RETURN NEW;
    END IF;

    -- Reject rebind (non-NULL -> different non-NULL) unconditionally:
    -- rebinding mutates the field's identity rather than evolving it.
    IF OLD.biosample_global_field_idx IS NOT NULL
       AND NEW.biosample_global_field_idx IS NOT NULL THEN
        RAISE EXCEPTION
            'cannot rebind biosample_study_field idx=% from biosample_global_field_idx=% to %; '
            'rebinding changes the semantic meaning of every metadata row through this field. '
            'Create a new biosample_study_field bound to the desired global field instead.',
            NEW.idx, OLD.biosample_global_field_idx, NEW.biosample_global_field_idx;
    END IF;

    -- Reject unlink (non-NULL -> NULL) when metadata rows exist through
    -- this field: clearing global_field_idx on those rows would silently
    -- break cross-study reads. Caller must delete the metadata rows first
    -- to surface the data loss explicitly.
    IF OLD.biosample_global_field_idx IS NOT NULL
       AND NEW.biosample_global_field_idx IS NULL THEN
        SELECT COUNT(*) INTO metadata_row_count
          FROM qiita.biosample_metadata
         WHERE biosample_study_field_idx = NEW.idx;
        IF metadata_row_count > 0 THEN
            RAISE EXCEPTION
                'cannot unlink biosample_study_field idx=% from biosample_global_field_idx=%; '
                '% metadata row(s) reference this field. Delete those rows first '
                'if the unlink is intentional.',
                NEW.idx, OLD.biosample_global_field_idx, metadata_row_count;
        END IF;
        -- No metadata rows; nothing to propagate. The field is local going forward.
        RETURN NEW;
    END IF;

    -- Remaining case: NULL -> non-NULL (upgrade local to global).
    -- Propagate the new link to every metadata row through this field;
    -- the partial unique index gates collisions and the UPDATE rolls back
    -- if any row's (biosample_idx, new_global_field_idx) slot is taken.
    UPDATE qiita.biosample_metadata
       SET global_field_idx = NEW.biosample_global_field_idx
     WHERE biosample_study_field_idx = NEW.idx;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_study_field_propagate_global_link
    AFTER UPDATE OF biosample_global_field_idx ON qiita.biosample_study_field
    FOR EACH ROW EXECUTE FUNCTION qiita.propagate_global_field_link_to_biosample_metadata();


-- =============================================================================
-- TRIGGER: enforce non-retired-link invariant on biosample_metadata inserts
--
-- A biosample_metadata row cannot exist for a (biosample, study) pair whose
-- biosample_to_study link is retired. This invariant is what the access rules
-- for retirement depend on: once a link is retired, no new metadata may be
-- written through that link. Row-repointing is separately forbidden by the
-- immutability trigger further down, so only the INSERT path needs guarding
-- here.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.biosample_metadata_reject_if_link_retired()
RETURNS TRIGGER AS $$
DECLARE
    link_retired BOOLEAN;
    field_study_idx BIGINT;
BEGIN
    SELECT bsf.study_idx
      INTO field_study_idx
      FROM qiita.biosample_study_field bsf
     WHERE bsf.idx = NEW.biosample_study_field_idx;

    SELECT bts.retired
      INTO link_retired
      FROM qiita.biosample_to_study bts
     WHERE bts.biosample_idx = NEW.biosample_idx
       AND bts.study_idx = field_study_idx;

    IF link_retired IS NULL THEN
        RAISE EXCEPTION 'biosample_metadata refers to (biosample=%, study=%) but no biosample_to_study row exists',
            NEW.biosample_idx, field_study_idx;
    END IF;

    IF link_retired = true THEN
        RAISE EXCEPTION 'biosample_metadata cannot be written: biosample_to_study(%, %) is retired',
            NEW.biosample_idx, field_study_idx;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_metadata_reject_if_link_retired_insert
    BEFORE INSERT ON qiita.biosample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.biosample_metadata_reject_if_link_retired();


-- =============================================================================
-- TRIGGER: enforce immutability of biosample_metadata key columns
--
-- A biosample_metadata row's (biosample_idx, biosample_study_field_idx) pair
-- identifies which biosample and which study field the value is FOR. Those
-- references cannot change once a row exists: if either was wrong, the
-- correct flow is to DELETE the row and INSERT a new one for the right pair.
-- This trigger raises an exception on any attempt to update either column,
-- catching the mistake at the schema layer rather than leaving it to
-- application discipline.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.biosample_metadata_reject_key_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.biosample_idx IS DISTINCT FROM OLD.biosample_idx THEN
        RAISE EXCEPTION 'biosample_metadata.biosample_idx is immutable; delete and re-insert instead';
    END IF;
    IF NEW.biosample_study_field_idx IS DISTINCT FROM OLD.biosample_study_field_idx THEN
        RAISE EXCEPTION 'biosample_metadata.biosample_study_field_idx is immutable; delete and re-insert instead';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_metadata_reject_key_update
    BEFORE UPDATE OF biosample_idx, biosample_study_field_idx ON qiita.biosample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.biosample_metadata_reject_key_update();


-- =============================================================================
-- TRIGGER: bump biosample.last_metadata_change_at on biosample_metadata writes
--
-- A biosample's last_metadata_change_at is set to now() whenever a
-- biosample_metadata row for it is inserted or updated. biosample_idx is
-- immutable on UPDATE (enforced above), so only one biosample is ever touched
-- per firing.
-- =============================================================================

CREATE OR REPLACE FUNCTION qiita.biosample_metadata_touch_biosample()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE qiita.biosample
       SET last_metadata_change_at = now()
     WHERE idx = NEW.biosample_idx;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER biosample_metadata_touch_biosample
    AFTER INSERT OR UPDATE ON qiita.biosample_metadata
    FOR EACH ROW EXECUTE FUNCTION qiita.biosample_metadata_touch_biosample();


-- migrate:down

-- Drop the trigger that lives on a previous migration's table (biosample_study_field).
-- Dropping the tables below takes their own triggers with them, but this one
-- would otherwise orphan against an empty target.
DROP TRIGGER IF EXISTS biosample_study_field_propagate_global_link ON qiita.biosample_study_field;

DROP TABLE IF EXISTS qiita.biosample_field_exception;
DROP TABLE IF EXISTS qiita.biosample_metadata;
DROP TABLE IF EXISTS qiita.biosample_to_study;
DROP TABLE IF EXISTS qiita.biosample;

DROP FUNCTION IF EXISTS qiita.biosample_metadata_reject_if_link_retired();
DROP FUNCTION IF EXISTS qiita.propagate_global_field_link_to_biosample_metadata();
DROP FUNCTION IF EXISTS qiita.biosample_metadata_apply_field_contract();
DROP FUNCTION IF EXISTS qiita.biosample_clear_submission_error_on_new_attempt();
DROP FUNCTION IF EXISTS qiita.biosample_metadata_reject_key_update();
DROP FUNCTION IF EXISTS qiita.biosample_metadata_touch_biosample();
