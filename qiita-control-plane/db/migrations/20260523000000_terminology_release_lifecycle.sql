-- migrate:up

-- =============================================================================
-- TERMINOLOGY release lifecycle, obsoletion tracking, and curated reasons seed
-- =============================================================================
-- Layered on top of 20260501000004_terminology.sql. Three additive concerns:
--   1. terminology.status enum + column for in-flight load lifecycle
--   2. terminology_term obsoletion tracking (kind, version, FK replaced_by,
--      notes) and a composite-FK invariant on terminology_closure that ties
--      both endpoints to the same terminology
--   3. missing_value_reason obsoletion columns and seed of the
--      pre-2023 + 2023 INSDC curated vocabulary

-- -----------------------------------------------------------------------------
-- terminology.status
-- -----------------------------------------------------------------------------

CREATE TYPE qiita.terminology_status AS ENUM (
    'loading',
    'active',
    'failed'
);

ALTER TABLE qiita.terminology
    ADD COLUMN status qiita.terminology_status NOT NULL DEFAULT 'loading';

COMMENT ON TABLE qiita.terminology IS
    'A controlled vocabulary or ontology (UBERON, ENVO, NCBI Taxonomy, etc.). '
    'Terminology rows are created as part of a load: a terminology does not '
    'exist in this database until content for it has been loaded. '
    'Each subsequent reload updates version, loaded_at, and status on the '
    'existing row. '
    'Note terminology rows are never deleted on reload — '
    'terminology_term.terminology_idx ON DELETE RESTRICT requires that '
    'any referenced terminology remain resolvable.';

COMMENT ON COLUMN qiita.terminology.status IS
    'Lifecycle of the row. ''loading'' during an in-flight load; '
    '''active'' when the load is complete and the row reflects a '
    'consistent terminology version; ''failed'' when a load aborted '
    'and the row''s contents may be inconsistent with the source. '
    'Defaults to ''loading'' so the column is set the moment the '
    'loader inserts the row.';


-- -----------------------------------------------------------------------------
-- terminology_term obsoletion tracking + composite-uniqueness target
-- -----------------------------------------------------------------------------

CREATE TYPE qiita.terminology_term_obsoletion_kind AS ENUM (
    'source_deprecated',
    'source_merged',
    'silently_dropped'
);

-- replaced_by changes semantics: VARCHAR term_id string -> BIGINT FK to
-- terminology_term(idx). Drop+add is safe here because the deployed
-- terminology_term table is empty.
ALTER TABLE qiita.terminology_term
    DROP COLUMN replaced_by;

ALTER TABLE qiita.terminology_term
    ADD COLUMN obsoletion_kind       qiita.terminology_term_obsoletion_kind,
    ADD COLUMN obsoleted_in_version  VARCHAR(50),
    ADD COLUMN replaced_by           BIGINT REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT,
    ADD COLUMN notes                 TEXT,
    ADD CONSTRAINT terminology_term_idx_terminology_unique
        UNIQUE (idx, terminology_idx),
    ADD CONSTRAINT terminology_term_replaced_by_only_if_obsolete
        CHECK (replaced_by IS NULL OR is_obsolete = true),
    ADD CONSTRAINT terminology_term_obsoletion_columns_aligned
        CHECK (
            (is_obsolete = false AND obsoletion_kind IS NULL AND obsoleted_in_version IS NULL)
            OR
            (is_obsolete = true AND obsoletion_kind IS NOT NULL AND obsoleted_in_version IS NOT NULL)
        );

COMMENT ON TABLE qiita.terminology_term IS
    'A term from a controlled vocabulary or ontology (UBERON, ENVO, NCBI Taxonomy, etc.). '
    'Load of a new ontology version upserts (NOT replaces) terminology_term rows by '
    '(terminology_idx, term_id) (preserving idx for any term_id already present, marking dropped term_ids '
    'is_obsolete=true and recording replaced_by when the source supplies a replacement). '
    'Note terminology and term rows are never deleted on reload — '
    'biosample_metadata.value_terminology_term_idx '
    'ON DELETE RESTRICT requires that any referenced term remain resolvable.';

COMMENT ON COLUMN qiita.terminology_term.is_obsolete IS
    'True when the term has been dropped from the source vocabulary on '
    'a subsequent reload. Term rows are never deleted; obsoletion is '
    'recorded by flipping this flag instead.';

COMMENT ON COLUMN qiita.terminology_term.replaced_by IS
    'Optional pointer to the term that supersedes this one when the '
    'source vocabulary records a replacement. May reference a term in '
    'a different terminology (cross-ontology repointing). Populated '
    'only when is_obsolete is true.';

COMMENT ON COLUMN qiita.terminology_term.obsoletion_kind IS
    'Why this term was marked obsolete on the most recent load. '
    'Constrained by terminology_term_obsoletion_columns_aligned to be '
    'non-null iff is_obsolete is true.';

COMMENT ON COLUMN qiita.terminology_term.obsoleted_in_version IS
    'The first terminology.version in which this database recorded the '
    'term as obsolete. Set-once: once written, subsequent reloads keep '
    'the original value even if the term remains obsolete across many '
    'versions. Cleared back to NULL only on un-obsoletion (a later '
    'release un-deprecates the term). Constrained by '
    'terminology_term_obsoletion_columns_aligned to be non-null iff '
    'is_obsolete is true.';

COMMENT ON COLUMN qiita.terminology_term.notes IS
    'Free-text notes for cases that do not fit the structured columns '
    '(partial-mapping caveats, source-doc references, repointing '
    'context). Shared between the loader (which appends audit lines '
    'for tolerated anomalies, e.g. unresolved replaced_by attempts) '
    'and operator-added content; entries are newline-separated and '
    'accumulate across reloads. Null when nothing extra needs recording.';


-- -----------------------------------------------------------------------------
-- terminology_closure: lift FKs to composite so endpoints share terminology
-- -----------------------------------------------------------------------------

ALTER TABLE qiita.terminology_closure
    DROP CONSTRAINT terminology_closure_ancestor_term_idx_fkey,
    DROP CONSTRAINT terminology_closure_descendant_term_idx_fkey,
    DROP CONSTRAINT terminology_closure_terminology_idx_fkey,
    ADD CONSTRAINT terminology_closure_ancestor_fk
        FOREIGN KEY (ancestor_term_idx, terminology_idx)
        REFERENCES qiita.terminology_term (idx, terminology_idx) ON DELETE RESTRICT,
    ADD CONSTRAINT terminology_closure_descendant_fk
        FOREIGN KEY (descendant_term_idx, terminology_idx)
        REFERENCES qiita.terminology_term (idx, terminology_idx) ON DELETE RESTRICT;

COMMENT ON TABLE qiita.terminology_closure IS
    'Transitive closure of a terminology. On reload the loader '
    'rebuilds closure rows scoped to a single terminology_idx rather '
    'than truncating the table — closure rows for other terminologies '
    'remain untouched. The rebuilt rows are consistent with the parent '
    'terminology''s current version and loaded_at; to answer "what '
    'version of this ontology is the closure against?", read '
    'terminology.version for the parent row. The denormalised '
    'terminology_idx column is structurally constrained to equal both '
    'endpoint terminologies via composite FKs against terminology_term.';


-- -----------------------------------------------------------------------------
-- missing_value_reason: obsoletion columns + INSDC seed
-- -----------------------------------------------------------------------------

ALTER TABLE qiita.missing_value_reason
    ADD COLUMN is_obsolete  BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN replaced_by  BIGINT REFERENCES qiita.missing_value_reason(idx) ON DELETE RESTRICT,
    ADD COLUMN notes        TEXT,
    ADD CONSTRAINT missing_value_reason_replaced_by_only_if_obsolete
        CHECK (replaced_by IS NULL OR is_obsolete = true);

COMMENT ON COLUMN qiita.missing_value_reason.is_obsolete IS
    'True when this reason has been retired from the curated set. '
    'Reason rows are never deleted — biosample_metadata.value_missing_reason_idx '
    'and prep_sample_metadata.value_missing_reason_idx ON DELETE RESTRICT '
    'require that any referenced reason remain resolvable; retirement is '
    'recorded by flipping this flag instead.';

COMMENT ON COLUMN qiita.missing_value_reason.replaced_by IS
    'Optional pointer to the reason that supersedes this one when a '
    'retired reason has a curated replacement. Populated only when '
    'is_obsolete is true.';

COMMENT ON COLUMN qiita.missing_value_reason.notes IS
    'Free-text notes for the row — e.g., the schema version in which '
    'the reason was retired, the rationale, or mapping caveats. Null '
    'when nothing extra needs recording.';

-- Pre-2023 reasons plus additional 2023 INSDC vocabulary. Descriptions are the
-- Definition column verbatim from
-- https://www.insdc.org/technical-specifications/missing-value-reporting/
-- as of 5/2026.
INSERT INTO qiita.missing_value_reason (name, description) VALUES
    ('not applicable',    'Information is inappropriate to report, can indicate that the standard itself fails to model or represent the information appropriately'),
    ('not collected',     'Information of an expected format was not given because it has not been collected'),
    ('not provided',      'Information of an expected format was not given, a value may be given at the later stage'),
    ('restricted access', 'Information exists but can not be released openly because of privacy concerns'),
    ('missing: control sample',
     'Information is not applicable as the sample represents a negative control sample collected in a lab.'),
    ('missing: sample group',
     'Information is not applicable as the sample represents a group of samples that do not have a single origin.'),
    ('missing: synthetic construct',
     'Information does not exist as the sample represents an ab-initio synthetic construct.'),
    ('missing: lab stock',
     'Information was not collected as the sample represents a cultured cell line or model organism under long-term lab control.'),
    ('missing: third party data',
     'Information does not exist as the metadata was not collected or reported in records predating the 2023 agreement.'),
    ('missing: data agreement established pre-2023',
     'Data agreements were established before the 2023 INSDC standard and metadata can not be provided.'),
    ('missing: endangered species',
     'Information can not be reported as the target organism is endangered e.g. on the IUCN red-list.'),
    ('missing: human-identifiable',
     'Information can not be reported as the metadata would make the sample human-identifiable.');


-- migrate:down

-- Reverse in opposite order so dependencies unwind cleanly.

-- NOTE: Rollback succeeds only when no biosample_metadata.value_missing_reason_idx
-- or prep_sample_metadata.value_missing_reason_idx row references a seeded
-- reason: both FKs are ON DELETE RESTRICT, so this DELETE will fail with a
-- FK violation otherwise. Clear referencing rows manually before rolling back.
DELETE FROM qiita.missing_value_reason
WHERE name IN (
    'not applicable',
    'not collected',
    'not provided',
    'restricted access',
    'missing: control sample',
    'missing: sample group',
    'missing: synthetic construct',
    'missing: lab stock',
    'missing: third party data',
    'missing: data agreement established pre-2023',
    'missing: endangered species',
    'missing: human-identifiable'
);

ALTER TABLE qiita.missing_value_reason
    DROP CONSTRAINT missing_value_reason_replaced_by_only_if_obsolete,
    DROP COLUMN notes,
    DROP COLUMN replaced_by,
    DROP COLUMN is_obsolete;

ALTER TABLE qiita.terminology_closure
    DROP CONSTRAINT terminology_closure_descendant_fk,
    DROP CONSTRAINT terminology_closure_ancestor_fk,
    ADD CONSTRAINT terminology_closure_terminology_idx_fkey
        FOREIGN KEY (terminology_idx) REFERENCES qiita.terminology(idx) ON DELETE RESTRICT,
    ADD CONSTRAINT terminology_closure_ancestor_term_idx_fkey
        FOREIGN KEY (ancestor_term_idx) REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT,
    ADD CONSTRAINT terminology_closure_descendant_term_idx_fkey
        FOREIGN KEY (descendant_term_idx) REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT;

COMMENT ON TABLE qiita.terminology_closure IS
    'Transitive closure of a terminology. Rebuilt as part of each '
    'terminology reload, and therefore consistent with the parent '
    'terminology''s current version and loaded_at. To answer "what '
    'version of this ontology is the closure against?", read '
    'terminology.version for the parent row.';

ALTER TABLE qiita.terminology_term
    DROP CONSTRAINT terminology_term_obsoletion_columns_aligned,
    DROP CONSTRAINT terminology_term_replaced_by_only_if_obsolete,
    DROP CONSTRAINT terminology_term_idx_terminology_unique,
    DROP COLUMN notes,
    DROP COLUMN replaced_by,
    DROP COLUMN obsoleted_in_version,
    DROP COLUMN obsoletion_kind,
    ADD COLUMN replaced_by VARCHAR(255);

COMMENT ON TABLE qiita.terminology_term IS NULL;

DROP TYPE qiita.terminology_term_obsoletion_kind;

ALTER TABLE qiita.terminology DROP COLUMN status;

COMMENT ON TABLE qiita.terminology IS
    'A controlled vocabulary or ontology (UBERON, ENVO, NCBI Taxonomy, etc.). '
    'Terminology rows are created as part of a load: a terminology does not '
    'exist in this database until content for it has been loaded. Each '
    'subsequent reload updates version and loaded_at on the existing row '
    'and rebuilds terminology_term and terminology_closure. As a result, '
    'version and loaded_at always describe the content currently in the '
    'database.';

DROP TYPE qiita.terminology_status;
