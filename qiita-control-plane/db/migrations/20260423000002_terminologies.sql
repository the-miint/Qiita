-- migrate:up

-- =============================================================================
-- TERMINOLOGY (ontologies and controlled vocabularies)
-- =============================================================================

CREATE TABLE qiita.terminology (
    idx          BIGSERIAL PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    version      VARCHAR(50) NOT NULL,
    loaded_at    TIMESTAMPTZ NOT NULL,

    CONSTRAINT terminology_name_unique UNIQUE (name)
);

COMMENT ON TABLE qiita.terminology IS
    'A controlled vocabulary or ontology (UBERON, ENVO, NCBI Taxonomy, etc.). '
    'Terminology rows are created as part of a load: a terminology does not '
    'exist in this database until content for it has been loaded. Each '
    'subsequent reload updates version and loaded_at on the existing row '
    'and rebuilds terminology_term and terminology_closure. As a result, '
    'version and loaded_at always describe the content currently in the '
    'database.';

COMMENT ON COLUMN qiita.terminology.version IS
    'The release version of the terminology currently loaded (e.g., an '
    'UBERON release tag or ontology version IRI). Required. Updated '
    'atomically with the term and closure rebuilds on each reload.';

COMMENT ON COLUMN qiita.terminology.loaded_at IS
    'The wall-clock time at which the current version was loaded into '
    'the database. Required. Updated atomically with the term and '
    'closure rebuilds on each reload.';


CREATE TABLE qiita.terminology_term (
    idx              BIGSERIAL PRIMARY KEY,
    terminology_idx  BIGINT NOT NULL REFERENCES qiita.terminology(idx) ON DELETE RESTRICT,
    term_id          VARCHAR(255) NOT NULL,
    label            VARCHAR(500) NOT NULL,
    is_obsolete      BOOLEAN NOT NULL DEFAULT false,
    replaced_by      VARCHAR(255),

    CONSTRAINT terminology_term_unique UNIQUE (terminology_idx, term_id)
);

CREATE INDEX terminology_term_label_idx ON qiita.terminology_term (label);
CREATE INDEX terminology_term_active_idx ON qiita.terminology_term (terminology_idx) WHERE is_obsolete = false;


CREATE TABLE qiita.terminology_closure (
    idx                    BIGSERIAL PRIMARY KEY,
    terminology_idx        BIGINT NOT NULL REFERENCES qiita.terminology(idx) ON DELETE RESTRICT,
    ancestor_term_idx      BIGINT NOT NULL REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT,
    descendant_term_idx    BIGINT NOT NULL REFERENCES qiita.terminology_term(idx) ON DELETE RESTRICT,
    distance               INT NOT NULL,

    CONSTRAINT terminology_closure_unique UNIQUE (ancestor_term_idx, descendant_term_idx),
    CONSTRAINT terminology_closure_distance_nonneg CHECK (distance >= 0)
);

COMMENT ON TABLE qiita.terminology_closure IS
    'Transitive closure of a terminology. Rebuilt as part of each '
    'terminology reload, and therefore consistent with the parent '
    'terminology''s current version and loaded_at. To answer "what '
    'version of this ontology is the closure against?", read '
    'terminology.version for the parent row.';

CREATE INDEX terminology_closure_ancestor_idx ON qiita.terminology_closure (ancestor_term_idx);
CREATE INDEX terminology_closure_descendant_idx ON qiita.terminology_closure (descendant_term_idx);


-- =============================================================================
-- MISSING VALUE REASONS
-- =============================================================================
--
-- Controlled vocabulary of reasons a metadata value may be missing (e.g.,
-- 'not_collected', 'not_applicable', 'withheld_for_privacy'). Referenced by
-- the metadata EAV tables: a metadata row either carries a value in one of
-- the value_* columns or a reference to one of these rows via
-- value_missing_reason_idx. Grouped with the terminology tables because it
-- is the same kind of artifact: a curated lookup vocabulary rather than
-- user-authored content.

CREATE TABLE qiita.missing_value_reason (
    idx          BIGSERIAL PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    description  TEXT,

    CONSTRAINT missing_value_reason_name_unique UNIQUE (name)
);


-- migrate:down

DROP TABLE IF EXISTS qiita.missing_value_reason;
DROP TABLE IF EXISTS qiita.terminology_closure;
DROP TABLE IF EXISTS qiita.terminology_term;
DROP TABLE IF EXISTS qiita.terminology;
