-- migrate:up

-- =============================================================================
-- CHECKLIST FIELDS, AND THE REQUIREMENTS THAT BIND THEM TO QIITA FIELDS
-- =============================================================================
-- A checklist names its required fields in the publisher's vocabulary; Qiita
-- holds the values under its own. Splitting the two means a checklist field's
-- name is stated once however many checklists require it, instead of once per
-- requirement row where copies can disagree.
--
-- metadata_checklist_field is renamed to metadata_checklist_requirement: a row
-- is one requirement a checklist places, which is what it was already, and
-- *_field no longer distinguishes it from the vocabulary table now that the
-- two concepts are separate.

ALTER TABLE qiita.metadata_checklist_field RENAME TO metadata_checklist_requirement;

-- Comments attach by OID and survive a rename, so metadata_checklist's own
-- comment still names the old table. Restate it with the new name.
COMMENT ON TABLE qiita.metadata_checklist IS
    'A published external metadata specification (e.g., MIxS, MIMARKS, MIMS) '
    'that describes which fields a sample should carry to be considered '
    'compliant. A metadata checklist''s required-field list is declared by '
    'the metadata_checklist_requirement table. Checklists are not enforced at '
    'write time; they are checked at query time when a sample claims '
    'conformance. Metadata checklists are not retired: conformance against '
    'a currently-defined checklist is always meaningful, even for samples '
    'created long ago. A metadata_checklist row does not carry creation-audit '
    'columns (created_by_idx, created_at) because the checklist is a mutable '
    'curated list: metadata_checklist_requirement rows may be added and '
    'removed over time.';

-- A table rename leaves constraint and index names behind, so move them too.
ALTER TABLE qiita.metadata_checklist_requirement
    RENAME CONSTRAINT metadata_checklist_field_exactly_one_target
                   TO metadata_checklist_requirement_exactly_one_target;
ALTER INDEX qiita.metadata_checklist_field_unique_biosample
    RENAME TO metadata_checklist_requirement_unique_biosample;
ALTER INDEX qiita.metadata_checklist_field_unique_prep_sample
    RENAME TO metadata_checklist_requirement_unique_prep_sample;
ALTER INDEX qiita.metadata_checklist_field_biosample_idx
    RENAME TO metadata_checklist_requirement_biosample_idx;
ALTER INDEX qiita.metadata_checklist_field_prep_sample_idx
    RENAME TO metadata_checklist_requirement_prep_sample_idx;


CREATE TABLE qiita.checklist_field (
    idx     BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name    TEXT NOT NULL,

    CONSTRAINT checklist_field_name_unique UNIQUE (name)
);

COMMENT ON TABLE qiita.checklist_field IS
    'A field named by a published metadata checklist, in the publisher''s own '
    'vocabulary. The name is what the publisher matches on the wire -- for an '
    'ENA checklist, the LABEL its XML declares -- so it is stored verbatim and '
    'is not a Qiita display name. One row serves every checklist that names '
    'the same field; which Qiita field supplies the value, and in what unit, '
    'belongs to the requirement that links them.';


ALTER TABLE qiita.metadata_checklist_requirement
    ADD COLUMN checklist_field_idx BIGINT NOT NULL
        REFERENCES qiita.checklist_field(idx) ON DELETE RESTRICT,
    ADD COLUMN unit TEXT;

COMMENT ON COLUMN qiita.metadata_checklist_requirement.unit IS
    'The unit this requirement''s value is submitted in, or NULL for a field '
    'that takes none. It belongs here rather than on checklist_field because '
    'the unit follows the Qiita field supplying the value, not the checklist '
    'field receiving it: a checklist may accept several, and which one applies '
    'is decided by the source. Free text, not an enumeration -- the vocabulary '
    'is the publisher''s, it is large, and it carries spellings that differ by '
    'codepoint, so a value must match what the checklist declares rather than '
    'any normalization of it.';

-- At most one requirement per checklist field per checklist.
CREATE UNIQUE INDEX metadata_checklist_requirement_unique_field
    ON qiita.metadata_checklist_requirement (metadata_checklist_idx, checklist_field_idx);


-- The parent link records a declared lineage between published specifications.
-- It does NOT mean a child requires or accepts its parent's fields: publishers
-- issue each checklist complete, and a child's accepted-field set is not a
-- superset of its parent's. Resolve a checklist's requirements from its own
-- rows rather than by walking this link.
COMMENT ON COLUMN qiita.metadata_checklist.parent_metadata_checklist_idx IS
    'Declared lineage between published checklist specifications. Does not '
    'imply a child requires or accepts its parent''s fields; resolve a '
    'checklist''s requirements from its own metadata_checklist_requirement '
    'rows, not by walking this link.';


-- The three environmental-context fields already in the registry are bound to
-- ENVO, and the values people actually supply for them are free text that no
-- ontology resolves. These hold what was supplied, verbatim. created_by_idx = 1
-- names the seeded system principal, as in the original global-field seed.
INSERT INTO qiita.biosample_global_field
    (internal_name, display_name, description, data_type, required, created_by_idx)
VALUES
    ('broad_scale_environmental_context_verbatim', 'env_broad_scale',
     'Broad-scale environmental context as supplied, unresolved against any ontology',
     'text', false, 1),
    ('local_environmental_context_verbatim', 'env_local_scale',
     'Local environmental context as supplied, unresolved against any ontology',
     'text', false, 1),
    ('environmental_medium_verbatim', 'env_medium',
     'Environmental medium as supplied, unresolved against any ontology',
     'text', false, 1)
ON CONFLICT (internal_name) DO NOTHING;


-- The ENVO-bound originals carry no description, which leaves the newer
-- verbatim field of each pair the only documented one. State what each holds
-- so the pair reads as the contrast it is.
UPDATE qiita.biosample_global_field
   SET description = 'Broad-scale environmental context, resolved against ENVO'
 WHERE internal_name = 'broad_scale_environmental_context';
UPDATE qiita.biosample_global_field
   SET description = 'Local environmental context, resolved against ENVO'
 WHERE internal_name = 'local_environmental_context';
UPDATE qiita.biosample_global_field
   SET description = 'Environmental medium, resolved against ENVO'
 WHERE internal_name = 'environmental_medium';


-- Seed the checklist fields and their requirements. The spec is staged in a
-- temp table so each field's name, source and unit are written once however
-- many checklists require it: a data-modifying CTE could not do this in one
-- statement, since a sibling CTE reading checklist_field sees the pre-statement
-- snapshot and so cannot join to rows being inserted alongside it.
CREATE TEMP TABLE checklist_field_spec (
    name                  TEXT NOT NULL,
    source_internal_name  TEXT NOT NULL,
    unit                  TEXT,
    checklist_names       TEXT[] NOT NULL
);

INSERT INTO checklist_field_spec (name, source_internal_name, unit, checklist_names)
VALUES
    ('collection date', 'collection_date', NULL,
     ARRAY['ERC000011', 'ERC000013', 'ERC000014', 'ERC000015', 'ERC000024']),
    ('geographic location (country and/or sea)', 'geographic_location_country_or_sea', NULL,
     ARRAY['ERC000011', 'ERC000013', 'ERC000014', 'ERC000015', 'ERC000024']),
    ('geographic location (latitude)', 'geographic_location_latitude', 'DD',
     ARRAY['ERC000013', 'ERC000014', 'ERC000015', 'ERC000024']),
    ('geographic location (longitude)', 'geographic_location_longitude', 'DD',
     ARRAY['ERC000013', 'ERC000014', 'ERC000015', 'ERC000024']),
    ('broad-scale environmental context', 'broad_scale_environmental_context_verbatim', NULL,
     ARRAY['ERC000013', 'ERC000014', 'ERC000015', 'ERC000024']),
    ('local environmental context', 'local_environmental_context_verbatim', NULL,
     ARRAY['ERC000013', 'ERC000014', 'ERC000015', 'ERC000024']),
    ('environmental medium', 'environmental_medium_verbatim', NULL,
     ARRAY['ERC000013', 'ERC000014', 'ERC000015', 'ERC000024']),
    ('depth', 'depth_m', 'm',
     ARRAY['ERC000024']);

INSERT INTO qiita.checklist_field (name)
SELECT name FROM checklist_field_spec
ON CONFLICT (name) DO NOTHING;

INSERT INTO qiita.metadata_checklist_requirement
    (metadata_checklist_idx, biosample_global_field_idx, checklist_field_idx, unit)
SELECT mc.idx, bgf.idx, cf.idx, fs.unit
  FROM checklist_field_spec fs
  CROSS JOIN LATERAL unnest(fs.checklist_names) AS cn(checklist_name)
  JOIN qiita.checklist_field cf ON cf.name = fs.name
  JOIN qiita.metadata_checklist mc ON mc.name = cn.checklist_name
  JOIN qiita.biosample_global_field bgf ON bgf.internal_name = fs.source_internal_name
ON CONFLICT DO NOTHING;

DROP TABLE checklist_field_spec;

-- migrate:down

DELETE FROM qiita.metadata_checklist_requirement
 WHERE checklist_field_idx IN (SELECT idx FROM qiita.checklist_field);

UPDATE qiita.biosample_global_field
   SET description = NULL
 WHERE internal_name IN (
     'broad_scale_environmental_context',
     'local_environmental_context',
     'environmental_medium'
 );

-- Every inbound reference to a global field is ON DELETE RESTRICT, so this
-- aborts the rollback once a study has adopted one of these fields, before
-- any value is even recorded against it.
DELETE FROM qiita.biosample_global_field
 WHERE internal_name IN (
     'broad_scale_environmental_context_verbatim',
     'local_environmental_context_verbatim',
     'environmental_medium_verbatim'
 );

COMMENT ON COLUMN qiita.metadata_checklist.parent_metadata_checklist_idx IS NULL;

COMMENT ON TABLE qiita.metadata_checklist IS
    'A published external metadata specification (e.g., MIxS, MIMARKS, MIMS) '
    'that describes which fields a sample should carry to be considered '
    'compliant. A metadata checklist''s required-field list is declared by '
    'the metadata_checklist_field table. Checklists are not enforced at '
    'write time; they are checked at query time when a sample claims '
    'conformance. Metadata checklists are not retired: conformance against '
    'a currently-defined checklist is always meaningful, even for samples '
    'created long ago. A metadata_checklist row does not carry creation-audit '
    'columns (created_by_idx, created_at) because the checklist is a mutable '
    'curated list: metadata_checklist_field rows may be added and removed '
    'over time.';

DROP INDEX qiita.metadata_checklist_requirement_unique_field;

ALTER TABLE qiita.metadata_checklist_requirement
    DROP COLUMN unit,
    DROP COLUMN checklist_field_idx;

DROP TABLE qiita.checklist_field;

ALTER INDEX qiita.metadata_checklist_requirement_prep_sample_idx
    RENAME TO metadata_checklist_field_prep_sample_idx;
ALTER INDEX qiita.metadata_checklist_requirement_biosample_idx
    RENAME TO metadata_checklist_field_biosample_idx;
ALTER INDEX qiita.metadata_checklist_requirement_unique_prep_sample
    RENAME TO metadata_checklist_field_unique_prep_sample;
ALTER INDEX qiita.metadata_checklist_requirement_unique_biosample
    RENAME TO metadata_checklist_field_unique_biosample;
ALTER TABLE qiita.metadata_checklist_requirement
    RENAME CONSTRAINT metadata_checklist_requirement_exactly_one_target
                   TO metadata_checklist_field_exactly_one_target;

ALTER TABLE qiita.metadata_checklist_requirement RENAME TO metadata_checklist_field;
