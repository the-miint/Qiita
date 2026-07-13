-- migrate:up

-- These placeholder MVP seeds append terms directly rather than running the
-- terminology reload pipeline, so version/loaded_at on the NCBI Taxonomy and
-- ENVO rows are left as originally seeded (the reload contract moves both
-- together and does not apply here).

-- Add the marine/estuarine metagenome taxa and the human host taxon to the
-- existing NCBI Taxonomy terminology (seeded earlier as 'pre-release MVP').
INSERT INTO qiita.terminology_term (terminology_idx, term_id, label)
SELECT t.idx, v.term_id, v.label
  FROM qiita.terminology t,
       (VALUES
           ('1561972', 'seawater metagenome'),
           ('1649191', 'estuary metagenome'),
           ('9606', 'Homo sapiens')
       ) AS v(term_id, label)
 WHERE t.name = 'NCBI Taxonomy'
ON CONFLICT DO NOTHING;

-- Seed the GSC MIxS water checklist under the ENA default sample checklist,
-- matching the parent linkage of the other seeded MIxS checklists.
INSERT INTO qiita.metadata_checklist (name, description, parent_metadata_checklist_idx)
SELECT 'ERC000024', 'GSC MIxS water', idx
  FROM qiita.metadata_checklist
 WHERE name = 'ERC000011'
ON CONFLICT DO NOTHING;

-- Add the depth global field. created_by_idx = 1 names the seeded system
-- principal (SYSTEM_PRINCIPAL_IDX), as in the original global-field seed.
INSERT INTO qiita.biosample_global_field
    (internal_name, display_name, description, data_type, required, created_by_idx)
VALUES
    ('depth_m', 'depth', 'Depth in meters', 'numeric', false, 1)
ON CONFLICT DO NOTHING;

-- Add the host taxon global field, bound to the NCBI Taxonomy terminology.
-- created_by_idx = 1 names the seeded system principal (SYSTEM_PRINCIPAL_IDX).
INSERT INTO qiita.biosample_global_field
    (internal_name, display_name, data_type, required, terminology_idx, created_by_idx)
SELECT 'host_taxon_id', 'host taxon id', 'terminology', true, idx, 1
  FROM qiita.terminology
 WHERE name = 'NCBI Taxonomy'
ON CONFLICT DO NOTHING;

-- Add the marine/aquatic environmental-context terms to the existing ENVO
-- terminology (seeded earlier as 'pre-release MVP').
INSERT INTO qiita.terminology_term (terminology_idx, term_id, label)
SELECT t.idx, v.term_id, v.label
  FROM qiita.terminology t,
       (VALUES
           ('ENVO:00000447', 'marine biome'),
           ('ENVO:00000015', 'ocean'),
           ('ENVO:00000022', 'river'),
           ('ENVO:00002149', 'sea water'),
           ('ENVO:00002010', 'saline water'),
           ('ENVO:01000301', 'estuarine water'),
           ('ENVO:01001201', 'marine environmental zone'),
           ('ENVO:01000407', 'littoral zone')
       ) AS v(term_id, label)
 WHERE t.name = 'ENVO'
ON CONFLICT DO NOTHING;


-- migrate:down

DELETE FROM qiita.terminology_term
 WHERE terminology_idx = (SELECT idx FROM qiita.terminology WHERE name = 'ENVO')
   AND term_id IN ('ENVO:00000447', 'ENVO:00000015', 'ENVO:00000022',
                   'ENVO:00002149', 'ENVO:00002010', 'ENVO:01000301',
                   'ENVO:01001201', 'ENVO:01000407');

DELETE FROM qiita.biosample_global_field
 WHERE internal_name IN ('depth_m', 'host_taxon_id');

DELETE FROM qiita.metadata_checklist
 WHERE name = 'ERC000024';

DELETE FROM qiita.terminology_term
 WHERE terminology_idx = (SELECT idx FROM qiita.terminology WHERE name = 'NCBI Taxonomy')
   AND term_id IN ('1561972', '1649191', '9606');
