-- migrate:up

-- These placeholder MVP seeds append terms directly rather than running the
-- terminology reload pipeline, so version/loaded_at on the NCBI Taxonomy and
-- ENVO rows are left as originally seeded (the reload contract moves both
-- together and does not apply here).

-- Add the mouse gut metagenome taxon and the mouse host taxon to the existing
-- NCBI Taxonomy terminology (seeded earlier as 'pre-release MVP').
INSERT INTO qiita.terminology_term (terminology_idx, term_id, label)
SELECT t.idx, v.term_id, v.label
  FROM qiita.terminology t,
       (VALUES
           ('410661', 'mouse gut metagenome'),
           ('10090', 'Mus musculus')
       ) AS v(term_id, label)
 WHERE t.name = 'NCBI Taxonomy'
ON CONFLICT DO NOTHING;

-- Add the animal-associated habitat context term to the existing ENVO
-- terminology. This term is obsolete at source but is inserted anyway because
-- it appears in data we need to import.
INSERT INTO qiita.terminology_term
    (terminology_idx, term_id, label, is_obsolete, obsoletion_kind, obsoleted_in_version)
SELECT t.idx, v.term_id, v.label, v.is_obsolete,
       v.obsoletion_kind::qiita.terminology_term_obsoletion_kind, v.obsoleted_in_version
  FROM qiita.terminology t,
       (VALUES
           ('ENVO:00006776', 'animal-associated habitat', true, 'source_deprecated', 'pre-release MVP')
       ) AS v(term_id, label, is_obsolete, obsoletion_kind, obsoleted_in_version)
 WHERE t.name = 'ENVO'
ON CONFLICT DO NOTHING;


-- migrate:down

DELETE FROM qiita.terminology_term
 WHERE terminology_idx = (SELECT idx FROM qiita.terminology WHERE name = 'ENVO')
   AND term_id IN ('ENVO:00006776');

DELETE FROM qiita.terminology_term
 WHERE terminology_idx = (SELECT idx FROM qiita.terminology WHERE name = 'NCBI Taxonomy')
   AND term_id IN ('410661', '10090');
