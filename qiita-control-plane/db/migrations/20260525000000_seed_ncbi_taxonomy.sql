-- migrate:up

-- Seed the NCBI Taxonomy terminology with the metagenome subtree.
-- This insert acts as both the schema seed and the load, so status
-- starts at 'active' rather than the usual 'loading'.

INSERT INTO qiita.terminology (name, version, loaded_at, status)
VALUES ('NCBI Taxonomy', 'pre-release MVP', NOW(), 'active')
ON CONFLICT DO NOTHING;

INSERT INTO qiita.terminology_term (terminology_idx, term_id, label)
SELECT t.idx, v.term_id, v.label
  FROM qiita.terminology t,
       (VALUES
           ('256318',  'metagenome'),
           ('646099',  'human metagenome'),
           ('408170',  'human gut metagenome'),
           ('1504969', 'human blood metagenome')
       ) AS v(term_id, label)
 WHERE t.name = 'NCBI Taxonomy'
ON CONFLICT DO NOTHING;

-- Refuse to flip taxon_id's data_type if any biosample_metadata rows
-- already reference it: the metadata-side field-contract trigger fires
-- on metadata DML, not on this field UPDATE, so pre-existing rows would
-- survive silently misaligned under the new data_type.
DO $$
DECLARE n int;
BEGIN
    SELECT COUNT(*) INTO n
      FROM qiita.biosample_metadata m
      JOIN qiita.biosample_study_field bsf
        ON bsf.idx = m.biosample_study_field_idx
      JOIN qiita.biosample_global_field bgf
        ON bgf.idx = bsf.biosample_global_field_idx
     WHERE bgf.internal_name = 'taxon_id';
    IF n > 0 THEN
        RAISE EXCEPTION
            'taxon_id has % biosample_metadata rows; data_type flip to terminology unsafe', n;
    END IF;
END $$;

-- Bind taxon_id to NCBI Taxonomy now that the terminology exists.
UPDATE qiita.biosample_global_field
   SET data_type = 'terminology',
       terminology_idx = (SELECT idx FROM qiita.terminology WHERE name = 'NCBI Taxonomy')
 WHERE internal_name = 'taxon_id';


-- migrate:down

-- Mirror of the up-direction guard: same hazard in reverse, since
-- flipping back to 'text' would leave any value_terminology_term_idx
-- rows silently inconsistent under the now-text data_type.
-- same-pattern-ok: dbmate migrations cannot share code across the
-- up/down blocks, so the guard is repeated rather than factored.
DO $$
DECLARE n int;
BEGIN
    SELECT COUNT(*) INTO n
      FROM qiita.biosample_metadata m
      JOIN qiita.biosample_study_field bsf
        ON bsf.idx = m.biosample_study_field_idx
      JOIN qiita.biosample_global_field bgf
        ON bgf.idx = bsf.biosample_global_field_idx
     WHERE bgf.internal_name = 'taxon_id';
    IF n > 0 THEN
        RAISE EXCEPTION
            'taxon_id has % biosample_metadata rows; data_type flip to text unsafe', n;
    END IF;
END $$;

-- Reverse the taxon_id rebind first; the terminology FK is ON DELETE
-- RESTRICT, so the terminology row cannot be deleted while taxon_id
-- references it.
UPDATE qiita.biosample_global_field
   SET data_type = 'text',
       terminology_idx = NULL
 WHERE internal_name = 'taxon_id';

DELETE FROM qiita.terminology_term
 WHERE terminology_idx IN (
     SELECT idx FROM qiita.terminology WHERE name = 'NCBI Taxonomy'
 );

DELETE FROM qiita.terminology
 WHERE name = 'NCBI Taxonomy';
