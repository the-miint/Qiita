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

-- Bind taxon_id to NCBI Taxonomy now that the terminology exists.
-- Safe to flip data_type without backfill: no biosample_metadata
-- rows reference this field in deployed state.
UPDATE qiita.biosample_global_field
   SET data_type = 'terminology',
       terminology_idx = (SELECT idx FROM qiita.terminology WHERE name = 'NCBI Taxonomy')
 WHERE internal_name = 'taxon_id';


-- migrate:down

-- Reverse the taxon_id rebind first; the terminology FK is ON DELETE RESTRICT,
-- so the terminology row cannot be deleted while taxon_id references it.
-- Any biosample_metadata rows that wrote value_terminology_term_idx against
-- taxon_id while it was terminology-typed survive this flip (the trigger
-- fires on metadata DML, not on the field UPDATE), but become silently
-- inconsistent under the now-text data_type until they are deleted.
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
