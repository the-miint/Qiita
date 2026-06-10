-- migrate:up

-- Seed a curated subset of ENVO terms used by the MVP metadata columns.
-- This insert acts as both the schema seed and the load, so status
-- starts at 'active' rather than the usual 'loading'.
INSERT INTO qiita.terminology (name, version, loaded_at, status)
VALUES ('ENVO', 'pre-release MVP', NOW(), 'active')
ON CONFLICT DO NOTHING;

INSERT INTO qiita.terminology_term
    (terminology_idx, term_id, label, is_obsolete, obsoletion_kind, obsoleted_in_version)
SELECT t.idx, v.term_id, v.label, v.is_obsolete,
       v.obsoletion_kind::qiita.terminology_term_obsoletion_kind, v.obsoleted_in_version
  FROM qiita.terminology t,
       (VALUES
           ('ENVO:01000249', 'urban biome',                          false, NULL,                NULL),
           ('ENVO:00000469', 'research facility',                     false, NULL,                NULL),
           ('ENVO:0010001',  'anthropogenic environmental material',  false, NULL,                NULL),
           ('ENVO:02000027', 'blood plasma material',                 false, NULL,                NULL),
           ('ENVO:02000029', 'cerebrospinal fluid material',          false, NULL,                NULL),
           ('ENVO:00005791', 'sterile water',                         false, NULL,                NULL),
           ('ENVO:00002003', 'fecal material',                        false, NULL,                NULL),
           ('ENVO:00009003', 'human-associated habitat',              true,  'source_deprecated', 'pre-release MVP')
       ) AS v(term_id, label, is_obsolete, obsoletion_kind, obsoleted_in_version)
 WHERE t.name = 'ENVO'
ON CONFLICT DO NOTHING;

-- Bind the three environmental-context fields to the newly seeded ENVO.
SELECT qiita.rebind_biosample_global_field_data_type(
    ARRAY['broad_scale_environmental_context',
          'local_environmental_context',
          'environmental_medium'],
    'terminology',
    'ENVO');


-- migrate:down

-- Unbind first; the terminology FK is ON DELETE RESTRICT, so the terminology
-- row cannot be deleted while these fields reference it. This rebind RAISES
-- (aborting the rollback) if any biosample_metadata rows already reference
-- these fields, so a down-migration is only possible before any environmental-
-- context metadata has been ingested.
SELECT qiita.rebind_biosample_global_field_data_type(
    ARRAY['broad_scale_environmental_context',
          'local_environmental_context',
          'environmental_medium'],
    'text');

DELETE FROM qiita.terminology_term
 WHERE terminology_idx IN (
     SELECT idx FROM qiita.terminology WHERE name = 'ENVO'
 );

DELETE FROM qiita.terminology
 WHERE name = 'ENVO';
