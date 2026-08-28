-- migrate:up

-- =============================================================================
-- QIITA SAMPLE TYPE terminology, and the sample_type biosample global field
-- =============================================================================
--
-- An internally-governed controlled vocabulary with no external source: this
-- database is where its terms are defined, so there is no release to load and
-- new terms are appended directly. version carries the date the vocabulary's
-- content last changed, and an append moves it.
--
-- This insert acts as both the schema seed and the load, so status starts at
-- 'active' rather than the usual 'loading'.

INSERT INTO qiita.terminology (name, version, loaded_at, status)
VALUES ('Qiita Sample Type', '2026-08-27', NOW(), 'active')
ON CONFLICT DO NOTHING;

-- term_id is the string a metadata import supplies for the value; label is how
-- the term is displayed.
INSERT INTO qiita.terminology_term (terminology_idx, term_id, label)
SELECT t.idx, v.term_id, v.label
  FROM qiita.terminology t,
       (VALUES
           ('control_blank',          'control blank'),
           ('control_spike_in',       'control spike in'),
           ('control_sterile_water',  'control sterile water'),
           ('control_sea_water',      'control sea water'),
           ('artificial_sea_water',   'artificial sea water'),
           ('estuarine_water_filter', 'estuarine water filter'),
           ('sea_water',              'sea water'),
           ('sea_water_filter',       'sea water filter'),
           ('cerebrospinal_fluid',    'CSF'),
           ('plasma',                 'plasma'),
           ('feces',                  'feces')
       ) AS v(term_id, label)
 WHERE t.name = 'Qiita Sample Type'
ON CONFLICT DO NOTHING;

-- Add the sample_type global field, bound to the terminology seeded above.
-- created_by_idx = 1 names the seeded system principal (SYSTEM_PRINCIPAL_IDX),
-- as in the original global-field seed.
INSERT INTO qiita.biosample_global_field
    (internal_name, display_name, data_type, required, terminology_idx, created_by_idx)
SELECT 'sample_type', 'sample type', 'terminology', true, idx, 1
  FROM qiita.terminology
 WHERE name = 'Qiita Sample Type'
ON CONFLICT DO NOTHING;


-- migrate:down

-- Unwind field -> terms -> terminology, since every inbound reference is
-- ON DELETE RESTRICT. The field DELETE aborts the rollback if a study field,
-- metadata row, field exception, or checklist row already links it; the term
-- DELETE aborts if any metadata row holds one of these terms. Clear referencing
-- rows manually before rolling back.

DELETE FROM qiita.biosample_global_field
 WHERE internal_name = 'sample_type';

DELETE FROM qiita.terminology_term
 WHERE terminology_idx = (
     SELECT idx FROM qiita.terminology WHERE name = 'Qiita Sample Type'
 );

DELETE FROM qiita.terminology
 WHERE name = 'Qiita Sample Type';
