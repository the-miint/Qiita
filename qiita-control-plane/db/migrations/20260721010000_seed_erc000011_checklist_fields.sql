-- migrate:up

-- Bind the ENA default sample checklist (ERC000011, seeded by
-- 20260524000000_seed_metadata_checklist.sql) to its MANDATORY field set --
-- fetched verbatim from https://www.ebi.ac.uk/ena/browser/api/xml/ERC000011
-- on 2026-07-21 (T03 owner decision D-C: seed the REAL ERC000011 mandatory
-- set, not an assumed one). Of ERC000011's 30 <FIELD> entries, exactly two
-- carry <MANDATORY>mandatory</MANDATORY> -- every other field (lat_lon, host
-- scientific name, strain, isolate, sex, serovar, ...) is
-- <MANDATORY>optional</MANDATORY>:
--
--   <LABEL>collection date</LABEL>
--   <NAME>collection_date</NAME>
--   <MANDATORY>mandatory</MANDATORY>
--
--   <LABEL>geographic location (country and/or sea)</LABEL>
--   <NAME>geographic_location_country_andor_sea</NAME>
--   <MANDATORY>mandatory</MANDATORY>
--
-- Both are covered by an existing seeded biosample_global_field row (matched
-- here by display_name against 20260501000007_biosample_field.sql's seed):
-- 'collection date' and 'geographic location (country and/or sea)'. No
-- ERC000011-mandatory field is left uncovered, so this migration seeds
-- exactly these two metadata_checklist_field rows and invents nothing for
-- fields the checklist does not actually require (lat/long, the
-- environmental-context triad, and depth are optional-or-absent in
-- ERC000011 itself; they belong to the GSC MIxS extension checklists, e.g.
-- ERC000024, already seeded separately).
--
-- ON CONFLICT DO NOTHING keeps the seed re-runnable, same pattern as
-- 20260501000007's global-field seed.

INSERT INTO qiita.metadata_checklist_field (metadata_checklist_idx, biosample_global_field_idx)
SELECT mc.idx, gf.idx
  FROM qiita.metadata_checklist mc
  JOIN qiita.biosample_global_field gf
    ON gf.display_name IN (
        'collection date',
        'geographic location (country and/or sea)'
    )
 WHERE mc.name = 'ERC000011'
ON CONFLICT DO NOTHING;


-- migrate:down

DELETE FROM qiita.metadata_checklist_field
 WHERE metadata_checklist_idx = (SELECT idx FROM qiita.metadata_checklist WHERE name = 'ERC000011')
   AND biosample_global_field_idx IN (
       SELECT idx FROM qiita.biosample_global_field
        WHERE display_name IN (
            'collection date',
            'geographic location (country and/or sea)'
        )
   );
