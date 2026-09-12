-- migrate:up

-- =============================================================================
-- OWNER-BIOSAMPLE-ID FIELDS BECOME UNIQUE WITHIN THEIR STUDY
-- =============================================================================
--
-- An owner's identifier for a sample only identifies it if the study's other
-- samples cannot carry the same one. Imports now mint the owner-id field with
-- unique_in_study set and refuse to write through a field that lacks it, so
-- the fields minted before that rule existed are brought up to it here.
--
-- The field row carries no marker of its own -- is_owner_biosample_id lives on
-- the metadata row -- so an owner-id field is identified by having at least one
-- metadata row flagged that way.
--
-- Globally-linked rows are excluded. The write path refuses to put an owner id
-- through a linked field, but a field that once held them could have been
-- upgraded since, and setting the flag on a linked row trips the inheritance
-- CHECK with a message about inheritance rather than about owner ids.
--
-- THIS MIGRATION CAN FAIL, AND THAT IS THE POINT. The UPDATE fires the
-- propagation trigger, which mirrors the flag onto every metadata row through
-- each field; the partial unique index then rejects any study whose samples
-- already share an owner id, and the whole migration rolls back. That is a
-- report about the data, not a defect in the migration: two samples in one
-- study answering to the same owner id means at least one of them is
-- mislabelled, and which one is a decision no migration can take.
--
-- To list them before or after an abort:
--
--   SELECT sf.study_idx,
--          sf.idx   AS study_field_idx,
--          sf.display_name,
--          m.value_text,
--          count(*) AS biosample_count
--     FROM qiita.biosample_study_field sf
--     JOIN qiita.biosample_metadata m
--       ON m.biosample_study_field_idx = sf.idx
--      AND m.is_owner_biosample_id
--    WHERE sf.biosample_global_field_idx IS NULL
--      AND NOT sf.unique_in_study
--    GROUP BY sf.study_idx, sf.idx, sf.display_name, m.value_text
--   HAVING count(*) > 1
--    ORDER BY sf.study_idx, m.value_text;

UPDATE qiita.biosample_study_field sf
   SET unique_in_study = true
 WHERE sf.biosample_global_field_idx IS NULL
   AND NOT sf.unique_in_study
   AND EXISTS (
       SELECT 1
         FROM qiita.biosample_metadata m
        WHERE m.biosample_study_field_idx = sf.idx
          AND m.is_owner_biosample_id
   );


-- migrate:down

-- Deliberately empty. Clearing unique_in_study wholesale would not restore the
-- prior state: it would also clear the flag on any owner-id field a study set
-- deliberately through the field-edit route, and nothing here records which
-- rows this migration changed. A rollback that needs the flag cleared clears it
-- per field, which is what the edit route is for.
