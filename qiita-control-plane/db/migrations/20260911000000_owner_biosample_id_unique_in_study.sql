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
-- THIS MIGRATION CAN FAIL. The UPDATE fires the propagation trigger, which
-- mirrors the flag onto every metadata row written through each field -- not
-- only the rows carrying an owner id. Three separate rules can then reject the
-- flip, and any one of them rolls the whole migration back:
--
--   * two rows in one field hold the same value (the partial unique index).
--     Either row may carry an owner id or an ordinary value; the index does
--     not distinguish them. Two samples answering to the same value through
--     one field means at least one is mislabelled, and which one is a decision
--     no migration can take.
--
--   * a row in the field carries a missing-value marker (the no-missing-value
--     CHECK). A field whose job is to tell a study's samples apart cannot hold
--     a sample that declines to be told apart.
--
--   * a row's biosample reaches a published prep (the publication lock,
--     SQLSTATE P0001). Unlike the first two this is not a report about the
--     data and has no in-place resolution: a published biosample's metadata is
--     immutable.
--
-- The first two are the point: they name data a human has to settle before the
-- policy can hold. The third is a collision between this flip and publication,
-- and it stops the deploy rather than pointing at something to fix.
--
-- To list all three after an abort (the query reads unique_in_study, which the
-- first migration in this set adds, so it cannot run before the deploy):
--
--   WITH candidate AS (
--     SELECT sf.idx, sf.study_idx, sf.display_name
--       FROM qiita.biosample_study_field sf
--      WHERE sf.biosample_global_field_idx IS NULL
--        AND NOT sf.unique_in_study
--        AND EXISTS (SELECT 1 FROM qiita.biosample_metadata m
--                     WHERE m.biosample_study_field_idx = sf.idx
--                       AND m.is_owner_biosample_id)
--   )
--   SELECT c.study_idx, c.idx AS study_field_idx, c.display_name,
--          'repeated value' AS problem, m.value_text AS detail,
--          count(*) AS row_count
--     FROM candidate c
--     JOIN qiita.biosample_metadata m ON m.biosample_study_field_idx = c.idx
--    WHERE m.value_text IS NOT NULL
--    GROUP BY c.study_idx, c.idx, c.display_name, m.value_text
--   HAVING count(*) > 1
--   UNION ALL
--   SELECT c.study_idx, c.idx, c.display_name,
--          'missing-value marker', NULL, count(*)
--     FROM candidate c
--     JOIN qiita.biosample_metadata m ON m.biosample_study_field_idx = c.idx
--    WHERE m.value_missing_reason_idx IS NOT NULL
--    GROUP BY c.study_idx, c.idx, c.display_name
--   UNION ALL
--   SELECT c.study_idx, c.idx, c.display_name,
--          'biosample reaches a published prep', NULL, count(*)
--     FROM candidate c
--     JOIN qiita.biosample_metadata m ON m.biosample_study_field_idx = c.idx
--    WHERE qiita.is_biosample_reaching_published_prep(m.biosample_idx)
--    GROUP BY c.study_idx, c.idx, c.display_name
--    ORDER BY 1, 2;

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
