-- migrate:up

-- =============================================================================
-- EVERY STUDY AN ENA IMPORT CREATED BECOMES PUBLIC
-- =============================================================================
--
-- An ENA import now creates its studies public
-- (`repositories.study.get_or_create_study_by_ena_accessions`;
-- docs/architecture/data-model.md, "Default tiers", says why). This brings the
-- studies that imports created before it to the same tier. A study that an
-- import only matched by accession is not one it created
-- (`qiita.ena_import_batch_item.study_created`), and keeps its tier.
--
-- Making a study public publishes everything in it, so the migration refuses
-- while an import-created study holds data that did not come from ENA: a
-- biosample linked to it with no ena_sample_accession (the import writes one on
-- every biosample it creates), or a sequenced_sample in it with no
-- ena_run_accession (likewise). Only active links count: a sample retired from
-- the study is no longer in it.
DO $$
DECLARE
    offending TEXT;
BEGIN
    WITH imported AS (
        SELECT DISTINCT study_idx FROM qiita.ena_import_batch_item WHERE study_created
    ), not_from_ena AS (
        SELECT bts.study_idx
          FROM qiita.biosample_to_study bts
          JOIN qiita.biosample b ON b.idx = bts.biosample_idx
         WHERE NOT bts.retired
           AND b.ena_sample_accession IS NULL
           AND bts.study_idx IN (SELECT study_idx FROM imported)
        UNION
        SELECT pts.study_idx
          FROM qiita.prep_sample_to_study pts
          JOIN qiita.sequenced_sample ss ON ss.prep_sample_idx = pts.prep_sample_idx
         WHERE NOT pts.retired
           AND ss.ena_run_accession IS NULL
           AND pts.study_idx IN (SELECT study_idx FROM imported)
    )
    SELECT string_agg(study_idx::text, ', ' ORDER BY study_idx) INTO offending
      FROM not_from_ena;
    IF offending IS NOT NULL THEN
        RAISE EXCEPTION 'ENA-imported studies % hold data that did not come from ENA', offending
          USING HINT = 'Making these studies public would publish that data. Decide each study'
                       ' with its owner, for example by moving the samples that did not come'
                       ' from ENA into a study of their own, then apply again.';
    END IF;
END
$$;

UPDATE qiita.study
   SET default_tier = 'public'
 WHERE idx IN (SELECT study_idx FROM qiita.ena_import_batch_item WHERE study_created);

-- migrate:down

-- Every study an import created was `member` before the up step: default_tier
-- cannot be changed after creation, and imports created their studies at
-- `member` until this deploy.
UPDATE qiita.study
   SET default_tier = 'member'
 WHERE idx IN (SELECT study_idx FROM qiita.ena_import_batch_item WHERE study_created);
