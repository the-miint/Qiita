-- migrate:up

-- =============================================================================
-- EVERY STUDY AN ENA IMPORT CREATED BECOMES PUBLIC
-- =============================================================================
--
-- An ENA import now creates its studies public
-- (`repositories.study.get_or_create_study_by_ena_accessions`;
-- docs/architecture/data-model.md, "Default tiers", says why). This migration
-- brings the studies that imports created before this deploy to the same tier.
-- A study that an import only matched by accession is not one it created
-- (`qiita.ena_import_batch_item.study_created`), and keeps its tier.
--
-- Making a study public publishes its public view and its local fields with no
-- tier_override. So the migration refuses while an import-created study holds
-- a sample that did not come from ENA: a biosample linked to it with no
-- ena_sample_accession (the import writes one on every biosample it creates),
-- or a sequenced_sample in it with no ena_run_accession (likewise). A sample
-- whose link to the study is retired is no longer in it, and is not counted.
--
-- Two kinds of content that a person may have added after the import are
-- published without a check, by decision: the study's alias, description,
-- abstract and funding, and local fields added with no tier_override. The
-- deploy checklist lists both before this migration runs.
DO $$
DECLARE
    offending TEXT;
BEGIN
    WITH imported AS (
        SELECT study_idx FROM qiita.ena_import_batch_item WHERE study_created
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
          USING HINT = 'Making these studies public would publish their biosamples or'
                       ' sequenced_samples that did not come from ENA. Stop the deploy and'
                       ' raise each study with its owner; no route moves samples out of a study.';
    END IF;
END
$$;

UPDATE qiita.study
   SET default_tier = 'public'
 WHERE idx IN (SELECT study_idx FROM qiita.ena_import_batch_item WHERE study_created);

-- migrate:down

-- Every study an import created was `member` before the up step: no route
-- changes default_tier after creation, and imports created their studies at
-- `member` until this deploy.
UPDATE qiita.study
   SET default_tier = 'member'
 WHERE idx IN (SELECT study_idx FROM qiita.ena_import_batch_item WHERE study_created);
