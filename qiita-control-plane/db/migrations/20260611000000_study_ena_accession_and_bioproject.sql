-- migrate:up

-- Rename the study accession column and its UNIQUE constraint from the
-- ebi_* spelling to ena_*. RENAME CONSTRAINT also renames the backing
-- index in step, so all three names move together.
ALTER TABLE qiita.study
    RENAME COLUMN ebi_study_accession TO ena_study_accession;

ALTER TABLE qiita.study
    RENAME CONSTRAINT study_ebi_study_accession_unique TO study_ena_study_accession_unique;

-- New BioProject accession column. Postgres UNIQUE treats NULLs as
-- distinct, so "unique when present" needs no WHERE predicate: many
-- studies may leave it NULL but two non-NULL rows may not share a value.
ALTER TABLE qiita.study
    ADD COLUMN bioproject_accession VARCHAR(50);

ALTER TABLE qiita.study
    ADD CONSTRAINT study_bioproject_accession_unique UNIQUE (bioproject_accession);


-- migrate:down

ALTER TABLE qiita.study
    DROP CONSTRAINT IF EXISTS study_bioproject_accession_unique;

ALTER TABLE qiita.study
    DROP COLUMN IF EXISTS bioproject_accession;

ALTER TABLE qiita.study
    RENAME CONSTRAINT study_ena_study_accession_unique TO study_ebi_study_accession_unique;

ALTER TABLE qiita.study
    RENAME COLUMN ena_study_accession TO ebi_study_accession;
