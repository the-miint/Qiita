-- migrate:up

-- UNIQUE adds its own (non-partial) backing index covering the same
-- lookups the existing partial index served; drop the partial one to
-- avoid a duplicate index over the same column.
DROP INDEX IF EXISTS qiita.study_ebi_accession_idx;

-- Postgres UNIQUE treats NULLs as distinct, so "unique when present"
-- needs no WHERE predicate: multiple studies may leave the column
-- NULL, but two non-NULL rows may not share a value.
ALTER TABLE qiita.study
    ADD CONSTRAINT study_ebi_study_accession_unique UNIQUE (ebi_study_accession);


-- migrate:down

ALTER TABLE qiita.study
    DROP CONSTRAINT IF EXISTS study_ebi_study_accession_unique;

CREATE INDEX IF NOT EXISTS study_ebi_accession_idx
    ON qiita.study (ebi_study_accession)
    WHERE ebi_study_accession IS NOT NULL;
