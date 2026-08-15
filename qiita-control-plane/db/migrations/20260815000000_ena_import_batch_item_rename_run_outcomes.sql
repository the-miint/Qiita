-- migrate:up

-- "Run" is overloaded in this domain (Qiita's own sequencing_run vs. ENA's
-- "one sequencing of a prepped sample"); rename this column so it names the
-- latter explicitly. The original CREATE TABLE
-- (20260725000020_ena_import_batch.sql) is already applied and is not
-- edited -- see CLAUDE.md "Database migrations".
ALTER TABLE qiita.ena_import_batch_item RENAME COLUMN run_outcomes TO ena_run_outcomes;

-- migrate:down

ALTER TABLE qiita.ena_import_batch_item RENAME COLUMN ena_run_outcomes TO run_outcomes;
