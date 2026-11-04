-- An import item may name a list of ENA runs instead of a whole study. A run-list
-- item starts ungrouped (no study accession, the runs it was asked for); the
-- driver resolves the runs and splits it into one item per study they belong to.

-- migrate:up

ALTER TABLE qiita.ena_import_batch_item
    ALTER COLUMN ena_study_accession DROP NOT NULL,
    ADD COLUMN requested_run_accessions TEXT[];

ALTER TABLE qiita.ena_import_batch_item
    ADD CONSTRAINT ena_import_batch_item_accession_or_runs
        CHECK (ena_study_accession IS NOT NULL OR requested_run_accessions IS NOT NULL),
    ADD CONSTRAINT ena_import_batch_item_runs_nonempty
        CHECK (requested_run_accessions IS NULL OR cardinality(requested_run_accessions) > 0);

COMMENT ON COLUMN qiita.ena_import_batch_item.requested_run_accessions IS
    'The ENA runs this item imports, or NULL to import its whole study. A NULL '
    'ena_study_accession marks a run-list item not grouped by study.';

COMMENT ON TABLE qiita.ena_import_batch_item IS
    'One INSDC study accession, or one run list, within a qiita.ena_import_batch, '
    'tracked independently through resolve -> register -> download-submit so one '
    'item''s failure never affects its siblings. state mirrors '
    'qiita_common.models.ena_import.BatchItemState.';

CREATE UNIQUE INDEX ena_import_batch_item_one_ungrouped_per_batch
    ON qiita.ena_import_batch_item (batch_idx)
    WHERE ena_study_accession IS NULL;


-- migrate:down

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM qiita.ena_import_batch_item
        WHERE ena_study_accession IS NULL OR requested_run_accessions IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'cannot roll back: ena_import_batch_item has run-list rows; delete those batches first';
    END IF;
END $$;

DROP INDEX qiita.ena_import_batch_item_one_ungrouped_per_batch;

ALTER TABLE qiita.ena_import_batch_item
    DROP CONSTRAINT ena_import_batch_item_runs_nonempty,
    DROP CONSTRAINT ena_import_batch_item_accession_or_runs;

ALTER TABLE qiita.ena_import_batch_item
    DROP COLUMN requested_run_accessions,
    ALTER COLUMN ena_study_accession SET NOT NULL;

COMMENT ON TABLE qiita.ena_import_batch_item IS
    'One INSDC study accession within a qiita.ena_import_batch, '
    'tracked independently through resolve -> register -> download-submit so one '
    'accession''s failure never affects its siblings. state mirrors '
    'qiita_common.models.ena_import.BatchItemState.';
