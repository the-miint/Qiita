-- migrate:up

-- exported_identifier's next processing kind: a qiita.processing run (amplicon
-- today), per the table's FORWARD PLAN, following the alignment and mask kinds. A
-- processed sample of this kind is (processing_idx, prep_sample_idx); processing_idx
-- subsumes the whole run config because qiita.processing deduplicates on a SHA-256 of
-- it.
--
-- The two edits the FORWARD PLAN requires, plus the retirement trigger, which must
-- learn the new column for its ON DELETE SET NULL to stay legal. The CHECK and the
-- trigger already carry the alignment and mask kinds and the prep_sample-delete arm;
-- this adds the processing kind to each.
ALTER TABLE qiita.exported_identifier
    ADD COLUMN processing_idx BIGINT
        REFERENCES qiita.processing(processing_idx) ON DELETE SET NULL;

ALTER TABLE qiita.exported_identifier
    DROP CONSTRAINT exported_identifier_one_processing,
    ADD CONSTRAINT exported_identifier_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx, mask_idx, processing_idx) = 1);

-- One live identifier per (processing run, sample). Its own index: a live row of
-- this kind has alignment_idx and mask_idx NULL, which their indexes treat as
-- distinct. processing_idx leads for the same cohort-lookup reason alignment_idx and
-- mask_idx lead theirs.
CREATE UNIQUE INDEX exported_identifier_live_processing_sample
    ON qiita.exported_identifier (processing_idx, prep_sample_idx)
    WHERE NOT retired;

CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_identifier()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NOT NEW.retired THEN
        IF NEW.alignment_idx IS NULL AND OLD.alignment_idx IS NOT NULL THEN
            NEW.retired := true;
            NEW.retired_at := now();
            NEW.retire_reason := format(
                'alignment_definition %s was deleted; the processing this identifier named no longer exists',
                OLD.alignment_idx
            );
        ELSIF NEW.mask_idx IS NULL AND OLD.mask_idx IS NOT NULL THEN
            NEW.retired := true;
            NEW.retired_at := now();
            NEW.retire_reason := format(
                'mask_definition %s was deleted; the processing this identifier named no longer exists',
                OLD.mask_idx
            );
        ELSIF NEW.processing_idx IS NULL AND OLD.processing_idx IS NOT NULL THEN
            NEW.retired := true;
            NEW.retired_at := now();
            NEW.retire_reason := format(
                'processing %s was deleted; the processing this identifier named no longer exists',
                OLD.processing_idx
            );
        ELSIF NEW.prep_sample_idx IS NULL AND OLD.prep_sample_idx IS NOT NULL THEN
            NEW.retired := true;
            NEW.retired_at := now();
            NEW.retire_reason :=
                'the prep_sample this identifier named was deleted; '
                'named_biosample_accession and named_biosample_export_entity_id say '
                'which biosample it was of';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON COLUMN qiita.exported_identifier.processing_idx IS
    'The qiita.processing run (amplicon) this processed sample went through; NULL on '
    'an alignment- or mask-kind row. Exactly one of alignment_idx / mask_idx / '
    'processing_idx is set on a live row.';

COMMENT ON FUNCTION qiita.retire_detached_exported_identifier() IS
    'Retires an exported_identifier whose processing FK (alignment_idx, mask_idx or '
    'processing_idx) or prep_sample_idx was just nulled by an ON DELETE SET NULL. '
    'Required for that FK action to satisfy the one_processing and live_names_prep_sample '
    'CHECKs — without it deleting an alignment_definition, mask_definition, processing or '
    'prep_sample row that a live identifier names fails.';

COMMENT ON TABLE qiita.exported_identifier IS
    'Public handle (export_id, ''QM<idx>'') for one processed sample — a '
    'prep_sample plus its processing: (alignment_idx, prep_sample_idx), '
    '(mask_idx, prep_sample_idx) or (processing_idx, prep_sample_idx). Exists because no '
    'accession identifies a processed sample: a biosample accession cannot say WHICH '
    'sequencing of it, and an ENA run accession is NULL until submission. Never deleted, '
    'only retired, so a published citation keeps resolving. Extended by ADD COLUMN per '
    'new processing type — which must update the one_processing CHECK AND add its own '
    'partial unique index.';

-- migrate:down

-- Restore the trigger and CHECK to their pre-processing state: the alignment, mask
-- and prep_sample-delete arms, and the alignment/mask one_processing CHECK.
CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_identifier()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NOT NEW.retired THEN
        IF NEW.alignment_idx IS NULL AND OLD.alignment_idx IS NOT NULL THEN
            NEW.retired := true;
            NEW.retired_at := now();
            NEW.retire_reason := format(
                'alignment_definition %s was deleted; the processing this identifier named no longer exists',
                OLD.alignment_idx
            );
        ELSIF NEW.mask_idx IS NULL AND OLD.mask_idx IS NOT NULL THEN
            NEW.retired := true;
            NEW.retired_at := now();
            NEW.retire_reason := format(
                'mask_definition %s was deleted; the processing this identifier named no longer exists',
                OLD.mask_idx
            );
        ELSIF NEW.prep_sample_idx IS NULL AND OLD.prep_sample_idx IS NOT NULL THEN
            NEW.retired := true;
            NEW.retired_at := now();
            NEW.retire_reason :=
                'the prep_sample this identifier named was deleted; '
                'named_biosample_accession and named_biosample_export_entity_id say '
                'which biosample it was of';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION qiita.retire_detached_exported_identifier() IS
    'Retires an exported_identifier whose processing FK (alignment_idx or mask_idx) '
    'or prep_sample_idx was just nulled by an ON DELETE SET NULL. Required for that '
    'FK action to satisfy the one_processing and live_names_prep_sample CHECKs — '
    'without it deleting an alignment_definition, mask_definition or prep_sample row '
    'that a live identifier names fails.';

COMMENT ON TABLE qiita.exported_identifier IS
    'Public handle (export_id, ''QM<idx>'') for one processed sample — a '
    'prep_sample plus its processing: (alignment_idx, prep_sample_idx) or '
    '(mask_idx, prep_sample_idx). Exists because no accession identifies a processed '
    'sample: a biosample accession cannot say WHICH sequencing of it, and an ENA run '
    'accession is NULL until submission. Never deleted, only retired, so a published '
    'citation keeps resolving. Extended by ADD COLUMN per new processing type — which '
    'must update the one_processing CHECK AND add its own partial unique index.';

DROP INDEX IF EXISTS qiita.exported_identifier_live_processing_sample;
-- The processing-kind identifier (alignment_idx and mask_idx NULL, processing_idx
-- set) the up added is exactly the shape the restored alignment/mask CHECK below
-- rejects, so a rollback that kept those rows would abort on the ADD CONSTRAINT.
-- Rolling back removes the amplicon-identifier capability, so its identifiers go with
-- it.
DELETE FROM qiita.exported_identifier WHERE processing_idx IS NOT NULL;
ALTER TABLE qiita.exported_identifier
    DROP CONSTRAINT exported_identifier_one_processing,
    ADD CONSTRAINT exported_identifier_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx, mask_idx) = 1);
ALTER TABLE qiita.exported_identifier DROP COLUMN IF EXISTS processing_idx;
