-- migrate:up

-- exported_identifier's second processing kind: a qiita.processing run (amplicon
-- today), per the table's FORWARD PLAN. A processed sample of this kind is
-- (processing_idx, prep_sample_idx); processing_idx subsumes the whole run
-- config because qiita.processing deduplicates on a SHA-256 of it.
--
-- The two edits the FORWARD PLAN requires, plus the retirement trigger, which
-- must learn the new column for its ON DELETE SET NULL to stay legal.
ALTER TABLE qiita.exported_identifier
    ADD COLUMN processing_idx BIGINT
        REFERENCES qiita.processing(processing_idx) ON DELETE SET NULL;

ALTER TABLE qiita.exported_identifier
    DROP CONSTRAINT exported_identifier_one_processing,
    ADD CONSTRAINT exported_identifier_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx, processing_idx) = 1);

-- One live identifier per (processing run, sample). Its own index: a live row of
-- this kind has alignment_idx NULL, which the alignment index treats as distinct.
-- processing_idx leads for the same cohort-lookup reason alignment_idx leads there.
CREATE UNIQUE INDEX exported_identifier_live_processing_sample
    ON qiita.exported_identifier (processing_idx, prep_sample_idx)
    WHERE NOT retired;

CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_identifier()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.alignment_idx IS NULL AND OLD.alignment_idx IS NOT NULL AND NOT NEW.retired THEN
        NEW.retired := true;
        NEW.retired_at := now();
        NEW.retire_reason := format(
            'alignment_definition %s was deleted; the processing this identifier named no longer exists',
            OLD.alignment_idx
        );
    ELSIF NEW.processing_idx IS NULL AND OLD.processing_idx IS NOT NULL AND NOT NEW.retired THEN
        NEW.retired := true;
        NEW.retired_at := now();
        NEW.retire_reason := format(
            'processing %s was deleted; the processing this identifier named no longer exists',
            OLD.processing_idx
        );
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON COLUMN qiita.exported_identifier.processing_idx IS
    'The qiita.processing run (amplicon) this processed sample went through; '
    'NULL on an alignment row. Exactly one of alignment_idx / processing_idx is '
    'set on a live row.';

-- migrate:down

CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_identifier()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.alignment_idx IS NULL AND OLD.alignment_idx IS NOT NULL AND NOT NEW.retired THEN
        NEW.retired := true;
        NEW.retired_at := now();
        NEW.retire_reason := format(
            'alignment_definition %s was deleted; the processing this identifier named no longer exists',
            OLD.alignment_idx
        );
    END IF;
    RETURN NEW;
END;
$$;

DROP INDEX IF EXISTS qiita.exported_identifier_live_processing_sample;
ALTER TABLE qiita.exported_identifier
    DROP CONSTRAINT exported_identifier_one_processing,
    ADD CONSTRAINT exported_identifier_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx) = 1);
ALTER TABLE qiita.exported_identifier DROP COLUMN IF EXISTS processing_idx;
