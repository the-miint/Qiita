-- A mask is the second processing kind of qiita.exported_processing and
-- qiita.exported_identifier, following the FORWARD PLAN in
-- 20260813000001_exported_processing.sql and 20260810000000_exported_identifier.sql:
-- a `mask_idx` column with its own FK, the one_processing CHECK widened to name it,
-- a partial unique index of its own, and the detach trigger extended to retire a
-- row whose mask_definition is deleted.
--
-- exported_processing gets it so a masked-read export can cite which mask produced
-- each file without publishing mask_idx. exported_identifier gets it so each
-- exported sample has a public name: a processed sample is a prep_sample plus the
-- processing it went through, and a masked read set is a prep_sample under a mask.
--
-- The FK is ON DELETE SET NULL for the reason the alignment column has it: a
-- published handle outlives what it named.

-- migrate:up

ALTER TABLE qiita.exported_processing
    ADD COLUMN mask_idx BIGINT
        REFERENCES qiita.mask_definition(mask_idx) ON DELETE SET NULL;

ALTER TABLE qiita.exported_processing
    DROP CONSTRAINT exported_processing_one_processing,
    ADD CONSTRAINT exported_processing_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx, mask_idx) = 1);

-- One live handle per mask. A row of the alignment kind has mask_idx NULL, and a
-- unique index treats NULLs as distinct, so it does not collide here.
CREATE UNIQUE INDEX exported_processing_live_mask
    ON qiita.exported_processing (mask_idx)
    WHERE NOT retired;

ALTER TABLE qiita.exported_identifier
    ADD COLUMN mask_idx BIGINT
        REFERENCES qiita.mask_definition(mask_idx) ON DELETE SET NULL;

ALTER TABLE qiita.exported_identifier
    DROP CONSTRAINT exported_identifier_one_processing,
    ADD CONSTRAINT exported_identifier_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx, mask_idx) = 1);

-- One live identifier per (mask, prep_sample). mask_idx leads because the mint
-- looks a cohort up as `mask_idx = $1 AND prep_sample_idx = ANY($2)`.
CREATE UNIQUE INDEX exported_identifier_live_mask_sample
    ON qiita.exported_identifier (mask_idx, prep_sample_idx)
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
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION qiita.retire_detached_exported_identifier() IS
    'Retires an exported_identifier whose processing FK (alignment_idx or mask_idx) '
    'was just nulled by an ON DELETE SET NULL. Required for that FK action to satisfy '
    'the one_processing CHECK — without it deleting an alignment_definition or '
    'mask_definition row that a live identifier names fails.';

CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_processing()
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
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION qiita.retire_detached_exported_processing() IS
    'Retires an exported_processing whose processing FK (alignment_idx or mask_idx) '
    'was just nulled by an ON DELETE SET NULL. Required for that FK action to satisfy '
    'the one_processing CHECK — without it deleting an alignment_definition or '
    'mask_definition row that a live handle names fails.';

COMMENT ON TABLE qiita.exported_processing IS
    'Public handle (export_processing_id, ''QP<idx>'') for one processing — an '
    'alignment_definition or a mask_definition. What a published bundle''s manifest '
    'or a masked-read export cites in place of alignment_idx or mask_idx. Entirely '
    'minted: a processing is something we did, so no accession exists to prefer. '
    'Never deleted, only retired, so a published citation keeps resolving. Extended '
    'by ADD COLUMN per new processing type — which must update the one_processing '
    'CHECK AND add its own partial unique index.';

COMMENT ON TABLE qiita.exported_identifier IS
    'Public handle (export_id, ''QM<idx>'') for one processed sample — a '
    'prep_sample plus its processing: (alignment_idx, prep_sample_idx) or '
    '(mask_idx, prep_sample_idx). Exists because no accession identifies a processed '
    'sample: a biosample accession cannot say WHICH sequencing of it, and an ENA run '
    'accession is NULL until submission. Never deleted, only retired, so a published '
    'citation keeps resolving. Extended by ADD COLUMN per new processing type — which '
    'must update the one_processing CHECK AND add its own partial unique index.';

COMMENT ON COLUMN qiita.exported_processing.mask_idx IS
    'The mask this handle names, for the mask kind; NULL on an alignment-kind row and '
    'on a row detached by deleting its mask_definition.';

COMMENT ON COLUMN qiita.exported_identifier.mask_idx IS
    'The mask the prep_sample was processed under, for the mask kind; NULL on an '
    'alignment-kind row and on a row detached by deleting its mask_definition.';


-- migrate:down

-- Dropping mask_idx leaves any live mask-kind row naming no processing, so the
-- re-added alignment-only CHECK fails and the rollback aborts while such rows exist.

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

COMMENT ON FUNCTION qiita.retire_detached_exported_identifier() IS
    'Retires an exported_identifier whose processing FK was just nulled by an '
    'ON DELETE SET NULL. Required for that FK action to satisfy the '
    'one_processing CHECK — without it deleting an alignment_definition row fails.';

CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_processing()
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

COMMENT ON FUNCTION qiita.retire_detached_exported_processing() IS
    'Retires an exported_processing whose processing FK was just nulled by an '
    'ON DELETE SET NULL. Required for that FK action to satisfy the '
    'one_processing CHECK — without it deleting an alignment_definition row fails.';

DROP INDEX IF EXISTS qiita.exported_identifier_live_mask_sample;
ALTER TABLE qiita.exported_identifier
    DROP CONSTRAINT exported_identifier_one_processing,
    DROP COLUMN mask_idx;
ALTER TABLE qiita.exported_identifier
    ADD CONSTRAINT exported_identifier_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx) = 1);

DROP INDEX IF EXISTS qiita.exported_processing_live_mask;
ALTER TABLE qiita.exported_processing
    DROP CONSTRAINT exported_processing_one_processing,
    DROP COLUMN mask_idx;
ALTER TABLE qiita.exported_processing
    ADD CONSTRAINT exported_processing_one_processing
        CHECK (retired OR num_nonnulls(alignment_idx) = 1);

COMMENT ON TABLE qiita.exported_processing IS
    'Public handle (export_processing_id, ''QP<idx>'') for one processing — an '
    'alignment_definition today. What a published bundle''s manifest cites so the '
    'table is reproducible without carrying alignment_idx, alongside the '
    'content-derived params_hash (which says whether two processings were the same, '
    'where this says which one). Entirely minted: a processing is something we did, '
    'so no accession exists to prefer. Never deleted, only retired, so a published '
    'citation keeps resolving. Extended by ADD COLUMN per new processing type — '
    'which must update the one_processing CHECK AND add its own partial unique index.';

COMMENT ON TABLE qiita.exported_identifier IS
    'Public handle (export_id, ''QM<idx>'') for one processed sample — a '
    'prep_sample plus its processing, (alignment_idx, prep_sample_idx) today. '
    'Exists because no accession identifies a processed sample: a biosample '
    'accession cannot say WHICH sequencing of it, and an ENA run accession is '
    'NULL until submission. Never deleted, only retired, so a published citation '
    'keeps resolving. Extended by ADD COLUMN per new processing type — which must '
    'update the one_processing CHECK AND add its own partial unique index (the '
    'existing one cannot cover a kind whose alignment_idx is NULL).';
