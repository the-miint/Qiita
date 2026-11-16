-- migrate:up

-- Deleting a prep_sample (a sequenced-pool delete, or any other path) retires the
-- `exported_identifier` rows naming it instead of being refused by their FK. The
-- retired row stays, with its `export_id`, its kind, and the accession and `QB<n>`
-- handle of the deleted prep_sample's biosample. No route resolves a `QM<n>` today;
-- these columns are the record of what a published handle named.
--
--   * `prep_sample_idx` becomes nullable and its FK ON DELETE SET NULL, as
--     `alignment_idx` and `mask_idx` already are. A live row still names its
--     prep_sample (`exported_identifier_live_names_prep_sample`).
--   * `named_biosample_accession` / `named_biosample_export_entity_id` keep a
--     public description of the deleted prep_sample's biosample. A BEFORE
--     DELETE trigger on prep_sample writes them, since the FK action runs after the
--     prep_sample row (and the join to its biosample) is gone. The kind stays on
--     the row: `alignment_idx` or `mask_idx` is untouched by a prep_sample delete.
--   * `retire_detached_exported_identifier` also retires a row whose
--     `prep_sample_idx` the FK action nulls.
--
-- The trigger writes the two columns while `prep_sample_idx` is still set (the FK
-- action nulls it afterwards, in the same statement), so a CHECK tying them to a
-- NULL `prep_sample_idx` would refuse the trigger's own UPDATE.

ALTER TABLE qiita.exported_identifier
    ALTER COLUMN prep_sample_idx DROP NOT NULL,
    DROP CONSTRAINT exported_identifier_prep_sample_idx_fkey,
    ADD CONSTRAINT exported_identifier_prep_sample_idx_fkey
        FOREIGN KEY (prep_sample_idx) REFERENCES qiita.prep_sample(idx) ON DELETE SET NULL,
    ADD COLUMN named_biosample_accession TEXT,
    ADD COLUMN named_biosample_export_entity_id TEXT,
    ADD CONSTRAINT exported_identifier_live_names_prep_sample
        CHECK (retired OR prep_sample_idx IS NOT NULL);

COMMENT ON COLUMN qiita.exported_identifier.prep_sample_idx IS
    'The prep_sample this identifier names. NULL only on a retired row whose '
    'prep_sample was deleted (ON DELETE SET NULL); named_biosample_accession and '
    'named_biosample_export_entity_id then say what it was.';
COMMENT ON COLUMN qiita.exported_identifier.named_biosample_accession IS
    'The biosample accession of the deleted prep_sample this row named, written by '
    'qiita.describe_exported_identifier_prep_sample when the prep_sample is deleted. '
    'NULL while the prep_sample exists, and when its biosample had no accession.';
COMMENT ON COLUMN qiita.exported_identifier.named_biosample_export_entity_id IS
    'The public handle (exported_entity.export_entity_id, ''QB<n>'') of that '
    'biosample, when one was minted; written with named_biosample_accession.';

-- ---------------------------------------------------------------------------
-- describe_exported_identifier_prep_sample
-- ---------------------------------------------------------------------------
CREATE FUNCTION qiita.describe_exported_identifier_prep_sample()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    UPDATE qiita.exported_identifier ei
       SET named_biosample_accession = bs.biosample_accession,
           named_biosample_export_entity_id = ee.export_entity_id
      FROM qiita.biosample bs
      LEFT JOIN qiita.exported_entity ee ON ee.biosample_idx = bs.idx
     WHERE ei.prep_sample_idx = OLD.idx
       AND bs.idx = OLD.biosample_idx;
    RETURN OLD;
END;
$$;

COMMENT ON FUNCTION qiita.describe_exported_identifier_prep_sample() IS
    'BEFORE DELETE on prep_sample: writes the biosample accession and its '
    'export_entity_id onto every exported_identifier naming the prep_sample, before '
    'the FK action nulls prep_sample_idx and retire_detached_exported_identifier '
    'retires the row.';

CREATE TRIGGER prep_sample_describe_exported_identifier
    BEFORE DELETE ON qiita.prep_sample
    FOR EACH ROW EXECUTE FUNCTION qiita.describe_exported_identifier_prep_sample();

-- ---------------------------------------------------------------------------
-- retire_detached_exported_identifier, extended to prep_sample_idx
-- ---------------------------------------------------------------------------
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


-- migrate:down

-- Refuses (NOT NULL) while a row's prep_sample was deleted: those rows name nothing
-- the RESTRICT FK could point at.
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

DROP TRIGGER IF EXISTS prep_sample_describe_exported_identifier ON qiita.prep_sample;
DROP FUNCTION IF EXISTS qiita.describe_exported_identifier_prep_sample();

ALTER TABLE qiita.exported_identifier
    DROP CONSTRAINT exported_identifier_live_names_prep_sample,
    DROP COLUMN named_biosample_export_entity_id,
    DROP COLUMN named_biosample_accession,
    DROP CONSTRAINT exported_identifier_prep_sample_idx_fkey,
    ADD CONSTRAINT exported_identifier_prep_sample_idx_fkey
        FOREIGN KEY (prep_sample_idx) REFERENCES qiita.prep_sample(idx) ON DELETE RESTRICT,
    ALTER COLUMN prep_sample_idx SET NOT NULL;
