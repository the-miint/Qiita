-- migrate:up

-- No route deletes an alignment_definition any more, so the retirement the two
-- detach triggers write no longer follows an admin purge. The row can still be
-- deleted by hand, and the `ON DELETE SET NULL` FK action then nulls the column
-- these triggers watch; what they do is unchanged, only the wording they write and
-- their COMMENTs. Rows retired before this migration keep the text they were
-- written with.
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

-- migrate:down

CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_identifier()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.alignment_idx IS NULL AND OLD.alignment_idx IS NOT NULL AND NOT NEW.retired THEN
        NEW.retired := true;
        NEW.retired_at := now();
        NEW.retire_reason := format(
            'alignment_definition %s was purged; the processing this identifier named no longer exists',
            OLD.alignment_idx
        );
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION qiita.retire_detached_exported_identifier() IS
    'Retires an exported_identifier whose processing FK was just nulled by an '
    'ON DELETE SET NULL. Required for that FK action to satisfy the '
    'one_processing CHECK — without it an alignment purge fails.';

CREATE OR REPLACE FUNCTION qiita.retire_detached_exported_processing()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.alignment_idx IS NULL AND OLD.alignment_idx IS NOT NULL AND NOT NEW.retired THEN
        NEW.retired := true;
        NEW.retired_at := now();
        NEW.retire_reason := format(
            'alignment_definition %s was purged; the processing this identifier named no longer exists',
            OLD.alignment_idx
        );
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION qiita.retire_detached_exported_processing() IS
    'Retires an exported_processing whose processing FK was just nulled by an '
    'ON DELETE SET NULL. Required for that FK action to satisfy the '
    'one_processing CHECK — without it an alignment purge fails.';
