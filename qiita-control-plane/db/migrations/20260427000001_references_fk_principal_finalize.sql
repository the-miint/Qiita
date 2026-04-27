-- migrate:up
-- Phase H.c: finalise the FK migration. By this point Phase H.b's dual-write
-- has been running long enough that every row has a non-NULL created_by_idx
-- (or was backfilled by H.a). We can therefore commit to created_by_idx as
-- the canonical creator reference and drop the legacy UUID column.
--
-- This is the irreversible end of the migration window — recovering the
-- legacy column from a backup means joining against the principal table to
-- regenerate the uuid5 derivation.

ALTER TABLE qiita.references ALTER COLUMN created_by_idx SET NOT NULL;
ALTER TABLE qiita.references DROP COLUMN created_by;


-- migrate:down
-- Reverse the column drop with an all-zeros placeholder, since the original
-- per-row UUID values are unrecoverable without the principal table state at
-- migration time. Operators rolling back will need to backfill from a uuid5
-- mapping if the legacy values matter forensically.
ALTER TABLE qiita.references ADD COLUMN created_by UUID;
UPDATE qiita.references SET created_by = '00000000-0000-0000-0000-000000000000';
ALTER TABLE qiita.references ALTER COLUMN created_by SET NOT NULL;
ALTER TABLE qiita.references ALTER COLUMN created_by_idx DROP NOT NULL;
