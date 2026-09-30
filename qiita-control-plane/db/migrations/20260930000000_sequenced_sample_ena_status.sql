-- ENA availability flag for a held sequenced_sample.
--
-- A run Qiita already holds can stop being public between imports (ENA
-- suppresses, withdraws, or replaces it). `ena_status` NULL means available;
-- any other value is ENA's own `statusDescription` for the run.
-- `ena_availability_checked_at` is when that value was last confirmed. Both
-- NULL on a fresh row -- checked only on re-import, never at registration
-- time. A Browser API error (including HTTP 500) fails the re-import item
-- loudly rather than setting either column.
--
-- Additive and backfill-free: no existing row is touched. No Postgres ENUM --
-- `ena_status` holds free-form ENA description text, not a closed set this
-- codebase controls.

-- migrate:up
ALTER TABLE qiita.sequenced_sample
  ADD COLUMN ena_status                  TEXT,
  ADD COLUMN ena_availability_checked_at TIMESTAMPTZ;

COMMENT ON COLUMN qiita.sequenced_sample.ena_status IS
  'NULL means available. Otherwise ENA''s statusDescription for the run '
  '(Browser API summary/{accession}). Set and cleared only when a '
  're-import of the run''s study checks it.';
COMMENT ON COLUMN qiita.sequenced_sample.ena_availability_checked_at IS
  'When ena_status was last confirmed against ENA. NULL until the first check.';

-- migrate:down
ALTER TABLE qiita.sequenced_sample
  DROP COLUMN ena_availability_checked_at,
  DROP COLUMN ena_status;
