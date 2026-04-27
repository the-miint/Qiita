-- migrate:up
-- Phase H.a: add a nullable created_by_idx FK to qiita.principal alongside
-- the existing created_by UUID column. The new column is populated for
-- pre-existing rows by a single UPDATE backfill to the system principal
-- (idx=1, seeded by 20260426000000_auth.sql); new INSERTs from route
-- handlers will use real principal_idx values once Phase H.b flips the
-- routes to dual-write.
--
-- This step is fully reversible: H.a's migrate:down drops the column.
-- Phase H.b dual-writes both columns; Phase H.c finalises by setting
-- created_by_idx NOT NULL and dropping the legacy created_by UUID column.
ALTER TABLE qiita.references
    ADD COLUMN created_by_idx BIGINT REFERENCES qiita.principal(idx);

-- Backfill is safe because:
--   * The system principal at idx=1 is seeded by 20260426000000_auth.sql,
--     so this UPDATE cannot run before that principal exists.
--   * The CHECK constraints on qiita.user / qiita.service_account forbid
--     idx=1 in either subtype, so the system principal stays bare and
--     can never authenticate — pointing historical rows at it is purely
--     an audit-attribution choice, not a permission grant.
--   * At current scale (<10k references) this UPDATE is instant.
--     If references ever exceeds ~1M rows, replace with a chunked
--     UPDATE (e.g. WHERE reference_idx < N in batches of 50k) to bound
--     the row-exclusive lock duration.
UPDATE qiita.references SET created_by_idx = 1 WHERE created_by_idx IS NULL;

-- migrate:down
ALTER TABLE qiita.references DROP COLUMN created_by_idx;
